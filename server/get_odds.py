from datetime import datetime
from typing import Dict, Tuple, Optional
from dotenv import load_dotenv  # type: ignore
import requests  # type: ignore
import calendar
import pytz  # type: ignore
import json
import os
import time
from time_utils import parse_iso_z_to_eastern
from simulation import simulation_enabled, get_simulated_games

# minimum time between requests
REQUEST_COOLDOWN = 900  # 0.25 hour
DEFAULT_ODDS_REQUEST_TIMEOUT_SEC = 10.0
DEFAULT_ODDS_REQUEST_RETRIES = 3
DEFAULT_ODDS_REQUEST_BACKOFF_SEC = 1.5
DEFAULT_ODDS_CIRCUIT_FAILURE_THRESHOLD = 3
DEFAULT_ODDS_CIRCUIT_COOLDOWN_SEC = 600.0

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_odds_consecutive_failures = 0
_odds_circuit_open_until = 0.0


def _read_cached_odds(data_file: str) -> Optional[Tuple[Optional[Dict], Optional[datetime]]]:
    if not os.path.exists(data_file):
        return None
    modified_time = os.path.getmtime(data_file)
    with open(data_file, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data, datetime.fromtimestamp(modified_time)


def _odds_circuit_open() -> bool:
    return time.time() < _odds_circuit_open_until


def _mark_odds_success() -> None:
    global _odds_consecutive_failures, _odds_circuit_open_until
    _odds_consecutive_failures = 0
    _odds_circuit_open_until = 0.0


def _mark_odds_failure() -> None:
    global _odds_consecutive_failures, _odds_circuit_open_until
    _odds_consecutive_failures += 1
    failure_threshold = max(
        int(os.getenv("ODDS_CIRCUIT_FAILURE_THRESHOLD", str(DEFAULT_ODDS_CIRCUIT_FAILURE_THRESHOLD))),
        1,
    )
    if _odds_consecutive_failures < failure_threshold:
        return
    cooldown = max(float(os.getenv("ODDS_CIRCUIT_COOLDOWN_SEC", str(DEFAULT_ODDS_CIRCUIT_COOLDOWN_SEC))), 1.0)
    _odds_circuit_open_until = time.time() + cooldown
    print(f"[odds-circuit] open for {cooldown:.0f}s after {_odds_consecutive_failures} consecutive failure(s)")


def make_request() -> Optional[Tuple[Optional[Dict], Optional[datetime]]]:
    env_file_path = os.path.join(parent_dir, ".env")
    load_dotenv(env_file_path)

    apikey = os.getenv("ODDS_API_KEY")
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    params = {
        "apiKey": apikey,
        "regions": "us",
        "markets": "h2h",
        "oddsFormat": "american",
    }

    data_file = os.path.join(parent_dir, "data/todays_odds.json")
    cached = _read_cached_odds(data_file)
    if cached:
        data, cached_time = cached
        if (datetime.now().timestamp() - cached_time.timestamp()) < REQUEST_COOLDOWN:
            return data, cached_time

    if _odds_circuit_open():
        if cached:
            print("[odds-circuit] open, using stale cached odds data")
            return cached
        print("[odds-circuit] open, no cached odds available")
        return None

    timeout_sec = max(float(os.getenv("ODDS_REQUEST_TIMEOUT_SEC", str(DEFAULT_ODDS_REQUEST_TIMEOUT_SEC))), 1.0)
    max_attempts = max(int(os.getenv("ODDS_REQUEST_RETRIES", str(DEFAULT_ODDS_REQUEST_RETRIES))), 1)
    backoff = max(float(os.getenv("ODDS_REQUEST_BACKOFF_SEC", str(DEFAULT_ODDS_REQUEST_BACKOFF_SEC))), 0.0)

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout_sec)
            if response.status_code == 200:
                data = response.json()
                with open(data_file, "w", encoding="utf-8") as file:
                    json.dump(data, file)
                _mark_odds_success()
                return data, datetime.now()

            status = int(response.status_code)
            print(f"Odds API error status={status} attempt={attempt}/{max_attempts}")
            retryable = status == 429 or status >= 500
            if not retryable:
                break
        except requests.RequestException as exc:
            print(f"Odds API request exception attempt={attempt}/{max_attempts}: {exc}")

        if attempt < max_attempts:
            time.sleep(backoff * (2 ** (attempt - 1)))

    _mark_odds_failure()
    if cached:
        print("[odds] API unavailable, falling back to stale cached odds")
        return cached
    return None


def get_favorite(game: Dict) -> Optional[str]:
    """function to calculate the team favorited to win the game"""
    outcomes = game["bookmakers"][0]["markets"][0]["outcomes"]
    favorite = None
    lowest_odds = float("inf")
    for outcome in outcomes:
        odds = outcome["price"]
        if odds < lowest_odds:
            lowest_odds = odds
            favorite = outcome["name"]
    return favorite


def get_best_odds(game: Dict) -> Dict:
    best_odds: Dict = {}
    for bookmaker in game["bookmakers"]:
        for market in bookmaker["markets"]:
            if market["key"] == "h2h":
                for outcome in market["outcomes"]:
                    team = outcome["name"]
                    odds = outcome["price"]
                    if team in best_odds:
                        if int(odds) > int(best_odds[team]["odds"]):
                            best_odds[team]["odds"] = (
                                str(odds) if (int(odds) <= 0) else f"+{odds}"
                            )
                            best_odds[team]["bookmaker"] = bookmaker["title"]
                    else:
                        best_odds[team] = {
                            "odds": str(odds) if (int(odds) <= 0) else f"+{odds}",
                            "bookmaker": bookmaker["title"],
                        }
    return best_odds


def make_twelve_hour(time):
    """returns 12 hour version of 24 hour clock time"""
    time_parts = time.split(":")
    hour = int(time_parts[0])
    minute = int(time_parts[1])
    ending = " am"
    if hour == 0:
        hour = 12
    elif hour > 12:
        ending = " pm"
        hour -= 12
    elif hour == 12:
        ending = " pm"
    time_formatted = f"{hour:02d}:{minute:02d}{ending}"
    return time_formatted


def format_date(date):
    """returns formatted date in EST"""
    now = datetime.now(pytz.timezone("US/Eastern"))
    if date.date() == now.date():
        date_formatted = "Today"
    else:
        month = calendar.month_name[int(date.strftime("%m"))]
        day = str(int(date.strftime("%d")))
        date_formatted = f"{day} {month}"
    return date_formatted


def process_data(data):
    """returns list of objects with pertinent data for simple display"""
    games = []
    # current time
    for game in data:
        game_info = {}
        EST_date = parse_iso_z_to_eastern(game["commence_time"])
        formatted_date = format_date(EST_date)
        if formatted_date != "Today":
            continue
        game_info["date"] = formatted_date
        game_info["time"] = make_twelve_hour(EST_date.strftime("%H:%M"))
        game_info["commence_time"] = game["commence_time"]
        game_info["home_team"] = game["home_team"]
        game_info["away_team"] = game["away_team"]
        best_odds = get_best_odds(game)
        game_info["favorite"] = get_favorite(game)
        for team, odds_info in best_odds.items():
            game_info[f"{team}_odds"] = int(odds_info["odds"])
            game_info[f"{team}_bookmaker"] = odds_info["bookmaker"]
        games.append(game_info)
    return games


def get_todays_odds():
    """
    function to get the odds of games occurring today

    Returns:
        games: list of python dictionaries representing individual MLB games
        time: time of the request to get the odds (most recent odds retrieval)
    """
    if simulation_enabled():
        return get_simulated_games(), datetime.now()

    res = make_request()
    if not res:
        return None
    data, time = res
    games = process_data(data)
    return games, time
