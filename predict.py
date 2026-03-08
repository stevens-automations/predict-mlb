from server.tweet_generator import gen_result_tweet, gen_game_line, create_tweets
from apscheduler.schedulers.background import BlockingScheduler  # type: ignore
from apscheduler.events import (
    EVENT_SCHEDULER_STARTED,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
)
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from uuid import uuid4
import json
from server.get_odds import get_todays_odds
from server.prep_tweet import prepare
from data import LeagueStats
from paths import load_env
from runtime import validate_runtime
from reliability_utils import get_predicted_winner_location
from storage import WriteStats, get_primary_storage
import pandas as pd  # type: ignore
import subprocess
import threading
import statsapi  # type: ignore
import pytz  # type: ignore
import time
import os
import hashlib

selected_model = "mlb4year"
cwd = os.path.dirname(os.path.abspath(__file__))
load_env()
ret = os.getenv("SELECTED_MODEL")
selected_model = ret if ret is not None else selected_model

global_correct: int = 0
global_wrong: int = 0
global_biggest_upset: Optional[List] = None
global_upset_diff: int = 0
global_results: Optional[Tuple[str, str]] = None

mlb = LeagueStats()
lock = threading.Lock()
eastern = pytz.timezone("America/New_York")
daily_scheduler = None
current_run_id: Optional[str] = None


COLUMN_ORDER = [
    "prediction_accuracy",
    "date",
    "time",
    "home",
    "home_probable",
    "away",
    "away_probable",
    "predicted_winner",
    "model",
    "favorite",
    "home_odds",
    "home_odds_bookmaker",
    "away_odds",
    "away_odds_bookmaker",
    "home_score",
    "away_score",
    "winning_pitcher",
    "losing_pitcher",
    "prediction_value",
    "venue",
    "series_status",
    "national_broadcasts",
    "odds_retrieval_time",
    "prediction_generation_time",
    "datetime",
    "game_id",
    "summary",
    "tweet",
    "time_to_tweet",
    "tweeted?",
]


def log_event(stage: str, result: str, game_id: Optional[str] = None, run_id: Optional[str] = None) -> None:
    payload = {
        "run_id": run_id or current_run_id or "unknown",
        "stage": stage,
        "game_id": game_id,
        "result": result,
    }
    print(f"[predict-log] {json.dumps(payload)}")


def print_next_job(event) -> None:
    time.sleep(1)
    ret = daily_scheduler.get_jobs()
    if daily_scheduler.running and len(ret) == 0:
        time.sleep(5)
        daily_scheduler.shutdown(wait=False)
        return
    next_job = ret[0] if (ret != []) else None
    if next_job is not None:
        print(f"{datetime.now(eastern).strftime('%D - %I:%M:%S %p')}... Next Scheduled Job")
        print(f"Job Name: {next_job.name}")
        et_time = next_job.next_run_time.astimezone(eastern)
        print(f"Next Execution Time: {et_time.strftime('%I:%M %p')} ET")
        time.sleep(1)


def update_row(row: pd.Series) -> pd.Series:
    global global_correct, global_wrong, global_biggest_upset, global_upset_diff
    predicted_winner = row["predicted_winner"]
    id = row["game_id"]
    games = statsapi.schedule(game_id=id)
    if not games:
        return row
    game = games[-1]
    if game["status"] != "Final":
        return row
    actual_winner = game.get("winning_team")
    prediction_accuracy = 1.0 if (actual_winner == predicted_winner) else (0.0 if actual_winner is not None else None)
    losing_team = row["home"] if actual_winner == row["away"] else row["away"]
    if actual_winner == row["home"]:
        winner_odds, loser_odds = row["home_odds"], row["away_odds"]
    else:
        winner_odds, loser_odds = row["away_odds"], row["home_odds"]
    if prediction_accuracy == 1.0:
        global_correct += 1
        odds_diff = int((abs(winner_odds) - 100) + (abs(loser_odds) - 100))
        if odds_diff > global_upset_diff and winner_odds > 100:
            global_upset_diff = odds_diff
            global_biggest_upset = [actual_winner, winner_odds, losing_team, loser_odds]
    else:
        global_wrong += 1

    updated_row = row.copy()
    updated_row["prediction_accuracy"] = prediction_accuracy
    updated_row["home_score"] = game["home_score"]
    updated_row["away_score"] = game["away_score"]
    updated_row["winning_pitcher"] = game["winning_pitcher"]
    updated_row["losing_pitcher"] = game["losing_pitcher"]
    updated_row["datetime"] = game.get("datetime")
    updated_row["game_id"] = game["game_id"]
    updated_row["summary"] = game["summary"]
    return updated_row


def load_unchecked_predictions(storage, write_stats: Optional[WriteStats] = None) -> Optional[pd.DataFrame]:
    global global_results, global_correct, global_wrong
    global global_biggest_upset, global_upset_diff
    global_correct = 0
    global_wrong = 0
    global_biggest_upset = None
    global_upset_diff = 0
    global_results = None

    df = storage.read_predictions()
    if df.empty or "prediction_accuracy" not in df.columns:
        return df

    df_missing_accuracy = df[df["prediction_accuracy"].isnull()]
    df_missing_accuracy = df_missing_accuracy.apply(update_row, axis=1)
    if (global_correct + global_wrong) > 0:
        percentage = str(int(100 * round((global_correct / (global_correct + global_wrong)), 2))) + "%"
        correct_wrong = f"{str(global_correct)}/{str(global_wrong + global_correct)}"
        global_results = correct_wrong, percentage
        if global_biggest_upset is not None:
            is_upset = True
            upset_winner, upset_w_odds, upset_loser, upset_l_odds = global_biggest_upset
            res = gen_result_tweet(correct_wrong, percentage, is_upset, upset_winner, upset_loser, upset_w_odds, upset_l_odds)
        else:
            res = f"I was {percentage} ({correct_wrong}) accurate in predicting yesterday's MLB games. "
        if res:
            send_tweet(res)

    df.update(df_missing_accuracy)
    success, failure = storage.replace_predictions(df)
    if write_stats is not None:
        write_stats.add(success, failure)
    return df


def safely_prepare(row: pd.Series) -> str:
    try:
        lock.acquire()
        tweet_line = prepare(row)
    finally:
        lock.release()
    return tweet_line


def are_within_30_minutes(dt1, dt2):
    dt1 = datetime.fromisoformat(dt1.rstrip('Z'))
    dt2 = datetime.fromisoformat(dt2.rstrip('Z'))
    return abs(dt1 - dt2) <= timedelta(minutes=30)


def normalize_tweet_line(line: str) -> str:
    return line.replace("•", "").strip()


def unique_tweet_lines(tweet_lines: List[str]) -> List[str]:
    seen: Set[str] = set()
    unique_lines: List[str] = []
    for line in tweet_lines:
        normalized = normalize_tweet_line(line)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_lines.append(normalized)
    return unique_lines


def get_tweet_job_id(tweet: str) -> str:
    digest = hashlib.sha1(tweet.encode("utf-8")).hexdigest()[:12]
    return f"tweet_{digest}"


def generate_daily_predictions(storage, model: str = selected_model, date=datetime.now(), run_id: Optional[str] = None, write_stats: Optional[WriteStats] = None) -> tuple[List[str], int]:
    if date is not datetime.now():
        pass

    scheduled_ids = []
    predicted_ids = []
    model = selected_model
    tweet_lines: List[str] = []
    log_event(stage="generate_daily_predictions", result="started", run_id=run_id)

    df = storage.read_predictions()
    required_cols = {"date", "tweeted?", "game_id"}
    if required_cols.issubset(df.columns):
        dates = pd.to_datetime(df["date"], errors="coerce", utc=True).dropna()
        existing_dates = dates.dt.tz_convert(eastern).dt.date.unique()
        check_date = str(date.date())
        if check_date in [str(v) for v in existing_dates]:
            d = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.tz_convert(eastern).dt.date
            mask = (d == date.date()) & (df["tweeted?"] == False)
            to_tweet_today = df[mask]
        else:
            to_tweet_today = pd.DataFrame()
    else:
        to_tweet_today = pd.DataFrame()

    if not to_tweet_today.empty:
        for _, row in to_tweet_today.iterrows():
            scheduled_ids.append(row["game_id"])
            predicted_ids.append(row["game_id"])
            tweet_lines.append(safely_prepare(row))

    all_games, odds_time = get_todays_odds()
    game_predictions: List[Dict] = []
    scheduled_doubleheaders = []

    for game in all_games:
        if game.get("date") != "Today":
            continue
        today = datetime.now().strftime("%m/%d/%Y")
        teams_games = mlb.get_days_games(game.get("home_team"), today)
        if not teams_games:
            continue
        if len(teams_games) == 2:
            first, second = teams_games[0], teams_games[1]
            if first.get("game_id") and second.get("game_id") in scheduled_doubleheaders:
                continue
            for game in all_games:
                if game['home_team'] != first['home_name']:
                    continue
                scheduled_ids.append((first.get("game_id"), game, first.get("game_num")))
                scheduled_doubleheaders.append(first.get("game_id"))
                first_ct = game.get("commence_time")
                break
            for game in all_games:
                if game['home_team'] != second['home_name']:
                    continue
                if game.get("commence_time") == first_ct:
                    continue
                scheduled_ids.append((second.get("game_id"), game, second.get("game_num")))
                scheduled_doubleheaders.append(second.get("game_id"))
                break
            continue
        elif len(teams_games) == 1:
            day_game = teams_games[0]
            if not are_within_30_minutes(day_game['game_datetime'], game['commence_time']):
                continue
            if (day_game.get("game_id") not in scheduled_ids) and (day_game.get("game_date") == datetime.now(eastern).date().strftime("%Y-%m-%d")):
                scheduled_ids.append((day_game.get("game_id"), game))

    for gameObj in scheduled_ids:
        try:
            gamePk = gameObj[0]
            if gamePk in predicted_ids:
                continue
            game = gameObj[1]
            ret = mlb.predict_game(gamePk)
            if ret is None or ret[0] is None:
                continue
            winner, prediction, info = ret[0], ret[1], ret[2]
        except Exception as e:
            log_event(stage="predict_game", game_id=str(gameObj[0]) if len(gameObj) > 0 else None, result=f"error: {e}", run_id=run_id)
            continue

        if not winner:
            continue
        home, away = info["home"], info["away"]
        info["predicted_winner"] = winner
        info["model"] = model
        info["predicted_winner_location"] = get_predicted_winner_location(winner, home)
        info["prediction_value"] = prediction
        info["time"] = game["time"]
        info["favorite"] = game.get("favorite")
        info["home_odds"] = str(game.get(f"{home}_odds"))
        info["away_odds"] = str(game.get(f"{away}_odds"))
        info["home_odds_bookmaker"] = game.get(f"{home}_bookmaker")
        info["away_odds_bookmaker"] = game.get(f"{away}_bookmaker")
        info["odds_retrieval_time"] = odds_time
        info["prediction_generation_time"] = datetime.now()
        info["prediction_accuracy"] = None
        info["home_score"] = None
        info["away_score"] = None
        info["winning_pitcher"] = None
        info["losing_pitcher"] = None
        info["tweeted?"] = False
        tweet = gen_game_line(info)
        if len(gameObj) == 3:
            tweet = f"{tweet} ({game['time']})"
        info["tweet"] = tweet
        info["time_to_tweet"] = datetime.now().replace(hour=9, minute=45, second=0, microsecond=0).replace(tzinfo=None)

        tweet_lines.append(tweet)
        game_predictions.append(info)
        log_event(stage="predict_game", game_id=str(gamePk), result="queued_for_tweet", run_id=run_id)

    df_new = pd.DataFrame(game_predictions)
    if len(df_new) > 0:
        df_new = df_new[COLUMN_ORDER]
        success, failure = storage.upsert_predictions(df_new)
        if write_stats is not None:
            write_stats.add(success, failure)
    else:
        print(f"{datetime.now(eastern).strftime('%D - %I:%M:%S %p')}... \nNo new predictions made for games\n")

    log_event(stage="generate_daily_predictions", result=f"completed: {len(tweet_lines)} tweet lines", run_id=run_id)
    return tweet_lines, len(game_predictions)


def mark_as_tweeted(tweet: str) -> None:
    storage = get_primary_storage()
    lines = [normalize_tweet_line(line) for line in tweet.split('\n')]
    lines = [line for line in lines if line]
    storage.mark_tweeted(lines)


def send_tweet(tweet: str) -> bool:
    try:
        tweet_script = os.path.join(cwd, "server/tweet.py")
        process = subprocess.Popen(["python3", tweet_script, tweet], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        process.wait()
        stdout, stderr = process.communicate()
        print(stdout.strip())
        print(stderr.strip())
        return_code = process.poll()
        if return_code != 0:
            print(f"Error calling tweet.py: return code={return_code}")
            return False
        mark_as_tweeted(tweet)
        return True
    except subprocess.CalledProcessError as e:
        print(f"{datetime.now(eastern).strftime('%D - %I:%M:%S %p')}... \nError tweeting results{e}\n")
        return False


def schedule_tweets(tweet_lines: List[str], run_id: Optional[str] = None) -> int:
    global daily_scheduler
    if not tweet_lines or daily_scheduler is None:
        return 0
    tweet_lines = unique_tweet_lines(tweet_lines)
    if not tweet_lines:
        return 0

    tweets = create_tweets(tweet_lines)
    now = datetime.now(eastern)
    start_time = now.replace(hour=9, minute=45, second=0, microsecond=0)
    end_time = now.replace(hour=23, minute=59, second=59, microsecond=0)
    delay = 0
    scheduled_jobs = 0

    for tweet in tweets[::-1]:
        now = datetime.now(eastern)
        tweet_time = (now + timedelta(minutes=1, seconds=delay)) if (start_time <= now <= end_time) else datetime.now().replace(hour=9, minute=45, second=delay, microsecond=0)

        job_id = get_tweet_job_id(tweet)
        if daily_scheduler.get_job(job_id) is not None:
            log_event(stage="schedule_tweets", result="skipped_duplicate_job", game_id=job_id, run_id=run_id)
            continue

        daily_scheduler.add_job(send_tweet, args=[tweet], trigger="date", run_date=tweet_time, id=job_id, replace_existing=False)
        log_event(stage="schedule_tweets", result="scheduled", game_id=job_id, run_id=run_id)
        delay += 5
        scheduled_jobs += 1
    return scheduled_jobs


def check_and_predict():
    global daily_scheduler, current_run_id
    daily_scheduler = None
    current_run_id = str(uuid4())
    log_event(stage="check_and_predict", result="started", run_id=current_run_id)

    validate_runtime()
    storage = get_primary_storage()
    write_stats = WriteStats()

    try:
        load_unchecked_predictions(storage=storage, write_stats=write_stats)
    except Exception as e:
        log_event(stage="load_unchecked_predictions", result=f"error: {e}", run_id=current_run_id)

    daily_scheduler = BlockingScheduler(job_defaults={"coalesce": False}, timezone=eastern)
    daily_scheduler.add_listener(print_next_job, EVENT_SCHEDULER_STARTED)
    daily_scheduler.add_listener(print_next_job, EVENT_JOB_EXECUTED)
    daily_scheduler.add_listener(print_next_job, EVENT_JOB_MISSED)

    tweet_lines, predicted_games = generate_daily_predictions(storage=storage, run_id=current_run_id, write_stats=write_stats)
    scheduled_jobs = schedule_tweets(tweet_lines, run_id=current_run_id)

    daily_scheduler.start()
    time.sleep(10)
    daily_scheduler = None
    summary = (
        f"run_summary predicted_games={predicted_games} "
        f"scheduled_jobs={scheduled_jobs} "
        f"sqlite_write_successes={write_stats.success} "
        f"sqlite_write_failures={write_stats.failure}"
    )
    print(f"[predict-summary] {summary}")
    log_event(stage="check_and_predict", result=summary, run_id=current_run_id)
    current_run_id = None


if __name__ == "__main__":
    check_and_predict()
