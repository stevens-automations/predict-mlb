from datetime import datetime
from typing import Dict, List, Optional, Tuple
import hashlib
import json
import os

import pandas as pd
import pytz

TWITTER_MAX_CHAR_COUNT = 268
DEFAULT_CONFIDENCE_THRESHOLDS: Tuple[float, float] = (0.62, 0.55)
CONFIDENCE_LABELS: Dict[str, str] = {
    "high": "H",
    "medium": "M",
    "low": "L",
}
WIN_PHRASE_BANK = ["over", "to beat", "vs"]


def _to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_team_ids() -> Dict[str, Dict[str, str]]:
    with open("data/ids.json", "r") as f:
        return json.load(f)


def _parse_confidence_thresholds() -> Tuple[float, float]:
    raw = os.getenv("PREDICTION_CONFIDENCE_THRESHOLDS", "")
    if not raw:
        return DEFAULT_CONFIDENCE_THRESHOLDS
    try:
        high_s, medium_s = [v.strip() for v in raw.split(",", 1)]
        high = float(high_s)
        medium = float(medium_s)
        if 0 <= medium <= high <= 1:
            return high, medium
    except (ValueError, TypeError):
        pass
    return DEFAULT_CONFIDENCE_THRESHOLDS


def derive_confidence_tier(prediction_value: object) -> str:
    value = _to_float(prediction_value)
    high, medium = _parse_confidence_thresholds()
    if value is None:
        return "low"
    if value >= high:
        return "high"
    if value >= medium:
        return "medium"
    return "low"


def _confidence_tag(prediction_value: object) -> str:
    tier = derive_confidence_tier(prediction_value)
    label = CONFIDENCE_LABELS.get(tier, tier[:1].upper())
    return f"[{label}]"


def _format_odds(odds: object) -> str:
    if odds is None:
        return "N/A"
    try:
        odds_i = int(float(str(odds).strip()))
    except (TypeError, ValueError):
        return "N/A"
    if odds_i >= 100:
        return f"+{odds_i}"
    return str(odds_i)


def _normalize_team(value: object) -> str:
    return str(value).strip().lower()


def has_market_mismatch(row: pd.Series, predicted_winner: str) -> bool:
    favorite = _normalize_team(row.get("favorite"))
    if not favorite or favorite in {"none", "n/a", "nan"}:
        return False

    winner_norm = _normalize_team(predicted_winner)
    if favorite == winner_norm:
        return False

    home = _normalize_team(row.get("home"))
    away = _normalize_team(row.get("away"))
    return favorite in {home, away}


def _pick_phrase(seed: str) -> str:
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    idx = int(digest[:8], 16) % len(WIN_PHRASE_BANK)
    return WIN_PHRASE_BANK[idx]


def gen_game_line(row: pd.Series) -> str:
    home = row["home"]
    away = row["away"]
    home_odds = _format_odds(row.get("home_odds"))
    away_odds = _format_odds(row.get("away_odds"))

    pred = row["predicted_winner"]
    if pred == home:
        winner, loser = home, away
        winner_odds, loser_odds = home_odds, away_odds
    else:
        winner, loser = away, home
        winner_odds, loser_odds = away_odds, home_odds

    data = _load_team_ids()
    winner_id = data["team_to_id"][winner]
    winner_abb = data["id_to_team"][str(winner_id)]["abbreviation"]
    loser_id = data["team_to_id"][loser]
    loser_abb = data["id_to_team"][str(loser_id)]["abbreviation"]

    phrase_seed = f"{row.get('game_id', '')}:{winner}:{loser}:{row.get('date', '')}"
    verb = _pick_phrase(phrase_seed)
    confidence = _confidence_tag(row.get("prediction_value"))
    mismatch = " | value" if has_market_mismatch(row, winner) else ""

    tweet_line = f"{winner_abb} ({winner_odds}) {verb} {loser_abb} ({loser_odds}) {confidence}{mismatch}"
    return tweet_line.strip()


def create_tweets(tweet_lines: List[str]) -> List[str]:
    tweets = []
    num_lines = len(tweet_lines)
    eastern = pytz.timezone("America/New_York")
    today = datetime.now(eastern).date()
    formatted_date = today.strftime("%d %B %Y")
    leadin_msg = f"Predictions for {formatted_date}"
    num_tweet_map = {
        1: [1],
        2: [2],
        3: [3],
        4: [4],
        5: [5],
        6: [6],
        7: [4, 3],
        8: [4, 4],
        9: [5, 4],
        10: [5, 5],
        11: [6, 5],
        12: [6, 6],
        13: [5, 4, 4],
        14: [5, 5, 4],
        15: [5, 5, 5],
        16: [6, 5, 5],
        17: [6, 6, 5],
        18: [6, 6, 6],
    }
    tweets_layout = num_tweet_map.get(num_lines)
    if not tweets_layout:
        return []

    num_tweets = len(tweets_layout)
    cursor = 0
    for i, line_ct in enumerate(tweets_layout):
        header = leadin_msg if num_tweets == 1 else f"{leadin_msg} ({str(i + 1)}/{str(num_tweets)})"
        current_tweet = header
        for _ in range(int(line_ct)):
            if cursor >= num_lines:
                break
            line = tweet_lines[cursor]
            candidate = f"{current_tweet}\n• {line}"
            if len(candidate) <= TWITTER_MAX_CHAR_COUNT:
                current_tweet = candidate
            else:
                remaining = TWITTER_MAX_CHAR_COUNT - len(current_tweet)
                # Need at least room for "\n• x"
                if remaining > 4:
                    safe_line = line[: remaining - 3].rstrip()
                    current_tweet = f"{current_tweet}\n• {safe_line}"
            cursor += 1
        tweets.append(current_tweet)

    return tweets


def gen_result_tweet(
    correct_wrong: str,
    percentage: str,
    is_upset: bool,
    upset_winner: str,
    upset_loser: str,
    upset_winner_odds: str,
    upset_loser_odds: str,
) -> str:
    msg = (
        f"I was {percentage} ({correct_wrong}) accurate "
        f"in predicting yesterday's MLB games."
    )
    if is_upset:
        msg += (
            f" My best pick was the {upset_winner} (+{upset_winner_odds}) upsetting"
            f" the {upset_loser} ({upset_loser_odds}) (odds from 09:30 gameday)"
        )
    return msg
