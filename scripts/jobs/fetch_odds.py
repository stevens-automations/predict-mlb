#!/usr/bin/env python3
"""
Fetch today's MLB odds from The Odds API v4.

Function: fetch_odds(conn) -> dict

- Check if data/todays_odds.json exists and is < 23 hours old → use cached version
- Otherwise: call The Odds API v4 (read key from .env as ODDS_API_KEY)
- For each game: find BEST odds per team across all bookmakers
  (highest value = most favorable to underdog)
- Match to today_schedule by team names → update home_odds, away_odds, odds_bookmaker
- Return dict of game_id → odds dict

Returns:
    {
        game_id (int): {
            "home_odds": str (American format),
            "away_odds": str (American format),
            "odds_bookmaker": str
        },
        ...
    }
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
ODDS_CACHE_PATH = ROOT / "data" / "todays_odds.json"
ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
CACHE_MAX_AGE_HOURS = 23

CREATE_PIPELINE_LOG_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT DEFAULT (datetime('now')),
    job         TEXT,
    status      TEXT,
    message     TEXT,
    duration_s  REAL
)
"""


def _log(conn: sqlite3.Connection, job: str, status: str, message: str, duration_s: float = 0.0):
    conn.execute(
        "INSERT INTO pipeline_log (job, status, message, duration_s) VALUES (?, ?, ?, ?)",
        (job, status, message, duration_s),
    )
    conn.commit()


def _load_cached_odds() -> Optional[list]:
    """Return cached odds data if file exists and is < 23 hours old."""
    if not ODDS_CACHE_PATH.exists():
        return None
    mtime = ODDS_CACHE_PATH.stat().st_mtime
    age_hours = (time.time() - mtime) / 3600
    if age_hours >= CACHE_MAX_AGE_HOURS:
        return None
    try:
        with open(ODDS_CACHE_PATH) as f:
            return json.load(f)
    except Exception:
        return None


def _save_cached_odds(data: list):
    ODDS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ODDS_CACHE_PATH, "w") as f:
        json.dump(data, f, indent=2)


def _fetch_from_api(api_key: str) -> list:
    """Fetch odds from The Odds API v4."""
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "h2h",
        "oddsFormat": "american",
    }
    resp = requests.get(ODDS_API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _best_odds_for_team(bookmakers: list, team_name: str) -> tuple[Optional[str], Optional[str]]:
    """
    Find the best (highest American odds) for team_name across all bookmakers.
    Returns (best_odds_str, bookmaker_name) or (None, None) if not found.

    Highest American odds = most favorable to underdog:
    - For positive odds: higher is better (+200 > +150)
    - For negative odds: less negative is better (-110 > -150)
    - Positive always beats negative
    """
    best_odds_val: Optional[int] = None
    best_bookmaker: Optional[str] = None

    for bm in bookmakers:
        bm_name = bm.get("key", "")
        for market in bm.get("markets", []):
            if market.get("key") != "h2h":
                continue
            for outcome in market.get("outcomes", []):
                if outcome.get("name", "").lower() == team_name.lower():
                    price = outcome.get("price")
                    if price is None:
                        continue
                    price = int(price)
                    if best_odds_val is None or price > best_odds_val:
                        best_odds_val = price
                        best_bookmaker = bm_name

    if best_odds_val is None:
        return None, None

    odds_str = f"+{best_odds_val}" if best_odds_val > 0 else str(best_odds_val)
    return odds_str, best_bookmaker


def _normalize_team_name(name: str) -> str:
    """Lowercase + strip for fuzzy matching."""
    return name.lower().strip()


def _match_game(odds_game: dict, schedule_games: list) -> Optional[dict]:
    """
    Match an odds game to a schedule game by team names.
    The odds API uses team names like 'New York Yankees', schedule uses 'New York Yankees'.
    Returns the matching schedule game dict or None.
    """
    home_team_odds = _normalize_team_name(odds_game.get("home_team", ""))
    away_team_odds = _normalize_team_name(odds_game.get("away_team", ""))

    for sg in schedule_games:
        home_sched = _normalize_team_name(sg.get("home_team", ""))
        away_sched = _normalize_team_name(sg.get("away_team", ""))
        if home_team_odds == home_sched and away_team_odds == away_sched:
            return sg
    return None


def fetch_odds(conn: sqlite3.Connection) -> dict:
    """
    Fetch and cache today's MLB odds, update today_schedule, return dict of game_id → odds.

    Args:
        conn: SQLite3 connection.

    Returns:
        dict of game_id (int) → {"home_odds": str, "away_odds": str, "odds_bookmaker": str}
    """
    t0 = time.time()
    JOB = "fetch_odds"

    conn.execute(CREATE_PIPELINE_LOG_SQL)
    conn.commit()

    _log(conn, JOB, "started", "fetching MLB odds")

    try:
        # Load today's schedule from DB
        schedule_rows = conn.execute(
            "SELECT game_id, home_team, away_team FROM today_schedule"
        ).fetchall()

        if not schedule_rows:
            _log(conn, JOB, "completed", "no games in today_schedule, skipping odds", time.time() - t0)
            return {}

        schedule_games = [dict(r) for r in schedule_rows] if hasattr(schedule_rows[0], "keys") else [
            {"game_id": r[0], "home_team": r[1], "away_team": r[2]} for r in schedule_rows
        ]

        # Try cache first
        raw_odds = _load_cached_odds()
        source = "cache"

        if raw_odds is None:
            # Load API key from .env
            load_dotenv(ROOT / ".env")
            api_key = os.getenv("ODDS_API_KEY", "")
            if not api_key:
                _log(conn, JOB, "failed", "ODDS_API_KEY not set in .env", time.time() - t0)
                return {}

            raw_odds = _fetch_from_api(api_key)
            _save_cached_odds(raw_odds)
            source = "api"

        # Build results dict
        results: dict = {}

        for odds_game in raw_odds:
            matched = _match_game(odds_game, schedule_games)
            if matched is None:
                continue

            game_id = matched["game_id"]
            bookmakers = odds_game.get("bookmakers", [])

            home_odds, home_bm = _best_odds_for_team(bookmakers, odds_game.get("home_team", ""))
            away_odds, away_bm = _best_odds_for_team(bookmakers, odds_game.get("away_team", ""))

            # Use home bookmaker for label (could differ per team, pick home)
            best_bm = home_bm or away_bm

            results[game_id] = {
                "home_odds": home_odds,
                "away_odds": away_odds,
                "odds_bookmaker": best_bm,
            }

            # Update today_schedule
            conn.execute(
                """
                UPDATE today_schedule
                SET home_odds = ?, away_odds = ?, odds_bookmaker = ?
                WHERE game_id = ?
                """,
                (home_odds, away_odds, best_bm, game_id),
            )

        conn.commit()

        duration = time.time() - t0
        _log(conn, JOB, "completed", f"matched {len(results)} games from {source}", duration)
        return results

    except Exception as e:
        duration = time.time() - t0
        _log(conn, JOB, "failed", str(e), duration)
        raise


if __name__ == "__main__":
    import sys

    db_path = ROOT / "data" / "mlb_history.db"
    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        results = fetch_odds(conn)
        print(json.dumps(results, indent=2))
        print(f"\nOdds found for {len(results)} games.")
    finally:
        conn.close()
