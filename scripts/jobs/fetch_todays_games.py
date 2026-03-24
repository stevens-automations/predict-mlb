#!/usr/bin/env python3
"""
Fetch today's MLB schedule and upsert into today_schedule table.

Function: fetch_todays_games(conn, date_str=None) -> list

Args:
    conn: SQLite3 connection (caller manages lifecycle).
    date_str: Date string 'YYYY-MM-DD'. Defaults to today (ET).

Returns:
    List of game dicts: game_id, home_team_id, away_team_id, home_team,
    away_team, first_pitch_et, game_date.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Optional

import statsapi
import pytz

ROOT = Path(__file__).resolve().parents[2]

ET_TZ = pytz.timezone("America/New_York")

CREATE_TODAY_SCHEDULE_SQL = """
CREATE TABLE IF NOT EXISTS today_schedule (
    game_id         INTEGER PRIMARY KEY,
    game_date       TEXT,
    home_team       TEXT,
    away_team       TEXT,
    home_team_id    INTEGER,
    away_team_id    INTEGER,
    first_pitch_et  TEXT,
    home_odds       TEXT,
    away_odds       TEXT,
    odds_bookmaker  TEXT,
    fetched_at      TEXT
)
"""

CREATE_GAMES_SQL = """
CREATE TABLE IF NOT EXISTS games (
    game_id             INTEGER PRIMARY KEY,
    season              INTEGER NOT NULL,
    game_date           TEXT NOT NULL,
    game_type           TEXT,
    status              TEXT,
    scheduled_datetime  TEXT,
    home_team_id        INTEGER,
    away_team_id        INTEGER,
    home_score          INTEGER,
    away_score          INTEGER,
    winning_team_id     INTEGER,
    source_updated_at   TEXT,
    ingested_at         TEXT DEFAULT (datetime('now')),
    venue_id            INTEGER,
    day_night           TEXT
)
"""

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


def _parse_first_pitch_et(game: dict) -> Optional[str]:
    """
    Parse first pitch time from statsapi game dict and convert to ET ISO string.
    Returns None if time is unavailable.
    Handles both 'game_datetime' (MLB-StatsAPI library) and 'gameDate' (raw API) keys.
    """
    game_datetime = game.get("game_datetime") or game.get("gameDate")
    if not game_datetime:
        return None
    try:
        dt_utc = datetime.fromisoformat(game_datetime.replace("Z", "+00:00"))
        dt_et = dt_utc.astimezone(ET_TZ)
        return dt_et.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def fetch_todays_games(
    conn: sqlite3.Connection,
    date_str: Optional[str] = None,
) -> list:
    """
    Pull today's schedule from statsapi and upsert into today_schedule table.

    Args:
        conn: SQLite3 connection.
        date_str: Date string 'YYYY-MM-DD'. Defaults to today ET.

    Returns:
        List of game dicts.
    """
    t0 = time.time()
    JOB = "fetch_todays_games"

    # Ensure tables exist
    conn.execute(CREATE_TODAY_SCHEDULE_SQL)
    conn.execute(CREATE_GAMES_SQL)
    conn.execute(CREATE_PIPELINE_LOG_SQL)
    conn.commit()

    _log(conn, JOB, "started", f"date={date_str or 'today'}")

    try:
        if date_str is None:
            date_str = datetime.now(ET_TZ).strftime("%Y-%m-%d")

        # Fetch schedule from statsapi
        schedule = statsapi.schedule(date=date_str, sportId=1)

        games = []
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for game in schedule:
            # Filter: regular season only
            if game.get("game_type") != "R":
                continue

            game_id = game.get("game_id")
            if not game_id:
                continue

            home_team = game.get("home_name", "")
            away_team = game.get("away_name", "")
            home_team_id = game.get("home_id")
            away_team_id = game.get("away_id")
            first_pitch_et = _parse_first_pitch_et(game)

            game_dict = {
                "game_id": game_id,
                "game_date": date_str,
                "home_team": home_team,
                "away_team": away_team,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "first_pitch_et": first_pitch_et,
            }
            games.append(game_dict)

            # Upsert into today_schedule
            conn.execute(
                """
                INSERT INTO today_schedule
                    (game_id, game_date, home_team, away_team, home_team_id, away_team_id,
                     first_pitch_et, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(game_id) DO UPDATE SET
                    game_date = excluded.game_date,
                    home_team = excluded.home_team,
                    away_team = excluded.away_team,
                    home_team_id = excluded.home_team_id,
                    away_team_id = excluded.away_team_id,
                    first_pitch_et = excluded.first_pitch_et,
                    fetched_at = excluded.fetched_at
                """,
                (
                    game_id,
                    date_str,
                    home_team,
                    away_team,
                    home_team_id,
                    away_team_id,
                    first_pitch_et,
                    fetched_at,
                ),
            )

            # Pre-populate games table so feature_builder can look up game metadata
            game_datetime_str = game.get("game_datetime") or game.get("gameDate")
            if game_datetime_str:
                try:
                    dt_utc = datetime.fromisoformat(game_datetime_str.replace("Z", "+00:00"))
                    scheduled_datetime = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    scheduled_datetime = None
            else:
                scheduled_datetime = None

            venue_id_val = game.get("venue_id")
            day_night_val = game.get("day_night")
            game_status = game.get("status") or "Scheduled"
            season_val = int(date_str[:4])

            conn.execute(
                """
                INSERT OR IGNORE INTO games
                    (game_id, season, game_date, game_type, status, scheduled_datetime,
                     home_team_id, away_team_id, venue_id, day_night)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (game_id, season_val, date_str, "R", game_status, scheduled_datetime,
                 home_team_id, away_team_id, venue_id_val, day_night_val),
            )

        conn.commit()

        duration = time.time() - t0
        _log(conn, JOB, "completed", f"fetched {len(games)} games for {date_str}", duration)
        return games

    except Exception as e:
        duration = time.time() - t0
        _log(conn, JOB, "failed", str(e), duration)
        raise


if __name__ == "__main__":
    import sys
    import json

    db_path = ROOT / "data" / "mlb_history.db"
    date_str = sys.argv[1] if len(sys.argv) > 1 else None

    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        games = fetch_todays_games(conn, date_str)
        print(json.dumps(games, indent=2))
        print(f"\nFetched {len(games)} regular season games.")
    finally:
        conn.close()
