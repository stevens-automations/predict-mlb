#!/usr/bin/env python3
"""
Ingest yesterday's completed MLB games into raw DB tables.

Fetches from the MLB Stats API (statsapi) for all Final regular-season games
on yesterday's date. Updates: games, labels, game_team_stats,
game_pitcher_appearances, game_lineup_snapshots.
Called nightly by run_daily.py after midnight ET.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pytz

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import statsapi  # noqa: E402
from history_ingest import (  # noqa: E402
    _boxscore_side_payload,
    _iter_boxscore_pitching_lines,
    _to_float,
    _to_int,
    build_game_lineup_snapshot_rows,
    upsert_game,
    upsert_game_lineup_snapshot,
    upsert_game_pitcher_appearance,
    upsert_game_team_stats,
    upsert_label,
    utc_now,
)

ET_TZ = pytz.timezone("America/New_York")
logger = logging.getLogger(__name__)


def _log(
    conn: sqlite3.Connection,
    job: str,
    status: str,
    message: str,
    duration_s: float = 0.0,
) -> None:
    try:
        conn.execute(
            "INSERT INTO pipeline_log (job, status, message, duration_s) VALUES (?, ?, ?, ?)",
            (job, status, message, duration_s),
        )
        conn.commit()
    except Exception as exc:
        logger.warning(f"pipeline_log write failed: {exc}")


def ingest_yesterday(conn: sqlite3.Connection, date_str: Optional[str] = None) -> None:
    """Pull yesterday's completed MLB games and update raw DB tables.

    Args:
        conn: SQLite3 connection (caller manages lifecycle). Should have
              row_factory = sqlite3.Row set.
        date_str: Override date as 'YYYY-MM-DD'. Defaults to yesterday ET.
    """
    JOB = "ingest_yesterday"
    t0 = time.time()

    if date_str:
        yesterday = date_str
    else:
        now_et = datetime.now(ET_TZ)
        yesterday = (now_et.date() - timedelta(days=1)).isoformat()

    _log(conn, JOB, "started", f"Ingesting games for {yesterday}")
    logger.info(f"[{JOB}] ingesting {yesterday}")

    try:
        schedule = statsapi.schedule(date=yesterday, sportId=1)
        games = [
            g
            for g in schedule
            if g.get("status") == "Final" and g.get("game_type") == "R"
        ]
        logger.info(f"[{JOB}] found {len(games)} final regular-season games for {yesterday}")

        ingested = 0
        errors = 0
        for game_entry in games:
            game_id = game_entry.get("game_id")
            if not game_id:
                continue
            try:
                _ingest_one_game(conn, game_entry, yesterday)
                ingested += 1
            except Exception as exc:
                errors += 1
                logger.error(f"[{JOB}] game {game_id} failed: {exc}")

        elapsed = time.time() - t0
        msg = f"Ingested {ingested} games, {errors} errors for {yesterday}"
        _log(conn, JOB, "completed", msg, elapsed)
        logger.info(f"[{JOB}] {msg} ({elapsed:.1f}s)")

    except Exception as exc:
        elapsed = time.time() - t0
        _log(conn, JOB, "failed", str(exc), elapsed)
        logger.error(f"[{JOB}] failed: {exc}")
        raise


def _ingest_one_game(
    conn: sqlite3.Connection, game_entry: dict[str, Any], game_date: str
) -> None:
    """Ingest a single game: upsert games, labels, team_stats, pitchers, lineup."""
    game_id = game_entry["game_id"]
    home_id = game_entry.get("home_id")
    away_id = game_entry.get("away_id")
    home_score = game_entry.get("home_score")
    away_score = game_entry.get("away_score")
    home_score_int = int(home_score or 0)
    away_score_int = int(away_score or 0)

    # --- Upsert game row ---
    winning_team_id = (
        home_id if home_score_int > away_score_int else away_id
    )
    game_row: dict[str, Any] = {
        "game_id": game_id,
        "season": int(game_date[:4]),
        "game_date": game_date,
        "game_type": game_entry.get("game_type", "R"),
        "status": game_entry.get("status"),
        "scheduled_datetime": game_entry.get("game_datetime"),
        "venue_id": game_entry.get("venue_id"),
        "day_night": game_entry.get("day_night"),
        "home_team_id": home_id,
        "away_team_id": away_id,
        "home_score": home_score_int,
        "away_score": away_score_int,
        "winning_team_id": winning_team_id,
        "source_updated_at": utc_now(),
    }
    upsert_game(conn, game_row)

    # --- Upsert label ---
    did_home_win = 1 if home_score_int > away_score_int else 0
    upsert_label(
        conn,
        {
            "game_id": game_id,
            "did_home_win": did_home_win,
            "home_score": home_score_int,
            "away_score": away_score_int,
            "run_differential": home_score_int - away_score_int,
            "total_runs": home_score_int + away_score_int,
            "label_source": "statsapi_daily",
            "settled_at": utc_now(),
        },
    )

    # --- Fetch boxscore ---
    boxscore = statsapi.boxscore_data(game_id)
    if not boxscore:
        raise ValueError(f"empty boxscore for game {game_id}")

    # --- Upsert game_team_stats (home + away) ---
    for side in ("home", "away"):
        side_payload = _boxscore_side_payload(boxscore, side)
        if not isinstance(side_payload, dict):
            continue
        team = (
            side_payload.get("team")
            if isinstance(side_payload.get("team"), dict)
            else {}
        )
        team_id = _to_int(team.get("id")) or (
            home_id if side == "home" else away_id
        )
        team_stats_payload = (
            side_payload.get("teamStats")
            if isinstance(side_payload.get("teamStats"), dict)
            else {}
        )
        batting = (
            team_stats_payload.get("batting")
            if isinstance(team_stats_payload.get("batting"), dict)
            else {}
        )
        upsert_game_team_stats(
            conn,
            {
                "game_id": game_id,
                "team_id": team_id,
                "side": side,
                "runs": _to_int(batting.get("runs"))
                or (home_score_int if side == "home" else away_score_int),
                "hits": _to_int(batting.get("hits")),
                "errors": _to_int(side_payload.get("errors")),
                "batting_avg": _to_float(batting.get("avg")),
                "obp": _to_float(batting.get("obp")),
                "slg": _to_float(batting.get("slg")),
                "ops": _to_float(batting.get("ops")),
                "strikeouts": _to_int(batting.get("strikeOuts")),
                "walks": _to_int(batting.get("baseOnBalls")),
                "source_updated_at": utc_now(),
            },
        )

    # --- Upsert pitcher appearances ---
    for line in _iter_boxscore_pitching_lines(boxscore):
        if _to_int(line.get("team_id")) is None:
            continue
        row = dict(line)
        row["game_id"] = game_id
        row.pop("outs", None)
        upsert_game_pitcher_appearance(conn, row)

    # --- Upsert lineup snapshots ---
    game_dict: dict[str, Any] = {
        "game_id": game_id,
        "game_date": game_date,
        "status": game_entry.get("status"),
        "scheduled_datetime": game_entry.get("game_datetime"),
        "home_team_id": home_id,
        "away_team_id": away_id,
    }
    snapshot_rows = build_game_lineup_snapshot_rows(
        game_dict,
        boxscore,
        snapshot_type="final",
    )
    for snap_row in snapshot_rows:
        upsert_game_lineup_snapshot(conn, snap_row)
