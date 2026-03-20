#!/usr/bin/env python3
"""
Rebuild Layer 2 derived feature tables for the current MLB season.

Recomputes: team_pregame_stats, starter_pregame_stats, bullpen_pregame_stats,
lineup_pregame_context, team_vs_hand_pregame_stats.
Called after ingest_yesterday so inference features reflect yesterday's results.
Season scope defaults to the current calendar year.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import build_layer2_team_pregame_stats as _team_mod  # noqa: E402
import build_layer2_starter_pregame_stats as _starter_mod  # noqa: E402
import build_layer2_bullpen_pregame_stats as _bullpen_mod  # noqa: E402  (actual filename)
import build_layer2_lineup_pregame_context as _lineup_mod  # noqa: E402
import build_layer2_team_vs_hand_pregame_stats as _vs_hand_mod  # noqa: E402

logger = logging.getLogger(__name__)

# Each entry: (log_name, module)
_LAYER2_STEPS = [
    ("team_pregame_stats", _team_mod),
    ("starter_pregame_stats", _starter_mod),
    ("bullpen_pregame_stats", _bullpen_mod),
    ("lineup_pregame_context", _lineup_mod),
    ("team_vs_hand_pregame_stats", _vs_hand_mod),
]


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


def update_layer2(conn: sqlite3.Connection, season: int = 2026) -> None:
    """Recompute all Layer 2 tables for the given season.

    Args:
        conn: SQLite3 connection (caller manages lifecycle).
        season: Season year to recompute. Default 2026 (current season).
    """
    JOB = "update_layer2"
    t0 = time.time()
    computed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    _log(conn, JOB, "started", f"Recomputing layer2 for season={season}")
    logger.info(f"[{JOB}] recomputing layer2 for season={season}")

    failed_steps: list[str] = []

    for step_name, mod in _LAYER2_STEPS:
        step_t0 = time.time()
        step_job = f"{JOB}:{step_name}"
        _log(conn, step_job, "started", f"starting {step_name}")
        try:
            # Ensure the table exists before recomputing
            mod.create_table(conn)
            # Run season computation; all modules expose process_season(conn, season, computed_at)
            count = mod.process_season(conn, season, computed_at)
            step_elapsed = time.time() - step_t0
            msg = f"{step_name}: {count} rows ({step_elapsed:.1f}s)"
            _log(conn, step_job, "completed", msg, step_elapsed)
            logger.info(f"[{JOB}] {msg}")
        except Exception as exc:
            step_elapsed = time.time() - step_t0
            msg = f"{step_name} FAILED: {exc}"
            _log(conn, step_job, "failed", msg, step_elapsed)
            logger.error(f"[{JOB}] {msg}")
            failed_steps.append(step_name)

    elapsed = time.time() - t0
    if failed_steps:
        summary = (
            f"layer2 season={season} completed with errors in: {failed_steps} ({elapsed:.1f}s)"
        )
        _log(conn, JOB, "completed_with_errors", summary, elapsed)
        logger.warning(f"[{JOB}] {summary}")
    else:
        summary = f"layer2 season={season} all 5 steps done ({elapsed:.1f}s)"
        _log(conn, JOB, "completed", summary, elapsed)
        logger.info(f"[{JOB}] {summary}")
