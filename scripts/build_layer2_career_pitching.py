#!/usr/bin/env python3
"""Build the Layer 2 player_career_pitching_stats table."""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import statsapi  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "mlb_history.db"
TABLE_NAME = "player_career_pitching_stats"
PROGRESS_EVERY = 50
API_SLEEP_SECONDS = 0.1


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def create_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
          pitcher_id INTEGER PRIMARY KEY,
          pitcher_name TEXT,
          career_era REAL,
          career_whip REAL,
          career_k_pct REAL,
          career_bb_pct REAL,
          career_avg_allowed REAL,
          career_ip REAL,
          fetched_at TEXT
        )
        """
    )
    conn.commit()


def fetch_starter_pitchers(conn: sqlite3.Connection) -> list[tuple[int, str | None]]:
    rows = conn.execute(
        """
        SELECT pitcher_id, MAX(NULLIF(pitcher_name, '')) AS pitcher_name
        FROM game_pitcher_appearances
        WHERE is_starter = 1
          AND pitcher_id IS NOT NULL
        GROUP BY pitcher_id
        ORDER BY pitcher_id
        """
    ).fetchall()
    return [(int(row[0]), row[1]) for row in rows]


def parse_float(value: Any) -> float | None:
    if value in (None, "", "--"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int(value: Any) -> int:
    if value in (None, "", "--"):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def extract_career_pitching_stats(data: dict[str, Any]) -> dict[str, Any]:
    for entry in data.get("stats", []):
        stat_group = entry.get("group")
        stat_type = entry.get("type")
        if stat_group == "pitching" and stat_type == "career":
            stat = entry.get("stats") or entry.get("stat") or {}
            batters_faced = parse_int(stat.get("battersFaced"))
            walks = parse_int(stat.get("baseOnBalls"))
            hits = parse_int(stat.get("hits"))
            strikeouts = parse_int(stat.get("strikeOuts"))
            avg_denominator = batters_faced - walks
            return {
                "career_era": parse_float(stat.get("era")),
                "career_whip": parse_float(stat.get("whip")),
                "career_k_pct": (
                    strikeouts / batters_faced if batters_faced > 0 else None
                ),
                "career_bb_pct": walks / batters_faced if batters_faced > 0 else None,
                "career_avg_allowed": (
                    hits / avg_denominator if avg_denominator > 0 else None
                ),
                "career_ip": parse_float(stat.get("inningsPitched")),
            }
    raise ValueError("career pitching stats not found in statsapi response")


def upsert_pitcher(
    conn: sqlite3.Connection,
    *,
    pitcher_id: int,
    pitcher_name: str | None,
    stats: dict[str, Any],
    fetched_at: str,
) -> None:
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {TABLE_NAME} (
          pitcher_id,
          pitcher_name,
          career_era,
          career_whip,
          career_k_pct,
          career_bb_pct,
          career_avg_allowed,
          career_ip,
          fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pitcher_id,
            pitcher_name,
            stats["career_era"],
            stats["career_whip"],
            stats["career_k_pct"],
            stats["career_bb_pct"],
            stats["career_avg_allowed"],
            stats["career_ip"],
            fetched_at,
        ),
    )


def build_table() -> int:
    configure_logging()
    fetched_at = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        create_table(conn)
        pitchers = fetch_starter_pitchers(conn)
        total = len(pitchers)
        success_count = 0
        failure_count = 0

        logging.info("Found %s starter pitchers to process.", total)

        for index, (pitcher_id, pitcher_name) in enumerate(pitchers, start=1):
            try:
                data = statsapi.player_stat_data(
                    pitcher_id, type="career", group="pitching"
                )
                stats = extract_career_pitching_stats(data)
                upsert_pitcher(
                    conn,
                    pitcher_id=pitcher_id,
                    pitcher_name=pitcher_name or data.get("fullName"),
                    stats=stats,
                    fetched_at=fetched_at,
                )
                success_count += 1
            except Exception as exc:  # noqa: BLE001
                failure_count += 1
                logging.error(
                    "Failed pitcher_id=%s name=%r: %s", pitcher_id, pitcher_name, exc
                )
            finally:
                if index % PROGRESS_EVERY == 0 or index == total:
                    logging.info(
                        "Progress: %s/%s processed, %s succeeded, %s failed.",
                        index,
                        total,
                        success_count,
                        failure_count,
                    )
                time.sleep(API_SLEEP_SECONDS)

        conn.commit()

        row = conn.execute(
            f"""
            SELECT pitcher_id, pitcher_name, career_era, career_whip, career_k_pct,
                   career_bb_pct, career_avg_allowed, career_ip, fetched_at
            FROM {TABLE_NAME}
            WHERE pitcher_id = 628711
            """
        ).fetchone()

        logging.info(
            "Finished %s: %s processed, %s succeeded, %s failed.",
            TABLE_NAME,
            total,
            success_count,
            failure_count,
        )
        logging.info("Verification row pitcher_id=628711: %s", row)
        return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(build_table())
