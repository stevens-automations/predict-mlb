#!/usr/bin/env python3
"""Build the Layer 2 lineup_pregame_context table."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "mlb_history.db"
TABLE_NAME = "lineup_pregame_context"
SEASONS = tuple(range(2020, 2026))
INSERT_SQL = f"""
    INSERT OR REPLACE INTO {TABLE_NAME} (
      game_id,
      side,
      season,
      lineup_known_flag,
      lineup_lefty_count,
      lineup_righty_count,
      lineup_switch_count,
      lineup_lefty_share,
      lineup_righty_share,
      top3_lefty_count,
      top3_righty_count,
      opposing_starter_hand,
      lineup_vs_starter_hand_advantage,
      computed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass(slots=True)
class LineupBatter:
    batting_order: int
    bat_side: str | None


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 60000")
    return conn


def create_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
          game_id INTEGER NOT NULL,
          side TEXT NOT NULL CHECK(side IN ('home', 'away')),
          season INTEGER NOT NULL,
          lineup_known_flag INTEGER DEFAULT 0,
          lineup_lefty_count INTEGER,
          lineup_righty_count INTEGER,
          lineup_switch_count INTEGER,
          lineup_lefty_share REAL,
          lineup_righty_share REAL,
          top3_lefty_count INTEGER,
          top3_righty_count INTEGER,
          opposing_starter_hand TEXT,
          lineup_vs_starter_hand_advantage REAL,
          computed_at TEXT,
          PRIMARY KEY (game_id, side),
          FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


def safe_divide(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def fetch_games(conn: sqlite3.Connection, season: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT game_id, season
        FROM games
        WHERE season = ?
        ORDER BY game_date, COALESCE(scheduled_datetime, ''), game_id
        """,
        (season,),
    ).fetchall()


def fetch_lineups(conn: sqlite3.Connection, season: int) -> dict[tuple[int, str], list[LineupBatter]]:
    rows = conn.execute(
        """
        WITH latest_snapshot AS (
          SELECT gls.game_id, gls.side, MAX(gls.as_of_ts) AS as_of_ts
          FROM game_lineup_snapshots gls
          INNER JOIN games g
            ON g.game_id = gls.game_id
          WHERE g.season = ?
          GROUP BY gls.game_id, gls.side
        )
        SELECT
          gls.game_id,
          gls.side,
          gls.batting_order,
          COALESCE(gls.bat_side, phd.bat_side) AS resolved_bat_side
        FROM latest_snapshot ls
        INNER JOIN game_lineup_snapshots gls
          ON gls.game_id = ls.game_id
         AND gls.side = ls.side
         AND gls.as_of_ts = ls.as_of_ts
        LEFT JOIN player_handedness_dim phd
          ON phd.player_id = gls.player_id
        WHERE gls.batting_order IS NOT NULL
        ORDER BY gls.game_id, gls.side, gls.batting_order, gls.player_id
        """,
        (season,),
    ).fetchall()

    lineups: dict[tuple[int, str], list[LineupBatter]] = {}
    for row in rows:
        key = (int(row["game_id"]), str(row["side"]))
        lineups.setdefault(key, []).append(
            LineupBatter(
                batting_order=int(row["batting_order"]),
                bat_side=str(row["resolved_bat_side"]) if row["resolved_bat_side"] is not None else None,
            )
        )
    return lineups


def fetch_starter_hands(conn: sqlite3.Connection, season: int) -> dict[tuple[int, str], str | None]:
    rows = conn.execute(
        """
        SELECT sps.game_id, sps.side, sps.pitcher_hand
        FROM starter_pregame_stats sps
        INNER JOIN games g
          ON g.game_id = sps.game_id
        WHERE g.season = ?
        """,
        (season,),
    ).fetchall()
    return {
        (int(row["game_id"]), str(row["side"])): str(row["pitcher_hand"]) if row["pitcher_hand"] is not None else None
        for row in rows
    }


def count_bat_side(batters: list[LineupBatter], side: str) -> int:
    return sum(1 for batter in batters if batter.bat_side == side)


def build_row(
    *,
    game: sqlite3.Row,
    side: str,
    lineup: list[LineupBatter],
    starter_hands: dict[tuple[int, str], str | None],
    computed_at: str,
) -> tuple[Any, ...]:
    lineup_known_flag = 1 if lineup else 0
    lineup_lefty_count = count_bat_side(lineup, "L")
    lineup_righty_count = count_bat_side(lineup, "R")
    lineup_switch_count = count_bat_side(lineup, "S")

    known_batters = [batter for batter in lineup if batter.bat_side in {"L", "R", "S"}]
    known_count = len(known_batters)
    lineup_lefty_share = safe_divide(lineup_lefty_count, known_count)
    lineup_righty_share = safe_divide(lineup_righty_count, known_count)

    top3 = [batter for batter in lineup if 1 <= batter.batting_order <= 3]
    top3_lefty_count = count_bat_side(top3, "L")
    top3_righty_count = count_bat_side(top3, "R")

    opposing_side = "home" if side == "away" else "away"
    opposing_starter_hand = starter_hands.get((int(game["game_id"]), opposing_side))

    lineup_vs_starter_hand_advantage: float | None
    if opposing_starter_hand == "R":
        advantage_count = sum(1 for batter in known_batters if batter.bat_side in {"L", "S"})
        lineup_vs_starter_hand_advantage = safe_divide(advantage_count, known_count)
    elif opposing_starter_hand == "L":
        advantage_count = sum(1 for batter in known_batters if batter.bat_side in {"R", "S"})
        lineup_vs_starter_hand_advantage = safe_divide(advantage_count, known_count)
    else:
        lineup_vs_starter_hand_advantage = None

    return (
        int(game["game_id"]),
        side,
        int(game["season"]),
        lineup_known_flag,
        lineup_lefty_count,
        lineup_righty_count,
        lineup_switch_count,
        lineup_lefty_share,
        lineup_righty_share,
        top3_lefty_count,
        top3_righty_count,
        opposing_starter_hand,
        lineup_vs_starter_hand_advantage,
        computed_at,
    )


def process_season(conn: sqlite3.Connection, season: int, computed_at: str) -> int:
    games = fetch_games(conn, season)
    lineups = fetch_lineups(conn, season)
    starter_hands = fetch_starter_hands(conn, season)
    rows_to_upsert: list[tuple[Any, ...]] = []

    for game in games:
        game_id = int(game["game_id"])
        for side in ("home", "away"):
            rows_to_upsert.append(
                build_row(
                    game=game,
                    side=side,
                    lineup=lineups.get((game_id, side), []),
                    starter_hands=starter_hands,
                    computed_at=computed_at,
                )
            )

    conn.executemany(INSERT_SQL, rows_to_upsert)
    conn.commit()
    return len(rows_to_upsert)


def print_verification(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE game_id = 661199
        ORDER BY side
        """
    ).fetchall()
    print("Verification rows for game_id=661199:")
    if not rows:
        print("  <missing>")
        return
    for row in rows:
        print(f"  side={row['side']}")
        for key in row.keys():
            print(f"    {key}: {row[key]}")


def build_table() -> int:
    computed_at = datetime.now(timezone.utc).isoformat()
    with connect_db() as conn:
        create_table(conn)
        total_rows = 0
        for season in SEASONS:
            print(f"Processing season {season}...")
            rows = process_season(conn, season, computed_at)
            total_rows += rows
            print(f"Season {season}: upserted {rows} rows.")
        total_count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(f"Finished {TABLE_NAME}: processed {total_rows} season rows, table count={total_count}.")
        print_verification(conn)
    return 0


if __name__ == "__main__":
    raise SystemExit(build_table())
