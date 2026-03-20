#!/usr/bin/env python3
"""Add lineup OPS features to game_matchup_features."""

from __future__ import annotations

import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "mlb_history.db"
TABLE_NAME = "game_matchup_features"
VERIFY_GAME_ID = 661199

FEATURE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("home_lineup_top5_ops", "REAL"),
    ("away_lineup_top5_ops", "REAL"),
    ("home_lineup_top5_batting_avg", "REAL"),
    ("away_lineup_top5_batting_avg", "REAL"),
    ("lineup_top5_ops_delta", "REAL"),
)

UPDATE_SQL = f"""
WITH latest_lineup_snapshot AS (
    SELECT gls.game_id, gls.side, MAX(gls.as_of_ts) AS as_of_ts
    FROM game_lineup_snapshots gls
    GROUP BY gls.game_id, gls.side
),
top5_lineup_stats AS (
    SELECT
        gls.game_id,
        gls.side,
        g.season,
        COUNT(psbs.player_id) AS matched_player_count,
        AVG(psbs.ops) AS avg_ops,
        AVG(psbs.batting_avg) AS avg_batting_avg
    FROM latest_lineup_snapshot lls
    INNER JOIN game_lineup_snapshots gls
        ON gls.game_id = lls.game_id
       AND gls.side = lls.side
       AND gls.as_of_ts = lls.as_of_ts
    INNER JOIN games g
        ON g.game_id = gls.game_id
    LEFT JOIN player_season_batting_stats psbs
        ON psbs.player_id = gls.player_id
       AND psbs.season = g.season
    WHERE gls.batting_order BETWEEN 1 AND 5
    GROUP BY gls.game_id, gls.side, g.season
),
per_game_features AS (
    SELECT
        game_id,
        MAX(CASE WHEN side = 'home' AND matched_player_count >= 3 THEN avg_ops END) AS home_lineup_top5_ops,
        MAX(CASE WHEN side = 'away' AND matched_player_count >= 3 THEN avg_ops END) AS away_lineup_top5_ops,
        MAX(CASE WHEN side = 'home' AND matched_player_count >= 3 THEN avg_batting_avg END) AS home_lineup_top5_batting_avg,
        MAX(CASE WHEN side = 'away' AND matched_player_count >= 3 THEN avg_batting_avg END) AS away_lineup_top5_batting_avg
    FROM top5_lineup_stats
    GROUP BY game_id
)
UPDATE {TABLE_NAME} AS gmf
SET
    home_lineup_top5_ops = (
        SELECT pgf.home_lineup_top5_ops
        FROM per_game_features pgf
        WHERE pgf.game_id = gmf.game_id
    ),
    away_lineup_top5_ops = (
        SELECT pgf.away_lineup_top5_ops
        FROM per_game_features pgf
        WHERE pgf.game_id = gmf.game_id
    ),
    home_lineup_top5_batting_avg = (
        SELECT pgf.home_lineup_top5_batting_avg
        FROM per_game_features pgf
        WHERE pgf.game_id = gmf.game_id
    ),
    away_lineup_top5_batting_avg = (
        SELECT pgf.away_lineup_top5_batting_avg
        FROM per_game_features pgf
        WHERE pgf.game_id = gmf.game_id
    ),
    lineup_top5_ops_delta = (
        SELECT
            CASE
                WHEN pgf.home_lineup_top5_ops IS NOT NULL AND pgf.away_lineup_top5_ops IS NOT NULL
                    THEN pgf.home_lineup_top5_ops - pgf.away_lineup_top5_ops
            END
        FROM per_game_features pgf
        WHERE pgf.game_id = gmf.game_id
    )
"""


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 60000")
    return conn


def ensure_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row["name"])
        for row in conn.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()
    }
    for column_name, column_type in FEATURE_COLUMNS:
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {column_name} {column_type}")
        print(f"Added column: {column_name}")
    conn.commit()


def populate_features(conn: sqlite3.Connection) -> None:
    conn.execute(UPDATE_SQL)
    conn.commit()


def print_coverage(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN home_lineup_top5_ops IS NOT NULL THEN 1 ELSE 0 END) AS home_ops_rows,
            SUM(CASE WHEN away_lineup_top5_ops IS NOT NULL THEN 1 ELSE 0 END) AS away_ops_rows,
            SUM(CASE WHEN home_lineup_top5_batting_avg IS NOT NULL THEN 1 ELSE 0 END) AS home_avg_rows,
            SUM(CASE WHEN away_lineup_top5_batting_avg IS NOT NULL THEN 1 ELSE 0 END) AS away_avg_rows,
            SUM(CASE WHEN lineup_top5_ops_delta IS NOT NULL THEN 1 ELSE 0 END) AS delta_rows
        FROM {TABLE_NAME}
        """
    ).fetchone()

    total_rows = int(row["total_rows"])
    print("Coverage stats:")
    print(f"  total_rows={total_rows}")
    print(f"  home_lineup_top5_ops={row['home_ops_rows']}/{total_rows}")
    print(f"  away_lineup_top5_ops={row['away_ops_rows']}/{total_rows}")
    print(f"  home_lineup_top5_batting_avg={row['home_avg_rows']}/{total_rows}")
    print(f"  away_lineup_top5_batting_avg={row['away_avg_rows']}/{total_rows}")
    print(f"  lineup_top5_ops_delta={row['delta_rows']}/{total_rows}")


def print_verification(conn: sqlite3.Connection) -> None:
    matchup_row = conn.execute(
        f"""
        SELECT
            game_id,
            season,
            home_lineup_top5_ops,
            away_lineup_top5_ops,
            home_lineup_top5_batting_avg,
            away_lineup_top5_batting_avg,
            lineup_top5_ops_delta
        FROM {TABLE_NAME}
        WHERE game_id = ?
        """,
        (VERIFY_GAME_ID,),
    ).fetchone()

    source_rows = conn.execute(
        """
        WITH latest_lineup_snapshot AS (
            SELECT gls.game_id, gls.side, MAX(gls.as_of_ts) AS as_of_ts
            FROM game_lineup_snapshots gls
            WHERE gls.game_id = ?
            GROUP BY gls.game_id, gls.side
        )
        SELECT
            gls.side,
            gls.batting_order,
            gls.player_name,
            psbs.ops,
            psbs.batting_avg
        FROM latest_lineup_snapshot lls
        INNER JOIN game_lineup_snapshots gls
            ON gls.game_id = lls.game_id
           AND gls.side = lls.side
           AND gls.as_of_ts = lls.as_of_ts
        INNER JOIN games g
            ON g.game_id = gls.game_id
        LEFT JOIN player_season_batting_stats psbs
            ON psbs.player_id = gls.player_id
           AND psbs.season = g.season
        WHERE gls.batting_order BETWEEN 1 AND 5
        ORDER BY gls.side, gls.batting_order
        """,
        (VERIFY_GAME_ID,),
    ).fetchall()

    print(f"Verification for game_id={VERIFY_GAME_ID}:")
    if matchup_row is None:
        print("  game not found in game_matchup_features")
        return

    print(
        "  matchup_features="
        f"home_ops={matchup_row['home_lineup_top5_ops']}, "
        f"away_ops={matchup_row['away_lineup_top5_ops']}, "
        f"home_avg={matchup_row['home_lineup_top5_batting_avg']}, "
        f"away_avg={matchup_row['away_lineup_top5_batting_avg']}, "
        f"delta={matchup_row['lineup_top5_ops_delta']}"
    )
    for row in source_rows:
        print(
            f"  {row['side']}#{row['batting_order']}: "
            f"{row['player_name']} ops={row['ops']} batting_avg={row['batting_avg']}"
        )


def main() -> None:
    with connect_db() as conn:
        ensure_columns(conn)
        populate_features(conn)
        print_coverage(conn)
        print_verification(conn)


if __name__ == "__main__":
    main()
