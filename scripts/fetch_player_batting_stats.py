#!/usr/bin/env python3
"""
Fetch season batting stats for all players in game_lineup_snapshots (2020-2025).
Creates/populates player_season_batting_stats table.
Resumable/idempotent: skips (player_id, season) pairs already in the table.

Usage: python3 scripts/fetch_player_batting_stats.py
"""

import sqlite3
import time
from datetime import datetime, timezone

import statsapi

DB_PATH = "data/mlb_history.db"
SLEEP_BETWEEN = 0.15
SEASONS = list(range(2020, 2026))  # 2020-2025


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS player_season_batting_stats (
    player_id         INTEGER NOT NULL,
    player_name       TEXT,
    season            INTEGER NOT NULL,
    batting_avg       REAL,
    obp               REAL,
    slg               REAL,
    ops               REAL,
    home_runs         INTEGER,
    rbi               INTEGER,
    hits              INTEGER,
    at_bats           INTEGER,
    plate_appearances INTEGER,
    strikeout_rate    REAL,
    walk_rate         REAL,
    fetched_at        TEXT,
    PRIMARY KEY (player_id, season)
)
"""


def ensure_table(conn):
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()


def get_all_player_season_pairs(conn):
    """Get all unique (player_id, season) from game_lineup_snapshots via games join."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT gls.player_id, CAST(strftime('%Y', g.game_date) AS INTEGER) as season
        FROM game_lineup_snapshots gls
        JOIN games g ON gls.game_id = g.game_id
        WHERE CAST(strftime('%Y', g.game_date) AS INTEGER) BETWEEN 2020 AND 2025
        ORDER BY season, gls.player_id
        """
    )
    return cur.fetchall()


def get_already_fetched(conn):
    """Get set of (player_id, season) already in table."""
    cur = conn.cursor()
    cur.execute("SELECT player_id, season FROM player_season_batting_stats")
    return set(cur.fetchall())


def fetch_season_hitting(player_id, season):
    """
    Returns dict of batting stats or None values on failure.
    """
    try:
        data = statsapi.player_stat_data(player_id, group="hitting", type="season", season=season)
    except Exception as e:
        print(f"  API error for player {player_id} season {season}: {e}")
        return None

    first = data.get("first_name", "")
    last = data.get("last_name", "")
    player_name = f"{first} {last}".strip() or f"Player {player_id}"

    stats_list = data.get("stats", [])
    if not stats_list:
        return {"player_name": player_name, "stats": None}

    # Find season hitting entry
    season_stats = None
    for entry in stats_list:
        if entry.get("type") == "season" and entry.get("group") == "hitting":
            season_stats = entry.get("stats", {})
            break

    if not season_stats:
        return {"player_name": player_name, "stats": None}

    return {"player_name": player_name, "stats": season_stats}


def safe_float(val):
    try:
        return float(val) if val not in (None, "", "--", "-.--") else None
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def insert_row(conn, player_id, player_name, season, s):
    """Insert a row. s is the stats dict or None."""
    fetched_at = datetime.now(timezone.utc).isoformat()

    if s is None:
        conn.execute(
            """
            INSERT OR IGNORE INTO player_season_batting_stats
                (player_id, player_name, season, fetched_at)
            VALUES (?, ?, ?, ?)
            """,
            (player_id, player_name, season, fetched_at),
        )
    else:
        plate_appearances = safe_int(s.get("plateAppearances"))
        at_bats = safe_int(s.get("atBats"))
        strikeouts = safe_int(s.get("strikeOuts"))
        walks = safe_int(s.get("baseOnBalls"))
        strikeout_rate = (strikeouts / plate_appearances) if plate_appearances else None
        walk_rate = (walks / plate_appearances) if plate_appearances else None

        conn.execute(
            """
            INSERT OR IGNORE INTO player_season_batting_stats
                (player_id, player_name, season,
                 batting_avg, obp, slg, ops,
                 home_runs, rbi, hits, at_bats, plate_appearances,
                 strikeout_rate, walk_rate, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id, player_name, season,
                safe_float(s.get("avg")), safe_float(s.get("obp")),
                safe_float(s.get("slg")), safe_float(s.get("ops")),
                safe_int(s.get("homeRuns")), safe_int(s.get("rbi")),
                safe_int(s.get("hits")), at_bats, plate_appearances,
                strikeout_rate, walk_rate, fetched_at,
            ),
        )
    conn.commit()


def print_summary(conn):
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM player_season_batting_stats").fetchone()[0]
    with_stats = cur.execute(
        "SELECT COUNT(*) FROM player_season_batting_stats WHERE batting_avg IS NOT NULL"
    ).fetchone()[0]
    by_season = cur.execute(
        "SELECT season, COUNT(*), SUM(CASE WHEN batting_avg IS NOT NULL THEN 1 ELSE 0 END) "
        "FROM player_season_batting_stats GROUP BY season ORDER BY season"
    ).fetchall()
    print(f"\n=== player_season_batting_stats Summary ===")
    print(f"Total rows: {total}")
    print(f"Rows with batting_avg populated: {with_stats}")
    print(f"{'Season':>8} {'Rows':>8} {'With Stats':>12}")
    for row in by_season:
        print(f"{row[0]:>8} {row[1]:>8} {row[2]:>12}")


def main():
    conn = sqlite3.connect(DB_PATH)

    ensure_table(conn)

    all_pairs = get_all_player_season_pairs(conn)
    already_fetched = get_already_fetched(conn)

    to_fetch = [(pid, s) for pid, s in all_pairs if (pid, s) not in already_fetched]
    total = len(to_fetch)
    print(f"Total player-season pairs: {len(all_pairs)}")
    print(f"Already fetched: {len(already_fetched)}")
    print(f"To fetch: {total}")

    if total == 0:
        print("Nothing to do — all player-season pairs already fetched.")
        print_summary(conn)
        conn.close()
        return

    errors = 0
    no_stats = 0
    for i, (player_id, season) in enumerate(to_fetch, 1):
        result = fetch_season_hitting(player_id, season)

        if result is None:
            # API error — insert placeholder with no stats so we don't retry indefinitely
            insert_row(conn, player_id, f"Player {player_id}", season, None)
            errors += 1
        else:
            insert_row(conn, player_id, result["player_name"], season, result["stats"])
            if result["stats"] is None:
                no_stats += 1

        if i % 100 == 0 or i == total:
            print(f"Fetched {i}/{total} (api_errors: {errors}, no_batting_stats: {no_stats})")

        time.sleep(SLEEP_BETWEEN)

    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
