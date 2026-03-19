#!/usr/bin/env python3
"""
Fetch career pitching stats for pitchers missing from player_career_pitching_stats.
Resumable/idempotent: skips pitchers already in the table.

Usage: python3 scripts/fetch_career_pitcher_stats.py
"""

import sqlite3
import time
from datetime import datetime, timezone

import statsapi

DB_PATH = "data/mlb_history.db"
SLEEP_BETWEEN = 0.15


def ensure_columns(conn):
    """Add career_games and career_stats_fetched columns if not present."""
    cur = conn.cursor()
    existing = {row[1] for row in cur.execute("PRAGMA table_info(player_career_pitching_stats)")}

    if "career_games" not in existing:
        print("Adding career_games column...")
        cur.execute("ALTER TABLE player_career_pitching_stats ADD COLUMN career_games INTEGER")
    if "career_stats_fetched" not in existing:
        print("Adding career_stats_fetched column...")
        cur.execute(
            "ALTER TABLE player_career_pitching_stats ADD COLUMN career_stats_fetched INTEGER DEFAULT 0"
        )
    conn.commit()


def get_missing_pitchers(conn):
    """Get pitchers in game_pitcher_appearances not yet in player_career_pitching_stats."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT gpa.pitcher_id
        FROM game_pitcher_appearances gpa
        WHERE gpa.pitcher_id NOT IN (
            SELECT pitcher_id FROM player_career_pitching_stats
        )
        ORDER BY gpa.pitcher_id
        """
    )
    return [row[0] for row in cur.fetchall()]


def fetch_career_stats(pitcher_id):
    """
    Returns (pitcher_name, era, whip, ip, games, k_pct, bb_pct, avg_allowed) or None values.
    """
    try:
        data = statsapi.player_stat_data(pitcher_id, group="pitching", type="career")
    except Exception as e:
        print(f"  API error for pitcher {pitcher_id}: {e}")
        return None, None, None, None, None, None, None, None

    first = data.get("first_name", "")
    last = data.get("last_name", "")
    pitcher_name = f"{first} {last}".strip() or f"Player {pitcher_id}"

    stats_list = data.get("stats", [])
    if not stats_list:
        return pitcher_name, None, None, None, None, None, None, None

    # Find career pitching entry
    career_stats = None
    for entry in stats_list:
        if entry.get("type") == "career" and entry.get("group") == "pitching":
            career_stats = entry.get("stats", {})
            break

    if not career_stats:
        return pitcher_name, None, None, None, None, None, None, None

    def safe_float(val):
        try:
            return float(val) if val not in (None, "", "--", "-.--") else None
        except (ValueError, TypeError):
            return None

    era = safe_float(career_stats.get("era"))
    whip = safe_float(career_stats.get("whip"))
    ip_str = career_stats.get("inningsPitched")
    ip = safe_float(ip_str)
    games = career_stats.get("gamesPlayed")

    # Compute k_pct, bb_pct, avg_allowed from raw stats
    batters_faced = career_stats.get("battersFaced", 0) or 0
    strikeouts = career_stats.get("strikeOuts", 0) or 0
    walks = career_stats.get("baseOnBalls", 0) or 0
    avg_allowed_str = career_stats.get("avg")
    avg_allowed = safe_float(avg_allowed_str)

    k_pct = (strikeouts / batters_faced) if batters_faced > 0 else None
    bb_pct = (walks / batters_faced) if batters_faced > 0 else None

    return pitcher_name, era, whip, ip, games, k_pct, bb_pct, avg_allowed


def insert_pitcher(conn, pitcher_id, pitcher_name, era, whip, ip, games, k_pct, bb_pct, avg_allowed):
    fetched_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO player_career_pitching_stats
            (pitcher_id, pitcher_name, career_era, career_whip, career_ip, career_games,
             career_k_pct, career_bb_pct, career_avg_allowed, career_stats_fetched, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (pitcher_id, pitcher_name, era, whip, ip, games, k_pct, bb_pct, avg_allowed, fetched_at),
    )
    conn.commit()


def print_summary(conn):
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM player_career_pitching_stats").fetchone()[0]
    with_era = cur.execute(
        "SELECT COUNT(*) FROM player_career_pitching_stats WHERE career_era IS NOT NULL"
    ).fetchone()[0]
    fetched = cur.execute(
        "SELECT COUNT(*) FROM player_career_pitching_stats WHERE career_stats_fetched = 1"
    ).fetchone()[0]
    print(f"\n=== Summary ===")
    print(f"Total pitchers in table: {total}")
    print(f"Pitchers with career_era populated: {with_era}")
    print(f"Pitchers marked career_stats_fetched=1: {fetched}")


def main():
    conn = sqlite3.connect(DB_PATH)

    ensure_columns(conn)

    missing = get_missing_pitchers(conn)
    total = len(missing)
    print(f"Found {total} pitchers to fetch")

    if total == 0:
        print("Nothing to do — all pitchers already fetched.")
        print_summary(conn)
        conn.close()
        return

    errors = 0
    for i, pitcher_id in enumerate(missing, 1):
        pitcher_name, era, whip, ip, games, k_pct, bb_pct, avg_allowed = fetch_career_stats(
            pitcher_id
        )

        insert_pitcher(
            conn, pitcher_id, pitcher_name or f"Player {pitcher_id}",
            era, whip, ip, games, k_pct, bb_pct, avg_allowed
        )

        if pitcher_name is None:
            errors += 1

        if i % 50 == 0 or i == total:
            print(f"Fetched {i}/{total} (errors so far: {errors})")

        time.sleep(SLEEP_BETWEEN)

    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
