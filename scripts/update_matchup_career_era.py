#!/usr/bin/env python3
"""
Add and populate home_starter_career_era / away_starter_career_era in game_matchup_features.
Sources from player_career_pitching_stats joined on pitcher_id via starter_pregame_stats.

Idempotent: safe to run multiple times.

Usage: python3 scripts/update_matchup_career_era.py
"""

import sqlite3

DB_PATH = "data/mlb_history.db"


def ensure_columns(conn):
    cur = conn.cursor()
    existing = {row[1] for row in cur.execute("PRAGMA table_info(game_matchup_features)")}

    added = []
    for col in ("home_starter_career_era", "away_starter_career_era"):
        if col not in existing:
            conn.execute(f"ALTER TABLE game_matchup_features ADD COLUMN {col} REAL")
            added.append(col)
    if added:
        conn.commit()
        print(f"Added columns: {added}")
    else:
        print("Columns already exist.")


def populate_home(conn):
    """Update home/away_starter_career_era from starter_pregame_stats (side col) + player_career_pitching_stats."""

    # starter_pregame_stats has: game_id, side ('home'/'away'), probable_pitcher_id
    result = conn.execute(
        """
        UPDATE game_matchup_features
        SET home_starter_career_era = (
            SELECT cps.career_era
            FROM starter_pregame_stats sps
            JOIN player_career_pitching_stats cps ON sps.probable_pitcher_id = cps.pitcher_id
            WHERE sps.game_id = game_matchup_features.game_id
              AND sps.side = 'home'
              AND sps.probable_pitcher_known = 1
            LIMIT 1
        )
        WHERE home_starter_career_era IS NULL
        """
    )
    conn.commit()
    print(f"Updated home_starter_career_era: {result.rowcount} rows affected")

    result = conn.execute(
        """
        UPDATE game_matchup_features
        SET away_starter_career_era = (
            SELECT cps.career_era
            FROM starter_pregame_stats sps
            JOIN player_career_pitching_stats cps ON sps.probable_pitcher_id = cps.pitcher_id
            WHERE sps.game_id = game_matchup_features.game_id
              AND sps.side = 'away'
              AND sps.probable_pitcher_known = 1
            LIMIT 1
        )
        WHERE away_starter_career_era IS NULL
        """
    )
    conn.commit()
    print(f"Updated away_starter_career_era: {result.rowcount} rows affected")


def print_summary(conn):
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM game_matchup_features").fetchone()[0]
    home_era = cur.execute(
        "SELECT COUNT(*) FROM game_matchup_features WHERE home_starter_career_era IS NOT NULL"
    ).fetchone()[0]
    away_era = cur.execute(
        "SELECT COUNT(*) FROM game_matchup_features WHERE away_starter_career_era IS NOT NULL"
    ).fetchone()[0]
    sample = cur.execute(
        "SELECT game_id, home_starter_career_era, away_starter_career_era "
        "FROM game_matchup_features WHERE game_id = 661199"
    ).fetchone()
    print(f"\n=== game_matchup_features career ERA Summary ===")
    print(f"Total rows: {total}")
    print(f"home_starter_career_era populated: {home_era} ({home_era/total*100:.1f}%)")
    print(f"away_starter_career_era populated: {away_era} ({away_era/total*100:.1f}%)")
    if sample:
        print(f"Verification game 661199: home_career_era={sample[1]}, away_career_era={sample[2]}")


def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_columns(conn)
    populate_home(conn)
    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
