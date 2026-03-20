#!/usr/bin/env python3
"""Build the Layer 3 game_matchup_features table.

One flat row per game joining all 6 Layer 2 tables + weather + labels.
This is the training/inference input for the prediction model.

Usage:
    python3 scripts/build_layer2_matchup_features.py [--drop-recreate]

Seasons: 2020-2025 (regular season only, game_type='R').
Only games with labels (did_home_win IS NOT NULL) are included.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "mlb_history.db"
TABLE_NAME = "game_matchup_features"
SEASONS = tuple(range(2020, 2026))

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
  -- Identifiers (not model inputs)
  game_id                             INTEGER PRIMARY KEY,
  game_date                           TEXT,
  season                              INTEGER,
  home_team_id                        INTEGER,
  away_team_id                        INTEGER,

  -- Labels (not model inputs)
  did_home_win                        INTEGER,
  home_score                          INTEGER,
  away_score                          INTEGER,
  run_differential                    INTEGER,  -- home - away

  -- ── Team strength: home (~18) ──────────────────────────────────────────
  home_team_season_games              INTEGER,
  home_team_season_wins               INTEGER,
  home_team_season_win_pct            REAL,
  home_team_season_run_diff_per_game  REAL,
  home_team_season_runs_scored_per_game REAL,
  home_team_season_runs_allowed_per_game REAL,
  home_team_season_batting_avg        REAL,
  home_team_season_obp                REAL,
  home_team_season_slg                REAL,
  home_team_season_ops                REAL,
  home_team_season_strikeouts_per_game REAL,
  home_team_season_walks_per_game     REAL,
  home_team_rolling_last10_win_pct    REAL,
  home_team_rolling_last10_runs_scored_per_game REAL,
  home_team_rolling_last10_runs_allowed_per_game REAL,
  home_team_rolling_last10_ops        REAL,
  home_team_rolling_last10_obp        REAL,
  home_team_rolling_last10_batting_avg REAL,
  home_team_days_rest                 INTEGER,
  home_team_doubleheader_flag         INTEGER,

  -- ── Team strength: away (~18) ──────────────────────────────────────────
  away_team_season_games              INTEGER,
  away_team_season_wins               INTEGER,
  away_team_season_win_pct            REAL,
  away_team_season_run_diff_per_game  REAL,
  away_team_season_runs_scored_per_game REAL,
  away_team_season_runs_allowed_per_game REAL,
  away_team_season_batting_avg        REAL,
  away_team_season_obp                REAL,
  away_team_season_slg                REAL,
  away_team_season_ops                REAL,
  away_team_season_strikeouts_per_game REAL,
  away_team_season_walks_per_game     REAL,
  away_team_rolling_last10_win_pct    REAL,
  away_team_rolling_last10_runs_scored_per_game REAL,
  away_team_rolling_last10_runs_allowed_per_game REAL,
  away_team_rolling_last10_ops        REAL,
  away_team_rolling_last10_obp        REAL,
  away_team_rolling_last10_batting_avg REAL,
  away_team_days_rest                 INTEGER,
  away_team_doubleheader_flag         INTEGER,

  -- ── Starter quality: home (~12) ────────────────────────────────────────
  home_starter_probable_pitcher_known INTEGER,
  home_starter_hand_l_flag            INTEGER,
  home_starter_hand_r_flag            INTEGER,
  home_starter_season_starts          INTEGER,
  home_starter_era                    REAL,
  home_starter_whip                   REAL,
  home_starter_k_pct                  REAL,
  home_starter_bb_pct                 REAL,
  home_starter_hr_per_9               REAL,
  home_starter_avg_allowed            REAL,
  home_starter_strike_pct             REAL,
  home_starter_win_pct                REAL,
  home_starter_stats_available_flag   INTEGER,

  -- ── Starter quality: away (~12) ────────────────────────────────────────
  away_starter_probable_pitcher_known INTEGER,
  away_starter_hand_l_flag            INTEGER,
  away_starter_hand_r_flag            INTEGER,
  away_starter_season_starts          INTEGER,
  away_starter_era                    REAL,
  away_starter_whip                   REAL,
  away_starter_k_pct                  REAL,
  away_starter_bb_pct                 REAL,
  away_starter_hr_per_9               REAL,
  away_starter_avg_allowed            REAL,
  away_starter_strike_pct             REAL,
  away_starter_win_pct                REAL,
  away_starter_stats_available_flag   INTEGER,

  -- ── Bullpen: home (~10) ────────────────────────────────────────────────
  home_bullpen_season_bullpen_era     REAL,
  home_bullpen_season_bullpen_whip    REAL,
  home_bullpen_season_bullpen_k_pct   REAL,
  home_bullpen_season_bullpen_bb_pct  REAL,
  home_bullpen_season_bullpen_hr_per_9 REAL,
  home_bullpen_season_appearances     INTEGER,
  home_bullpen_outs_last1d            INTEGER,
  home_bullpen_outs_last3d            INTEGER,
  home_bullpen_outs_last5d            INTEGER,
  home_bullpen_pitches_last1d         INTEGER,
  home_bullpen_pitches_last3d         INTEGER,
  home_bullpen_relievers_used_last3d_count INTEGER,
  home_bullpen_high_usage_relievers_last3d INTEGER,
  home_bullpen_back_to_back_relievers_count INTEGER,

  -- ── Bullpen: away (~10) ────────────────────────────────────────────────
  away_bullpen_season_bullpen_era     REAL,
  away_bullpen_season_bullpen_whip    REAL,
  away_bullpen_season_bullpen_k_pct   REAL,
  away_bullpen_season_bullpen_bb_pct  REAL,
  away_bullpen_season_bullpen_hr_per_9 REAL,
  away_bullpen_season_appearances     INTEGER,
  away_bullpen_outs_last1d            INTEGER,
  away_bullpen_outs_last3d            INTEGER,
  away_bullpen_outs_last5d            INTEGER,
  away_bullpen_pitches_last1d         INTEGER,
  away_bullpen_pitches_last3d         INTEGER,
  away_bullpen_relievers_used_last3d_count INTEGER,
  away_bullpen_high_usage_relievers_last3d INTEGER,
  away_bullpen_back_to_back_relievers_count INTEGER,

  -- ── Lineup composition: home (~9) ─────────────────────────────────────
  home_lineup_known_flag              INTEGER,
  home_lineup_lefty_count             INTEGER,
  home_lineup_righty_count            INTEGER,
  home_lineup_switch_count            INTEGER,
  home_lineup_lefty_share             REAL,
  home_lineup_righty_share            REAL,
  home_lineup_top3_lefty_count        INTEGER,
  home_lineup_top3_righty_count       INTEGER,
  home_lineup_vs_starter_hand_advantage REAL,

  -- ── Lineup composition: away (~9) ─────────────────────────────────────
  away_lineup_known_flag              INTEGER,
  away_lineup_lefty_count             INTEGER,
  away_lineup_righty_count            INTEGER,
  away_lineup_switch_count            INTEGER,
  away_lineup_lefty_share             REAL,
  away_lineup_righty_share            REAL,
  away_lineup_top3_lefty_count        INTEGER,
  away_lineup_top3_righty_count       INTEGER,
  away_lineup_vs_starter_hand_advantage REAL,

  -- ── Handedness matchup: home bats vs away starter's hand (~4) ─────────
  home_vs_starter_hand_ops            REAL,
  home_vs_starter_hand_batting_avg    REAL,
  home_vs_starter_hand_runs_per_game  REAL,
  home_vs_starter_hand_games          INTEGER,

  -- ── Handedness matchup: away bats vs home starter's hand (~4) ─────────
  away_vs_starter_hand_ops            REAL,
  away_vs_starter_hand_batting_avg    REAL,
  away_vs_starter_hand_runs_per_game  REAL,
  away_vs_starter_hand_games          INTEGER,

  -- ── Delta features (home - away, ~13) ─────────────────────────────────
  win_pct_delta                       REAL,
  run_diff_per_game_delta             REAL,
  ops_delta                           REAL,
  batting_avg_delta                   REAL,
  rolling_last10_ops_delta            REAL,
  rolling_last10_win_pct_delta        REAL,
  starter_era_delta                   REAL,
  starter_k_pct_delta                 REAL,
  starter_whip_delta                  REAL,
  bullpen_era_delta                   REAL,
  bullpen_fatigue_outs_last3d_delta   REAL,
  vs_starter_hand_ops_delta           REAL,
  days_rest_delta                     REAL,

  -- ── Weather (~12) ──────────────────────────────────────────────────────
  temperature_f                       REAL,
  wind_speed_mph                      REAL,
  wind_direction_deg                  REAL,
  wind_gust_mph                       REAL,
  humidity_pct                        REAL,
  precipitation_mm                    REAL,
  cloud_cover_pct                     REAL,
  pressure_hpa                        REAL,
  roof_closed_or_fixed_flag           INTEGER,
  weather_exposed_flag                INTEGER,
  is_day                              INTEGER,
  hour_offset_from_first_pitch        REAL,

  -- ── Context ────────────────────────────────────────────────────────────
  home_field_advantage                INTEGER DEFAULT 1,

  -- Metadata
  built_at                            TEXT
)
"""

BUILD_SQL = f"""
INSERT OR REPLACE INTO {TABLE_NAME}
SELECT
  g.game_id,
  g.game_date,
  g.season,
  g.home_team_id,
  g.away_team_id,

  -- Labels
  lb.did_home_win,
  lb.home_score,
  lb.away_score,
  lb.run_differential,

  -- ── Home team strength ─────────────────────────────────────────────────
  ht.season_games,
  ht.season_wins,
  ht.season_win_pct,
  ht.season_run_diff_per_game,
  ht.season_runs_scored_per_game,
  ht.season_runs_allowed_per_game,
  ht.season_batting_avg,
  ht.season_obp,
  ht.season_slg,
  ht.season_ops,
  ht.season_strikeouts_per_game,
  ht.season_walks_per_game,
  ht.rolling_last10_win_pct,
  ht.rolling_last10_runs_scored_per_game,
  ht.rolling_last10_runs_allowed_per_game,
  ht.rolling_last10_ops,
  ht.rolling_last10_obp,
  ht.rolling_last10_batting_avg,
  ht.days_rest,
  ht.doubleheader_flag,

  -- ── Away team strength ─────────────────────────────────────────────────
  at_.season_games,
  at_.season_wins,
  at_.season_win_pct,
  at_.season_run_diff_per_game,
  at_.season_runs_scored_per_game,
  at_.season_runs_allowed_per_game,
  at_.season_batting_avg,
  at_.season_obp,
  at_.season_slg,
  at_.season_ops,
  at_.season_strikeouts_per_game,
  at_.season_walks_per_game,
  at_.rolling_last10_win_pct,
  at_.rolling_last10_runs_scored_per_game,
  at_.rolling_last10_runs_allowed_per_game,
  at_.rolling_last10_ops,
  at_.rolling_last10_obp,
  at_.rolling_last10_batting_avg,
  at_.days_rest,
  at_.doubleheader_flag,

  -- ── Home starter ───────────────────────────────────────────────────────
  hs.probable_pitcher_known,
  CASE WHEN hs.pitcher_hand = 'L' THEN 1 ELSE 0 END,
  CASE WHEN hs.pitcher_hand = 'R' THEN 1 ELSE 0 END,
  hs.season_starts,
  hs.season_era,
  hs.season_whip,
  hs.season_k_pct,
  hs.season_bb_pct,
  hs.season_hr_per_9,
  hs.season_avg_allowed,
  hs.season_strike_pct,
  hs.season_win_pct,
  hs.stats_available_flag,

  -- ── Away starter ───────────────────────────────────────────────────────
  as_.probable_pitcher_known,
  CASE WHEN as_.pitcher_hand = 'L' THEN 1 ELSE 0 END,
  CASE WHEN as_.pitcher_hand = 'R' THEN 1 ELSE 0 END,
  as_.season_starts,
  as_.season_era,
  as_.season_whip,
  as_.season_k_pct,
  as_.season_bb_pct,
  as_.season_hr_per_9,
  as_.season_avg_allowed,
  as_.season_strike_pct,
  as_.season_win_pct,
  as_.stats_available_flag,

  -- ── Home bullpen ───────────────────────────────────────────────────────
  hb.season_bullpen_era,
  hb.season_bullpen_whip,
  hb.season_bullpen_k_pct,
  hb.season_bullpen_bb_pct,
  hb.season_bullpen_hr_per_9,
  hb.season_appearances,
  hb.bullpen_outs_last1d,
  hb.bullpen_outs_last3d,
  hb.bullpen_outs_last5d,
  hb.bullpen_pitches_last1d,
  hb.bullpen_pitches_last3d,
  hb.relievers_used_last3d_count,
  hb.high_usage_relievers_last3d,
  hb.back_to_back_relievers_count,

  -- ── Away bullpen ───────────────────────────────────────────────────────
  ab.season_bullpen_era,
  ab.season_bullpen_whip,
  ab.season_bullpen_k_pct,
  ab.season_bullpen_bb_pct,
  ab.season_bullpen_hr_per_9,
  ab.season_appearances,
  ab.bullpen_outs_last1d,
  ab.bullpen_outs_last3d,
  ab.bullpen_outs_last5d,
  ab.bullpen_pitches_last1d,
  ab.bullpen_pitches_last3d,
  ab.relievers_used_last3d_count,
  ab.high_usage_relievers_last3d,
  ab.back_to_back_relievers_count,

  -- ── Home lineup ────────────────────────────────────────────────────────
  hl.lineup_known_flag,
  hl.lineup_lefty_count,
  hl.lineup_righty_count,
  hl.lineup_switch_count,
  hl.lineup_lefty_share,
  hl.lineup_righty_share,
  hl.top3_lefty_count,
  hl.top3_righty_count,
  hl.lineup_vs_starter_hand_advantage,

  -- ── Away lineup ────────────────────────────────────────────────────────
  al.lineup_known_flag,
  al.lineup_lefty_count,
  al.lineup_righty_count,
  al.lineup_switch_count,
  al.lineup_lefty_share,
  al.lineup_righty_share,
  al.top3_lefty_count,
  al.top3_righty_count,
  al.lineup_vs_starter_hand_advantage,

  -- ── Home bats vs away starter's hand ──────────────────────────────────
  -- Select the vs_rhp or vs_lhp stats depending on away starter's hand
  CASE as_.pitcher_hand
    WHEN 'R' THEN hvh.vs_rhp_ops
    WHEN 'L' THEN hvh.vs_lhp_ops
    ELSE NULL
  END,
  CASE as_.pitcher_hand
    WHEN 'R' THEN hvh.vs_rhp_batting_avg
    WHEN 'L' THEN hvh.vs_lhp_batting_avg
    ELSE NULL
  END,
  CASE as_.pitcher_hand
    WHEN 'R' THEN hvh.vs_rhp_runs_per_game
    WHEN 'L' THEN hvh.vs_lhp_runs_per_game
    ELSE NULL
  END,
  CASE as_.pitcher_hand
    WHEN 'R' THEN hvh.vs_rhp_games
    WHEN 'L' THEN hvh.vs_lhp_games
    ELSE NULL
  END,

  -- ── Away bats vs home starter's hand ──────────────────────────────────
  CASE hs.pitcher_hand
    WHEN 'R' THEN avh.vs_rhp_ops
    WHEN 'L' THEN avh.vs_lhp_ops
    ELSE NULL
  END,
  CASE hs.pitcher_hand
    WHEN 'R' THEN avh.vs_rhp_batting_avg
    WHEN 'L' THEN avh.vs_lhp_batting_avg
    ELSE NULL
  END,
  CASE hs.pitcher_hand
    WHEN 'R' THEN avh.vs_rhp_runs_per_game
    WHEN 'L' THEN avh.vs_lhp_runs_per_game
    ELSE NULL
  END,
  CASE hs.pitcher_hand
    WHEN 'R' THEN avh.vs_rhp_games
    WHEN 'L' THEN avh.vs_lhp_games
    ELSE NULL
  END,

  -- ── Delta features (home - away) ──────────────────────────────────────
  ht.season_win_pct      - at_.season_win_pct,
  ht.season_run_diff_per_game - at_.season_run_diff_per_game,
  ht.season_ops          - at_.season_ops,
  ht.season_batting_avg  - at_.season_batting_avg,
  ht.rolling_last10_ops  - at_.rolling_last10_ops,
  ht.rolling_last10_win_pct - at_.rolling_last10_win_pct,
  -- ERA delta: lower is better for home team -> home_era - away_era
  hs.season_era          - as_.season_era,
  hs.season_k_pct        - as_.season_k_pct,
  hs.season_whip         - as_.season_whip,
  hb.season_bullpen_era  - ab.season_bullpen_era,
  CAST(hb.bullpen_outs_last3d AS REAL) - CAST(ab.bullpen_outs_last3d AS REAL),
  -- vs_starter_hand_ops delta: home bats vs away hand - away bats vs home hand
  (CASE as_.pitcher_hand WHEN 'R' THEN hvh.vs_rhp_ops WHEN 'L' THEN hvh.vs_lhp_ops ELSE NULL END)
  - (CASE hs.pitcher_hand WHEN 'R' THEN avh.vs_rhp_ops WHEN 'L' THEN avh.vs_lhp_ops ELSE NULL END),
  CAST(ht.days_rest AS REAL) - CAST(at_.days_rest AS REAL),

  -- ── Weather (best available snapshot per game) ─────────────────────────
  w.temperature_f,
  w.wind_speed_mph,
  w.wind_direction_deg,
  w.wind_gust_mph,
  w.humidity_pct,
  w.precipitation_mm,
  w.cloud_cover_pct,
  w.pressure_hpa,
  -- roof_closed_or_fixed_flag: 1 if roof type is 'fixed' or 'retractable'
  CASE WHEN vd.roof_type IN ('fixed', 'retractable') THEN 1 ELSE 0 END,
  COALESCE(w.weather_exposure_flag, vd.weather_exposure_default),
  w.is_day,
  w.hour_offset_from_first_pitch,

  -- Context
  1,

  -- built_at
  strftime('%Y-%m-%dT%H:%M:%SZ', 'now')

FROM games g

-- Labels: must exist (exclude unplayed games)
INNER JOIN labels lb
  ON lb.game_id = g.game_id

-- Home team pregame stats
LEFT JOIN team_pregame_stats ht
  ON ht.game_id = g.game_id AND ht.side = 'home'

-- Away team pregame stats
LEFT JOIN team_pregame_stats at_
  ON at_.game_id = g.game_id AND at_.side = 'away'

-- Home starter
LEFT JOIN starter_pregame_stats hs
  ON hs.game_id = g.game_id AND hs.side = 'home'

-- Away starter
LEFT JOIN starter_pregame_stats as_
  ON as_.game_id = g.game_id AND as_.side = 'away'

-- Home bullpen
LEFT JOIN bullpen_pregame_stats hb
  ON hb.game_id = g.game_id AND hb.side = 'home'

-- Away bullpen
LEFT JOIN bullpen_pregame_stats ab
  ON ab.game_id = g.game_id AND ab.side = 'away'

-- Home lineup
LEFT JOIN lineup_pregame_context hl
  ON hl.game_id = g.game_id AND hl.side = 'home'

-- Away lineup
LEFT JOIN lineup_pregame_context al
  ON al.game_id = g.game_id AND al.side = 'away'

-- Home team vs hand stats
LEFT JOIN team_vs_hand_pregame_stats hvh
  ON hvh.game_id = g.game_id AND hvh.side = 'home'

-- Away team vs hand stats
LEFT JOIN team_vs_hand_pregame_stats avh
  ON avh.game_id = g.game_id AND avh.side = 'away'

-- Best weather snapshot: lowest source_priority, then closest to game time
LEFT JOIN (
  SELECT gws.*
  FROM game_weather_snapshots gws
  INNER JOIN (
    SELECT game_id,
           MIN(source_priority * 1000 + ABS(COALESCE(hour_offset_from_first_pitch, 999))) AS best_score
    FROM game_weather_snapshots
    GROUP BY game_id
  ) best
    ON best.game_id = gws.game_id
   AND (gws.source_priority * 1000 + ABS(COALESCE(gws.hour_offset_from_first_pitch, 999))) = best.best_score
) w
  ON w.game_id = g.game_id

-- Venue for roof type
LEFT JOIN venue_dim vd
  ON vd.venue_id = g.venue_id

WHERE g.game_type = 'R'
  AND g.season BETWEEN 2020 AND 2025
  AND lb.did_home_win IS NOT NULL
"""


def build_table(conn: sqlite3.Connection, drop_recreate: bool = False) -> int:
    cur = conn.cursor()
    if drop_recreate:
        print(f"Dropping existing {TABLE_NAME} table...")
        cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print(f"Table {TABLE_NAME} ready. Running INSERT...")
    cur.execute(BUILD_SQL)
    conn.commit()
    count = cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    return count


def verify_game(conn: sqlite3.Connection, game_id: int) -> dict:
    cur = conn.cursor()
    row = cur.execute(
        f"SELECT * FROM {TABLE_NAME} WHERE game_id = ?", (game_id,)
    ).fetchone()
    if row is None:
        return {"error": f"game_id {game_id} not found"}
    cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


def null_audit(conn: sqlite3.Connection) -> dict[str, int]:
    """Return count of NULLs for key feature columns."""
    key_cols = [
        "home_team_season_win_pct",
        "away_team_season_win_pct",
        "home_starter_era",
        "away_starter_era",
        "home_bullpen_season_bullpen_era",
        "away_bullpen_season_bullpen_era",
        "home_lineup_known_flag",
        "away_lineup_known_flag",
        "home_vs_starter_hand_ops",
        "away_vs_starter_hand_ops",
        "temperature_f",
        "win_pct_delta",
    ]
    cur = conn.cursor()
    total = cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    result: dict[str, int] = {"total_rows": total}
    for col in key_cols:
        null_count = cur.execute(
            f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {col} IS NULL"
        ).fetchone()[0]
        result[col + "_nulls"] = null_count
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--drop-recreate",
        action="store_true",
        help="Drop and recreate the table (default: upsert into existing)",
    )
    args = parser.parse_args()

    print(f"Connecting to {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    # Run build
    total = build_table(conn, drop_recreate=args.drop_recreate)
    print(f"\n✅ Built {total:,} rows in {TABLE_NAME}")

    # Verify game 661199 (Aug 15, 2022 — LAD at Diamondbacks)
    VERIFY_GAME = 661199
    print(f"\n── Verification: game_id={VERIFY_GAME} ──")
    row = verify_game(conn, VERIFY_GAME)
    if "error" in row:
        print(f"  ❌ {row['error']}")
    else:
        checks = [
            ("game_date", row.get("game_date")),
            ("did_home_win", row.get("did_home_win"), "expected 0"),
            ("away_team_season_win_pct", row.get("away_team_season_win_pct"), "expected ~0.699"),
            ("away_starter_era", row.get("away_starter_era"), "expected ~2.504"),
            ("away_vs_starter_hand_ops", row.get("away_vs_starter_hand_ops"), "expected ~0.752 (LAD vs RHP)"),
            ("temperature_f", row.get("temperature_f"), "expected populated"),
            ("win_pct_delta", row.get("win_pct_delta")),
            ("home_field_advantage", row.get("home_field_advantage"), "expected 1"),
        ]
        for item in checks:
            label, val = item[0], item[1]
            note = item[2] if len(item) > 2 else ""
            print(f"  {label}: {val!r}  {note}")

    # NULL audit
    print("\n── NULL audit ──")
    audit = null_audit(conn)
    for k, v in audit.items():
        if k == "total_rows":
            print(f"  total rows: {v:,}")
        else:
            col = k.replace("_nulls", "")
            pct = 100 * v / audit["total_rows"] if audit["total_rows"] else 0
            print(f"  {col}: {v:,} NULLs ({pct:.1f}%)")

    # Cold start check: first game of 2022 season (opening day)
    print("\n── Cold-start check (first 2022 game) ──")
    first_2022 = conn.execute(
        f"SELECT game_id, game_date, home_team_season_win_pct, away_team_season_win_pct "
        f"FROM {TABLE_NAME} WHERE season=2022 ORDER BY game_date ASC LIMIT 1"
    ).fetchone()
    if first_2022:
        print(f"  game_id={first_2022[0]}, date={first_2022[1]}, "
              f"home_win_pct={first_2022[2]}, away_win_pct={first_2022[3]} "
              f"(both NULL expected for opening day)")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
