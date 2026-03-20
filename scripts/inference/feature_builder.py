#!/usr/bin/env python3
"""
Inference feature builder for predict-mlb.

Function: build_feature_row(game_id, conn) -> dict

Queries Layer 2 tables to assemble the exact feature dict used in training.
Uses the same FEATURE_COLS as scripts/training/train_matchup_lgbm.py.
Returns a flat dict with all FEATURE_COLS keys; None where data unavailable.

Cold-start metadata: if team_pregame_stats.season_games < 15 for either team,
adds cold_start=True to the returned dict (does not affect features).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

# ── Feature columns (must match train_matchup_lgbm.py FEATURE_COLS_SPEC + aliases resolved) ──
# Note: train script uses aliases: home_bullpen_era → home_bullpen_season_bullpen_era
#       In the returned dict we use the ACTUAL column names used by the model (from gridsearch metrics.json)
FEATURE_COLS = [
    # Team record — delta
    "win_pct_delta",
    "run_diff_per_game_delta",
    "ops_delta",
    "batting_avg_delta",
    "rolling_last10_win_pct_delta",
    "rolling_last10_ops_delta",
    # Team record — raw
    "home_team_season_win_pct",
    "away_team_season_win_pct",
    "home_team_season_run_diff_per_game",
    "away_team_season_run_diff_per_game",
    "home_team_season_ops",
    "away_team_season_ops",
    "home_team_rolling_last10_win_pct",
    "away_team_rolling_last10_win_pct",
    # Starter — delta
    "starter_era_delta",
    "starter_k_pct_delta",
    "starter_whip_delta",
    # Starter — raw
    "home_starter_era",
    "away_starter_era",
    "home_starter_k_pct",
    "away_starter_k_pct",
    "home_starter_season_starts",
    "away_starter_season_starts",
    "home_starter_career_era",
    "away_starter_career_era",
    # Starter hand flags
    "home_starter_hand_l_flag",
    "home_starter_hand_r_flag",
    "away_starter_hand_l_flag",
    "away_starter_hand_r_flag",
    # Bullpen (actual DB column names, aliases resolved)
    "bullpen_era_delta",
    "home_bullpen_season_bullpen_era",
    "away_bullpen_season_bullpen_era",
    "bullpen_fatigue_outs_last3d_delta",
    "home_bullpen_pitches_last3d",
    "away_bullpen_pitches_last3d",
    # Handedness matchup
    "vs_starter_hand_ops_delta",
    "home_vs_starter_hand_ops",
    "away_vs_starter_hand_ops",
    "home_vs_starter_hand_games",
    "away_vs_starter_hand_games",
    # Lineup
    "home_lineup_lefty_share",
    "away_lineup_lefty_share",
    "home_lineup_righty_share",
    "away_lineup_righty_share",
    "home_lineup_top5_ops",
    "away_lineup_top5_ops",
    "home_lineup_top5_batting_avg",
    "away_lineup_top5_batting_avg",
    "lineup_top5_ops_delta",
    # Rest/fatigue
    "days_rest_delta",
    "home_team_days_rest",
    "away_team_days_rest",
    "home_team_doubleheader_flag",
    "away_team_doubleheader_flag",
    # Weather
    "temperature_f",
    "wind_speed_mph",
    "wind_direction_deg",
    "wind_gust_mph",
    "humidity_pct",
    "precipitation_mm",
    "cloud_cover_pct",
    "roof_closed_or_fixed_flag",
    "weather_exposed_flag",
]


def _fetch_one(cur: sqlite3.Cursor, sql: str, params: tuple) -> Optional[dict]:
    """Execute query and return single row as dict, or None."""
    cur.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


def build_feature_row(game_id: int, conn: sqlite3.Connection) -> dict:
    """
    Build the feature dict for a single game.

    Args:
        game_id: The game ID (must exist in games table).
        conn: SQLite3 connection (caller manages lifecycle).

    Returns:
        dict with all FEATURE_COLS keys (None where data unavailable),
        plus metadata keys: game_id, home_team_id, away_team_id, cold_start.
    """
    cur = conn.cursor()

    # ── Verify game exists ────────────────────────────────────────────────────
    game_row = _fetch_one(
        cur,
        "SELECT game_id, home_team_id, away_team_id, venue_id, game_date, season FROM games WHERE game_id = ?",
        (game_id,),
    )
    if game_row is None:
        raise ValueError(f"game_id {game_id} not found in games table")

    home_team_id = game_row["home_team_id"]
    away_team_id = game_row["away_team_id"]
    venue_id = game_row["venue_id"]

    # ── Team pregame stats ────────────────────────────────────────────────────
    home_team = _fetch_one(
        cur,
        "SELECT * FROM team_pregame_stats WHERE game_id = ? AND side = 'home'",
        (game_id,),
    )
    away_team = _fetch_one(
        cur,
        "SELECT * FROM team_pregame_stats WHERE game_id = ? AND side = 'away'",
        (game_id,),
    )

    # Cold-start check
    cold_start = False
    if home_team is not None and (home_team.get("season_games") or 0) < 15:
        cold_start = True
    if away_team is not None and (away_team.get("season_games") or 0) < 15:
        cold_start = True

    # ── Starter pregame stats ─────────────────────────────────────────────────
    home_starter = _fetch_one(
        cur,
        "SELECT * FROM starter_pregame_stats WHERE game_id = ? AND side = 'home'",
        (game_id,),
    )
    away_starter = _fetch_one(
        cur,
        "SELECT * FROM starter_pregame_stats WHERE game_id = ? AND side = 'away'",
        (game_id,),
    )

    # ── Career ERA fallback ───────────────────────────────────────────────────
    home_pitcher_id = home_starter.get("probable_pitcher_id") if home_starter else None
    away_pitcher_id = away_starter.get("probable_pitcher_id") if away_starter else None

    home_career = None
    if home_pitcher_id:
        home_career = _fetch_one(
            cur,
            "SELECT * FROM player_career_pitching_stats WHERE pitcher_id = ?",
            (home_pitcher_id,),
        )

    away_career = None
    if away_pitcher_id:
        away_career = _fetch_one(
            cur,
            "SELECT * FROM player_career_pitching_stats WHERE pitcher_id = ?",
            (away_pitcher_id,),
        )

    # ── Bullpen pregame stats ─────────────────────────────────────────────────
    home_bullpen = _fetch_one(
        cur,
        "SELECT * FROM bullpen_pregame_stats WHERE game_id = ? AND side = 'home'",
        (game_id,),
    )
    away_bullpen = _fetch_one(
        cur,
        "SELECT * FROM bullpen_pregame_stats WHERE game_id = ? AND side = 'away'",
        (game_id,),
    )

    # ── Lineup pregame context ────────────────────────────────────────────────
    home_lineup = _fetch_one(
        cur,
        "SELECT * FROM lineup_pregame_context WHERE game_id = ? AND side = 'home'",
        (game_id,),
    )
    away_lineup = _fetch_one(
        cur,
        "SELECT * FROM lineup_pregame_context WHERE game_id = ? AND side = 'away'",
        (game_id,),
    )

    # ── Team vs hand pregame stats ────────────────────────────────────────────
    # Home bats vs away starter's hand; away bats vs home starter's hand
    away_starter_hand = away_starter.get("pitcher_hand") if away_starter else None
    home_starter_hand = home_starter.get("pitcher_hand") if home_starter else None

    home_vs_hand = _fetch_one(
        cur,
        "SELECT * FROM team_vs_hand_pregame_stats WHERE game_id = ? AND side = 'home'",
        (game_id,),
    )
    away_vs_hand = _fetch_one(
        cur,
        "SELECT * FROM team_vs_hand_pregame_stats WHERE game_id = ? AND side = 'away'",
        (game_id,),
    )

    def _vs_hand_ops(vs_hand_row: Optional[dict], pitcher_hand: Optional[str]) -> Optional[float]:
        if vs_hand_row is None or pitcher_hand is None:
            return None
        if pitcher_hand == "R":
            return vs_hand_row.get("vs_rhp_ops")
        elif pitcher_hand == "L":
            return vs_hand_row.get("vs_lhp_ops")
        return None

    def _vs_hand_games(vs_hand_row: Optional[dict], pitcher_hand: Optional[str]) -> Optional[int]:
        if vs_hand_row is None or pitcher_hand is None:
            return None
        if pitcher_hand == "R":
            return vs_hand_row.get("vs_rhp_games")
        elif pitcher_hand == "L":
            return vs_hand_row.get("vs_lhp_games")
        return None

    home_vs_hand_ops = _vs_hand_ops(home_vs_hand, away_starter_hand)
    away_vs_hand_ops = _vs_hand_ops(away_vs_hand, home_starter_hand)
    home_vs_hand_games = _vs_hand_games(home_vs_hand, away_starter_hand)
    away_vs_hand_games = _vs_hand_games(away_vs_hand, home_starter_hand)

    # ── Weather ───────────────────────────────────────────────────────────────
    weather = _fetch_one(
        cur,
        """
        SELECT gws.*
        FROM game_weather_snapshots gws
        INNER JOIN (
            SELECT game_id,
                   MIN(source_priority * 1000 + ABS(COALESCE(hour_offset_from_first_pitch, 999))) AS best_score
            FROM game_weather_snapshots
            WHERE game_id = ?
            GROUP BY game_id
        ) best
            ON best.game_id = gws.game_id
           AND (gws.source_priority * 1000 + ABS(COALESCE(gws.hour_offset_from_first_pitch, 999))) = best.best_score
        LIMIT 1
        """,
        (game_id,),
    )

    # ── Venue ─────────────────────────────────────────────────────────────────
    venue = _fetch_one(
        cur,
        "SELECT * FROM venue_dim WHERE venue_id = ?",
        (venue_id,),
    ) if venue_id else None

    # ── Lineup top5 OPS (from game_lineup_snapshots + player_season_batting_stats) ──
    season = game_row.get("season")
    top5_result = cur.execute(
        """
        WITH latest_lineup_snapshot AS (
            SELECT gls.game_id, gls.side, MAX(gls.as_of_ts) AS as_of_ts
            FROM game_lineup_snapshots gls
            WHERE gls.game_id = ?
            GROUP BY gls.game_id, gls.side
        ),
        top5_lineup_stats AS (
            SELECT
                gls.game_id,
                gls.side,
                COUNT(psbs.player_id) AS matched_player_count,
                AVG(psbs.ops) AS avg_ops,
                AVG(psbs.batting_avg) AS avg_batting_avg
            FROM latest_lineup_snapshot lls
            INNER JOIN game_lineup_snapshots gls
                ON gls.game_id = lls.game_id
               AND gls.side = lls.side
               AND gls.as_of_ts = lls.as_of_ts
            LEFT JOIN player_season_batting_stats psbs
                ON psbs.player_id = gls.player_id
               AND psbs.season = ?
            WHERE gls.batting_order BETWEEN 1 AND 5
            GROUP BY gls.game_id, gls.side
        )
        SELECT
            MAX(CASE WHEN side = 'home' AND matched_player_count >= 3 THEN avg_ops END) AS home_lineup_top5_ops,
            MAX(CASE WHEN side = 'away' AND matched_player_count >= 3 THEN avg_ops END) AS away_lineup_top5_ops,
            MAX(CASE WHEN side = 'home' AND matched_player_count >= 3 THEN avg_batting_avg END) AS home_lineup_top5_batting_avg,
            MAX(CASE WHEN side = 'away' AND matched_player_count >= 3 THEN avg_batting_avg END) AS away_lineup_top5_batting_avg
        FROM top5_lineup_stats
        """,
        (game_id, season),
    ).fetchone()

    home_lineup_top5_ops = top5_result[0] if top5_result else None
    away_lineup_top5_ops = top5_result[1] if top5_result else None
    home_lineup_top5_batting_avg = top5_result[2] if top5_result else None
    away_lineup_top5_batting_avg = top5_result[3] if top5_result else None
    lineup_top5_ops_delta = (
        home_lineup_top5_ops - away_lineup_top5_ops
        if home_lineup_top5_ops is not None and away_lineup_top5_ops is not None
        else None
    )

    # ── Helper: safe subtraction ─────────────────────────────────────────────
    def _delta(a, b):
        if a is None or b is None:
            return None
        return a - b

    def _get(d: Optional[dict], key: str):
        if d is None:
            return None
        return d.get(key)

    # ── Compute delta features ────────────────────────────────────────────────
    home_season_win_pct = _get(home_team, "season_win_pct")
    away_season_win_pct = _get(away_team, "season_win_pct")
    home_run_diff = _get(home_team, "season_run_diff_per_game")
    away_run_diff = _get(away_team, "season_run_diff_per_game")
    home_ops = _get(home_team, "season_ops")
    away_ops = _get(away_team, "season_ops")
    home_batting_avg = _get(home_team, "season_batting_avg")
    away_batting_avg = _get(away_team, "season_batting_avg")
    home_rolling10_win = _get(home_team, "rolling_last10_win_pct")
    away_rolling10_win = _get(away_team, "rolling_last10_win_pct")
    home_rolling10_ops = _get(home_team, "rolling_last10_ops")
    away_rolling10_ops = _get(away_team, "rolling_last10_ops")

    home_starter_era = _get(home_starter, "season_era")
    away_starter_era = _get(away_starter, "season_era")
    home_starter_k_pct = _get(home_starter, "season_k_pct")
    away_starter_k_pct = _get(away_starter, "season_k_pct")
    home_starter_whip = _get(home_starter, "season_whip")
    away_starter_whip = _get(away_starter, "season_whip")

    home_bullpen_era = _get(home_bullpen, "season_bullpen_era")
    away_bullpen_era = _get(away_bullpen, "season_bullpen_era")
    home_bullpen_outs3d = _get(home_bullpen, "bullpen_outs_last3d")
    away_bullpen_outs3d = _get(away_bullpen, "bullpen_outs_last3d")

    home_days_rest = _get(home_team, "days_rest")
    away_days_rest = _get(away_team, "days_rest")

    # ── Roof/weather exposure ─────────────────────────────────────────────────
    roof_type = _get(venue, "roof_type")
    roof_closed_or_fixed_flag = (
        1 if roof_type in ("fixed", "retractable") else 0
    ) if roof_type is not None else None

    weather_exposed_flag = _get(weather, "weather_exposure_flag")
    if weather_exposed_flag is None and venue is not None:
        weather_exposed_flag = venue.get("weather_exposure_default")

    # ── Assemble feature dict ─────────────────────────────────────────────────
    features: dict = {
        # Delta features
        "win_pct_delta": _delta(home_season_win_pct, away_season_win_pct),
        "run_diff_per_game_delta": _delta(home_run_diff, away_run_diff),
        "ops_delta": _delta(home_ops, away_ops),
        "batting_avg_delta": _delta(home_batting_avg, away_batting_avg),
        "rolling_last10_win_pct_delta": _delta(home_rolling10_win, away_rolling10_win),
        "rolling_last10_ops_delta": _delta(home_rolling10_ops, away_rolling10_ops),
        # Team raw
        "home_team_season_win_pct": home_season_win_pct,
        "away_team_season_win_pct": away_season_win_pct,
        "home_team_season_run_diff_per_game": home_run_diff,
        "away_team_season_run_diff_per_game": away_run_diff,
        "home_team_season_ops": home_ops,
        "away_team_season_ops": away_ops,
        "home_team_rolling_last10_win_pct": home_rolling10_win,
        "away_team_rolling_last10_win_pct": away_rolling10_win,
        # Starter delta
        "starter_era_delta": _delta(home_starter_era, away_starter_era),
        "starter_k_pct_delta": _delta(home_starter_k_pct, away_starter_k_pct),
        "starter_whip_delta": _delta(home_starter_whip, away_starter_whip),
        # Starter raw
        "home_starter_era": home_starter_era,
        "away_starter_era": away_starter_era,
        "home_starter_k_pct": home_starter_k_pct,
        "away_starter_k_pct": away_starter_k_pct,
        "home_starter_season_starts": _get(home_starter, "season_starts"),
        "away_starter_season_starts": _get(away_starter, "season_starts"),
        "home_starter_career_era": _get(home_career, "career_era"),
        "away_starter_career_era": _get(away_career, "career_era"),
        # Starter hand flags
        "home_starter_hand_l_flag": 1 if home_starter_hand == "L" else (0 if home_starter_hand is not None else None),
        "home_starter_hand_r_flag": 1 if home_starter_hand == "R" else (0 if home_starter_hand is not None else None),
        "away_starter_hand_l_flag": 1 if away_starter_hand == "L" else (0 if away_starter_hand is not None else None),
        "away_starter_hand_r_flag": 1 if away_starter_hand == "R" else (0 if away_starter_hand is not None else None),
        # Bullpen
        "bullpen_era_delta": _delta(home_bullpen_era, away_bullpen_era),
        "home_bullpen_season_bullpen_era": home_bullpen_era,
        "away_bullpen_season_bullpen_era": away_bullpen_era,
        "bullpen_fatigue_outs_last3d_delta": _delta(
            float(home_bullpen_outs3d) if home_bullpen_outs3d is not None else None,
            float(away_bullpen_outs3d) if away_bullpen_outs3d is not None else None,
        ),
        "home_bullpen_pitches_last3d": _get(home_bullpen, "bullpen_pitches_last3d"),
        "away_bullpen_pitches_last3d": _get(away_bullpen, "bullpen_pitches_last3d"),
        # Handedness matchup
        "vs_starter_hand_ops_delta": _delta(home_vs_hand_ops, away_vs_hand_ops),
        "home_vs_starter_hand_ops": home_vs_hand_ops,
        "away_vs_starter_hand_ops": away_vs_hand_ops,
        "home_vs_starter_hand_games": home_vs_hand_games,
        "away_vs_starter_hand_games": away_vs_hand_games,
        # Lineup
        "home_lineup_lefty_share": _get(home_lineup, "lineup_lefty_share"),
        "away_lineup_lefty_share": _get(away_lineup, "lineup_lefty_share"),
        "home_lineup_righty_share": _get(home_lineup, "lineup_righty_share"),
        "away_lineup_righty_share": _get(away_lineup, "lineup_righty_share"),
        "home_lineup_top5_ops": home_lineup_top5_ops,
        "away_lineup_top5_ops": away_lineup_top5_ops,
        "home_lineup_top5_batting_avg": home_lineup_top5_batting_avg,
        "away_lineup_top5_batting_avg": away_lineup_top5_batting_avg,
        "lineup_top5_ops_delta": lineup_top5_ops_delta,
        # Rest/fatigue
        "days_rest_delta": _delta(
            float(home_days_rest) if home_days_rest is not None else None,
            float(away_days_rest) if away_days_rest is not None else None,
        ),
        "home_team_days_rest": home_days_rest,
        "away_team_days_rest": away_days_rest,
        "home_team_doubleheader_flag": _get(home_team, "doubleheader_flag"),
        "away_team_doubleheader_flag": _get(away_team, "doubleheader_flag"),
        # Weather
        "temperature_f": _get(weather, "temperature_f"),
        "wind_speed_mph": _get(weather, "wind_speed_mph"),
        "wind_direction_deg": _get(weather, "wind_direction_deg"),
        "wind_gust_mph": _get(weather, "wind_gust_mph"),
        "humidity_pct": _get(weather, "humidity_pct"),
        "precipitation_mm": _get(weather, "precipitation_mm"),
        "cloud_cover_pct": _get(weather, "cloud_cover_pct"),
        "roof_closed_or_fixed_flag": roof_closed_or_fixed_flag,
        "weather_exposed_flag": weather_exposed_flag,
    }

    # ── Metadata ──────────────────────────────────────────────────────────────
    features["game_id"] = game_id
    features["home_team_id"] = home_team_id
    features["away_team_id"] = away_team_id
    features["cold_start"] = cold_start

    return features


if __name__ == "__main__":
    import sys
    import json

    db_path = Path(__file__).parents[2] / "data" / "mlb_history.db"
    game_id = int(sys.argv[1]) if len(sys.argv) > 1 else 744825

    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        row = build_feature_row(game_id, conn)
        print(json.dumps(row, indent=2, default=str))
    finally:
        conn.close()
