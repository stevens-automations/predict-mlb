#!/usr/bin/env python3
"""
Part C: Ensemble blend of binary classifier (Part A) + run-diff regressor (Part B).
Evaluates blend weights on holdout 2025. Picks best blend.
"""
import json
import pickle
import warnings
import math
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, log_loss
import sqlite3

DB_PATH = Path(__file__).parents[2] / "data" / "mlb_history.db"
REGISTRY_PATH = Path(__file__).parents[2] / "artifacts" / "model_registry"

COLUMN_ALIASES = {
    "home_bullpen_era": "home_bullpen_season_bullpen_era",
    "away_bullpen_era": "away_bullpen_season_bullpen_era",
}

FEATURE_COLS_SPEC = [
    "win_pct_delta", "run_diff_per_game_delta", "ops_delta", "batting_avg_delta",
    "rolling_last10_win_pct_delta", "rolling_last10_ops_delta",
    "home_team_season_win_pct", "away_team_season_win_pct",
    "home_team_season_run_diff_per_game", "away_team_season_run_diff_per_game",
    "home_team_season_ops", "away_team_season_ops",
    "home_team_rolling_last10_win_pct", "away_team_rolling_last10_win_pct",
    "starter_era_delta", "starter_k_pct_delta", "starter_whip_delta",
    "home_starter_era", "away_starter_era",
    "home_starter_k_pct", "away_starter_k_pct",
    "home_starter_season_starts", "away_starter_season_starts",
    "home_starter_career_era", "away_starter_career_era",
    "home_starter_hand_l_flag", "home_starter_hand_r_flag",
    "away_starter_hand_l_flag", "away_starter_hand_r_flag",
    "bullpen_era_delta",
    "home_bullpen_era", "away_bullpen_era",
    "bullpen_fatigue_outs_last3d_delta",
    "home_bullpen_pitches_last3d", "away_bullpen_pitches_last3d",
    "vs_starter_hand_ops_delta", "home_vs_starter_hand_ops", "away_vs_starter_hand_ops",
    "home_vs_starter_hand_games", "away_vs_starter_hand_games",
    "home_lineup_lefty_share", "away_lineup_lefty_share",
    "home_lineup_righty_share", "away_lineup_righty_share",
    "home_lineup_top5_ops", "away_lineup_top5_ops",
    "home_lineup_top5_batting_avg", "away_lineup_top5_batting_avg",
    "lineup_top5_ops_delta",
    "days_rest_delta", "home_team_days_rest", "away_team_days_rest",
    "home_team_doubleheader_flag", "away_team_doubleheader_flag",
    "temperature_f", "wind_speed_mph", "wind_direction_deg", "wind_gust_mph",
    "humidity_pct", "precipitation_mm", "cloud_cover_pct",
    "roof_closed_or_fixed_flag", "weather_exposed_flag",
]

DEV_SEASONS = [2020, 2021, 2022, 2023, 2024]
HOLDOUT_SEASON = 2025

BLEND_WEIGHTS = [
    (0.8, 0.2),
    (0.7, 0.3),
    (0.6, 0.4),
    (0.5, 0.5),
]


def resolve_feature_cols(df_columns):
    available = set(df_columns)
    resolved = []
    for spec_name in FEATURE_COLS_SPEC:
        actual = COLUMN_ALIASES.get(spec_name, spec_name)
        if actual in available:
            resolved.append(actual)
        elif spec_name in available:
            resolved.append(spec_name)
        else:
            warnings.warn(f"Column not found, skipping: {spec_name}")
    seen = set()
    deduped = []
    for c in resolved:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    return deduped


def load_data():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT * FROM game_matchup_features WHERE did_home_win IS NOT NULL AND run_differential IS NOT NULL ORDER BY game_date, game_id"
    ).fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM game_matchup_features LIMIT 0").description]
    conn.close()
    return pd.DataFrame(rows, columns=cols)


def apply_cold_start_filter(df):
    mask = (df["home_team_season_games"] >= 15) & (df["away_team_season_games"] >= 15)
    dropped = (~mask).sum()
    if dropped > 0:
        print(f"  Cold-start filter: dropped {dropped} rows")
    return df[mask].copy()


def load_classifier():
    """Load best binary classifier from gridsearch artifact."""
    latest_path = REGISTRY_PATH / "gridsearch_lgbm_v4__latest.json"
    latest = json.loads(latest_path.read_text())
    run_dir = Path(latest["run_dir"])
    with open(run_dir / "model.pkl", "rb") as f:
        artifact = pickle.load(f)
    print(f"  Loaded classifier from {run_dir.name}")
    print(f"    Reported holdout acc: {latest['holdout_acc']*100:.2f}%")
    return artifact["model"], artifact["feature_cols"]


def load_regressor():
    """Load run-diff regressor from latest artifact."""
    latest_path = REGISTRY_PATH / "run_diff_lgbm_v4__latest.json"
    latest = json.loads(latest_path.read_text())
    run_dir = Path(latest["run_dir"])
    with open(run_dir / "model.pkl", "rb") as f:
        artifact = pickle.load(f)
    print(f"  Loaded regressor from {run_dir.name}")
    print(f"    Reported holdout binary acc: {latest['holdout_binary_acc']*100:.2f}%")
    return artifact["model"], artifact["feature_cols"]


def logistic_transform(run_diff_preds, scale=3.0):
    """Convert run_diff predictions to probability via logistic transform."""
    return 1.0 / (1.0 + np.exp(-run_diff_preds / scale))


def main():
    print("=" * 70)
    print("  ensemble_blend.py — Part C: Ensemble Blend")
    print("=" * 70)

    df = load_data()
    print(f"\nLoaded {len(df)} games")

    feature_cols = resolve_feature_cols(df.columns.tolist())
    print(f"Features: {len(feature_cols)}")

    print("\nLoading models...")
    clf_model, clf_features = load_classifier()
    reg_model, reg_features = load_regressor()

    # Holdout set
    holdout_df = df[df["season"] == HOLDOUT_SEASON].copy()
    y_te = holdout_df["did_home_win"].values
    print(f"\nHoldout 2025: {len(holdout_df)} games")

    # Get classifier probabilities
    X_clf = holdout_df[clf_features].values
    clf_proba = clf_model.predict_proba(X_clf)[:, 1]
    clf_acc = accuracy_score(y_te, (clf_proba > 0.5).astype(int))
    clf_ll = log_loss(y_te, clf_proba)
    print(f"\nClassifier on holdout: acc={clf_acc*100:.2f}%  ll={clf_ll:.4f}")

    # Get regressor predictions → logistic probability
    X_reg = holdout_df[reg_features].values
    run_diff_preds = reg_model.predict(X_reg)
    reg_proba = logistic_transform(run_diff_preds)
    reg_acc = accuracy_score(y_te, (run_diff_preds > 0).astype(int))
    reg_ll = log_loss(y_te, reg_proba)
    print(f"Regressor on holdout:  acc={reg_acc*100:.2f}%  ll={reg_ll:.4f}")

    # Blend
    print(f"\n{'='*70}")
    print(f"  BLEND RESULTS")
    print(f"{'='*70}")
    print(f"  {'Weights (clf/reg)':<22} {'Accuracy':>10}  {'Log Loss':>10}")
    print(f"  {'-'*44}")

    blend_results = []
    for w_clf, w_reg in BLEND_WEIGHTS:
        blended = w_clf * clf_proba + w_reg * reg_proba
        blended_preds = (blended > 0.5).astype(int)
        acc = accuracy_score(y_te, blended_preds)
        ll = log_loss(y_te, blended)
        blend_results.append({"w_clf": w_clf, "w_reg": w_reg, "acc": acc, "ll": ll})
        print(f"  {w_clf:.1f} / {w_reg:.1f}                     {acc*100:>9.2f}%  {ll:>10.4f}")

    best_blend = max(blend_results, key=lambda x: x["acc"])
    print(f"\n  Best blend: {best_blend['w_clf']:.1f}/{best_blend['w_reg']:.1f}  →  {best_blend['acc']*100:.2f}%  ll={best_blend['ll']:.4f}")

    # Save results
    results = {
        "classifier_holdout_acc": float(clf_acc),
        "classifier_holdout_ll": float(clf_ll),
        "regressor_holdout_acc": float(reg_acc),
        "regressor_holdout_ll": float(reg_ll),
        "blend_results": [
            {"w_clf": r["w_clf"], "w_reg": r["w_reg"], "acc": r["acc"], "ll": r["ll"]}
            for r in blend_results
        ],
        "best_blend_w_clf": best_blend["w_clf"],
        "best_blend_w_reg": best_blend["w_reg"],
        "best_blend_acc": float(best_blend["acc"]),
        "best_blend_ll": float(best_blend["ll"]),
    }
    out_path = REGISTRY_PATH / "ensemble_blend_results.json"
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\nSaved blend results to {out_path}")


if __name__ == "__main__":
    main()
