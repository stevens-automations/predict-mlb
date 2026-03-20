#!/usr/bin/env python3
"""
Part B: Run differential regression model.
Trains LightGBM regressor on run_differential (home - away).
Same feature set, CV/holdout splits, cold-start filter as v4 classifier.
"""
import json
import pickle
import warnings
from datetime import datetime
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, mean_squared_error
import sqlite3
import math

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
FOLDS = [
    {"train": [2020, 2021], "test": [2022]},
    {"train": [2020, 2021, 2022], "test": [2023]},
    {"train": [2020, 2021, 2022, 2023], "test": [2024]},
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
    df = pd.DataFrame(rows, columns=cols)
    return df


def apply_cold_start_filter(df):
    mask = (df["home_team_season_games"] >= 15) & (df["away_team_season_games"] >= 15)
    dropped = (~mask).sum()
    if dropped > 0:
        print(f"    Cold-start filter: dropped {dropped} rows")
    return df[mask].copy()


def train_regressor(X_tr, y_tr, X_te, y_te):
    params = {
        "objective": "regression",
        "metric": ["rmse"],
        "num_leaves": 63,
        "learning_rate": 0.03,
        "feature_fraction": 0.7,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "min_data_in_leaf": 30,
        "n_estimators": 1000,
        "verbose": -1,
        "random_state": 42,
    }
    model = lgb.LGBMRegressor(**params)
    model.fit(
        X_tr, y_tr,
        eval_set=[(X_te, y_te)],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
    )
    preds = model.predict(X_te)
    rmse = math.sqrt(mean_squared_error(y_te, preds))
    binary_preds = (preds > 0).astype(int)
    acc = accuracy_score((y_te > 0).astype(int), binary_preds)
    return model, preds, rmse, acc


def run_cv(df, feature_cols):
    fold_rmses, fold_accs = [], []
    for fold in FOLDS:
        train_df = apply_cold_start_filter(df[df["season"].isin(fold["train"])])
        test_df = df[df["season"].isin(fold["test"])]
        X_tr = train_df[feature_cols].values
        y_tr = train_df["run_differential"].values
        X_te = test_df[feature_cols].values
        y_te = test_df["run_differential"].values
        _, preds, rmse, acc = train_regressor(X_tr, y_tr, X_te, y_te)
        fold_rmses.append(rmse)
        fold_accs.append(acc)
        print(f"    fold test={fold['test']}: rmse={rmse:.4f}  binary_acc={acc:.4f}")
    return float(np.mean(fold_rmses)), float(np.mean(fold_accs))


def main():
    print("=" * 70)
    print("  train_run_diff.py — Part B: Run Differential Regressor")
    print("=" * 70)

    df = load_data()
    print(f"Loaded {len(df)} games with run_differential")
    # Quick sanity: verify run_differential exists
    sample = df["run_differential"].describe()
    print(f"  run_differential stats: mean={sample['mean']:.2f}  std={sample['std']:.2f}  min={sample['min']:.0f}  max={sample['max']:.0f}")

    feature_cols = resolve_feature_cols(df.columns.tolist())
    print(f"  Features: {len(feature_cols)}")

    print("\n--- CV (3 folds) ---")
    cv_rmse, cv_acc = run_cv(df, feature_cols)
    print(f"  CV avg: rmse={cv_rmse:.4f}  binary_acc={cv_acc:.4f} ({cv_acc*100:.2f}%)")

    print("\n--- Holdout 2025 ---")
    train_df = apply_cold_start_filter(df[df["season"].isin(DEV_SEASONS)])
    test_df = df[df["season"] == HOLDOUT_SEASON]
    X_tr = train_df[feature_cols].values
    y_tr = train_df["run_differential"].values
    X_te = test_df[feature_cols].values
    y_te = test_df["run_differential"].values
    print(f"  train={len(train_df)} rows, test={len(test_df)} rows")

    model, holdout_preds, holdout_rmse, holdout_acc = train_regressor(X_tr, y_tr, X_te, y_te)
    print(f"  Holdout RMSE:         {holdout_rmse:.4f}")
    print(f"  Holdout binary acc:   {holdout_acc:.4f} ({holdout_acc*100:.2f}%)")

    # Save artifact
    run_id = f"run_diff_lgbm_v4__{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    run_dir = REGISTRY_PATH / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    metrics = {
        "run_id": run_id,
        "model_type": "regressor",
        "target": "run_differential",
        "cv_rmse": cv_rmse,
        "cv_binary_acc": cv_acc,
        "holdout_rmse": float(holdout_rmse),
        "holdout_binary_acc": float(holdout_acc),
        "feature_cols": feature_cols,
        "n_features": len(feature_cols),
    }
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

    with open(run_dir / "model.pkl", "wb") as f:
        pickle.dump({"model": model, "feature_cols": feature_cols}, f)

    # Save "latest" pointer
    latest_path = REGISTRY_PATH / "run_diff_lgbm_v4__latest.json"
    latest_data = {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "holdout_binary_acc": float(holdout_acc),
        "holdout_rmse": float(holdout_rmse),
    }
    latest_path.write_text(json.dumps(latest_data, indent=2))

    print(f"\nSaved to {run_dir}")
    print(f"\n{'='*70}")
    print(f"  RUN-DIFF REGRESSOR SUMMARY")
    print(f"{'='*70}")
    print(f"  CV avg RMSE:          {cv_rmse:.4f}")
    print(f"  CV avg binary acc:    {cv_acc*100:.2f}%")
    print(f"  Holdout RMSE:         {holdout_rmse:.4f}")
    print(f"  Holdout binary acc:   {holdout_acc*100:.2f}%")


if __name__ == "__main__":
    main()
