#!/usr/bin/env python3
"""
Part A: LightGBM hyperparameter grid search.
Tries 5 param combinations, picks best CV params, runs holdout, saves artifact.
Uses same FEATURE_COLS and cold-start filter as train_matchup_lgbm.py v4.
"""
import json
import pickle
import warnings
from datetime import datetime
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
FOLDS = [
    {"train": [2020, 2021], "test": [2022]},
    {"train": [2020, 2021, 2022], "test": [2023]},
    {"train": [2020, 2021, 2022, 2023], "test": [2024]},
]

PARAM_GRID = [
    {"num_leaves": 31,  "learning_rate": 0.03, "feature_fraction": 0.7, "min_data_in_leaf": 30, "n_estimators": 1000},
    {"num_leaves": 63,  "learning_rate": 0.03, "feature_fraction": 0.7, "min_data_in_leaf": 30, "n_estimators": 1000},
    {"num_leaves": 31,  "learning_rate": 0.05, "feature_fraction": 0.8, "min_data_in_leaf": 20, "n_estimators": 800},
    {"num_leaves": 127, "learning_rate": 0.02, "feature_fraction": 0.6, "min_data_in_leaf": 40, "n_estimators": 1200},
    {"num_leaves": 31,  "learning_rate": 0.01, "feature_fraction": 0.7, "min_data_in_leaf": 50, "n_estimators": 1500},
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
        "SELECT * FROM game_matchup_features WHERE did_home_win IS NOT NULL ORDER BY game_date, game_id"
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


def train_lgbm(X_tr, y_tr, X_te, y_te, params):
    base = {
        "objective": "binary",
        "metric": ["binary_logloss", "binary_error"],
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
        "random_state": 42,
    }
    base.update(params)
    model = lgb.LGBMClassifier(**base)
    model.fit(
        X_tr, y_tr,
        eval_set=[(X_te, y_te)],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
    )
    preds = model.predict(X_te)
    proba = model.predict_proba(X_te)[:, 1]
    return model, accuracy_score(y_te, preds), log_loss(y_te, proba)


def run_cv(df, feature_cols, params):
    fold_accs, fold_lls = [], []
    for fold in FOLDS:
        train_df = apply_cold_start_filter(df[df["season"].isin(fold["train"])])
        test_df = df[df["season"].isin(fold["test"])]
        X_tr = train_df[feature_cols].values
        y_tr = train_df["did_home_win"].values
        X_te = test_df[feature_cols].values
        y_te = test_df["did_home_win"].values
        _, acc, ll = train_lgbm(X_tr, y_tr, X_te, y_te, params)
        fold_accs.append(acc)
        fold_lls.append(ll)
        print(f"      fold test={fold['test']}: acc={acc:.4f} ll={ll:.4f}")
    return float(np.mean(fold_accs)), float(np.mean(fold_lls))


def run_holdout(df, feature_cols, params):
    train_df = apply_cold_start_filter(df[df["season"].isin(DEV_SEASONS)])
    test_df = df[df["season"] == HOLDOUT_SEASON]
    X_tr = train_df[feature_cols].values
    y_tr = train_df["did_home_win"].values
    X_te = test_df[feature_cols].values
    y_te = test_df["did_home_win"].values
    print(f"  Holdout: train={len(train_df)} rows, test={len(test_df)} rows")
    model, acc, ll = train_lgbm(X_tr, y_tr, X_te, y_te, params)
    return model, acc, ll, X_te, y_te


def main():
    print("=" * 70)
    print("  tune_lgbm.py — Part A: LightGBM Hyperparameter Grid Search")
    print("=" * 70)

    df = load_data()
    print(f"Loaded {len(df)} games, seasons {sorted(df['season'].unique())}")
    feature_cols = resolve_feature_cols(df.columns.tolist())
    print(f"Features: {len(feature_cols)}")

    grid_results = []
    for i, params in enumerate(PARAM_GRID):
        print(f"\n--- Grid combo {i+1}/{len(PARAM_GRID)}: {params} ---")
        avg_acc, avg_ll = run_cv(df, feature_cols, params)
        print(f"  CV avg: acc={avg_acc:.4f} ({avg_acc*100:.2f}%)  ll={avg_ll:.4f}")
        grid_results.append({"params": params, "cv_acc": avg_acc, "cv_ll": avg_ll})

    # Pick best by CV accuracy
    best = max(grid_results, key=lambda x: x["cv_acc"])
    best_idx = grid_results.index(best)
    print(f"\n{'='*70}")
    print(f"Best CV params (combo {best_idx+1}): cv_acc={best['cv_acc']*100:.2f}%  cv_ll={best['cv_ll']:.4f}")
    print(f"  Params: {best['params']}")

    print(f"\n--- Running holdout with best params ---")
    model, h_acc, h_ll, X_te, y_te = run_holdout(df, feature_cols, best["params"])
    print(f"  Holdout: acc={h_acc:.4f} ({h_acc*100:.2f}%)  ll={h_ll:.4f}")

    # Save artifact
    run_id = f"gridsearch_lgbm_v4__{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    run_dir = REGISTRY_PATH / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    metrics = {
        "run_id": run_id,
        "grid_results": [
            {"combo": i+1, "params": r["params"], "cv_acc": r["cv_acc"], "cv_ll": r["cv_ll"]}
            for i, r in enumerate(grid_results)
        ],
        "best_combo": best_idx + 1,
        "best_params": best["params"],
        "best_cv_acc": best["cv_acc"],
        "best_cv_ll": best["cv_ll"],
        "holdout_acc": float(h_acc),
        "holdout_ll": float(h_ll),
        "feature_cols": feature_cols,
        "n_features": len(feature_cols),
    }
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

    # Save model pickle
    with open(run_dir / "model.pkl", "wb") as f:
        pickle.dump({"model": model, "feature_cols": feature_cols}, f)

    # Save "latest" pointer
    latest_path = REGISTRY_PATH / "gridsearch_lgbm_v4__latest.json"
    latest_data = {"run_id": run_id, "run_dir": str(run_dir), "holdout_acc": float(h_acc), "best_params": best["params"]}
    latest_path.write_text(json.dumps(latest_data, indent=2))

    print(f"\nSaved to {run_dir}")
    print(f"\n{'='*70}")
    print(f"  GRID SEARCH SUMMARY")
    print(f"{'='*70}")
    for i, r in enumerate(grid_results):
        marker = " ← BEST" if i == best_idx else ""
        print(f"  Combo {i+1}: cv={r['cv_acc']*100:.2f}%  ll={r['cv_ll']:.4f}  {r['params']}{marker}")
    print(f"\n  Best holdout:  {h_acc*100:.2f}%")
    print(f"  vs v4 tuned:   57.37%  (delta: {(h_acc - 0.5737)*100:+.2f}pp)")


if __name__ == "__main__":
    main()
