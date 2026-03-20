#!/usr/bin/env python3
"""
Train the final tuned LightGBM matchup model on all dev seasons and save it.
"""
import json
import pickle
import sqlite3
from datetime import datetime
from pathlib import Path

from train_matchup_lgbm import (
    apply_cold_start_filter,
    eval_lgbm,
    load_data,
    print_feature_importance,
    resolve_feature_cols,
)

ROOT = Path(__file__).parents[2]
DB_PATH = ROOT / "data" / "mlb_history.db"
REGISTRY_PATH = ROOT / "artifacts" / "model_registry"

DEV_SEASONS = [2020, 2021, 2022, 2023, 2024]
HOLDOUT_SEASON = 2025
TUNED_PARAMS = {
    "num_leaves": 63,
    "learning_rate": 0.03,
    "feature_fraction": 0.7,
    "bagging_fraction": 0.7,
    "min_data_in_leaf": 30,
    "n_estimators": 800,
}


def main() -> None:
    print("=" * 70)
    print("  retrain_v4_tuned_final.py")
    print("=" * 70)
    print(f"DB path: {DB_PATH}")

    # Keep a direct sqlite dependency in this script and fail fast if the DB is absent.
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("SELECT 1")

    df = load_data()
    feature_cols = resolve_feature_cols(df.columns.tolist())

    train_df = apply_cold_start_filter(df[df["season"].isin(DEV_SEASONS)])
    holdout_df = df[df["season"] == HOLDOUT_SEASON].copy()

    X_train = train_df[feature_cols].values
    y_train = train_df["did_home_win"].values
    X_holdout = holdout_df[feature_cols].values
    y_holdout = holdout_df["did_home_win"].values

    print(f"Train rows: {len(train_df)}")
    print(f"Holdout rows: {len(holdout_df)}")
    print(f"Features: {len(feature_cols)}")

    model, holdout_acc, holdout_ll = eval_lgbm(
        X_train,
        y_train,
        X_holdout,
        y_holdout,
        TUNED_PARAMS,
    )
    feature_importance = print_feature_importance(model, feature_cols, top_n=20)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_id = f"matchup_lgbm_v4_tuned_final__{timestamp}"
    run_dir = REGISTRY_PATH / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    with open(run_dir / "model.pkl", "wb") as f:
        pickle.dump({"model": model, "feature_cols": feature_cols}, f)

    metrics = {
        "run_id": run_id,
        "params": TUNED_PARAMS,
        "holdout_accuracy": float(holdout_acc),
        "holdout_log_loss": float(holdout_ll),
        "feature_cols": feature_cols,
    }
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

    top_50 = sorted(feature_importance.items(), key=lambda item: -item[1])[:50]
    (run_dir / "feature_importance.json").write_text(json.dumps(top_50, indent=2))

    print(f"\nSaved to {run_dir}")
    print(f"run_id={run_id}")
    print(f"holdout_accuracy={holdout_acc:.6f}")
    print(f"holdout_log_loss={holdout_ll:.6f}")


if __name__ == "__main__":
    main()
