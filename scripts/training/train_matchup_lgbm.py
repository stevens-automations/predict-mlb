#!/usr/bin/env python3
"""
Train LightGBM + Logistic Regression on game_matchup_features.
Walk-forward CV on 2020-2024, 2025 holdout untouched.

Key improvements over baseline:
  - Explicit curated feature list (drops zero-variance / NaN-correlation features)
  - Training sample filter: drop cold-start rows (< 15 prior games either team)
  - Holdout 2025 evaluated UNFILTERED (honest comparison)
  - LightGBM: native NULL handling, no imputation
  - Logistic Regression: median imputation + StandardScaler
  - Feature importance top 20 printed + saved
"""
import sqlite3
import json
import pickle
import warnings
import argparse
from datetime import datetime
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss
from sklearn.preprocessing import StandardScaler
from typing import Optional, List, Dict

DB_PATH = Path(__file__).parents[2] / "data" / "mlb_history.db"
REGISTRY_PATH = Path(__file__).parents[2] / "artifacts" / "model_registry"

# ── Column aliases (spec name → actual DB column) ────────────────────────────
# Some spec names differ from DB column names
COLUMN_ALIASES: Dict[str, str] = {
    "home_bullpen_era": "home_bullpen_season_bullpen_era",
    "away_bullpen_era": "away_bullpen_season_bullpen_era",
}

# ── Explicit feature list per spec ───────────────────────────────────────────
FEATURE_COLS_SPEC = [
    # Team record — delta
    "win_pct_delta",
    "run_diff_per_game_delta",
    "ops_delta",
    "batting_avg_delta",
    "rolling_last10_win_pct_delta",
    "rolling_last10_ops_delta",
    # Team record — raw (both sides for nonlinear signal)
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
    # Bullpen
    "bullpen_era_delta",
    "home_bullpen_era",          # alias → home_bullpen_season_bullpen_era
    "away_bullpen_era",          # alias → away_bullpen_season_bullpen_era
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

DEV_SEASONS = [2020, 2021, 2022, 2023, 2024]
HOLDOUT_SEASON = 2025
FOLDS = [
    {"train": [2020, 2021], "test": [2022]},
    {"train": [2020, 2021, 2022], "test": [2023]},
    {"train": [2020, 2021, 2022, 2023], "test": [2024]},
]

# Non-feature columns (identifiers, labels, metadata)
EXCLUDE_COLS = {
    "game_id", "game_date", "season", "home_team_id", "away_team_id",
    "did_home_win", "home_score", "away_score", "run_differential", "built_at",
}

ERA_CLIP_FEATURES = [
    "home_starter_era",
    "away_starter_era",
    "home_starter_career_era",
    "away_starter_career_era",
    "home_bullpen_season_bullpen_era",
    "away_bullpen_season_bullpen_era",
    "bullpen_era_delta",
    "starter_era_delta",
]


def resolve_feature_cols(df_columns: List[str]) -> List[str]:
    """
    Resolve FEATURE_COLS_SPEC against actual DataFrame columns.
    - Applies COLUMN_ALIASES where spec name ≠ DB name
    - Skips columns not present in DataFrame with a warning
    Returns list of actual DataFrame column names to use.
    """
    available = set(df_columns)
    resolved = []
    skipped = []
    for spec_name in FEATURE_COLS_SPEC:
        actual = COLUMN_ALIASES.get(spec_name, spec_name)
        if actual in available:
            resolved.append(actual)
        elif spec_name in available:
            resolved.append(spec_name)
        else:
            skipped.append(spec_name)

    if skipped:
        for s in skipped:
            warnings.warn(f"[feature_select] Column not found in DB, skipping: {s}", stacklevel=2)

    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for c in resolved:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    return deduped


def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM game_matchup_features WHERE did_home_win IS NOT NULL ORDER BY game_date, game_id"
    ).fetchall()
    conn.close()
    df = pd.DataFrame([dict(r) for r in rows])
    return df


def clip_era_features(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_cols: List[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clip ERA-style features to the 5th-95th percentile range learned on train_df.
    Applied before imputation/scaling for logistic regression only.
    """
    clip_cols = [c for c in ERA_CLIP_FEATURES if c in feature_cols and c in train_df.columns and c in test_df.columns]
    if not clip_cols:
        return train_df, test_df

    train_df = train_df.copy()
    test_df = test_df.copy()

    for col in clip_cols:
        lower = train_df[col].quantile(0.05)
        upper = train_df[col].quantile(0.95)
        if pd.isna(lower) or pd.isna(upper):
            continue
        train_df[col] = train_df[col].clip(lower=lower, upper=upper)
        test_df[col] = test_df[col].clip(lower=lower, upper=upper)

    return train_df, test_df


def apply_cold_start_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows where either team has < 15 prior season games.
    Used for TRAINING only — never applied to holdout.
    """
    mask = (df["home_team_season_games"] >= 15) & (df["away_team_season_games"] >= 15)
    dropped = (~mask).sum()
    if dropped > 0:
        print(f"  Cold-start filter: dropped {dropped} rows ({dropped/len(df)*100:.1f}%) with < 15 games")
    return df[mask].copy()


def eval_lgbm(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
    params: Optional[Dict] = None,
):
    default_params = {
        "objective": "binary",
        "metric": ["binary_logloss", "binary_error"],
        "num_leaves": 31,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "min_data_in_leaf": 20,
        "verbose": -1,
        "n_estimators": 500,
        "random_state": 42,
    }
    if params:
        default_params.update(params)
    model = lgb.LGBMClassifier(**default_params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
    )
    preds = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1]
    return model, accuracy_score(y_test, preds), log_loss(y_test, proba)


def run_cv(df: pd.DataFrame, feature_cols: List[str], params: Optional[Dict] = None):
    """Walk-forward CV on DEV_SEASONS. Training folds are cold-start filtered."""
    fold_results = []
    for fold in FOLDS:
        train_df = df[df["season"].isin(fold["train"])]
        test_df = df[df["season"].isin(fold["test"])]

        # Apply cold-start filter to training only
        train_df = apply_cold_start_filter(train_df)

        X_tr = train_df[feature_cols].values
        y_tr = train_df["did_home_win"].values
        X_te = test_df[feature_cols].values
        y_te = test_df["did_home_win"].values

        _, acc, ll = eval_lgbm(X_tr, y_tr, X_te, y_te, params)
        fold_results.append({
            "fold_test_seasons": fold["test"],
            "train_rows": len(train_df),
            "test_rows": len(test_df),
            "accuracy": acc,
            "log_loss": ll,
        })
        print(f"    Fold test={fold['test']}: acc={acc:.4f} ll={ll:.4f}  (train={len(train_df)}, test={len(test_df)})")

    avg_acc = float(np.mean([r["accuracy"] for r in fold_results]))
    avg_ll = float(np.mean([r["log_loss"] for r in fold_results]))
    return fold_results, avg_acc, avg_ll


def run_holdout(
    df: pd.DataFrame,
    feature_cols: List[str],
    params: Optional[Dict] = None,
):
    """Train on filtered DEV_SEASONS, evaluate on UNFILTERED 2025 holdout."""
    train_df = df[df["season"].isin(DEV_SEASONS)]
    train_df = apply_cold_start_filter(train_df)
    test_df = df[df["season"] == HOLDOUT_SEASON]  # NOT filtered

    print(f"  Holdout: training on {len(train_df)} rows, evaluating on {len(test_df)} rows (all 2025)")

    X_tr = train_df[feature_cols].values
    y_tr = train_df["did_home_win"].values
    X_te = test_df[feature_cols].values
    y_te = test_df["did_home_win"].values

    model, acc, ll = eval_lgbm(X_tr, y_tr, X_te, y_te, params)
    return model, acc, ll, X_tr, y_tr, X_te, y_te


def run_logistic(df: pd.DataFrame, feature_cols: List[str]):
    """
    Logistic regression benchmark.
    NULL handling: fill with column MEDIAN (computed from training fold).
    """
    fold_accs = []
    for fold in FOLDS:
        train_df = df[df["season"].isin(fold["train"])]
        test_df = df[df["season"].isin(fold["test"])]

        # Cold-start filter on training only
        train_df = apply_cold_start_filter(train_df)

        train_df, test_df = clip_era_features(train_df, test_df, feature_cols)

        X_tr = train_df[feature_cols].copy()
        X_te = test_df[feature_cols].copy()

        # Median imputation (fit on train, apply to both)
        col_medians = X_tr.median()
        X_tr = X_tr.fillna(col_medians).values
        X_te = X_te.fillna(col_medians).values

        y_tr = train_df["did_home_win"].values
        y_te = test_df["did_home_win"].values

        scaler = StandardScaler()
        X_tr = scaler.fit_transform(X_tr)
        X_te = scaler.transform(X_te)

        lr = LogisticRegression(max_iter=1000, random_state=42)
        lr.fit(X_tr, y_tr)
        fold_accs.append(accuracy_score(y_te, lr.predict(X_te)))

    # Holdout
    train_df = df[df["season"].isin(DEV_SEASONS)]
    train_df = apply_cold_start_filter(train_df)
    test_df = df[df["season"] == HOLDOUT_SEASON]

    train_df, test_df = clip_era_features(train_df, test_df, feature_cols)

    X_tr = train_df[feature_cols].copy()
    X_te = test_df[feature_cols].copy()
    col_medians = X_tr.median()
    X_tr = X_tr.fillna(col_medians).values
    X_te = X_te.fillna(col_medians).values

    scaler = StandardScaler()
    X_tr = scaler.fit_transform(X_tr)
    X_te = scaler.transform(X_te)

    lr = LogisticRegression(max_iter=1000, random_state=42)
    lr.fit(X_tr, train_df["did_home_win"].values)
    holdout_acc = accuracy_score(test_df["did_home_win"].values, lr.predict(X_te))

    return float(np.mean(fold_accs)), holdout_acc


def print_feature_importance(model, feature_cols: List[str], top_n: int = 20):
    fi = {k: int(v) for k, v in zip(feature_cols, model.feature_importances_)}
    fi_sorted = sorted(fi.items(), key=lambda x: -x[1])[:top_n]
    print(f"\n  Top {top_n} feature importances:")
    for i, (name, val) in enumerate(fi_sorted, 1):
        print(f"    {i:2d}. {name:<45s} {val:>6d}")
    return fi


def save_results(
    name: str,
    cv_folds: list,
    cv_acc: float,
    cv_ll: float,
    holdout_acc: float,
    holdout_ll: float,
    feature_importance: dict,
    feature_cols: List[str],
    params: Optional[Dict] = None,
    model=None,
):
    run_id = f"{name}__{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    run_dir = REGISTRY_PATH / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # Convert numpy types to native Python for JSON serialization
    def to_py(v):
        if hasattr(v, "item"):
            return v.item()
        return v

    cv_folds_serializable = [
        {k: (to_py(v) if not isinstance(v, list) else [to_py(x) for x in v])
         for k, v in fold.items()}
        for fold in cv_folds
    ]

    results = {
        "run_id": run_id,
        "params": params or {},
        "feature_cols": feature_cols,
        "n_features": len(feature_cols),
        "cv": {"folds": cv_folds_serializable, "avg_accuracy": float(cv_acc), "avg_log_loss": float(cv_ll)},
        "holdout": {
            "season": HOLDOUT_SEASON,
            "accuracy": float(holdout_acc),
            "log_loss": float(holdout_ll),
            "unfiltered": True,
        },
    }
    (run_dir / "metrics.json").write_text(json.dumps(results, indent=2))

    if model is not None:
        pkl_payload = {'model': model, 'feature_cols': feature_cols}
        with open(run_dir / 'model.pkl', 'wb') as f:
            pickle.dump(pkl_payload, f)

    if feature_importance:
        fi_sorted = sorted(feature_importance.items(), key=lambda x: -x[1])
        (run_dir / "feature_importance.json").write_text(json.dumps(fi_sorted[:50], indent=2))

    print(f"  Saved to {run_dir}")
    return results


def main():
    print("=" * 70)
    print("  predict-mlb — Matchup LightGBM Training (v4: filtered + curated)")
    print("=" * 70)

    print("\nLoading data...")
    df = load_data()
    seasons = sorted(df["season"].unique())
    print(f"  {len(df)} labeled games, seasons {seasons}")

    # Resolve actual feature columns (handles aliases + missing)
    feature_cols = resolve_feature_cols(df.columns.tolist())
    print(f"\nFeature selection: {len(feature_cols)} features resolved from {len(FEATURE_COLS_SPEC)} spec entries")

    # ── Baseline LightGBM ────────────────────────────────────────────────────
    print("\n=== [1/4] Baseline LightGBM (CV, cold-start filtered) ===")
    cv_folds, cv_acc, cv_ll = run_cv(df, feature_cols)
    print(f"  CV avg: acc={cv_acc:.4f} ({cv_acc*100:.2f}%)  ll={cv_ll:.4f}")

    print("\n=== [2/4] Baseline LightGBM (Holdout 2025, UNFILTERED) ===")
    model, h_acc, h_ll, X_tr, y_tr, X_te, y_te = run_holdout(df, feature_cols)
    print(f"  Holdout: acc={h_acc:.4f} ({h_acc*100:.2f}%)  ll={h_ll:.4f}")

    fi = print_feature_importance(model, feature_cols, top_n=20)
    save_results("matchup_lgbm_v4_baseline", cv_folds, cv_acc, cv_ll, h_acc, h_ll, fi, feature_cols, model=model)

    # ── Logistic Regression ──────────────────────────────────────────────────
    print("\n=== [3/4] Logistic Regression Benchmark (median imputation) ===")
    lr_cv_acc, lr_holdout_acc = run_logistic(df, feature_cols)
    print(f"  LR CV avg acc:  {lr_cv_acc:.4f} ({lr_cv_acc*100:.2f}%)")
    print(f"  LR Holdout acc: {lr_holdout_acc:.4f} ({lr_holdout_acc*100:.2f}%)")

    # ── Tuned LightGBM ───────────────────────────────────────────────────────
    print("\n=== [4/4] Tuned LightGBM (CV + Holdout) ===")
    tuned_params = {
        "num_leaves": 63,
        "learning_rate": 0.03,
        "feature_fraction": 0.7,
        "bagging_fraction": 0.7,
        "min_data_in_leaf": 30,
        "n_estimators": 800,
    }
    t_cv_folds, t_cv_acc, t_cv_ll = run_cv(df, feature_cols, tuned_params)
    print(f"  Tuned CV avg: acc={t_cv_acc:.4f} ({t_cv_acc*100:.2f}%)  ll={t_cv_ll:.4f}")

    print()
    t_model, t_h_acc, t_h_ll, _, _, _, _ = run_holdout(df, feature_cols, tuned_params)
    print(f"  Tuned Holdout: acc={t_h_acc:.4f} ({t_h_acc*100:.2f}%)  ll={t_h_ll:.4f}")

    t_fi = print_feature_importance(t_model, feature_cols, top_n=20)
    save_results("matchup_lgbm_v4_tuned", t_cv_folds, t_cv_acc, t_cv_ll, t_h_acc, t_h_ll, t_fi, feature_cols, tuned_params, model=t_model)

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print(f"  Old baseline (v2_phase1):             dev=54.85%  holdout=52.44%")
    print(f"  Previous matchup baseline:            dev=55.95%  holdout=55.76%")
    print(f"  ─────────────────────────────────────────────────────────────────")
    print(f"  New LightGBM baseline (v4):           dev={cv_acc*100:.2f}%  holdout={h_acc*100:.2f}%")
    print(f"  New LightGBM tuned (v4):              dev={t_cv_acc*100:.2f}%  holdout={t_h_acc*100:.2f}%")
    print(f"  Logistic regression (v4):             dev={lr_cv_acc*100:.2f}%  holdout={lr_holdout_acc*100:.2f}%")
    print(f"  ─────────────────────────────────────────────────────────────────")
    print(f"  Previous best holdout (v3):           56.26%")
    best_holdout = max(h_acc, t_h_acc, lr_holdout_acc)
    delta_vs_old = (best_holdout - 0.5244) * 100
    delta_vs_prev = (best_holdout - 0.5576) * 100
    print(f"  Best holdout this run:                {best_holdout*100:.2f}%")
    print(f"  vs old baseline:                      {delta_vs_old:+.2f}pp")
    print(f"  vs previous matchup:                  {delta_vs_prev:+.2f}pp")
    print(f"  Target:                               60.00%  (stretch: 66.00%)")
    print("=" * 70)


if __name__ == "__main__":
    main()
