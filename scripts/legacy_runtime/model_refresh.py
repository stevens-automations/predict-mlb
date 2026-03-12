#!/usr/bin/env python3
"""Preseason model refresh scaffold for predict-mlb.

Evaluates two LightGBM training configurations with walk-forward validation
on existing historical game-level data (xlsx/csv/parquet).
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import lightgbm as lgb
import numpy as np
import pandas as pd

LABEL_COL = "did-home-win"
DATE_COL = "date"

NON_FEATURE_COLS = {
    "game-id",
    "home-team",
    "away-team",
    LABEL_COL,
    DATE_COL,
}

ORDER2_FEATURES = [
    "home-win-percentage",
    "home-starter-season-era",
    "home-starter-season-win-percentage",
    "home-top5-hr-avg",
    "home-last10-avg-runs",
    "home-last10-avg-ops",
    "home-starter-season-whip",
    "home-top5-rbi-avg",
    "home-last10-avg-runs-allowed",
    "home-starter-season-avg",
    "home-top5-batting-avg",
    "home-starter-season-strike-percentage",
    "home-last10-avg-hits",
    "home-last10-avg-hits-allowed",
    "home-last10-avg-obp",
    "home-last10-avg-avg",
    "home-last10-avg-rbi",
    "home-starter-season-runs-per9",
    "home-top5-stolenBases-avg",
    "home-top5-totalBases-avg",
    "home-last10-avg-strikeouts",
    "home-starter-career-era",
    "away-win-percentage",
    "away-starter-season-era",
    "away-starter-season-win-percentage",
    "away-top5-hr-avg",
    "away-last10-avg-runs",
    "away-last10-avg-ops",
    "away-starter-season-whip",
    "away-top5-rbi-avg",
    "away-last10-avg-runs-allowed",
    "away-starter-season-avg",
    "away-top5-batting-avg",
    "away-starter-season-strike-percentage",
    "away-last10-avg-hits",
    "away-last10-avg-hits-allowed",
    "away-last10-avg-obp",
    "away-last10-avg-avg",
    "away-last10-avg-rbi",
    "away-starter-season-runs-per9",
    "away-top5-stolenBases-avg",
    "away-top5-totalBases-avg",
    "away-last10-avg-strikeouts",
    "away-starter-career-era",
]


@dataclass
class FoldMetric:
    fold: int
    n_train: int
    n_test: int
    accuracy: float
    logloss: float
    brier: float


@dataclass
class ModelResult:
    name: str
    train_seconds: float
    fold_metrics: List[FoldMetric]

    @property
    def avg_accuracy(self) -> float:
        return float(np.mean([f.accuracy for f in self.fold_metrics]))

    @property
    def avg_logloss(self) -> float:
        return float(np.mean([f.logloss for f in self.fold_metrics]))

    @property
    def avg_brier(self) -> float:
        return float(np.mean([f.brier for f in self.fold_metrics]))


def _load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input dataset not found: {path}")
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported input type: {suffix}")


def _pick_features(df: pd.DataFrame) -> List[str]:
    ordered = [c for c in ORDER2_FEATURES if c in df.columns]
    if len(ordered) >= 20:
        return ordered
    numeric_candidates = [
        c for c in df.columns if c not in NON_FEATURE_COLS and pd.api.types.is_numeric_dtype(df[c])
    ]
    if not numeric_candidates:
        raise ValueError("No numeric feature columns found in dataset")
    return numeric_candidates


def _coerce_xy(df: pd.DataFrame, features: Sequence[str]) -> Tuple[pd.DataFrame, np.ndarray]:
    if LABEL_COL not in df.columns:
        raise ValueError(f"Missing label column: {LABEL_COL}")
    y = pd.to_numeric(df[LABEL_COL], errors="coerce")
    keep = y.notna()
    y = y[keep].astype(float).to_numpy()

    X = df.loc[keep, list(features)].copy()
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")
    return X, y


def _time_splits(dates: pd.Series, n_splits: int = 3) -> List[Tuple[np.ndarray, np.ndarray]]:
    # expanding-window train with equally-sized date buckets for test blocks
    ordinal = pd.to_datetime(dates, errors="coerce")
    if ordinal.isna().all():
        idx = np.arange(len(dates))
        chunks = np.array_split(idx, n_splits + 1)
    else:
        ranked = ordinal.rank(method="first").to_numpy()
        idx = np.argsort(ranked)
        chunks = np.array_split(idx, n_splits + 1)

    splits: List[Tuple[np.ndarray, np.ndarray]] = []
    for i in range(1, len(chunks)):
        train_idx = np.concatenate(chunks[:i])
        test_idx = chunks[i]
        if len(train_idx) == 0 or len(test_idx) == 0:
            continue
        splits.append((train_idx, test_idx))
    return splits


def _logloss(y_true: np.ndarray, p: np.ndarray) -> float:
    eps = 1e-15
    p = np.clip(p, eps, 1 - eps)
    return float(-np.mean(y_true * np.log(p) + (1 - y_true) * np.log(1 - p)))


def _brier(y_true: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y_true) ** 2))


def _accuracy(y_true: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p >= 0.5) == (y_true >= 0.5)))


def _fit_eval(
    name: str,
    params: Dict[str, object],
    X: pd.DataFrame,
    y: np.ndarray,
    splits: List[Tuple[np.ndarray, np.ndarray]],
) -> ModelResult:
    start = time.perf_counter()
    fold_metrics: List[FoldMetric] = []

    for i, (tr, te) in enumerate(splits, start=1):
        dtrain = lgb.Dataset(X.iloc[tr], label=y[tr], free_raw_data=True)
        model = lgb.train(params=params, train_set=dtrain, num_boost_round=int(params.get("num_boost_round", 300)))
        p = model.predict(X.iloc[te])

        fold_metrics.append(
            FoldMetric(
                fold=i,
                n_train=len(tr),
                n_test=len(te),
                accuracy=_accuracy(y[te], p),
                logloss=_logloss(y[te], p),
                brier=_brier(y[te], p),
            )
        )

    elapsed = time.perf_counter() - start
    return ModelResult(name=name, train_seconds=elapsed, fold_metrics=fold_metrics)


def run(input_path: Path, output_path: Path, n_splits: int = 3) -> Dict[str, object]:
    df = _load_df(input_path)
    features = _pick_features(df)
    X, y = _coerce_xy(df, features)

    if DATE_COL in df.columns:
        dates = df.loc[X.index, DATE_COL]
    else:
        dates = pd.Series(np.arange(len(X)), index=X.index)

    splits = _time_splits(dates=dates, n_splits=n_splits)
    if len(splits) < 2:
        raise ValueError("Not enough data for walk-forward validation")

    baseline_params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "learning_rate": 0.08,
        "num_leaves": 31,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "min_data_in_leaf": 40,
        "verbose": -1,
        "num_boost_round": 250,
    }

    stronger_params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "learning_rate": 0.03,
        "num_leaves": 63,
        "feature_fraction": 0.85,
        "bagging_fraction": 0.85,
        "bagging_freq": 1,
        "min_data_in_leaf": 25,
        "lambda_l1": 0.5,
        "lambda_l2": 0.5,
        "verbose": -1,
        "num_boost_round": 500,
    }

    baseline = _fit_eval("lgbm_baseline", baseline_params, X, y, splits)
    stronger = _fit_eval("lgbm_stronger", stronger_params, X, y, splits)

    summary = {
        "input_path": str(input_path),
        "n_rows": int(len(X)),
        "n_features": int(len(features)),
        "n_splits": len(splits),
        "features_used": features,
        "models": [
            {
                "name": baseline.name,
                "train_seconds": baseline.train_seconds,
                "avg_accuracy": baseline.avg_accuracy,
                "avg_logloss": baseline.avg_logloss,
                "avg_brier": baseline.avg_brier,
                "fold_metrics": [asdict(m) for m in baseline.fold_metrics],
            },
            {
                "name": stronger.name,
                "train_seconds": stronger.train_seconds,
                "avg_accuracy": stronger.avg_accuracy,
                "avg_logloss": stronger.avg_logloss,
                "avg_brier": stronger.avg_brier,
                "fold_metrics": [asdict(m) for m in stronger.fold_metrics],
            },
        ],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run preseason model refresh backtest")
    parser.add_argument("--input", required=True, help="Path to historical training data (xlsx/csv/parquet)")
    parser.add_argument(
        "--output",
        default="docs/reports/model-refresh-latest.json",
        help="Path to write evaluation JSON report",
    )
    parser.add_argument("--splits", type=int, default=3, help="Number of walk-forward test splits")
    args = parser.parse_args()

    summary = run(Path(args.input), Path(args.output), n_splits=args.splits)
    print(json.dumps({
        "report": args.output,
        "n_rows": summary["n_rows"],
        "n_features": summary["n_features"],
        "models": [{
            "name": m["name"],
            "avg_accuracy": round(m["avg_accuracy"], 4),
            "avg_logloss": round(m["avg_logloss"], 4),
            "avg_brier": round(m["avg_brier"], 4),
        } for m in summary["models"]]
    }, indent=2))


if __name__ == "__main__":
    main()
