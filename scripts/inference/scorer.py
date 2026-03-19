#!/usr/bin/env python3
"""
Model scorer for predict-mlb.

Function: score_game(feature_dict) -> dict

Loads the trained model from the latest model_registry directory,
converts a feature dict to the correct input format, and returns
win probabilities + confidence tier.

Returns:
    {
        "home_win_prob": float,
        "away_win_prob": float,
        "predicted_winner": "home" | "away",
        "confidence_tier": "high" | "medium" | "low"
    }

Confidence tiers:
    high   >= 0.65
    medium  0.60 - 0.65
    low    < 0.60
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = ROOT / "artifacts" / "model_registry"

# ── Model loading ─────────────────────────────────────────────────────────────

_cached_model = None
_cached_feature_cols = None
_cached_model_path: Optional[Path] = None


def _find_latest_model() -> Path:
    """
    Find the latest model.pkl in model_registry.

    Priority order (highest first):
    1. matchup_lgbm_v4_tuned__* dirs (with model.pkl)
    2. gridsearch_lgbm_v4__* dirs (with model.pkl) — these are LGBMClassifier
    3. matchup_lgbm_* dirs (with model.pkl)
    4. Any dir with model.pkl that contains a classifier (has predict_proba)

    Sorted by directory name descending (timestamp in name → latest wins).
    """
    import pickle as _pickle
    import warnings as _warnings

    def _is_classifier(pkl_path: Path) -> bool:
        try:
            with open(pkl_path, "rb") as f:
                with _warnings.catch_warnings():
                    _warnings.simplefilter("ignore")
                    payload = _pickle.load(f)
            model = payload["model"] if isinstance(payload, dict) else payload
            return hasattr(model, "predict_proba")
        except Exception:
            return False

    prefixes_in_priority = [
        "matchup_lgbm_v4_tuned__",
        "gridsearch_lgbm_v4__",
        "matchup_lgbm_",
    ]

    for prefix in prefixes_in_priority:
        dirs = sorted(
            [d for d in REGISTRY_PATH.iterdir()
             if d.is_dir() and d.name.startswith(prefix) and (d / "model.pkl").exists()],
            key=lambda d: d.name,
            reverse=True,
        )
        if dirs:
            return dirs[0] / "model.pkl"

    # Final fallback: any dir with model.pkl that is a classifier
    all_dirs = sorted(
        [d for d in REGISTRY_PATH.iterdir()
         if d.is_dir() and (d / "model.pkl").exists()],
        key=lambda d: d.name,
        reverse=True,
    )
    for d in all_dirs:
        pkl = d / "model.pkl"
        if _is_classifier(pkl):
            return pkl

    raise FileNotFoundError(
        f"No classifier model.pkl found in any subdirectory of {REGISTRY_PATH}"
    )


def _load_model(model_path: Optional[Path] = None):
    """Load model + feature_cols from pickle. Returns (model, feature_cols)."""
    global _cached_model, _cached_feature_cols, _cached_model_path

    if model_path is None:
        model_path = _find_latest_model()

    # Return cached if path hasn't changed
    if _cached_model is not None and model_path == _cached_model_path:
        return _cached_model, _cached_feature_cols

    with open(model_path, "rb") as f:
        payload = pickle.load(f)

    # Payload may be a dict {"model": ..., "feature_cols": [...]} or a raw model
    if isinstance(payload, dict):
        model = payload["model"]
        feature_cols = payload.get("feature_cols")
    else:
        model = payload
        feature_cols = None

    # If feature_cols not in pkl, try metrics.json in same directory
    if feature_cols is None:
        metrics_path = model_path.parent / "metrics.json"
        if metrics_path.exists():
            import json
            with open(metrics_path) as mf:
                metrics = json.load(mf)
            feature_cols = metrics.get("feature_cols")

    if feature_cols is None:
        raise ValueError(
            f"Could not determine feature_cols from {model_path} or its metrics.json"
        )

    _cached_model = model
    _cached_feature_cols = feature_cols
    _cached_model_path = model_path
    return model, feature_cols


def _confidence_tier(prob: float) -> str:
    if prob >= 0.65:
        return "high"
    elif prob >= 0.60:
        return "medium"
    else:
        return "low"


def score_game(
    feature_dict: dict,
    model_path: Optional[Path] = None,
) -> dict:
    """
    Score a single game given a feature dict from feature_builder.build_feature_row().

    Args:
        feature_dict: Dict with feature values keyed by column name.
                      Metadata keys (game_id, home_team_id, away_team_id, cold_start)
                      are ignored during scoring.
        model_path: Optional explicit path to model.pkl. If None, uses latest.

    Returns:
        {
            "home_win_prob": float,
            "away_win_prob": float,
            "predicted_winner": "home" | "away",
            "confidence_tier": "high" | "medium" | "low"
        }
    """
    model, feature_cols = _load_model(model_path)

    import numpy as np

    # Build DataFrame row with correct column order
    # Convert None → np.nan so pandas uses float dtype (LightGBM requires numeric)
    row = {
        col: (feature_dict.get(col) if feature_dict.get(col) is not None else np.nan)
        for col in feature_cols
    }
    X = pd.DataFrame([row], columns=feature_cols).astype(float, errors="ignore")

    # Predict probability of home win (class 1)
    proba = model.predict_proba(X)[0]  # [P(away_win), P(home_win)]
    home_win_prob = float(proba[1])
    away_win_prob = float(proba[0])

    # Winner is whichever team has higher probability
    if home_win_prob >= away_win_prob:
        predicted_winner = "home"
        confidence_prob = home_win_prob
    else:
        predicted_winner = "away"
        confidence_prob = away_win_prob

    return {
        "home_win_prob": round(home_win_prob, 4),
        "away_win_prob": round(away_win_prob, 4),
        "predicted_winner": predicted_winner,
        "confidence_tier": _confidence_tier(confidence_prob),
    }


if __name__ == "__main__":
    import sys
    import json
    import sqlite3
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parents[2]))
    from scripts.inference.feature_builder import build_feature_row, FEATURE_COLS

    db_path = Path(__file__).parents[2] / "data" / "mlb_history.db"
    game_id = int(sys.argv[1]) if len(sys.argv) > 1 else 744825

    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        features = build_feature_row(game_id, conn)
        result = score_game(features)
        print(f"game_id={game_id}")
        print(json.dumps(result, indent=2))
        print(f"cold_start={features.get('cold_start')}")
    finally:
        conn.close()
