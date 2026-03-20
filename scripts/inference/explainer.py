#!/usr/bin/env python3
"""
SHAP-based prediction explainer for predict-mlb.

Function: explain_prediction(feature_dict, model_bundle) -> list[dict]

Returns top-5 human-readable SHAP reasons for a prediction.
"""
from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd

FEATURE_LABELS = {
    "away_starter_career_era": "away starter career ERA",
    "home_starter_career_era": "home starter career ERA",
    "starter_era_delta": "starter ERA edge",
    "starter_k_pct_delta": "strikeout rate edge",
    "starter_whip_delta": "WHIP edge",
    "run_diff_per_game_delta": "run differential edge",
    "bullpen_era_delta": "bullpen ERA edge",
    "win_pct_delta": "season win% edge",
    "rolling_last10_win_pct_delta": "recent form edge (last 10)",
    "rolling_last10_ops_delta": "recent offensive edge",
    "ops_delta": "season OPS edge",
    "vs_starter_hand_ops_delta": "lineup vs starter handedness",
    "lineup_top5_ops_delta": "lineup quality edge",
    "home_starter_era": "home starter season ERA",
    "away_starter_era": "away starter season ERA",
    "home_starter_k_pct": "home starter K rate",
    "away_starter_k_pct": "away starter K rate",
    "home_bullpen_season_bullpen_era": "home bullpen ERA",
    "away_bullpen_season_bullpen_era": "away bullpen ERA",
}

SKIP_FEATURES = {
    "humidity_pct", "temperature_f", "wind_speed_mph", "wind_direction_deg",
    "wind_gust_mph", "precipitation_mm", "cloud_cover_pct", "pressure_hpa",
    "roof_closed_or_fixed_flag", "weather_exposed_flag", "is_day",
    "home_team_doubleheader_flag", "away_team_doubleheader_flag",
    "home_team_days_rest", "away_team_days_rest", "days_rest_delta",
}

MIN_SHAP_MAGNITUDE = 0.01


def _human_summary(feature: str, value: float) -> str:
    """Generate a short human-readable description of a feature value."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""

    if feature == "starter_era_delta":
        if value < 0:
            return f"away starter ERA is {abs(value):.1f} runs better this season"
        else:
            return f"home starter ERA is {value:.1f} runs better this season"

    elif feature == "run_diff_per_game_delta":
        if value > 0:
            return f"home team outscoring opponents by {value:.1f} runs/game"
        else:
            return f"away team outscoring opponents by {abs(value):.1f} runs/game"

    elif feature == "win_pct_delta":
        pts = int(abs(value) * 100)
        if value > 0:
            return f"home team win% is {pts} points higher"
        else:
            return f"away team win% is {pts} points higher"

    elif feature == "rolling_last10_win_pct_delta":
        # value is delta win_pct; estimate wins from delta
        # e.g. +0.4 delta → home ~7-3 vs away ~3-7
        home_wins = min(10, max(0, round((0.5 + value / 2) * 10)))
        away_wins = 10 - home_wins
        if value > 0:
            return f"home team {home_wins}-{10 - home_wins} in last 10 vs away {away_wins}-{10 - away_wins}"
        else:
            return f"away team {away_wins}-{10 - away_wins} in last 10 vs home {home_wins}-{10 - home_wins}"

    elif feature == "bullpen_era_delta":
        if value < 0:
            return f"away bullpen ERA is {abs(value):.1f} runs better"
        else:
            return f"home bullpen ERA is {value:.1f} runs better"

    elif feature == "ops_delta":
        if value > 0:
            return f"home team OPS is {value:.3f} higher this season"
        else:
            return f"away team OPS is {abs(value):.3f} higher this season"

    elif feature == "rolling_last10_ops_delta":
        if value > 0:
            return f"home team OPS {value:.3f} better over last 10 games"
        else:
            return f"away team OPS {abs(value):.3f} better over last 10 games"

    elif feature == "vs_starter_hand_ops_delta":
        pts = int(abs(value) * 1000)
        if value < 0:
            return f"visiting lineup {pts} OPS points worse vs this starter handedness"
        else:
            return f"home lineup {pts} OPS points better vs this starter handedness"

    elif feature == "lineup_top5_ops_delta":
        if value > 0:
            return f"home lineup top-5 OPS edge: +{value:.3f}"
        else:
            return f"away lineup top-5 OPS edge: +{abs(value):.3f}"

    elif feature == "home_starter_era":
        return f"home starter season ERA: {value:.2f}"

    elif feature == "away_starter_era":
        return f"away starter season ERA: {value:.2f}"

    elif feature == "home_starter_k_pct":
        return f"home starter strikes out {value * 100:.1f}% of batters"

    elif feature == "away_starter_k_pct":
        return f"away starter strikes out {value * 100:.1f}% of batters"

    elif feature == "home_starter_career_era":
        return f"home starter career ERA: {value:.2f}"

    elif feature == "away_starter_career_era":
        return f"away starter career ERA: {value:.2f}"

    elif feature == "home_bullpen_season_bullpen_era":
        return f"home bullpen season ERA: {value:.2f}"

    elif feature == "away_bullpen_season_bullpen_era":
        return f"away bullpen season ERA: {value:.2f}"

    elif feature == "starter_k_pct_delta":
        if value > 0:
            return f"home starter K rate is {value * 100:.1f} points higher"
        else:
            return f"away starter K rate is {abs(value) * 100:.1f} points higher"

    elif feature == "starter_whip_delta":
        if value < 0:
            return f"away starter WHIP is {abs(value):.2f} lower (better)"
        else:
            return f"home starter WHIP is {value:.2f} lower (better)"

    # Fallback: use label if available, else feature name
    label = FEATURE_LABELS.get(feature, feature)
    return f"{label}: {value:.3f}"


def explain_prediction(feature_dict: dict, model_bundle: dict) -> list[dict]:
    """
    Compute SHAP-based explanations for a single game prediction.

    Args:
        feature_dict: Dict of feature name -> value (same format as scorer input).
        model_bundle: Dict with 'model' (LGBMClassifier) and 'feature_cols' (list[str]).

    Returns:
        List of up to 5 dicts, sorted by |SHAP| descending:
        [{
            'feature': str,
            'label': str,
            'shap': float,
            'direction': 'home' | 'away',
            'value': float,
            'human_summary': str,
        }]
    """
    lgbm_model = model_bundle["model"]
    feature_cols = model_bundle["feature_cols"]

    # Build input DataFrame
    row = {
        col: (feature_dict.get(col) if feature_dict.get(col) is not None else np.nan)
        for col in feature_cols
    }
    X = pd.DataFrame([row], columns=feature_cols)

    # Get SHAP values via LightGBM booster (excludes bias term at [-1])
    try:
        raw_shap = lgbm_model.booster_.predict(X, pred_contrib=True)[0][:-1]
    except Exception:
        raw_shap = np.zeros(len(feature_cols))

    results = []
    for feat, shap_val, feat_val in zip(feature_cols, raw_shap, X.iloc[0]):
        # Skip unwanted features
        if feat in SKIP_FEATURES:
            continue

        # Skip NaN shap
        try:
            if math.isnan(float(shap_val)):
                continue
        except (TypeError, ValueError):
            continue

        # Skip low-magnitude SHAP
        if abs(shap_val) < MIN_SHAP_MAGNITUDE:
            continue

        # Handle NaN feature value
        try:
            fval = float(feat_val)
            if math.isnan(fval):
                continue
        except (TypeError, ValueError):
            continue

        label = FEATURE_LABELS.get(feat, feat)
        direction = "home" if shap_val > 0 else "away"
        summary = _human_summary(feat, fval)

        results.append({
            "feature": feat,
            "label": label,
            "shap": round(float(shap_val), 4),
            "direction": direction,
            "value": round(fval, 4),
            "human_summary": summary,
        })

    # Sort by |shap| descending, return top 5
    results.sort(key=lambda x: abs(x["shap"]), reverse=True)
    return results[:5]
