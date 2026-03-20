from __future__ import annotations

from typing import Any

import numpy as np


def clip_probabilities(probabilities: np.ndarray, eps: float = 1e-15) -> np.ndarray:
    return np.clip(np.asarray(probabilities, dtype=float), eps, 1.0 - eps)


def log_loss(y_true: np.ndarray, probabilities: np.ndarray) -> float:
    y_true = np.asarray(y_true, dtype=float)
    probs = clip_probabilities(probabilities)
    return float(-np.mean(y_true * np.log(probs) + (1.0 - y_true) * np.log(1.0 - probs)))


def brier_score(y_true: np.ndarray, probabilities: np.ndarray) -> float:
    y_true = np.asarray(y_true, dtype=float)
    probs = np.asarray(probabilities, dtype=float)
    return float(np.mean((probs - y_true) ** 2))


def accuracy(y_true: np.ndarray, probabilities: np.ndarray, threshold: float = 0.5) -> float:
    y_true = np.asarray(y_true, dtype=float)
    probs = np.asarray(probabilities, dtype=float)
    return float(np.mean((probs >= threshold) == (y_true >= 0.5)))


def calibration_bins(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    *,
    n_bins: int = 10,
) -> list[dict[str, Any]]:
    y_true = np.asarray(y_true, dtype=float)
    probs = np.asarray(probabilities, dtype=float)
    if len(y_true) != len(probs):
        raise ValueError("y_true and probabilities must have the same length")
    if n_bins <= 0:
        raise ValueError("n_bins must be positive")

    edges = np.linspace(0.0, 1.0, n_bins + 1)
    assignments = np.digitize(probs, edges[1:-1], right=True)
    bins: list[dict[str, Any]] = []
    for index in range(n_bins):
        mask = assignments == index
        count = int(mask.sum())
        if count == 0:
            bins.append(
                {
                    "bin": index,
                    "lower": float(edges[index]),
                    "upper": float(edges[index + 1]),
                    "count": 0,
                    "avg_prediction": None,
                    "empirical_rate": None,
                }
            )
            continue
        bins.append(
            {
                "bin": index,
                "lower": float(edges[index]),
                "upper": float(edges[index + 1]),
                "count": count,
                "avg_prediction": float(probs[mask].mean()),
                "empirical_rate": float(y_true[mask].mean()),
            }
        )
    return bins


def expected_calibration_error(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    *,
    n_bins: int = 10,
) -> float:
    bins = calibration_bins(y_true, probabilities, n_bins=n_bins)
    total = max(1, len(np.asarray(y_true, dtype=float)))
    return float(
        sum(
            0.0
            if bucket["count"] == 0
            else abs(bucket["avg_prediction"] - bucket["empirical_rate"]) * bucket["count"] / total
            for bucket in bins
        )
    )


def max_calibration_gap(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    *,
    n_bins: int = 10,
) -> float:
    bins = calibration_bins(y_true, probabilities, n_bins=n_bins)
    gaps = [
        abs(bucket["avg_prediction"] - bucket["empirical_rate"])
        for bucket in bins
        if bucket["count"] > 0
    ]
    return float(max(gaps, default=0.0))


def prediction_summary(probabilities: np.ndarray) -> dict[str, float]:
    probs = np.asarray(probabilities, dtype=float)
    return {
        "count": int(len(probs)),
        "min_probability": float(np.min(probs)),
        "max_probability": float(np.max(probs)),
        "mean_probability": float(np.mean(probs)),
    }


def classification_metrics(y_true: np.ndarray, probabilities: np.ndarray, *, n_bins: int = 10) -> dict[str, Any]:
    return {
        "log_loss": log_loss(y_true, probabilities),
        "brier_score": brier_score(y_true, probabilities),
        "accuracy": accuracy(y_true, probabilities),
        "expected_calibration_error": expected_calibration_error(y_true, probabilities, n_bins=n_bins),
        "max_calibration_gap": max_calibration_gap(y_true, probabilities, n_bins=n_bins),
        "prediction_summary": prediction_summary(probabilities),
        "calibration_bins": calibration_bins(y_true, probabilities, n_bins=n_bins),
    }
