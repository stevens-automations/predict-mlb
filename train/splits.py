from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class WalkForwardSplit:
    fold: int
    train_idx: np.ndarray
    test_idx: np.ndarray
    train_start: str
    train_end: str
    test_start: str
    test_end: str


def _normalize_dates(dates: Iterable[object]) -> pd.Series:
    normalized = pd.to_datetime(pd.Series(list(dates)), errors="coerce", utc=False)
    if normalized.isna().any():
        missing = normalized.isna().sum()
        raise ValueError(f"Encountered {missing} invalid dates while creating walk-forward splits")
    return normalized


def generate_walk_forward_splits(
    dates: Iterable[object],
    *,
    min_train_samples: int = 500,
    test_size: int = 250,
    step_size: int | None = None,
    gap: int = 0,
    max_splits: int | None = None,
) -> list[WalkForwardSplit]:
    if test_size <= 0:
        raise ValueError("test_size must be positive")
    if min_train_samples <= 0:
        raise ValueError("min_train_samples must be positive")
    if gap < 0:
        raise ValueError("gap must be >= 0")

    ordered_dates = _normalize_dates(dates)
    if not ordered_dates.is_monotonic_increasing:
        raise ValueError("Walk-forward split generation requires dates sorted ascending before splitting")

    n_rows = len(ordered_dates)
    effective_step = step_size or test_size
    splits: list[WalkForwardSplit] = []

    train_end_exclusive = min_train_samples
    fold = 1
    while True:
        test_start = train_end_exclusive + gap
        test_end_exclusive = test_start + test_size
        if test_end_exclusive > n_rows:
            break

        train_idx = np.arange(0, train_end_exclusive, dtype=int)
        test_idx = np.arange(test_start, test_end_exclusive, dtype=int)

        splits.append(
            WalkForwardSplit(
                fold=fold,
                train_idx=train_idx,
                test_idx=test_idx,
                train_start=str(ordered_dates.iloc[train_idx[0]].date()),
                train_end=str(ordered_dates.iloc[train_idx[-1]].date()),
                test_start=str(ordered_dates.iloc[test_idx[0]].date()),
                test_end=str(ordered_dates.iloc[test_idx[-1]].date()),
            )
        )
        if max_splits is not None and len(splits) >= max_splits:
            break

        train_end_exclusive += effective_step
        fold += 1

    if not splits:
        raise ValueError(
            "Not enough ordered rows for the requested walk-forward parameters. "
            "Lower min_train_samples/test_size or load more data."
        )
    return splits

