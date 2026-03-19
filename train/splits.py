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


@dataclass(frozen=True)
class HoldoutSplit:
    train_idx: np.ndarray
    test_idx: np.ndarray
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    holdout_season: int


@dataclass(frozen=True)
class EvaluationPlan:
    strategy: str
    development_splits: list[WalkForwardSplit]
    holdout_split: HoldoutSplit | None
    development_seasons: list[int] | None = None
    holdout_season: int | None = None


def _normalize_dates(dates: Iterable[object]) -> pd.Series:
    normalized = pd.to_datetime(pd.Series(list(dates)), errors="coerce", utc=False)
    if normalized.isna().any():
        missing = normalized.isna().sum()
        raise ValueError(f"Encountered {missing} invalid dates while creating walk-forward splits")
    return normalized


def _normalize_seasons(seasons: Iterable[object]) -> pd.Series:
    normalized = pd.to_numeric(pd.Series(list(seasons)), errors="coerce").astype("Int64")
    if normalized.isna().any():
        missing = int(normalized.isna().sum())
        raise ValueError(f"Encountered {missing} invalid seasons while creating season-based splits")
    return normalized.astype(int)


def _build_split_window(
    *,
    train_idx: np.ndarray,
    test_idx: np.ndarray,
    ordered_dates: pd.Series,
    fold: int | None = None,
    holdout_season: int | None = None,
) -> WalkForwardSplit | HoldoutSplit:
    base = {
        "train_idx": train_idx,
        "test_idx": test_idx,
        "train_start": str(ordered_dates.iloc[train_idx[0]].date()),
        "train_end": str(ordered_dates.iloc[train_idx[-1]].date()),
        "test_start": str(ordered_dates.iloc[test_idx[0]].date()),
        "test_end": str(ordered_dates.iloc[test_idx[-1]].date()),
    }
    if fold is not None:
        return WalkForwardSplit(fold=fold, **base)
    if holdout_season is None:
        raise ValueError("holdout_season is required when fold is omitted")
    return HoldoutSplit(holdout_season=holdout_season, **base)


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
            _build_split_window(
                fold=fold,
                train_idx=train_idx,
                test_idx=test_idx,
                ordered_dates=ordered_dates,
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


def build_seasonal_evaluation_plan(
    *,
    seasons: Iterable[object],
    dates: Iterable[object],
    development_seasons: Iterable[int],
    holdout_season: int,
    min_train_seasons: int = 2,
) -> EvaluationPlan:
    if min_train_seasons <= 0:
        raise ValueError("min_train_seasons must be positive")

    ordered_dates = _normalize_dates(dates)
    if not ordered_dates.is_monotonic_increasing:
        raise ValueError("Season-based evaluation requires rows sorted ascending by date")

    ordered_seasons = _normalize_seasons(seasons)
    if len(ordered_seasons) != len(ordered_dates):
        raise ValueError("Season and date arrays must have the same length")

    dev_seasons = [int(season) for season in development_seasons]
    if dev_seasons != sorted(dev_seasons):
        raise ValueError("development_seasons must be sorted ascending")
    if len(set(dev_seasons)) != len(dev_seasons):
        raise ValueError("development_seasons must be unique")
    if dev_seasons[-1] >= holdout_season:
        raise ValueError("holdout_season must be later than every development season")
    if holdout_season in dev_seasons:
        raise ValueError("holdout_season must not overlap development_seasons")
    if len(dev_seasons) <= min_train_seasons:
        raise ValueError("development_seasons must contain enough seasons to produce at least one fold")

    available_seasons = set(ordered_seasons.tolist())
    missing_dev = [season for season in dev_seasons if season not in available_seasons]
    if missing_dev:
        raise ValueError(f"Development seasons missing from dataset: {missing_dev}")
    if holdout_season not in available_seasons:
        raise ValueError(f"Holdout season missing from dataset: {holdout_season}")

    splits: list[WalkForwardSplit] = []
    for fold, season_index in enumerate(range(min_train_seasons, len(dev_seasons)), start=1):
        train_seasons = dev_seasons[:season_index]
        test_season = dev_seasons[season_index]
        train_idx = np.flatnonzero(ordered_seasons.isin(train_seasons).to_numpy())
        test_idx = np.flatnonzero((ordered_seasons == test_season).to_numpy())
        if len(train_idx) == 0 or len(test_idx) == 0:
            raise ValueError(
                f"Season-based split could not be created for train={train_seasons} test={test_season}"
            )
        splits.append(
            _build_split_window(
                fold=fold,
                train_idx=train_idx,
                test_idx=test_idx,
                ordered_dates=ordered_dates,
            )
        )

    holdout_train_idx = np.flatnonzero(ordered_seasons.isin(dev_seasons).to_numpy())
    holdout_test_idx = np.flatnonzero((ordered_seasons == holdout_season).to_numpy())
    if len(holdout_train_idx) == 0 or len(holdout_test_idx) == 0:
        raise ValueError("Holdout split requires both development and holdout rows")

    return EvaluationPlan(
        strategy="seasonal_holdout",
        development_splits=splits,
        holdout_split=_build_split_window(
            train_idx=holdout_train_idx,
            test_idx=holdout_test_idx,
            ordered_dates=ordered_dates,
            holdout_season=holdout_season,
        ),
        development_seasons=dev_seasons,
        holdout_season=int(holdout_season),
    )


def resolve_evaluation_plan(
    *,
    seasons: Iterable[object],
    dates: Iterable[object],
    evaluation_config: dict[str, object],
) -> EvaluationPlan:
    if "seasonal_holdout" in evaluation_config:
        season_config = dict(evaluation_config["seasonal_holdout"])
        return build_seasonal_evaluation_plan(
            seasons=seasons,
            dates=dates,
            development_seasons=season_config["development_seasons"],
            holdout_season=int(season_config["holdout_season"]),
            min_train_seasons=int(season_config.get("min_train_seasons", 2)),
        )
    if "walk_forward" in evaluation_config:
        return EvaluationPlan(
            strategy="walk_forward",
            development_splits=generate_walk_forward_splits(dates, **dict(evaluation_config["walk_forward"])),
            holdout_split=None,
        )
    raise ValueError("Unsupported evaluation config. Expected `seasonal_holdout` or `walk_forward`.")
