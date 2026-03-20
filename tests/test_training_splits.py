import unittest

import numpy as np
import pandas as pd

from train.splits import build_seasonal_evaluation_plan, generate_walk_forward_splits


class TestTrainingSplits(unittest.TestCase):
    def test_walk_forward_splits_are_ordered_and_non_overlapping(self) -> None:
        dates = pd.date_range("2020-01-01", periods=20, freq="D")
        splits = generate_walk_forward_splits(
            dates,
            min_train_samples=8,
            test_size=4,
            step_size=4,
        )

        self.assertEqual(len(splits), 3)
        self.assertTrue(np.array_equal(splits[0].train_idx, np.arange(0, 8)))
        self.assertTrue(np.array_equal(splits[0].test_idx, np.arange(8, 12)))
        self.assertEqual(splits[0].train_end, "2020-01-08")
        self.assertEqual(splits[0].test_start, "2020-01-09")

        for split in splits:
            self.assertLess(split.train_idx.max(), split.test_idx.min())
            self.assertEqual(len(set(split.train_idx).intersection(set(split.test_idx))), 0)

    def test_walk_forward_requires_sorted_dates(self) -> None:
        with self.assertRaises(ValueError):
            generate_walk_forward_splits(
                ["2020-01-02", "2020-01-01", "2020-01-03"],
                min_train_samples=1,
                test_size=1,
            )

    def test_seasonal_evaluation_plan_uses_expanding_season_folds_and_fixed_holdout(self) -> None:
        dates = pd.to_datetime(
            [
                "2020-07-01",
                "2020-07-02",
                "2021-07-01",
                "2021-07-02",
                "2022-07-01",
                "2022-07-02",
                "2023-07-01",
                "2024-07-01",
                "2025-07-01",
                "2025-07-02",
            ]
        )
        seasons = [2020, 2020, 2021, 2021, 2022, 2022, 2023, 2024, 2025, 2025]

        plan = build_seasonal_evaluation_plan(
            seasons=seasons,
            dates=dates,
            development_seasons=[2020, 2021, 2022, 2023, 2024],
            holdout_season=2025,
            min_train_seasons=2,
        )

        self.assertEqual(plan.strategy, "seasonal_holdout")
        self.assertEqual(len(plan.development_splits), 3)
        self.assertTrue(np.array_equal(plan.development_splits[0].train_idx, np.arange(0, 4)))
        self.assertTrue(np.array_equal(plan.development_splits[0].test_idx, np.arange(4, 6)))
        self.assertEqual(plan.development_splits[1].test_start, "2023-07-01")
        self.assertEqual(plan.development_splits[2].test_end, "2024-07-01")
        self.assertIsNotNone(plan.holdout_split)
        assert plan.holdout_split is not None
        self.assertEqual(plan.holdout_split.holdout_season, 2025)
        self.assertTrue(np.array_equal(plan.holdout_split.train_idx, np.arange(0, 8)))
        self.assertTrue(np.array_equal(plan.holdout_split.test_idx, np.arange(8, 10)))

    def test_seasonal_evaluation_plan_requires_holdout_after_development_seasons(self) -> None:
        dates = pd.to_datetime(["2024-07-01", "2025-07-01"])

        with self.assertRaisesRegex(ValueError, "later than every development season"):
            build_seasonal_evaluation_plan(
                seasons=[2024, 2025],
                dates=dates,
                development_seasons=[2024, 2025],
                holdout_season=2025,
                min_train_seasons=1,
            )


if __name__ == "__main__":
    unittest.main()
