import unittest

import numpy as np
import pandas as pd

from train.splits import generate_walk_forward_splits


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


if __name__ == "__main__":
    unittest.main()

