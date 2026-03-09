import unittest

import numpy as np

from train.metrics import (
    accuracy,
    brier_score,
    calibration_bins,
    classification_metrics,
    expected_calibration_error,
    log_loss,
    max_calibration_gap,
)


class TestTrainingMetrics(unittest.TestCase):
    def test_scalar_metrics_match_expected_values(self) -> None:
        y_true = np.array([0.0, 1.0, 1.0, 0.0])
        probabilities = np.array([0.1, 0.8, 0.6, 0.3])

        self.assertAlmostEqual(accuracy(y_true, probabilities), 1.0)
        self.assertAlmostEqual(brier_score(y_true, probabilities), 0.075)
        self.assertAlmostEqual(log_loss(y_true, probabilities), 0.2990011586691898)

    def test_calibration_bins_cover_all_rows(self) -> None:
        y_true = np.array([0, 0, 1, 1])
        probabilities = np.array([0.1, 0.2, 0.7, 0.9])

        bins = calibration_bins(y_true, probabilities, n_bins=4)
        self.assertEqual(sum(item["count"] for item in bins), 4)
        self.assertEqual(bins[0]["count"], 2)
        self.assertEqual(bins[2]["count"], 1)
        self.assertEqual(bins[3]["count"], 1)

        summary = classification_metrics(y_true, probabilities, n_bins=4)
        self.assertIn("calibration_bins", summary)
        self.assertEqual(len(summary["calibration_bins"]), 4)
        self.assertAlmostEqual(expected_calibration_error(y_true, probabilities, n_bins=4), 0.175)
        self.assertAlmostEqual(max_calibration_gap(y_true, probabilities, n_bins=4), 0.3)
        self.assertIn("prediction_summary", summary)


if __name__ == "__main__":
    unittest.main()
