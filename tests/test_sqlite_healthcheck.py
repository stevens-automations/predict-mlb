import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.legacy_runtime.sqlite_healthcheck import build_report
from storage import SQLitePredictionStorage


class TestSQLiteHealthcheck(unittest.TestCase):
    def test_build_report_returns_expected_metrics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "predictions.db")
            storage = SQLitePredictionStorage(db_path=db_path, excel_path=str(Path(tmpdir) / "none.xlsx"))

            frame = pd.DataFrame([
                {
                    "game_id": 1,
                    "date": "2026-03-08",
                    "home": "A",
                    "away": "B",
                    "model": "m",
                    "prediction_accuracy": None,
                    "tweeted?": False,
                },
                {
                    "game_id": 2,
                    "date": "2026-03-07",
                    "home": "C",
                    "away": "D",
                    "model": "m",
                    "prediction_accuracy": 1.0,
                    "tweeted?": True,
                },
            ])
            storage.upsert_predictions(frame)

            report = build_report(db_path)
            self.assertEqual(report["total_predictions_rows"], 2)
            self.assertEqual(report["pending_unsent_tweets_count"], 1)
            self.assertEqual(report["rows_with_null_accuracy"], 1)
            self.assertTrue(any(item["date"] == "2026-03-08" for item in report["recent_date_coverage"]))


if __name__ == "__main__":
    unittest.main()
