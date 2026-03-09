import tempfile
import unittest
from pathlib import Path

import pandas as pd

from storage import SQLitePredictionStorage


class TestSQLiteStorageTransactions(unittest.TestCase):
    def test_replace_predictions_rolls_back_on_row_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "predictions.db")
            storage = SQLitePredictionStorage(db_path=db_path, excel_path=str(Path(tmpdir) / "none.xlsx"))

            baseline = pd.DataFrame([
                {
                    "game_id": 1,
                    "date": "2026-03-08",
                    "home": "A",
                    "away": "B",
                    "model": "m",
                    "tweet": "baseline",
                    "tweeted?": False,
                }
            ])
            storage.upsert_predictions(baseline)

            replacement = pd.DataFrame([
                {
                    "game_id": 2,
                    "date": "2026-03-09",
                    "home": "C",
                    "away": "D",
                    "model": "m",
                    "tweet": "valid-row",
                    "tweeted?": False,
                },
                {
                    "game_id": 3,
                    "date": None,
                    "home": "E",
                    "away": "F",
                    "model": "m",
                    "tweet": "invalid-row",
                    "tweeted?": False,
                },
            ])

            success, failure = storage.replace_predictions(replacement)
            self.assertEqual((success, failure), (0, 2))

            out = storage.read_predictions()
            self.assertEqual(len(out), 1)
            self.assertEqual(out.iloc[0]["game_id"], 1)
            self.assertEqual(out.iloc[0]["tweet"], "baseline")

    def test_replace_predictions_is_idempotent_for_same_payload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "predictions.db")
            storage = SQLitePredictionStorage(db_path=db_path, excel_path=str(Path(tmpdir) / "none.xlsx"))

            frame = pd.DataFrame([
                {
                    "game_id": 10,
                    "date": "2026-03-08",
                    "home": "A",
                    "away": "B",
                    "model": "m",
                    "tweet": "x",
                    "tweeted?": False,
                }
            ])

            self.assertEqual(storage.replace_predictions(frame), (1, 0))
            self.assertEqual(storage.replace_predictions(frame), (1, 0))

            out = storage.read_predictions()
            self.assertEqual(len(out), 1)
            self.assertEqual(out.iloc[0]["game_id"], 10)

    def test_upsert_predictions_is_idempotent_and_updates_existing_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "predictions.db")
            storage = SQLitePredictionStorage(db_path=db_path, excel_path=str(Path(tmpdir) / "none.xlsx"))

            frame = pd.DataFrame([
                {
                    "game_id": 20,
                    "date": "2026-03-08",
                    "home": "A",
                    "away": "B",
                    "model": "m",
                    "tweet": "v1",
                    "tweeted?": False,
                }
            ])
            storage.upsert_predictions(frame)

            updated = frame.copy()
            updated.loc[0, "tweet"] = "v2"
            storage.upsert_predictions(updated)
            storage.upsert_predictions(updated)

            out = storage.read_predictions()
            self.assertEqual(len(out), 1)
            self.assertEqual(out.iloc[0]["tweet"], "v2")


if __name__ == "__main__":
    unittest.main()
