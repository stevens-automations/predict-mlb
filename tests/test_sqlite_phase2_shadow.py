import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from storage import SQLitePredictionStorage, shadow_writer_from_env, NullShadowWriter


class TestSQLiteFirstBootstrap(unittest.TestCase):
    def test_bootstrap_imports_excel_once_when_db_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            excel_path = base / "predictions.xlsx"
            db_path = base / "predictions.db"

            pd.DataFrame([
                {"game_id": 1, "date": "2026-03-08", "home": "A", "away": "B", "model": "m", "tweeted?": False}
            ]).to_excel(excel_path, index=False)

            storage = SQLitePredictionStorage(db_path=str(db_path), excel_path=str(excel_path))
            migrated = storage.bootstrap_if_needed()
            self.assertTrue(migrated)
            self.assertEqual(len(storage.read_predictions()), 1)

            # second bootstrap should be a no-op
            migrated_again = storage.bootstrap_if_needed()
            self.assertFalse(migrated_again)
            self.assertEqual(len(storage.read_predictions()), 1)

    def test_upsert_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "predictions.db")
            storage = SQLitePredictionStorage(db_path=db_path, excel_path=str(Path(tmpdir) / "none.xlsx"))

            frame = pd.DataFrame([
                {"game_id": 10, "date": "2026-03-08", "home": "A", "away": "B", "model": "m", "tweet": "x", "tweeted?": False}
            ])
            storage.upsert_predictions(frame)
            storage.upsert_predictions(frame)

            out = storage.read_predictions()
            self.assertEqual(len(out), 1)


class TestShadowCompatibility(unittest.TestCase):
    def test_shadow_writer_flag_behavior(self):
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            writer = shadow_writer_from_env()
            self.assertIsInstance(writer, NullShadowWriter)


if __name__ == "__main__":
    unittest.main()
