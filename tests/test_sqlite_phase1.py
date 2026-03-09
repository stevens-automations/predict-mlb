import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from sqlite_phase1 import (
    check_excel_sqlite_parity,
    ensure_predictions_schema,
    import_excel_to_sqlite,
)


class TestSqlitePhase1(unittest.TestCase):
    def test_schema_creation_creates_table_and_indexes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "predictions.db")

            ensure_predictions_schema(db_path)

            with sqlite3.connect(db_path) as conn:
                table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='predictions';"
                ).fetchone()
                self.assertIsNotNone(table)

                indexes = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='predictions';"
                ).fetchall()
                index_names = {name for (name,) in indexes}
                self.assertIn("idx_predictions_game_id", index_names)
                self.assertIn("idx_predictions_date", index_names)
                self.assertIn("idx_predictions_tweeted_date", index_names)
                self.assertIn("uq_predictions_game_date_model", index_names)

    def test_import_excel_to_sqlite_transforms_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            excel_path = base / "predictions.xlsx"
            db_path = base / "predictions.db"

            df = pd.DataFrame(
                [
                    {
                        "game_id": 1001,
                        "date": "2026-03-08",
                        "home": "Yankees",
                        "away": "Mets",
                        "model": "mlb4year",
                        "tweeted?": True,
                        "tweet": "sample",
                    },
                    {
                        "game_id": 1002,
                        "date": "2026-03-08",
                        "home": "Dodgers",
                        "away": "Padres",
                        "model": "mlb4year",
                        "tweeted?": False,
                        "tweet": "sample2",
                    },
                ]
            )
            df.to_excel(excel_path, index=False)

            result = import_excel_to_sqlite(str(excel_path), str(db_path), replace=True)

            self.assertEqual(result.imported_rows, 2)
            self.assertEqual(result.sqlite_total_rows, 2)

            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    "SELECT game_id, tweeted FROM predictions ORDER BY game_id"
                ).fetchall()
            self.assertEqual(rows, [(1001, 1), (1002, 0)])

    def test_parity_checker_detects_match_and_mismatch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            excel_path = base / "predictions.xlsx"
            db_path = base / "predictions.db"

            df = pd.DataFrame(
                [
                    {
                        "game_id": 2001,
                        "date": "2026-03-08",
                        "home": "A",
                        "away": "B",
                        "model": "m",
                        "tweeted?": False,
                    },
                    {
                        "game_id": 2002,
                        "date": "2026-03-08",
                        "home": "C",
                        "away": "D",
                        "model": "m",
                        "tweeted?": True,
                    },
                ]
            )
            df.to_excel(excel_path, index=False)
            import_excel_to_sqlite(str(excel_path), str(db_path), replace=True)

            ok = check_excel_sqlite_parity(str(excel_path), str(db_path))
            self.assertTrue(ok.matches)

            with sqlite3.connect(db_path) as conn:
                conn.execute("DELETE FROM predictions WHERE game_id = 2002;")
                conn.commit()

            mismatch = check_excel_sqlite_parity(str(excel_path), str(db_path))
            self.assertFalse(mismatch.matches)
            self.assertEqual(mismatch.missing_in_sqlite, 1)


if __name__ == "__main__":
    unittest.main()
