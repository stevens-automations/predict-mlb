import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from train.readiness import build_training_readiness_report


SCHEMA = """
CREATE TABLE games (
  game_id INTEGER PRIMARY KEY,
  season INTEGER NOT NULL,
  game_date TEXT NOT NULL
);
CREATE TABLE labels (
  game_id INTEGER PRIMARY KEY,
  did_home_win INTEGER
);
CREATE TABLE feature_rows (
  game_id INTEGER NOT NULL,
  feature_version TEXT NOT NULL,
  as_of_ts TEXT NOT NULL,
  feature_payload_json TEXT NOT NULL,
  source_contract_status TEXT NOT NULL,
  PRIMARY KEY (game_id, feature_version, as_of_ts)
);
"""


class TestTrainingReadiness(unittest.TestCase):
    def _make_db(self) -> Path:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        db_path = Path(tempdir.name) / "history.db"
        with sqlite3.connect(db_path) as conn:
            conn.executescript(SCHEMA)
        return db_path

    def test_report_is_ready_when_all_required_seasons_are_trainable(self) -> None:
        db_path = self._make_db()
        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                "INSERT INTO games (game_id, season, game_date) VALUES (?, ?, ?)",
                [(1, 2020, "2020-07-24"), (2, 2021, "2021-04-01")],
            )
            conn.executemany(
                "INSERT INTO labels (game_id, did_home_win) VALUES (?, ?)",
                [(1, 1), (2, 0)],
            )
            conn.executemany(
                """
                INSERT INTO feature_rows (
                  game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (1, "v1", "2020-07-24T12:00:00Z", json.dumps({"game_id": 1}), "valid"),
                    (2, "v1", "2021-04-01T12:00:00Z", json.dumps({"game_id": 2}), "degraded"),
                ],
            )

        report = build_training_readiness_report(
            db_path=db_path,
            required_seasons=[2020, 2021],
            feature_version="v1",
            allowed_contract_statuses=["valid", "degraded"],
        )

        self.assertTrue(report["ready"])
        self.assertEqual(report["totals"]["trainable_games"], 2)
        self.assertAlmostEqual(report["totals"]["degraded_feature_share"], 0.5)

    def test_report_is_not_ready_when_labeled_games_are_missing_feature_rows(self) -> None:
        db_path = self._make_db()
        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                "INSERT INTO games (game_id, season, game_date) VALUES (?, ?, ?)",
                [(1, 2020, "2020-07-24"), (2, 2021, "2021-04-01")],
            )
            conn.executemany(
                "INSERT INTO labels (game_id, did_home_win) VALUES (?, ?)",
                [(1, 1), (2, 0)],
            )
            conn.execute(
                """
                INSERT INTO feature_rows (
                  game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (1, "v1", "2020-07-24T12:00:00Z", json.dumps({"game_id": 1}), "valid"),
            )

        report = build_training_readiness_report(
            db_path=db_path,
            required_seasons=[2020, 2021],
            feature_version="v1",
            allowed_contract_statuses=["valid", "degraded"],
        )

        self.assertFalse(report["ready"])
        self.assertIn("required seasons still missing feature_rows for labeled games: [2021]", report["reasons"])


if __name__ == "__main__":
    unittest.main()
