from __future__ import annotations

import json
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.history_ingest import build_parser, connect_db, ensure_schema, upsert_checkpoint, upsert_game


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


class TestHistoryIngestSchemaAndUpserts(unittest.TestCase):
    def test_init_schema_creates_required_tables(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                required = {
                    "games",
                    "game_team_stats",
                    "game_pitcher_context",
                    "feature_rows",
                    "labels",
                    "ingestion_runs",
                    "ingestion_checkpoints",
                    "dq_results",
                    "odds_snapshot",
                }
                missing = {name for name in required if not table_exists(conn, name)}
                self.assertFalse(missing)

                label_cols = {row["name"] for row in conn.execute("PRAGMA table_info(labels)").fetchall()}
                self.assertIn("run_differential", label_cols)
                self.assertIn("total_runs", label_cols)

    def test_checkpoint_upsert_is_idempotent_and_increments_attempts(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_checkpoint(
                    conn,
                    job_name="backfill",
                    partition_key="season=2024",
                    cursor={"last_game_id": 1},
                    status="running",
                    last_game_id=1,
                )
                upsert_checkpoint(
                    conn,
                    job_name="backfill",
                    partition_key="season=2024",
                    cursor={"last_game_id": 2},
                    status="running",
                    last_game_id=2,
                )
                row = conn.execute(
                    "SELECT attempts, last_game_id FROM ingestion_checkpoints WHERE job_name=? AND partition_key=?",
                    ("backfill", "season=2024"),
                ).fetchone()
                self.assertEqual(row["attempts"], 2)
                self.assertEqual(row["last_game_id"], 2)

    def test_game_upsert_updates_existing_row_without_duplicates(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 123,
                        "season": 2024,
                        "game_date": "2024-04-01",
                        "status": "Scheduled",
                        "home_team_id": 1,
                        "away_team_id": 2,
                    },
                )
                upsert_game(
                    conn,
                    {
                        "game_id": 123,
                        "season": 2024,
                        "game_date": "2024-04-01",
                        "status": "Final",
                        "home_team_id": 1,
                        "away_team_id": 2,
                        "home_score": 5,
                        "away_score": 4,
                        "winning_team_id": 1,
                    },
                )
                count_row = conn.execute("SELECT COUNT(*) AS c FROM games WHERE game_id=123").fetchone()
                game_row = conn.execute("SELECT status, home_score, away_score FROM games WHERE game_id=123").fetchone()
                self.assertEqual(count_row["c"], 1)
                self.assertEqual(game_row["status"], "Final")
                self.assertEqual(game_row["home_score"], 5)
                self.assertEqual(game_row["away_score"], 4)


class TestHistoryIngestCommands(unittest.TestCase):
    def test_backfill_ingests_bounded_schedule_and_labels_idempotently(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "--checkpoint-every",
                    "1",
                    "backfill",
                    "--season",
                    "2024",
                ]
            )
            schedule_rows = [
                {
                    "game_id": 1001,
                    "season": 2024,
                    "game_date": "2024-04-01",
                    "game_type": "R",
                    "status": "Final",
                    "game_datetime": "2024-04-01T23:05:00Z",
                    "home_id": 147,
                    "away_id": 121,
                    "home_score": 6,
                    "away_score": 3,
                },
                {
                    "game_id": 1002,
                    "season": 2024,
                    "game_date": "2024-04-02",
                    "game_type": "R",
                    "status": "Scheduled",
                    "game_datetime": "2024-04-02T23:05:00Z",
                    "home_id": 147,
                    "away_id": 121,
                },
                {
                    "game_id": 1003,
                    "season": 2024,
                    "game_date": "2024-03-10",
                    "game_type": "S",
                    "status": "Final",
                    "game_datetime": "2024-03-10T18:05:00Z",
                    "home_id": 147,
                    "away_id": 121,
                    "home_score": 2,
                    "away_score": 1,
                },
            ]
            stub_statsapi = types.SimpleNamespace(schedule=lambda **_kwargs: schedule_rows)
            with patch("scripts.history_ingest.statsapi", stub_statsapi):
                args.func(args)
                args.func(args)

            with connect_db(str(db_path)) as conn:
                game_count = conn.execute("SELECT COUNT(*) AS c FROM games").fetchone()["c"]
                label_count = conn.execute("SELECT COUNT(*) AS c FROM labels").fetchone()["c"]
                run_count = conn.execute("SELECT COUNT(*) AS c FROM ingestion_runs WHERE mode='backfill'").fetchone()["c"]
                run_rows = conn.execute(
                    "SELECT note, request_count FROM ingestion_runs WHERE mode='backfill' ORDER BY started_at"
                ).fetchall()
                label_row = conn.execute(
                    "SELECT did_home_win, run_differential, total_runs FROM labels WHERE game_id=1001"
                ).fetchone()
                checkpoint_row = conn.execute(
                    """
                    SELECT status, last_game_id, attempts, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='backfill' AND partition_key='season=2024'
                    """
                ).fetchone()
                request_counts = [row["request_count"] for row in run_rows]
                run_notes = [json.loads(row["note"]) for row in run_rows]
                checkpoint_cursor = json.loads(checkpoint_row["cursor_json"])

            self.assertEqual(game_count, 2)
            self.assertEqual(label_count, 1)
            self.assertEqual(run_count, 2)
            self.assertEqual(label_row["did_home_win"], 1)
            self.assertEqual(label_row["run_differential"], 3)
            self.assertEqual(label_row["total_runs"], 9)
            self.assertEqual(checkpoint_row["status"], "success")
            self.assertEqual(checkpoint_row["last_game_id"], 1002)
            self.assertGreaterEqual(checkpoint_row["attempts"], 2)
            self.assertEqual(request_counts, [1, 1])
            for run_note in run_notes:
                self.assertEqual(run_note["schedule_rows_fetched"], 3)
                self.assertEqual(run_note["relevant_rows_processed"], 2)
                self.assertEqual(run_note["distinct_games_touched"], 2)
                self.assertEqual(run_note["games_inserted"] + run_note["games_updated"], 2)
                self.assertEqual(run_note["labels_inserted"] + run_note["labels_updated"], 1)
                self.assertEqual(run_note["final_distinct_counts_snapshot"], {"games": 2, "labels": 1})
            self.assertEqual(run_notes[0]["games_inserted"], 2)
            self.assertEqual(run_notes[0]["games_updated"], 0)
            self.assertEqual(run_notes[0]["labels_inserted"], 1)
            self.assertEqual(run_notes[0]["labels_updated"], 0)
            self.assertEqual(run_notes[1]["games_inserted"], 0)
            self.assertEqual(run_notes[1]["games_updated"], 2)
            self.assertEqual(run_notes[1]["labels_inserted"], 0)
            self.assertEqual(run_notes[1]["labels_updated"], 1)
            self.assertEqual(checkpoint_cursor["schedule_rows_fetched"], 3)
            self.assertEqual(checkpoint_cursor["relevant_rows_processed"], 2)
            self.assertEqual(checkpoint_cursor["distinct_games_touched"], 2)
            self.assertEqual(checkpoint_cursor["games_inserted"], 0)
            self.assertEqual(checkpoint_cursor["games_updated"], 2)
            self.assertEqual(checkpoint_cursor["labels_inserted"], 0)
            self.assertEqual(checkpoint_cursor["labels_updated"], 1)
            self.assertEqual(checkpoint_cursor["final_distinct_counts_snapshot"], {"games": 2, "labels": 1})

    def test_incremental_one_day_schedule_ingest(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "--checkpoint-every",
                    "1",
                    "incremental",
                    "--date",
                    "2026-03-09",
                ]
            )
            schedule_rows = [
                {
                    "game_id": 2001,
                    "season": 2026,
                    "game_date": "2026-03-09",
                    "game_type": "R",
                    "status": "Final",
                    "home_id": 147,
                    "away_id": 121,
                    "home_score": 4,
                    "away_score": 5,
                }
            ]
            captured_kwargs = {}

            def fake_schedule(**kwargs):
                captured_kwargs.update(kwargs)
                return schedule_rows

            stub_statsapi = types.SimpleNamespace(schedule=fake_schedule)
            with patch("scripts.history_ingest.statsapi", stub_statsapi):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                run = conn.execute(
                    "SELECT status, request_count, note FROM ingestion_runs WHERE mode='incremental' ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                label = conn.execute(
                    "SELECT did_home_win, run_differential, total_runs FROM labels WHERE game_id=2001"
                ).fetchone()
                checkpoint = conn.execute(
                    """
                    SELECT status, partition_key, last_game_id, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='incremental' AND partition_key='date=2026-03-09'
                    """
                ).fetchone()
                run_note = json.loads(run["note"])
                checkpoint_cursor = json.loads(checkpoint["cursor_json"])

            self.assertEqual(captured_kwargs["start_date"], "2026-03-09")
            self.assertEqual(captured_kwargs["end_date"], "2026-03-09")
            self.assertEqual(run["status"], "success")
            self.assertEqual(run["request_count"], 1)
            self.assertEqual(label["did_home_win"], 0)
            self.assertEqual(label["run_differential"], -1)
            self.assertEqual(label["total_runs"], 9)
            self.assertEqual(checkpoint["status"], "success")
            self.assertEqual(checkpoint["partition_key"], "date=2026-03-09")
            self.assertEqual(checkpoint["last_game_id"], 2001)
            self.assertEqual(run_note["schedule_rows_fetched"], 1)
            self.assertEqual(run_note["relevant_rows_processed"], 1)
            self.assertEqual(run_note["distinct_games_touched"], 1)
            self.assertEqual(run_note["games_inserted"], 1)
            self.assertEqual(run_note["games_updated"], 0)
            self.assertEqual(run_note["labels_inserted"], 1)
            self.assertEqual(run_note["labels_updated"], 0)
            self.assertEqual(run_note["final_distinct_counts_snapshot"], {"games": 1, "labels": 1})
            self.assertEqual(checkpoint_cursor["schedule_rows_fetched"], 1)
            self.assertEqual(checkpoint_cursor["distinct_games_touched"], 1)
            self.assertEqual(checkpoint_cursor["games_inserted"], 1)
            self.assertEqual(checkpoint_cursor["labels_inserted"], 1)
            self.assertEqual(checkpoint_cursor["final_distinct_counts_snapshot"], {"games": 1, "labels": 1})

    def test_backfill_checkpoint_failed_on_schedule_error(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "--max-attempts",
                    "2",
                    "--initial-backoff-seconds",
                    "0",
                    "--max-backoff-seconds",
                    "0",
                    "--jitter-seconds",
                    "0",
                    "backfill",
                    "--season",
                    "2024",
                ]
            )
            stub_statsapi = types.SimpleNamespace(schedule=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
            with patch("scripts.history_ingest.statsapi", stub_statsapi):
                with patch("scripts.history_ingest.time.sleep"):
                    with self.assertRaises(RuntimeError):
                        args.func(args)

            with connect_db(str(db_path)) as conn:
                run = conn.execute(
                    "SELECT status, request_count, note FROM ingestion_runs WHERE mode='backfill' ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                checkpoint = conn.execute(
                    """
                    SELECT status, last_error
                    FROM ingestion_checkpoints
                    WHERE job_name='backfill' AND partition_key='season=2024'
                    """
                ).fetchone()

            self.assertEqual(run["status"], "failed")
            self.assertEqual(run["request_count"], 2)
            self.assertIn("boom", run["note"])
            self.assertEqual(checkpoint["status"], "failed")
            self.assertIn("boom", checkpoint["last_error"])


if __name__ == "__main__":
    unittest.main()
