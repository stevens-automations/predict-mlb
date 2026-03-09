from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.history_ingest import connect_db, ensure_schema, upsert_checkpoint, upsert_game


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def test_init_schema_creates_required_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
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
        assert not missing


def test_checkpoint_upsert_is_idempotent_and_increments_attempts(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
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
        assert row["attempts"] == 2
        assert row["last_game_id"] == 2


def test_game_upsert_updates_existing_row_without_duplicates(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
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
        assert count_row["c"] == 1
        assert game_row["status"] == "Final"
        assert game_row["home_score"] == 5
        assert game_row["away_score"] == 4
