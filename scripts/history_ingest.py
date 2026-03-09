#!/usr/bin/env python3
"""Historical MLB ingestion scaffold.

Safety guarantees:
- Initializes and validates historical schema at data/mlb_history.db by default.
- Provides checkpoint/run-ledger primitives.
- Backfill/incremental subcommands are intentionally safe stubs and do not perform
  large historical pulls by default.
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "mlb_history.db"
DEFAULT_SCHEMA_PATH = ROOT / "scripts" / "sql" / "history_schema.sql"


@dataclass(frozen=True)
class RequestPolicy:
    timeout_seconds: int = 25
    max_attempts: int = 5
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 16.0
    jitter_seconds: float = 0.4
    request_budget_per_run: int = 2500


@dataclass
class RequestBudget:
    limit: int
    used: int = 0

    def consume(self, amount: int = 1) -> None:
        if amount < 0:
            raise ValueError("amount must be non-negative")
        if self.used + amount > self.limit:
            raise RuntimeError(f"request budget exceeded: used={self.used}, limit={self.limit}, requested={amount}")
        self.used += amount


@dataclass(frozen=True)
class IngestConfig:
    db_path: str
    season_start: int = 2020
    season_end: int = 2025
    checkpoint_every: int = 25
    include_historical_odds: bool = False
    pregame_enabled: bool = True
    postgame_enabled: bool = True
    intraday_enabled: bool = False
    primary_model_metric: str = "log_loss"
    strict_contracts: bool = True
    allow_degraded_fallback: bool = True
    request_policy: RequestPolicy = RequestPolicy()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_db(db_path: str) -> sqlite3.Connection:
    db = Path(db_path)
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_schema(conn: sqlite3.Connection, schema_path: Path = DEFAULT_SCHEMA_PATH) -> None:
    sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()


def start_run(conn: sqlite3.Connection, mode: str, partition_key: str | None, config: IngestConfig) -> str:
    run_id = f"{mode}-{uuid.uuid4()}"
    conn.execute(
        """
        INSERT INTO ingestion_runs (run_id, mode, status, partition_key, started_at, config_json)
        VALUES (?, ?, 'running', ?, ?, ?)
        """,
        (run_id, mode, partition_key, utc_now(), json.dumps(asdict(config), sort_keys=True)),
    )
    conn.commit()
    return run_id


def finish_run(
    conn: sqlite3.Connection,
    run_id: str,
    status: str,
    note: str | None = None,
    request_count: int = 0,
) -> None:
    conn.execute(
        """
        UPDATE ingestion_runs
        SET status = ?, ended_at = ?, note = ?, request_count = ?
        WHERE run_id = ?
        """,
        (status, utc_now(), note, request_count, run_id),
    )
    conn.commit()


def upsert_checkpoint(
    conn: sqlite3.Connection,
    job_name: str,
    partition_key: str,
    cursor: dict[str, Any] | None,
    status: str,
    last_game_id: int | None = None,
    last_error: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO ingestion_checkpoints
          (job_name, partition_key, cursor_json, last_game_id, attempts, status, updated_at, last_error)
        VALUES (?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(job_name, partition_key)
        DO UPDATE SET
          cursor_json = excluded.cursor_json,
          last_game_id = excluded.last_game_id,
          attempts = ingestion_checkpoints.attempts + 1,
          status = excluded.status,
          updated_at = excluded.updated_at,
          last_error = excluded.last_error
        """,
        (
            job_name,
            partition_key,
            json.dumps(cursor, sort_keys=True) if cursor is not None else None,
            last_game_id,
            status,
            utc_now(),
            last_error,
        ),
    )
    conn.commit()


def upsert_game(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO games (
          game_id, season, game_date, game_type, status, scheduled_datetime,
          home_team_id, away_team_id, home_score, away_score, winning_team_id, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id) DO UPDATE SET
          season = excluded.season,
          game_date = excluded.game_date,
          game_type = excluded.game_type,
          status = excluded.status,
          scheduled_datetime = excluded.scheduled_datetime,
          home_team_id = excluded.home_team_id,
          away_team_id = excluded.away_team_id,
          home_score = excluded.home_score,
          away_score = excluded.away_score,
          winning_team_id = excluded.winning_team_id,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["season"],
            row["game_date"],
            row.get("game_type"),
            row.get("status"),
            row.get("scheduled_datetime"),
            row.get("home_team_id"),
            row.get("away_team_id"),
            row.get("home_score"),
            row.get("away_score"),
            row.get("winning_team_id"),
            row.get("source_updated_at"),
        ),
    )
    conn.commit()


def bounded_retry_sleep(attempt: int, policy: RequestPolicy) -> float:
    base = min(policy.max_backoff_seconds, policy.initial_backoff_seconds * (2 ** max(0, attempt - 1)))
    jitter = random.uniform(0, policy.jitter_seconds)
    delay = min(policy.max_backoff_seconds, base + jitter)
    time.sleep(delay)
    return delay


def run_with_bounded_retries(fn: Any, policy: RequestPolicy, budget: RequestBudget) -> Any:
    """Small reusable retry wrapper for future statsapi calls."""
    last_error: Exception | None = None
    for attempt in range(1, policy.max_attempts + 1):
        budget.consume(1)
        try:
            return fn()
        except Exception as exc:  # pragma: no cover - scaffold behavior
            last_error = exc
            if attempt >= policy.max_attempts:
                break
            bounded_retry_sleep(attempt, policy)
    if last_error is None:
        raise RuntimeError("retry loop exited unexpectedly without result")
    raise last_error


def cmd_init_db(args: argparse.Namespace) -> None:
    config = build_config(args)
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "init-db", partition_key=None, config=config)
        finish_run(conn, run_id, "success", note="historical schema initialized")
    print(f"Initialized schema at {config.db_path}")


def cmd_backfill(args: argparse.Namespace) -> None:
    config = build_config(args)
    _budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = f"season={args.season}" if args.season else f"range={config.season_start}-{config.season_end}"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=partition_key, config=config)
        upsert_checkpoint(
            conn,
            job_name="backfill",
            partition_key=partition_key,
            cursor={"season": args.season, "mode": "stub", "scope": "2020-2025"},
            status="stubbed",
        )
        finish_run(
            conn,
            run_id,
            "stubbed",
            note=(
                "Safety stub only. No historical ingestion executed. "
                "Requires explicit implementation approval before pulling statsapi partitions."
            ),
        )
    print(
        "Backfill scaffold executed in safe mode only (no statsapi pull performed). "
        f"Partition: {partition_key}"
    )


def cmd_incremental(args: argparse.Namespace) -> None:
    config = build_config(args)
    _budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    target_date = args.date or datetime.now().date().isoformat()
    partition_key = f"date={target_date}"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "incremental", partition_key=partition_key, config=config)
        upsert_checkpoint(
            conn,
            job_name="incremental",
            partition_key=partition_key,
            cursor={"date": target_date, "windows": ["pre-game", "post-game"]},
            status="stubbed",
        )
        finish_run(
            conn,
            run_id,
            "stubbed",
            note=(
                "Safety stub only. Incremental cadence currently defined as pre-game + post-game. "
                "No live API pulls performed by scaffold."
            ),
        )
    print(
        "Incremental scaffold executed in safe mode only (no statsapi pull performed). "
        f"Partition: {partition_key}"
    )


def cmd_dq(args: argparse.Namespace) -> None:
    config = build_config(args)
    partition_key = args.partition or "manual"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "dq", partition_key=partition_key, config=config)
        conn.execute(
            """
            INSERT INTO dq_results (run_id, check_name, severity, passed, expected_value, observed_value, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, check_name) DO UPDATE SET
              severity = excluded.severity,
              passed = excluded.passed,
              expected_value = excluded.expected_value,
              observed_value = excluded.observed_value,
              details_json = excluded.details_json
            """,
            (
                run_id,
                "scaffold_dq_placeholder",
                "info",
                1,
                1,
                1,
                json.dumps(
                    {
                        "message": "DQ scaffold placeholder",
                        "strict_contracts": config.strict_contracts,
                        "degraded_fallback": config.allow_degraded_fallback,
                    },
                    sort_keys=True,
                ),
            ),
        )
        conn.commit()
        finish_run(conn, run_id, "success", note="dq scaffold placeholder recorded")
    print(f"DQ scaffold completed for partition={partition_key}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Historical ingestion scaffold for MLB statsapi -> SQLite")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to SQLite DB (default: data/mlb_history.db)")
    parser.add_argument("--checkpoint-every", type=int, default=25)
    parser.add_argument("--timeout-seconds", type=int, default=25)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--initial-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--max-backoff-seconds", type=float, default=16.0)
    parser.add_argument("--jitter-seconds", type=float, default=0.4)
    parser.add_argument("--request-budget-per-run", type=int, default=2500)

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db", help="Initialize historical schema")
    init_db.set_defaults(func=cmd_init_db)

    backfill = subparsers.add_parser("backfill", help="Backfill historical partitions (safe scaffold stub)")
    backfill.add_argument("--season", type=int, help="Single season override")
    backfill.add_argument("--season-start", type=int, default=2020)
    backfill.add_argument("--season-end", type=int, default=2025)
    backfill.set_defaults(func=cmd_backfill)

    incremental = subparsers.add_parser("incremental", help="Run daily incremental sync (safe scaffold stub)")
    incremental.add_argument("--date", help="YYYY-MM-DD; defaults to today")
    incremental.set_defaults(func=cmd_incremental)

    dq = subparsers.add_parser("dq", help="Run data quality checks scaffold")
    dq.add_argument("--partition", help="Partition label, e.g. season=2024")
    dq.set_defaults(func=cmd_dq)

    return parser


def build_config(args: argparse.Namespace) -> IngestConfig:
    request_policy = RequestPolicy(
        timeout_seconds=args.timeout_seconds,
        max_attempts=args.max_attempts,
        initial_backoff_seconds=args.initial_backoff_seconds,
        max_backoff_seconds=args.max_backoff_seconds,
        jitter_seconds=args.jitter_seconds,
        request_budget_per_run=args.request_budget_per_run,
    )
    return IngestConfig(
        db_path=args.db,
        season_start=getattr(args, "season_start", 2020),
        season_end=getattr(args, "season_end", 2025),
        checkpoint_every=args.checkpoint_every,
        request_policy=request_policy,
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
