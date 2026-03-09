#!/usr/bin/env python3
"""Historical MLB ingestion entrypoint (statsapi -> SQLite)."""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

try:
    import statsapi  # type: ignore
except Exception:  # pragma: no cover - exercised through command failures/tests
    statsapi = None


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "mlb_history.db"
DEFAULT_SCHEMA_PATH = ROOT / "scripts" / "sql" / "history_schema.sql"
FINAL_STATUSES = {"Final", "Game Over", "Completed Early"}
RELEVANT_STATUSES = FINAL_STATUSES | {
    "In Progress",
    "Delayed",
    "Delayed Start",
    "Suspended",
    "Postponed",
    "Warmup",
    "Pre-Game",
    "Scheduled",
}
RELEVANT_GAME_TYPES = {"R", "F", "D", "L", "W"}


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
    _ensure_schema_migrations(conn)
    conn.commit()


def _ensure_schema_migrations(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(labels)")}
    if "run_differential" not in columns:
        conn.execute("ALTER TABLE labels ADD COLUMN run_differential INTEGER")
    if "total_runs" not in columns:
        conn.execute("ALTER TABLE labels ADD COLUMN total_runs INTEGER")


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


def upsert_label(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO labels (
          game_id, did_home_win, home_score, away_score, run_differential, total_runs, label_source, settled_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id) DO UPDATE SET
          did_home_win = excluded.did_home_win,
          home_score = excluded.home_score,
          away_score = excluded.away_score,
          run_differential = excluded.run_differential,
          total_runs = excluded.total_runs,
          label_source = excluded.label_source,
          settled_at = excluded.settled_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["did_home_win"],
            row["home_score"],
            row["away_score"],
            row["run_differential"],
            row["total_runs"],
            row.get("label_source", "statsapi"),
            row.get("settled_at"),
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


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value[:10])
    except (TypeError, ValueError):
        return None


def _require_statsapi_available() -> None:
    if statsapi is None:
        raise RuntimeError("statsapi package is unavailable in this environment")


def fetch_schedule_bounded(policy: RequestPolicy, budget: RequestBudget, **kwargs: Any) -> list[dict[str, Any]]:
    _require_statsapi_available()
    result = run_with_bounded_retries(lambda: statsapi.schedule(**kwargs), policy=policy, budget=budget)
    if not isinstance(result, list):
        return []
    return [row for row in result if isinstance(row, dict)]


def is_relevant_game(row: dict[str, Any]) -> bool:
    if _to_int(row.get("game_id")) is None:
        return False
    game_type = str(row.get("game_type") or "").upper()
    if game_type and game_type not in RELEVANT_GAME_TYPES:
        return False
    status = str(row.get("status") or "").strip()
    return not status or status in RELEVANT_STATUSES


def game_row_from_schedule(entry: dict[str, Any], default_season: int | None = None) -> dict[str, Any] | None:
    game_id = _to_int(entry.get("game_id"))
    if game_id is None:
        return None

    scheduled_datetime = entry.get("game_datetime")
    game_date = str(entry.get("game_date") or "")
    if not game_date and scheduled_datetime:
        game_date = str(scheduled_datetime)[:10]

    season = _to_int(entry.get("season"))
    if season is None:
        parsed = _parse_iso_date(game_date)
        season = parsed.year if parsed is not None else default_season
    if season is None or not game_date:
        return None

    home_score = _to_int(entry.get("home_score"))
    away_score = _to_int(entry.get("away_score"))
    home_team_id = _to_int(entry.get("home_id"))
    away_team_id = _to_int(entry.get("away_id"))
    winning_team_id: int | None = None
    if home_score is not None and away_score is not None and home_team_id is not None and away_team_id is not None:
        if home_score > away_score:
            winning_team_id = home_team_id
        elif away_score > home_score:
            winning_team_id = away_team_id

    return {
        "game_id": game_id,
        "season": season,
        "game_date": game_date,
        "game_type": entry.get("game_type"),
        "status": entry.get("status"),
        "scheduled_datetime": scheduled_datetime,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home_score": home_score,
        "away_score": away_score,
        "winning_team_id": winning_team_id,
        "source_updated_at": utc_now(),
    }


def label_row_from_game(game_row: dict[str, Any]) -> dict[str, Any] | None:
    status = str(game_row.get("status") or "").strip()
    if status not in FINAL_STATUSES:
        return None
    home_score = _to_int(game_row.get("home_score"))
    away_score = _to_int(game_row.get("away_score"))
    if home_score is None or away_score is None or home_score == away_score:
        return None
    return {
        "game_id": game_row["game_id"],
        "did_home_win": 1 if home_score > away_score else 0,
        "home_score": home_score,
        "away_score": away_score,
        "run_differential": home_score - away_score,
        "total_runs": home_score + away_score,
        "label_source": "statsapi",
        "settled_at": utc_now(),
    }


def ingest_schedule_partition(
    conn: sqlite3.Connection,
    *,
    job_name: str,
    partition_key: str,
    schedule_rows: list[dict[str, Any]],
    checkpoint_every: int,
    default_season: int | None = None,
) -> tuple[int, int, int | None]:
    processed = 0
    labels = 0
    last_game_id: int | None = None
    filtered_rows = [row for row in schedule_rows if is_relevant_game(row)]
    for entry in filtered_rows:
        game = game_row_from_schedule(entry, default_season=default_season)
        if game is None:
            continue
        upsert_game(conn, game)
        label = label_row_from_game(game)
        if label is not None:
            upsert_label(conn, label)
            labels += 1
        processed += 1
        last_game_id = int(game["game_id"])

        if checkpoint_every > 0 and processed % checkpoint_every == 0:
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor={"processed_games": processed, "labels_upserted": labels},
                status="running",
                last_game_id=last_game_id,
            )
    return processed, labels, last_game_id


def cmd_init_db(args: argparse.Namespace) -> None:
    config = build_config(args)
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "init-db", partition_key=None, config=config)
        finish_run(conn, run_id, "success", note="historical schema initialized")
    print(f"Initialized schema at {config.db_path}")


def cmd_backfill(args: argparse.Namespace) -> None:
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = f"season={args.season}" if args.season else f"range={config.season_start}-{config.season_end}"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=partition_key, config=config)
        total_games = 0
        total_labels = 0
        last_partition_key = partition_key
        try:
            seasons = [args.season] if args.season else list(range(config.season_start, config.season_end + 1))
            for season in seasons:
                last_partition_key = f"season={season}"
                schedule_rows = fetch_schedule_bounded(
                    config.request_policy,
                    budget,
                    season=season,
                    sportId=1,
                )
                processed, labels, last_game_id = ingest_schedule_partition(
                    conn,
                    job_name="backfill",
                    partition_key=last_partition_key,
                    schedule_rows=schedule_rows,
                    checkpoint_every=config.checkpoint_every,
                    default_season=season,
                )
                total_games += processed
                total_labels += labels
                upsert_checkpoint(
                    conn,
                    job_name="backfill",
                    partition_key=last_partition_key,
                    cursor={"season": season, "processed_games": processed, "labels_upserted": labels},
                    status="success",
                    last_game_id=last_game_id,
                )

            finish_run(
                conn,
                run_id,
                "success",
                note=f"games_upserted={total_games}, labels_upserted={total_labels}, odds_historical=disabled",
                request_count=budget.used,
            )
            print(
                f"Backfill complete for {partition_key}: games_upserted={total_games}, "
                f"labels_upserted={total_labels}, request_count={budget.used}"
            )
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name="backfill",
                partition_key=last_partition_key,
                cursor={"partition": last_partition_key},
                status="failed",
                last_error=error,
            )
            finish_run(conn, run_id, "failed", note=error, request_count=budget.used)
            raise


def cmd_incremental(args: argparse.Namespace) -> None:
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    target_date = args.date or datetime.now().date().isoformat()
    partition_key = f"date={target_date}"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "incremental", partition_key=partition_key, config=config)
        try:
            schedule_rows = fetch_schedule_bounded(
                config.request_policy,
                budget,
                start_date=target_date,
                end_date=target_date,
                sportId=1,
            )
            parsed_target_date = _parse_iso_date(target_date)
            default_season = parsed_target_date.year if parsed_target_date else None
            processed, labels, last_game_id = ingest_schedule_partition(
                conn,
                job_name="incremental",
                partition_key=partition_key,
                schedule_rows=schedule_rows,
                checkpoint_every=config.checkpoint_every,
                default_season=default_season,
            )
            upsert_checkpoint(
                conn,
                job_name="incremental",
                partition_key=partition_key,
                cursor={"date": target_date, "processed_games": processed, "labels_upserted": labels},
                status="success",
                last_game_id=last_game_id,
            )
            finish_run(
                conn,
                run_id,
                "success",
                note=f"games_upserted={processed}, labels_upserted={labels}, odds_historical=disabled",
                request_count=budget.used,
            )
            print(
                f"Incremental complete for {partition_key}: games_upserted={processed}, "
                f"labels_upserted={labels}, request_count={budget.used}"
            )
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name="incremental",
                partition_key=partition_key,
                cursor={"date": target_date},
                status="failed",
                last_error=error,
            )
            finish_run(conn, run_id, "failed", note=error, request_count=budget.used)
            raise


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

    backfill = subparsers.add_parser("backfill", help="Backfill historical partitions")
    backfill.add_argument("--season", type=int, help="Single season override")
    backfill.add_argument("--season-start", type=int, default=2020)
    backfill.add_argument("--season-end", type=int, default=2025)
    backfill.set_defaults(func=cmd_backfill)

    incremental = subparsers.add_parser("incremental", help="Run daily incremental sync")
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
