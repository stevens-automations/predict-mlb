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


@dataclass(frozen=True)
class PartitionSnapshot:
    games: int
    labels: int


@dataclass(frozen=True)
class PartitionIngestStats:
    schedule_rows_fetched: int
    relevant_rows_processed: int
    distinct_games_touched: int
    games_inserted: int
    games_updated: int
    labels_inserted: int
    labels_updated: int
    final_distinct_counts_snapshot: PartitionSnapshot

    @property
    def games_upserted(self) -> int:
        return self.relevant_rows_processed

    @property
    def labels_upserted(self) -> int:
        return self.labels_inserted + self.labels_updated

    def to_cursor(self) -> dict[str, Any]:
        return {
            "schedule_rows_fetched": self.schedule_rows_fetched,
            "relevant_rows_processed": self.relevant_rows_processed,
            "distinct_games_touched": self.distinct_games_touched,
            "games_upserted": self.games_upserted,
            "games_inserted": self.games_inserted,
            "games_updated": self.games_updated,
            "labels_upserted": self.labels_upserted,
            "labels_inserted": self.labels_inserted,
            "labels_updated": self.labels_updated,
            "final_distinct_counts_snapshot": asdict(self.final_distinct_counts_snapshot),
        }


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


def _chunked(values: list[int], size: int = 900) -> list[list[int]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _existing_game_ids(conn: sqlite3.Connection, table: str, game_ids: set[int]) -> set[int]:
    if not game_ids:
        return set()
    existing: set[int] = set()
    for chunk in _chunked(sorted(game_ids)):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(f"SELECT game_id FROM {table} WHERE game_id IN ({placeholders})", chunk).fetchall()
        existing.update(int(row["game_id"]) for row in rows)
    return existing


def partition_snapshot(conn: sqlite3.Connection, partition_key: str) -> PartitionSnapshot:
    if partition_key.startswith("season="):
        season = _to_int(partition_key.split("=", 1)[1])
        if season is None:
            return PartitionSnapshot(games=0, labels=0)
        games = conn.execute("SELECT COUNT(*) AS c FROM games WHERE season = ?", (season,)).fetchone()["c"]
        labels = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM labels
            INNER JOIN games ON games.game_id = labels.game_id
            WHERE games.season = ?
            """,
            (season,),
        ).fetchone()["c"]
        return PartitionSnapshot(games=games, labels=labels)
    if partition_key.startswith("date="):
        game_date = partition_key.split("=", 1)[1]
        games = conn.execute("SELECT COUNT(*) AS c FROM games WHERE game_date = ?", (game_date,)).fetchone()["c"]
        labels = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM labels
            INNER JOIN games ON games.game_id = labels.game_id
            WHERE games.game_date = ?
            """,
            (game_date,),
        ).fetchone()["c"]
        return PartitionSnapshot(games=games, labels=labels)
    return PartitionSnapshot(games=0, labels=0)


def merge_partition_snapshots(snapshots: list[PartitionSnapshot]) -> PartitionSnapshot:
    return PartitionSnapshot(
        games=sum(snapshot.games for snapshot in snapshots),
        labels=sum(snapshot.labels for snapshot in snapshots),
    )


def format_run_observability(stats: dict[str, Any]) -> str:
    return json.dumps(stats, sort_keys=True)


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
) -> tuple[PartitionIngestStats, int | None]:
    last_game_id: int | None = None
    filtered_rows = [row for row in schedule_rows if is_relevant_game(row)]
    game_rows: list[dict[str, Any]] = []
    for entry in filtered_rows:
        game = game_row_from_schedule(entry, default_season=default_season)
        if game is not None:
            game_rows.append(game)
    distinct_game_ids = {int(game["game_id"]) for game in game_rows}
    existing_games = _existing_game_ids(conn, "games", distinct_game_ids)
    existing_labels = _existing_game_ids(conn, "labels", distinct_game_ids)
    seen_games: set[int] = set()
    seen_labels: set[int] = set()
    games_inserted = 0
    games_updated = 0
    labels_inserted = 0
    labels_updated = 0
    processed = 0
    for game in game_rows:
        game_id = int(game["game_id"])
        if game_id not in seen_games:
            if game_id in existing_games:
                games_updated += 1
            else:
                games_inserted += 1
            seen_games.add(game_id)
        upsert_game(conn, game)
        label = label_row_from_game(game)
        if label is not None:
            if game_id not in seen_labels:
                if game_id in existing_labels:
                    labels_updated += 1
                else:
                    labels_inserted += 1
                seen_labels.add(game_id)
            upsert_label(conn, label)
        processed += 1
        last_game_id = game_id

        if checkpoint_every > 0 and processed % checkpoint_every == 0:
            checkpoint_stats = PartitionIngestStats(
                schedule_rows_fetched=len(schedule_rows),
                relevant_rows_processed=processed,
                distinct_games_touched=len(seen_games),
                games_inserted=games_inserted,
                games_updated=games_updated,
                labels_inserted=labels_inserted,
                labels_updated=labels_updated,
                final_distinct_counts_snapshot=partition_snapshot(conn, partition_key),
            )
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor=checkpoint_stats.to_cursor(),
                status="running",
                last_game_id=last_game_id,
            )
    final_stats = PartitionIngestStats(
        schedule_rows_fetched=len(schedule_rows),
        relevant_rows_processed=processed,
        distinct_games_touched=len(seen_games),
        games_inserted=games_inserted,
        games_updated=games_updated,
        labels_inserted=labels_inserted,
        labels_updated=labels_updated,
        final_distinct_counts_snapshot=partition_snapshot(conn, partition_key),
    )
    return final_stats, last_game_id


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
        total_schedule_rows_fetched = 0
        total_relevant_rows_processed = 0
        total_distinct_games_touched = 0
        total_games_inserted = 0
        total_games_updated = 0
        total_labels_inserted = 0
        total_labels_updated = 0
        partition_snapshots: list[PartitionSnapshot] = []
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
                stats, last_game_id = ingest_schedule_partition(
                    conn,
                    job_name="backfill",
                    partition_key=last_partition_key,
                    schedule_rows=schedule_rows,
                    checkpoint_every=config.checkpoint_every,
                    default_season=season,
                )
                total_schedule_rows_fetched += stats.schedule_rows_fetched
                total_relevant_rows_processed += stats.relevant_rows_processed
                total_distinct_games_touched += stats.distinct_games_touched
                total_games_inserted += stats.games_inserted
                total_games_updated += stats.games_updated
                total_labels_inserted += stats.labels_inserted
                total_labels_updated += stats.labels_updated
                partition_snapshots.append(stats.final_distinct_counts_snapshot)
                upsert_checkpoint(
                    conn,
                    job_name="backfill",
                    partition_key=last_partition_key,
                    cursor={"season": season, **stats.to_cursor()},
                    status="success",
                    last_game_id=last_game_id,
                )
            run_stats = {
                "schedule_rows_fetched": total_schedule_rows_fetched,
                "relevant_rows_processed": total_relevant_rows_processed,
                "distinct_games_touched": total_distinct_games_touched,
                "games_upserted": total_relevant_rows_processed,
                "games_inserted": total_games_inserted,
                "games_updated": total_games_updated,
                "labels_upserted": total_labels_inserted + total_labels_updated,
                "labels_inserted": total_labels_inserted,
                "labels_updated": total_labels_updated,
                "final_distinct_counts_snapshot": asdict(merge_partition_snapshots(partition_snapshots)),
                "odds_historical": "disabled",
            }

            finish_run(
                conn,
                run_id,
                "success",
                note=format_run_observability(run_stats),
                request_count=budget.used,
            )
            print(f"Backfill complete for {partition_key}: {format_run_observability({**run_stats, 'request_count': budget.used})}")
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
            stats, last_game_id = ingest_schedule_partition(
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
                cursor={"date": target_date, **stats.to_cursor()},
                status="success",
                last_game_id=last_game_id,
            )
            run_stats = {
                "schedule_rows_fetched": stats.schedule_rows_fetched,
                "relevant_rows_processed": stats.relevant_rows_processed,
                "distinct_games_touched": stats.distinct_games_touched,
                "games_upserted": stats.games_upserted,
                "games_inserted": stats.games_inserted,
                "games_updated": stats.games_updated,
                "labels_upserted": stats.labels_upserted,
                "labels_inserted": stats.labels_inserted,
                "labels_updated": stats.labels_updated,
                "final_distinct_counts_snapshot": asdict(stats.final_distinct_counts_snapshot),
                "odds_historical": "disabled",
            }
            finish_run(
                conn,
                run_id,
                "success",
                note=format_run_observability(run_stats),
                request_count=budget.used,
            )
            print(
                f"Incremental complete for {partition_key}: {format_run_observability({**run_stats, 'request_count': budget.used})}"
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
