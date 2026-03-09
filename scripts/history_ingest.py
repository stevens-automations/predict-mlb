#!/usr/bin/env python3
"""Historical MLB ingestion entrypoint (statsapi -> SQLite)."""

from __future__ import annotations

import argparse
import collections
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
FEATURE_VERSION_V1 = "v1"
MIN_SUPPORTED_SEASON = 2020
MAX_SUPPORTED_SEASON = 2025


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


def validate_supported_season(season: int) -> int:
    if not (MIN_SUPPORTED_SEASON <= season <= MAX_SUPPORTED_SEASON):
        raise ValueError(
            f"season must be between {MIN_SUPPORTED_SEASON} and {MAX_SUPPORTED_SEASON} (got {season})"
        )
    return season


def pitcher_context_job_name(season: int) -> str:
    return f"pitcher-context-{season}"


def feature_rows_job_name(season: int, feature_version: str) -> str:
    return f"feature-rows-{feature_version}-{season}"


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

    pitcher_columns = {row["name"] for row in conn.execute("PRAGMA table_info(game_pitcher_context)")}
    pitcher_migrations = {
        "probable_pitcher_id": "INTEGER",
        "probable_pitcher_name": "TEXT",
        "probable_pitcher_known": "INTEGER NOT NULL DEFAULT 0 CHECK(probable_pitcher_known IN (0, 1))",
        "season_era": "REAL",
        "season_whip": "REAL",
        "season_avg_allowed": "REAL",
        "season_runs_per_9": "REAL",
        "season_strike_pct": "REAL",
        "season_win_pct": "REAL",
        "career_era": "REAL",
        "stats_source": "TEXT",
        "stats_as_of_date": "TEXT",
        "season_stats_scope": "TEXT",
        "season_stats_leakage_risk": "INTEGER NOT NULL DEFAULT 1 CHECK(season_stats_leakage_risk IN (0, 1))",
    }
    for col, col_type in pitcher_migrations.items():
        if col not in pitcher_columns:
            conn.execute(f"ALTER TABLE game_pitcher_context ADD COLUMN {col} {col_type}")


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


def upsert_game_team_stats(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO game_team_stats (
          game_id, team_id, side, runs, hits, errors, batting_avg, obp, slg, ops, strikeouts, walks, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, team_id) DO UPDATE SET
          side = excluded.side,
          runs = excluded.runs,
          hits = excluded.hits,
          errors = excluded.errors,
          batting_avg = excluded.batting_avg,
          obp = excluded.obp,
          slg = excluded.slg,
          ops = excluded.ops,
          strikeouts = excluded.strikeouts,
          walks = excluded.walks,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["team_id"],
            row["side"],
            row.get("runs"),
            row.get("hits"),
            row.get("errors"),
            row.get("batting_avg"),
            row.get("obp"),
            row.get("slg"),
            row.get("ops"),
            row.get("strikeouts"),
            row.get("walks"),
            row.get("source_updated_at"),
        ),
    )
    conn.commit()


def upsert_game_pitcher_context(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO game_pitcher_context (
          game_id, side, pitcher_id, pitcher_name,
          probable_pitcher_id, probable_pitcher_name, probable_pitcher_known,
          season_era, season_whip, season_avg_allowed, season_runs_per_9, season_strike_pct, season_win_pct,
          career_era, stats_source, stats_as_of_date, season_stats_scope, season_stats_leakage_risk,
          source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, side) DO UPDATE SET
          pitcher_id = excluded.pitcher_id,
          pitcher_name = excluded.pitcher_name,
          probable_pitcher_id = excluded.probable_pitcher_id,
          probable_pitcher_name = excluded.probable_pitcher_name,
          probable_pitcher_known = excluded.probable_pitcher_known,
          season_era = excluded.season_era,
          season_whip = excluded.season_whip,
          season_avg_allowed = excluded.season_avg_allowed,
          season_runs_per_9 = excluded.season_runs_per_9,
          season_strike_pct = excluded.season_strike_pct,
          season_win_pct = excluded.season_win_pct,
          career_era = excluded.career_era,
          stats_source = excluded.stats_source,
          stats_as_of_date = excluded.stats_as_of_date,
          season_stats_scope = excluded.season_stats_scope,
          season_stats_leakage_risk = excluded.season_stats_leakage_risk,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["side"],
            row.get("pitcher_id"),
            row.get("pitcher_name"),
            row.get("probable_pitcher_id"),
            row.get("probable_pitcher_name"),
            row.get("probable_pitcher_known", 0),
            row.get("season_era"),
            row.get("season_whip"),
            row.get("season_avg_allowed"),
            row.get("season_runs_per_9"),
            row.get("season_strike_pct"),
            row.get("season_win_pct"),
            row.get("career_era"),
            row.get("stats_source"),
            row.get("stats_as_of_date"),
            row.get("season_stats_scope"),
            row.get("season_stats_leakage_risk", 1),
            row.get("source_updated_at") or utc_now(),
        ),
    )
    conn.commit()


def upsert_feature_row(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        DELETE FROM feature_rows
        WHERE game_id = ? AND feature_version = ? AND as_of_ts <> ?
        """,
        (row["game_id"], row["feature_version"], row["as_of_ts"]),
    )
    conn.execute(
        """
        INSERT INTO feature_rows (
          game_id, feature_version, as_of_ts, feature_payload_json,
          source_contract_status, source_contract_issues_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, feature_version, as_of_ts) DO UPDATE SET
          feature_payload_json = excluded.feature_payload_json,
          source_contract_status = excluded.source_contract_status,
          source_contract_issues_json = excluded.source_contract_issues_json,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["feature_version"],
            row["as_of_ts"],
            row["feature_payload_json"],
            row.get("source_contract_status", "valid"),
            row.get("source_contract_issues_json"),
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


def _existing_game_team_stat_keys(conn: sqlite3.Connection, game_ids: set[int]) -> set[tuple[int, int]]:
    if not game_ids:
        return set()
    keys: set[tuple[int, int]] = set()
    for chunk in _chunked(sorted(game_ids)):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT game_id, team_id FROM game_team_stats WHERE game_id IN ({placeholders})",
            chunk,
        ).fetchall()
        keys.update((int(row["game_id"]), int(row["team_id"])) for row in rows)
    return keys


def _completed_game_ids_for_season(conn: sqlite3.Connection, season: int, limit: int | None = None) -> list[int]:
    statuses = sorted(FINAL_STATUSES)
    placeholders = ",".join("?" for _ in statuses)
    sql = f"""
        SELECT game_id
        FROM games
        WHERE season = ? AND status IN ({placeholders})
        ORDER BY game_date, game_id
    """
    params: list[Any] = [season, *statuses]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [int(row["game_id"]) for row in rows]


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


def fetch_lookup_player_bounded(name: str, season: int, policy: RequestPolicy, budget: RequestBudget) -> list[dict[str, Any]]:
    _require_statsapi_available()
    result = run_with_bounded_retries(
        lambda: statsapi.lookup_player(name, season=str(season)), policy=policy, budget=budget
    )
    if not isinstance(result, list):
        return []
    return [row for row in result if isinstance(row, dict)]


def fetch_player_stat_data_bounded(player_id: int, stat_type: str, policy: RequestPolicy, budget: RequestBudget) -> dict[str, Any]:
    _require_statsapi_available()
    result = run_with_bounded_retries(
        lambda: statsapi.player_stat_data(player_id, group="pitching", type=stat_type),
        policy=policy,
        budget=budget,
    )
    return result if isinstance(result, dict) else {}


def fetch_boxscore_bounded(game_id: int, policy: RequestPolicy, budget: RequestBudget) -> dict[str, Any]:
    _require_statsapi_available()
    result = run_with_bounded_retries(lambda: statsapi.boxscore_data(game_id), policy=policy, budget=budget)
    if not isinstance(result, dict):
        return {}
    return result


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_int(stats: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        if key in stats:
            parsed = _to_int(stats.get(key))
            if parsed is not None:
                return parsed
    return None


def _extract_float(stats: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in stats:
            parsed = _to_float(stats.get(key))
            if parsed is not None:
                return parsed
    return None


def _team_stats_row_from_boxscore(game_id: int, side: str, boxscore: dict[str, Any]) -> dict[str, Any] | None:
    side_data = boxscore.get(side)
    if not isinstance(side_data, dict):
        return None
    team = side_data.get("team")
    if not isinstance(team, dict):
        return None
    team_id = _to_int(team.get("id"))
    if team_id is None:
        return None
    team_stats = side_data.get("teamStats")
    if not isinstance(team_stats, dict):
        return None
    batting = team_stats.get("batting") if isinstance(team_stats.get("batting"), dict) else {}
    pitching = team_stats.get("pitching") if isinstance(team_stats.get("pitching"), dict) else {}
    fielding = team_stats.get("fielding") if isinstance(team_stats.get("fielding"), dict) else {}

    obp = _extract_float(batting, "obp")
    slg = _extract_float(batting, "slg")
    ops = _extract_float(batting, "ops")
    if ops is None and obp is not None and slg is not None:
        ops = obp + slg

    return {
        "game_id": game_id,
        "team_id": team_id,
        "side": side,
        "runs": _extract_int(batting, "runs"),
        "hits": _extract_int(batting, "hits"),
        "errors": _extract_int(fielding, "errors"),
        "batting_avg": _extract_float(batting, "avg", "battingAverage"),
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "strikeouts": _extract_int(batting, "strikeOuts", "strikeouts")
        or _extract_int(pitching, "strikeOuts", "strikeouts"),
        "walks": _extract_int(batting, "baseOnBalls", "walks")
        or _extract_int(pitching, "baseOnBalls", "walks"),
        "source_updated_at": utc_now(),
    }


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


def _to_float(value: Any) -> float | None:
    if value in (None, "", "--", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _innings_to_outs(value: Any) -> int:
    if value in (None, "", "--", "-"):
        return 0
    text = str(value).strip()
    if not text:
        return 0
    if "." not in text:
        whole = _to_int(text)
        return 0 if whole is None else max(0, whole * 3)
    whole_str, frac_str = text.split(".", 1)
    whole = _to_int(whole_str) or 0
    frac = _to_int(frac_str) or 0
    frac_outs = 1 if frac == 1 else 2 if frac == 2 else 0
    return max(0, whole * 3 + frac_outs)


def _safe_round(value: float | None, digits: int = 3) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _safe_div(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _derive_rate_stats_from_pitcher_aggregate(aggregate: dict[str, Any]) -> dict[str, float | None]:
    outs = int(aggregate.get("outs", 0) or 0)
    innings = outs / 3.0
    hits = float(aggregate.get("hits", 0) or 0)
    walks = float(aggregate.get("walks", 0) or 0)
    earned_runs = float(aggregate.get("earned_runs", 0) or 0)
    runs = float(aggregate.get("runs", 0) or 0)
    at_bats = float(aggregate.get("at_bats", 0) or 0)
    strikes = float(aggregate.get("strikes", 0) or 0)
    pitches = float(aggregate.get("pitches", 0) or 0)
    wins = float(aggregate.get("wins", 0) or 0)
    losses = float(aggregate.get("losses", 0) or 0)

    return {
        "season_era": _safe_round(_safe_div(earned_runs * 9.0, innings), 3),
        "season_whip": _safe_round(_safe_div(hits + walks, innings), 3),
        "season_avg_allowed": _safe_round(_safe_div(hits, at_bats), 3),
        "season_runs_per_9": _safe_round(_safe_div(runs * 9.0, innings), 3),
        "season_strike_pct": _safe_round(_safe_div(strikes, pitches), 3),
        "season_win_pct": _safe_round(_safe_div(wins, wins + losses), 3),
    }


def _extract_pitcher_decisions(boxscore: dict[str, Any]) -> tuple[int | None, int | None]:
    candidates = []
    for key in ("decisions", "decisionMakers"):
        payload = boxscore.get(key)
        if isinstance(payload, dict):
            candidates.append(payload)
    for payload in candidates:
        winner = payload.get("winner") or payload.get("winningPitcher")
        loser = payload.get("loser") or payload.get("losingPitcher")
        winner_id = _to_int(winner.get("id")) if isinstance(winner, dict) else None
        loser_id = _to_int(loser.get("id")) if isinstance(loser, dict) else None
        if winner_id is not None or loser_id is not None:
            return winner_id, loser_id
    return None, None


def _iter_boxscore_pitching_lines(boxscore: dict[str, Any]) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    for side in ("home", "away"):
        side_payload = boxscore.get(side)
        if not isinstance(side_payload, dict):
            continue
        players = side_payload.get("players")
        if not isinstance(players, dict):
            continue
        for player_payload in players.values():
            if not isinstance(player_payload, dict):
                continue
            person = player_payload.get("person") if isinstance(player_payload.get("person"), dict) else {}
            stats = player_payload.get("stats") if isinstance(player_payload.get("stats"), dict) else {}
            pitching = stats.get("pitching") if isinstance(stats.get("pitching"), dict) else {}
            pitcher_id = _to_int(person.get("id")) or _to_int(player_payload.get("id"))
            outs = _innings_to_outs(pitching.get("inningsPitched"))
            if pitcher_id is None or (not pitching and outs == 0):
                continue
            lines.append(
                {
                    "pitcher_id": pitcher_id,
                    "pitcher_name": person.get("fullName") or player_payload.get("name"),
                    "outs": outs,
                    "hits": _to_int(pitching.get("hits")) or 0,
                    "walks": _to_int(pitching.get("baseOnBalls")) or _to_int(pitching.get("walks")) or 0,
                    "earned_runs": _to_int(pitching.get("earnedRuns")) or 0,
                    "runs": _to_int(pitching.get("runs")) or 0,
                    "at_bats": _to_int(pitching.get("atBats")) or 0,
                    "strikes": _to_int(pitching.get("strikes")) or 0,
                    "pitches": _to_int(pitching.get("numberOfPitches")) or _to_int(pitching.get("pitches")) or 0,
                }
            )
    return lines


def _update_pitcher_aggregate_from_boxscore(
    aggregates: dict[int, dict[str, Any]],
    boxscore: dict[str, Any],
) -> None:
    winner_id, loser_id = _extract_pitcher_decisions(boxscore)
    for line in _iter_boxscore_pitching_lines(boxscore):
        pitcher_id = int(line["pitcher_id"])
        bucket = aggregates.setdefault(
            pitcher_id,
            {
                "pitcher_name": line.get("pitcher_name"),
                "appearances": 0,
                "outs": 0,
                "hits": 0,
                "walks": 0,
                "earned_runs": 0,
                "runs": 0,
                "at_bats": 0,
                "strikes": 0,
                "pitches": 0,
                "wins": 0,
                "losses": 0,
            },
        )
        bucket["pitcher_name"] = bucket.get("pitcher_name") or line.get("pitcher_name")
        bucket["appearances"] += 1
        for key in ("outs", "hits", "walks", "earned_runs", "runs", "at_bats", "strikes", "pitches"):
            bucket[key] += int(line.get(key, 0) or 0)
        if pitcher_id == winner_id:
            bucket["wins"] += 1
        if pitcher_id == loser_id:
            bucket["losses"] += 1


def _is_completed_game(status: Any) -> bool:
    return str(status or "").strip() in FINAL_STATUSES


def _feature_as_of_ts(game_row: sqlite3.Row | dict[str, Any]) -> str:
    scheduled_datetime = game_row["scheduled_datetime"] if isinstance(game_row, sqlite3.Row) else game_row.get("scheduled_datetime")
    if scheduled_datetime:
        return str(scheduled_datetime)
    game_date = game_row["game_date"] if isinstance(game_row, sqlite3.Row) else game_row.get("game_date")
    return f"{game_date}T00:00:00Z"


def _existing_pitcher_identity_rows(conn: sqlite3.Connection, season: int) -> dict[int, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          g.game_id,
          p.side,
          p.probable_pitcher_id,
          p.probable_pitcher_name
        FROM games g
        LEFT JOIN game_pitcher_context p ON p.game_id = g.game_id
        WHERE g.season = ?
        ORDER BY g.game_date, g.game_id
        """,
        (season,),
    ).fetchall()
    out: dict[int, dict[str, Any]] = {}
    for row in rows:
        game_id = int(row["game_id"])
        bucket = out.setdefault(game_id, {})
        side = str(row["side"] or "").strip()
        if side not in {"home", "away"}:
            continue
        bucket[f"{side}_probable_pitcher_id"] = _to_int(row["probable_pitcher_id"])
        bucket[f"{side}_probable_pitcher"] = row["probable_pitcher_name"]
    return out


def build_pitcher_context_rows(
    game_id: int,
    game_date: str,
    game_schedule_row: dict[str, Any],
    season: int,
    lookup_cache: dict[str, int | None],
    prior_pitcher_aggregates: dict[int, dict[str, Any]],
    policy: RequestPolicy,
    budget: RequestBudget,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for side in ("home", "away"):
        probable_name = game_schedule_row.get(f"{side}_probable_pitcher")
        probable_name = probable_name.strip() if isinstance(probable_name, str) else None
        probable_id = _to_int(game_schedule_row.get(f"{side}_probable_pitcher_id"))
        if probable_name and probable_id is None and statsapi is not None:
            if probable_name not in lookup_cache:
                candidates = fetch_lookup_player_bounded(probable_name, season, policy, budget)
                lookup_cache[probable_name] = _to_int(candidates[0].get("id")) if candidates else None
            probable_id = lookup_cache[probable_name]

        aggregate = prior_pitcher_aggregates.get(probable_id or -1)
        derived_stats = _derive_rate_stats_from_pitcher_aggregate(aggregate) if aggregate else {}
        has_prior_pitching = bool(aggregate and int(aggregate.get("outs", 0) or 0) > 0)
        probable_known = 1 if probable_name else 0
        if probable_known and has_prior_pitching:
            stats_source = "statsapi.schedule+statsapi.lookup_player+statsapi.boxscore_data(prior_completed_games_only)"
        elif probable_known:
            stats_source = "leakage_safe_null_fallback(probable_pitcher_identity_without_prior_completed_pitching)"
        else:
            stats_source = "no_probable_pitcher_identity_available"

        rows.append(
            {
                "game_id": game_id,
                "side": side,
                "pitcher_id": probable_id,
                "pitcher_name": (aggregate or {}).get("pitcher_name") or probable_name,
                "probable_pitcher_id": probable_id,
                "probable_pitcher_name": probable_name,
                "probable_pitcher_known": probable_known,
                "season_era": derived_stats.get("season_era"),
                "season_whip": derived_stats.get("season_whip"),
                "season_avg_allowed": derived_stats.get("season_avg_allowed"),
                "season_runs_per_9": derived_stats.get("season_runs_per_9"),
                "season_strike_pct": derived_stats.get("season_strike_pct"),
                "season_win_pct": derived_stats.get("season_win_pct"),
                "career_era": None,
                "stats_source": stats_source,
                "stats_as_of_date": game_date,
                "season_stats_scope": "season_to_date_prior_completed_games" if probable_known else None,
                "season_stats_leakage_risk": 0,
                "source_updated_at": utc_now(),
            }
        )
        if probable_name and not has_prior_pitching:
            rows[-1]["pitcher_name"] = probable_name
    return rows


def cmd_backfill_pitcher_context(args: argparse.Namespace) -> None:
    season = validate_supported_season(args.season)
    job_name = pitcher_context_job_name(season)
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = f"season={season}"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=f"{job_name}:{partition_key}", config=config)
        last_game_id: int | None = None
        try:
            games_for_season = conn.execute(
                "SELECT game_id, game_date FROM games WHERE season=? ORDER BY game_date, game_id",
                (season,),
            ).fetchall()
            game_ids = [int(row["game_id"]) for row in games_for_season]
            if not game_ids:
                note = format_run_observability({"job": job_name, "season": season, "games_seen": 0})
                upsert_checkpoint(
                    conn,
                    job_name=job_name,
                    partition_key=partition_key,
                    cursor={"season": season, "games_seen": 0, "rows_upserted": 0},
                    status="success",
                )
                finish_run(conn, run_id, "success", note=note, request_count=budget.used)
                print(f"Pitcher context backfill complete for {partition_key}: {note}")
                return

            existing_identity_by_game_id = _existing_pitcher_identity_rows(conn, season)
            schedule_fallback_used = False
            try:
                schedule_rows = fetch_schedule_bounded(config.request_policy, budget, season=season, sportId=1)
                schedule_by_game_id = {
                    _to_int(row.get("game_id")): row for row in schedule_rows if _to_int(row.get("game_id")) is not None
                }
            except Exception:
                schedule_by_game_id = {}
                schedule_fallback_used = True
            if existing_identity_by_game_id:
                for game_id, existing in existing_identity_by_game_id.items():
                    merged = dict(existing)
                    merged.update(schedule_by_game_id.get(game_id, {}))
                    schedule_by_game_id[game_id] = merged
            if not schedule_by_game_id and game_ids:
                raise RuntimeError(
                    f"unable to source probable pitcher identities for season {season} from statsapi or existing DB rows"
                )

            lookup_cache: dict[str, int | None] = {}
            prior_pitcher_aggregates: dict[int, dict[str, Any]] = {}
            boxscore_cache: dict[int, dict[str, Any]] = {}
            boxscore_fallback_used = False
            rows_upserted = 0
            for idx, db_game in enumerate(games_for_season, start=1):
                game_id = int(db_game["game_id"])
                game_date = str(db_game["game_date"])
                schedule_row = schedule_by_game_id.get(game_id, {})
                context_rows = build_pitcher_context_rows(
                    game_id,
                    game_date,
                    schedule_row,
                    season,
                    lookup_cache,
                    prior_pitcher_aggregates,
                    config.request_policy,
                    budget,
                )
                for row in context_rows:
                    upsert_game_pitcher_context(conn, row)
                    rows_upserted += 1
                status_row = conn.execute("SELECT status FROM games WHERE game_id = ?", (game_id,)).fetchone()
                if status_row is not None and _is_completed_game(status_row["status"]):
                    try:
                        if game_id not in boxscore_cache:
                            boxscore_cache[game_id] = fetch_boxscore_bounded(game_id, config.request_policy, budget)
                        _update_pitcher_aggregate_from_boxscore(prior_pitcher_aggregates, boxscore_cache[game_id])
                    except Exception:
                        boxscore_fallback_used = True
                last_game_id = game_id
                if config.checkpoint_every > 0 and idx % config.checkpoint_every == 0:
                    upsert_checkpoint(
                        conn,
                        job_name=job_name,
                        partition_key=partition_key,
                        cursor={"season": season, "games_seen": idx, "rows_upserted": rows_upserted},
                        status="running",
                        last_game_id=last_game_id,
                    )

            cursor = {
                "season": season,
                "games_seen": len(games_for_season),
                "rows_upserted": rows_upserted,
                "distinct_pitchers_cached": len(prior_pitcher_aggregates),
                "schedule_fallback_used": schedule_fallback_used,
                "boxscore_fallback_used": boxscore_fallback_used,
            }
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor=cursor,
                status="success",
                last_game_id=last_game_id,
            )
            note = format_run_observability({"job": job_name, **cursor})
            finish_run(conn, run_id, "success", note=note, request_count=budget.used)
            print(f"Pitcher context backfill complete for {partition_key}: {note}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor={"season": season},
                status="failed",
                last_error=error,
                last_game_id=last_game_id,
            )
            finish_run(conn, run_id, "failed", note=error, request_count=budget.used)
            raise


def _build_team_feature_block(team_state: dict[str, Any] | None, game_date: str) -> dict[str, Any]:
    team_state = team_state or {}
    season_games = int(team_state.get("games", 0) or 0)
    rolling_games = int(len(team_state.get("rolling", ())))
    season_runs_for = float(team_state.get("runs_for", 0) or 0)
    season_runs_against = float(team_state.get("runs_against", 0) or 0)
    season_wins = float(team_state.get("wins", 0) or 0)
    rolling = list(team_state.get("rolling", ()))
    rolling_wins = sum(int(item.get("win", 0) or 0) for item in rolling)
    last_completed = _parse_iso_date(team_state.get("last_completed_game_date")) if team_state.get("last_completed_game_date") else None
    current_date = _parse_iso_date(game_date)
    days_rest: int | None = None
    doubleheader_flag = 0
    if last_completed is not None and current_date is not None:
        delta_days = (current_date - last_completed).days
        days_rest = max(delta_days - 1, 0)
        doubleheader_flag = 1 if delta_days == 0 else 0

    def rolling_avg(key: str) -> float | None:
        if not rolling:
            return None
        values = [float(item[key]) for item in rolling if item.get(key) is not None]
        if not values:
            return None
        return _safe_round(sum(values) / len(values), 3)

    return {
        "strength_available": 1 if season_games > 0 else 0,
        "season_games": season_games,
        "season_win_pct": _safe_round(_safe_div(season_wins, season_games), 3),
        "season_run_diff_per_game": _safe_round(_safe_div(season_runs_for - season_runs_against, season_games), 3),
        "rolling_available": 1 if rolling_games > 0 else 0,
        "rolling_games": rolling_games,
        "rolling_last10_win_pct": _safe_round(_safe_div(float(rolling_wins), rolling_games), 3),
        "rolling_last10_runs_for_per_game": rolling_avg("runs_for"),
        "rolling_last10_runs_against_per_game": rolling_avg("runs_against"),
        "rolling_last10_hits_per_game": rolling_avg("hits"),
        "rolling_last10_ops": rolling_avg("ops"),
        "rolling_last10_obp": rolling_avg("obp"),
        "rolling_last10_batting_avg": rolling_avg("batting_avg"),
        "days_rest": days_rest,
        "doubleheader_flag": doubleheader_flag,
    }


def _build_pitcher_feature_block(row: sqlite3.Row | None) -> tuple[dict[str, Any], list[str]]:
    if row is None:
        return {
            "starter_known": 0,
            "starter_stats_available": 0,
            "starter_id": None,
            "starter_era": None,
            "starter_whip": None,
            "starter_avg_allowed": None,
            "starter_runs_per_9": None,
            "starter_strike_pct": None,
            "starter_win_pct": None,
            "starter_career_era": None,
        }, ["missing_pitcher_context"]

    stats_available = int(any(row[field] is not None for field in ("season_era", "season_whip", "season_avg_allowed", "season_runs_per_9", "season_strike_pct", "season_win_pct")))
    issues: list[str] = []
    if int(row["probable_pitcher_known"] or 0) and not stats_available:
        issues.append("starter_stats_unavailable")
    return {
        "starter_known": int(row["probable_pitcher_known"] or 0),
        "starter_stats_available": stats_available,
        "starter_id": row["probable_pitcher_id"],
        "starter_era": row["season_era"],
        "starter_whip": row["season_whip"],
        "starter_avg_allowed": row["season_avg_allowed"],
        "starter_runs_per_9": row["season_runs_per_9"],
        "starter_strike_pct": row["season_strike_pct"],
        "starter_win_pct": row["season_win_pct"],
        "starter_career_era": row["career_era"],
    }, issues


def _update_team_state(
    team_states: dict[int, dict[str, Any]],
    team_id: int,
    game_date: str,
    won: int,
    runs_for: int,
    runs_against: int,
    team_stats_row: sqlite3.Row | None,
) -> None:
    state = team_states.setdefault(
        team_id,
        {
            "games": 0,
            "wins": 0,
            "runs_for": 0,
            "runs_against": 0,
            "rolling": collections.deque(maxlen=10),
            "last_completed_game_date": None,
        },
    )

    state["games"] += 1
    state["wins"] += int(won)
    state["runs_for"] += runs_for
    state["runs_against"] += runs_against
    state["last_completed_game_date"] = game_date
    state["rolling"].append(
        {
            "win": int(won),
            "runs_for": runs_for,
            "runs_against": runs_against,
            "hits": team_stats_row["hits"] if team_stats_row is not None else None,
            "ops": team_stats_row["ops"] if team_stats_row is not None else None,
            "obp": team_stats_row["obp"] if team_stats_row is not None else None,
            "batting_avg": team_stats_row["batting_avg"] if team_stats_row is not None else None,
        }
    )


def cmd_materialize_feature_rows(args: argparse.Namespace) -> None:
    season = validate_supported_season(args.season)
    job_name = feature_rows_job_name(season, args.feature_version)
    config = build_config(args)
    partition_key = f"feature-rows-season={season}:version={args.feature_version}"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=partition_key, config=config)
        last_game_id: int | None = None
        try:
            games = conn.execute(
                """
                SELECT game_id, season, game_date, scheduled_datetime, status, home_team_id, away_team_id
                FROM games
                WHERE season = ?
                ORDER BY game_date, game_id
                """,
                (season,),
            ).fetchall()
            team_stats_rows = conn.execute(
                """
                SELECT game_team_stats.game_id, game_team_stats.side, game_team_stats.hits,
                       game_team_stats.batting_avg, game_team_stats.obp, game_team_stats.ops
                FROM game_team_stats
                INNER JOIN games ON games.game_id = game_team_stats.game_id
                WHERE games.season = ?
                """,
                (season,),
            ).fetchall()
            pitcher_rows = conn.execute(
                """
                SELECT game_pitcher_context.*
                FROM game_pitcher_context
                INNER JOIN games ON games.game_id = game_pitcher_context.game_id
                WHERE games.season = ?
                """,
                (season,),
            ).fetchall()
            labels = {
                int(row["game_id"]): row
                for row in conn.execute(
                    """
                    SELECT labels.*
                    FROM labels
                    INNER JOIN games ON games.game_id = labels.game_id
                    WHERE games.season = ?
                    """,
                    (season,),
                ).fetchall()
            }

            team_stats_by_key = {(int(row["game_id"]), str(row["side"])): row for row in team_stats_rows}
            pitcher_by_key = {(int(row["game_id"]), str(row["side"])): row for row in pitcher_rows}
            team_states: dict[int, dict[str, Any]] = {}
            rows_upserted = 0

            for idx, game in enumerate(games, start=1):
                issues: list[str] = []
                home_team_id = int(game["home_team_id"]) if game["home_team_id"] is not None else None
                away_team_id = int(game["away_team_id"]) if game["away_team_id"] is not None else None
                home_state = team_states.get(home_team_id or -1)
                away_state = team_states.get(away_team_id or -1)
                home_pitcher, home_pitcher_issues = _build_pitcher_feature_block(pitcher_by_key.get((int(game["game_id"]), "home")))
                away_pitcher, away_pitcher_issues = _build_pitcher_feature_block(pitcher_by_key.get((int(game["game_id"]), "away")))
                issues.extend(f"home_{issue}" for issue in home_pitcher_issues)
                issues.extend(f"away_{issue}" for issue in away_pitcher_issues)

                payload = {
                    "game_id": int(game["game_id"]),
                    "season": int(game["season"]),
                    "game_date": str(game["game_date"]),
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "home_field_advantage": 1,
                }
                for prefix, block in (
                    ("home", _build_team_feature_block(home_state, str(game["game_date"]))),
                    ("away", _build_team_feature_block(away_state, str(game["game_date"]))),
                ):
                    for key, value in block.items():
                        payload[f"{prefix}_team_{key}"] = value
                for prefix, block in (("home", home_pitcher), ("away", away_pitcher)):
                    for key, value in block.items():
                        payload[f"{prefix}_{key}"] = value

                upsert_feature_row(
                    conn,
                    {
                        "game_id": int(game["game_id"]),
                        "feature_version": args.feature_version,
                        "as_of_ts": _feature_as_of_ts(game),
                        "feature_payload_json": json.dumps(payload, sort_keys=True),
                        "source_contract_status": "valid" if not issues else "degraded",
                        "source_contract_issues_json": json.dumps(sorted(issues)) if issues else None,
                    },
                )
                rows_upserted += 1
                last_game_id = int(game["game_id"])

                label = labels.get(last_game_id)
                if (
                    label is not None
                    and home_team_id is not None
                    and away_team_id is not None
                    and _is_completed_game(game["status"])
                ):
                    home_team_stats = team_stats_by_key.get((last_game_id, "home"))
                    away_team_stats = team_stats_by_key.get((last_game_id, "away"))
                    _update_team_state(
                        team_states,
                        home_team_id,
                        str(game["game_date"]),
                        int(label["did_home_win"] or 0),
                        int(label["home_score"] or 0),
                        int(label["away_score"] or 0),
                        home_team_stats,
                    )
                    _update_team_state(
                        team_states,
                        away_team_id,
                        str(game["game_date"]),
                        1 - int(label["did_home_win"] or 0),
                        int(label["away_score"] or 0),
                        int(label["home_score"] or 0),
                        away_team_stats,
                    )

                if config.checkpoint_every > 0 and idx % config.checkpoint_every == 0:
                    upsert_checkpoint(
                        conn,
                        job_name=job_name,
                        partition_key=partition_key,
                        cursor={"season": season, "feature_version": args.feature_version, "games_seen": idx, "rows_upserted": rows_upserted},
                        status="running",
                        last_game_id=last_game_id,
                    )

            cursor = {
                "season": season,
                "feature_version": args.feature_version,
                "games_seen": len(games),
                "rows_upserted": rows_upserted,
            }
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor=cursor,
                status="success",
                last_game_id=last_game_id,
            )
            note = format_run_observability({"job": job_name, **cursor})
            finish_run(conn, run_id, "success", note=note, request_count=0)
            print(f"Feature row materialization complete for {partition_key}: {note}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor={"season": season, "feature_version": args.feature_version},
                status="failed",
                last_error=error,
                last_game_id=last_game_id,
            )
            finish_run(conn, run_id, "failed", note=error, request_count=0)
            raise


def cmd_init_db(args: argparse.Namespace) -> None:
    config = build_config(args)
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "init-db", partition_key=None, config=config)
        finish_run(conn, run_id, "success", note="historical schema initialized")
    print(f"Initialized schema at {config.db_path}")


def cmd_backfill(args: argparse.Namespace) -> None:
    config = build_config(args)
    if args.season is not None:
        validate_supported_season(args.season)
    validate_supported_season(config.season_start)
    validate_supported_season(config.season_end)
    if config.season_start > config.season_end:
        raise ValueError("season-start must be less than or equal to season-end")
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


def cmd_backfill_team_stats(args: argparse.Namespace) -> None:
    season = validate_supported_season(args.season)
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = f"team-stats-season={season}"

    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=partition_key, config=config)
        processed_games = 0
        rows_inserted = 0
        rows_updated = 0
        total_rows_upserted = 0
        last_game_id: int | None = None
        try:
            game_ids = _completed_game_ids_for_season(conn, season, limit=args.max_games)
            existing_keys = _existing_game_team_stat_keys(conn, set(game_ids))

            for game_id in game_ids:
                boxscore = fetch_boxscore_bounded(game_id, config.request_policy, budget)
                row_count_for_game = 0
                for side in ("home", "away"):
                    row = _team_stats_row_from_boxscore(game_id, side, boxscore)
                    if row is None:
                        continue
                    key = (int(row["game_id"]), int(row["team_id"]))
                    if key in existing_keys:
                        rows_updated += 1
                    else:
                        rows_inserted += 1
                        existing_keys.add(key)
                    upsert_game_team_stats(conn, row)
                    row_count_for_game += 1
                total_rows_upserted += row_count_for_game
                processed_games += 1
                last_game_id = game_id

                if config.checkpoint_every > 0 and processed_games % config.checkpoint_every == 0:
                    upsert_checkpoint(
                        conn,
                        job_name="team-stats-backfill",
                        partition_key=partition_key,
                        cursor={
                            "season": season,
                            "games_processed": processed_games,
                            "rows_upserted": total_rows_upserted,
                            "rows_inserted": rows_inserted,
                            "rows_updated": rows_updated,
                        },
                        status="running",
                        last_game_id=last_game_id,
                    )

            run_stats = {
                "season": season,
                "games_selected": len(game_ids),
                "games_processed": processed_games,
                "rows_upserted": total_rows_upserted,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "odds_historical": "disabled",
            }
            upsert_checkpoint(
                conn,
                job_name="team-stats-backfill",
                partition_key=partition_key,
                cursor=run_stats,
                status="success",
                last_game_id=last_game_id,
            )
            finish_run(conn, run_id, "success", note=format_run_observability(run_stats), request_count=budget.used)
            print(f"Team stats backfill complete for {partition_key}: {format_run_observability({**run_stats, 'request_count': budget.used})}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name="team-stats-backfill",
                partition_key=partition_key,
                cursor={"season": season, "games_processed": processed_games},
                status="failed",
                last_game_id=last_game_id,
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
    backfill.add_argument("--season-start", type=int, default=MIN_SUPPORTED_SEASON)
    backfill.add_argument("--season-end", type=int, default=MAX_SUPPORTED_SEASON)
    backfill.set_defaults(func=cmd_backfill)

    incremental = subparsers.add_parser("incremental", help="Run daily incremental sync")
    incremental.add_argument("--date", help="YYYY-MM-DD; defaults to today")
    incremental.set_defaults(func=cmd_incremental)

    team_stats = subparsers.add_parser(
        "backfill-team-stats",
        help="Backfill game_team_stats from completed games in DB for one supported season",
    )
    team_stats.add_argument(
        "--season",
        type=int,
        default=MIN_SUPPORTED_SEASON,
        help=f"Season to process ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})",
    )
    team_stats.add_argument("--max-games", type=int, help="Optional cap for validation runs")
    team_stats.set_defaults(func=cmd_backfill_team_stats)

    pitcher_context = subparsers.add_parser(
        "backfill-pitcher-context",
        help="Backfill game_pitcher_context for one supported season",
    )
    pitcher_context.add_argument(
        "--season",
        type=int,
        default=MIN_SUPPORTED_SEASON,
        help=f"Season to process ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})",
    )
    pitcher_context.set_defaults(func=cmd_backfill_pitcher_context)

    pitcher_context_legacy = subparsers.add_parser(
        "backfill-pitcher-context-2020",
        help="Backfill game_pitcher_context for season 2020 only (legacy alias)",
    )
    pitcher_context_legacy.set_defaults(func=cmd_backfill_pitcher_context, season=2020)

    feature_rows = subparsers.add_parser(
        "materialize-feature-rows",
        help="Materialize canonical feature_rows for one supported season from existing support tables",
    )
    feature_rows.add_argument(
        "--season",
        type=int,
        default=MIN_SUPPORTED_SEASON,
        help=f"Season to process ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})",
    )
    feature_rows.add_argument("--feature-version", default=FEATURE_VERSION_V1, help="Feature version tag (default: v1)")
    feature_rows.set_defaults(func=cmd_materialize_feature_rows)

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
        season_start=getattr(args, "season_start", MIN_SUPPORTED_SEASON),
        season_end=getattr(args, "season_end", MAX_SUPPORTED_SEASON),
        checkpoint_every=args.checkpoint_every,
        request_policy=request_policy,
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
