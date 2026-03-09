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
PITCHER_CONTEXT_JOB = "pitcher-context-2020"


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


def build_pitcher_context_rows(
    game_id: int,
    game_date: str,
    game_schedule_row: dict[str, Any],
    season: int,
    lookup_cache: dict[str, int | None],
    stat_cache: dict[int, tuple[dict[str, Any], dict[str, Any]]],
    policy: RequestPolicy,
    budget: RequestBudget,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for side in ("home", "away"):
        probable_name = game_schedule_row.get(f"{side}_probable_pitcher")
        probable_name = probable_name.strip() if isinstance(probable_name, str) else None
        probable_id: int | None = None
        if probable_name:
            if probable_name not in lookup_cache:
                candidates = fetch_lookup_player_bounded(probable_name, season, policy, budget)
                lookup_cache[probable_name] = _to_int(candidates[0].get("id")) if candidates else None
            probable_id = lookup_cache[probable_name]

        season_stats: dict[str, Any] = {}
        career_stats: dict[str, Any] = {}
        if probable_id is not None:
            if probable_id not in stat_cache:
                year_data = fetch_player_stat_data_bounded(probable_id, "yearByYear", policy, budget)
                career_data = fetch_player_stat_data_bounded(probable_id, "career", policy, budget)
                matched_year = {}
                for row in year_data.get("stats", []) or []:
                    if str(row.get("season")) == str(season):
                        matched_year = row.get("stats") or {}
                        break
                career_stats_rows = career_data.get("stats", []) or []
                matched_career = career_stats_rows[0].get("stats") if career_stats_rows else {}
                stat_cache[probable_id] = (matched_year or {}, matched_career or {})
            season_stats, career_stats = stat_cache[probable_id]

        rows.append(
            {
                "game_id": game_id,
                "side": side,
                "pitcher_id": probable_id,
                "pitcher_name": probable_name,
                "probable_pitcher_id": probable_id,
                "probable_pitcher_name": probable_name,
                "probable_pitcher_known": 1 if probable_name else 0,
                "season_era": _to_float(season_stats.get("era")),
                "season_whip": _to_float(season_stats.get("whip")),
                "season_avg_allowed": _to_float(season_stats.get("avg")),
                "season_runs_per_9": _to_float(season_stats.get("runsScoredPer9")),
                "season_strike_pct": _to_float(season_stats.get("strikePercentage")),
                "season_win_pct": _to_float(season_stats.get("winPercentage")),
                "career_era": _to_float(career_stats.get("era")),
                "stats_source": "statsapi.player_stat_data(type=yearByYear,career)+lookup_player",
                "stats_as_of_date": game_date,
                "season_stats_scope": "full_season_year_aggregate",
                "season_stats_leakage_risk": 1,
                "source_updated_at": utc_now(),
            }
        )
    return rows


def cmd_backfill_pitcher_context_2020(args: argparse.Namespace) -> None:
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = "season=2020"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=f"{PITCHER_CONTEXT_JOB}:{partition_key}", config=config)
        last_game_id: int | None = None
        try:
            games_2020 = conn.execute(
                "SELECT game_id, game_date FROM games WHERE season=2020 ORDER BY game_date, game_id"
            ).fetchall()
            game_ids = [int(row["game_id"]) for row in games_2020]
            if not game_ids:
                note = format_run_observability({"job": PITCHER_CONTEXT_JOB, "season": 2020, "games_seen": 0})
                upsert_checkpoint(
                    conn,
                    job_name=PITCHER_CONTEXT_JOB,
                    partition_key=partition_key,
                    cursor={"season": 2020, "games_seen": 0, "rows_upserted": 0},
                    status="success",
                )
                finish_run(conn, run_id, "success", note=note, request_count=budget.used)
                print(f"Pitcher context backfill complete for {partition_key}: {note}")
                return

            schedule_rows = fetch_schedule_bounded(config.request_policy, budget, season=2020, sportId=1)
            schedule_by_game_id = {
                _to_int(row.get("game_id")): row for row in schedule_rows if _to_int(row.get("game_id")) is not None
            }

            lookup_cache: dict[str, int | None] = {}
            stat_cache: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
            rows_upserted = 0
            for idx, db_game in enumerate(games_2020, start=1):
                game_id = int(db_game["game_id"])
                game_date = str(db_game["game_date"])
                schedule_row = schedule_by_game_id.get(game_id, {})
                context_rows = build_pitcher_context_rows(
                    game_id,
                    game_date,
                    schedule_row,
                    2020,
                    lookup_cache,
                    stat_cache,
                    config.request_policy,
                    budget,
                )
                for row in context_rows:
                    upsert_game_pitcher_context(conn, row)
                    rows_upserted += 1
                last_game_id = game_id
                if config.checkpoint_every > 0 and idx % config.checkpoint_every == 0:
                    upsert_checkpoint(
                        conn,
                        job_name=PITCHER_CONTEXT_JOB,
                        partition_key=partition_key,
                        cursor={"season": 2020, "games_seen": idx, "rows_upserted": rows_upserted},
                        status="running",
                        last_game_id=last_game_id,
                    )

            cursor = {
                "season": 2020,
                "games_seen": len(games_2020),
                "rows_upserted": rows_upserted,
                "distinct_pitchers_cached": len(stat_cache),
            }
            upsert_checkpoint(
                conn,
                job_name=PITCHER_CONTEXT_JOB,
                partition_key=partition_key,
                cursor=cursor,
                status="success",
                last_game_id=last_game_id,
            )
            note = format_run_observability({"job": PITCHER_CONTEXT_JOB, **cursor})
            finish_run(conn, run_id, "success", note=note, request_count=budget.used)
            print(f"Pitcher context backfill complete for {partition_key}: {note}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name=PITCHER_CONTEXT_JOB,
                partition_key=partition_key,
                cursor={"season": 2020},
                status="failed",
                last_error=error,
                last_game_id=last_game_id,
            )
            finish_run(conn, run_id, "failed", note=error, request_count=budget.used)
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
    if args.season != 2020:
        raise ValueError("team-stats backfill is restricted to season 2020 only")

    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = f"team-stats-season={args.season}"

    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=partition_key, config=config)
        processed_games = 0
        rows_inserted = 0
        rows_updated = 0
        total_rows_upserted = 0
        last_game_id: int | None = None
        try:
            game_ids = _completed_game_ids_for_season(conn, args.season, limit=args.max_games)
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
                            "season": args.season,
                            "games_processed": processed_games,
                            "rows_upserted": total_rows_upserted,
                            "rows_inserted": rows_inserted,
                            "rows_updated": rows_updated,
                        },
                        status="running",
                        last_game_id=last_game_id,
                    )

            run_stats = {
                "season": args.season,
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
                cursor={"season": args.season, "games_processed": processed_games},
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
    backfill.add_argument("--season-start", type=int, default=2020)
    backfill.add_argument("--season-end", type=int, default=2025)
    backfill.set_defaults(func=cmd_backfill)

    incremental = subparsers.add_parser("incremental", help="Run daily incremental sync")
    incremental.add_argument("--date", help="YYYY-MM-DD; defaults to today")
    incremental.set_defaults(func=cmd_incremental)

    team_stats = subparsers.add_parser(
        "backfill-team-stats",
        help="Backfill game_team_stats from completed games in DB (restricted to season 2020)",
    )
    team_stats.add_argument("--season", type=int, default=2020, help="Season to process (must be 2020)")
    team_stats.add_argument("--max-games", type=int, help="Optional cap for validation runs")
    team_stats.set_defaults(func=cmd_backfill_team_stats)

    pitcher_context = subparsers.add_parser(
        "backfill-pitcher-context-2020",
        help="Backfill game_pitcher_context for season 2020 only",
    )
    pitcher_context.set_defaults(func=cmd_backfill_pitcher_context_2020)

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
