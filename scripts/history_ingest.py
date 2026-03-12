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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo

try:
    import statsapi  # type: ignore
except Exception:  # pragma: no cover - exercised through command failures/tests
    statsapi = None


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "mlb_history.db"
DEFAULT_SCHEMA_PATH = ROOT / "scripts" / "sql" / "history_schema.sql"
READ_ONLY_COMMANDS = frozenset({"audit-support-coverage", "audit-pitcher-context"})
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
FEATURE_VERSION_V2_PHASE1 = "v2_phase1"
MIN_SUPPORTED_SEASON = 2020
MAX_SUPPORTED_SEASON = 2025
DEFAULT_MAX_NULL_SAFE_FALLBACK_SHARE = 0.20
DEFAULT_MAX_MISSING_PROBABLE_SHARE = 0.05
BULLPEN_STATS_SCOPE = "prior_completed_games_only"
BULLPEN_FRESHNESS_METHOD_V1 = "calendar_day_usage_v1"
BULLPEN_TOP_RELIEVER_RANKING_METHOD_V1 = "appearances_ge_2_or_outs_ge_6__kbb_whip_runs_per_9_outs_v1"
BULLPEN_TOP_N_DEFAULTS = (3, 5)
BULLPEN_TOP_RELIEVER_MIN_APPEARANCES = 2
BULLPEN_TOP_RELIEVER_MIN_OUTS = 6
BULLPEN_HIGH_USAGE_PITCHES_LAST1D = 25
BULLPEN_HIGH_USAGE_PITCHES_LAST3D = 40
PLATOON_STATS_SCOPE = "prior_completed_games_only"
LINEUP_QUALITY_METRIC_UNAVAILABLE = "unavailable__player_offense_support_not_built"
LINEUP_QUALITY_METRIC_HAND_AFFINITY_PROXY_V1 = "handedness_affinity_proxy_v1"
PEOPLE_LOOKUP_BATCH_SIZE = 50
OPEN_METEO_HOURLY_FIELDS_BASE = (
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "weather_code",
    "cloud_cover",
    "is_day",
)
OPEN_METEO_HOURLY_FIELDS_FORECAST = OPEN_METEO_HOURLY_FIELDS_BASE + ("precipitation_probability",)
OPEN_METEO_HOURLY_FIELDS_ARCHIVE = OPEN_METEO_HOURLY_FIELDS_BASE
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_SOURCE_FORECAST = "open_meteo_forecast"
WEATHER_SOURCE_ARCHIVE = "open_meteo_archive"
WEATHER_SNAPSHOT_FORECAST = "forecast"
WEATHER_SNAPSHOT_OBSERVED = "observed_archive"
WEATHER_SOURCE_PRIORITY_DEFAULT = 1
WEATHER_ALIGNMENT_WINDOW_HOURS = 6.0
KNOWN_VENUE_METADATA: dict[int, dict[str, Any]] = {
    5340: {
        "venue_id": 5340,
        "venue_name": "Estadio Alfredo Harp Helu",
        "city": "Mexico City",
        "state": "CMX",
        "country": "MEX",
        "timezone": "America/Mexico_City",
        "latitude": 19.404,
        "longitude": -99.0855,
        "roof_type": "open",
        "statsapi_venue_name": "Estadio Alfredo Harp Helu",
    }
}
FIXED_DOME_VENUE_NAMES = frozenset({"loanDepot park", "Rogers Centre", "Tropicana Field"})
RETRACTABLE_ROOF_VENUE_NAMES = frozenset(
    {
        "American Family Field",
        "Chase Field",
        "Daikin Park",
        "Globe Life Field",
        "Marlins Park",
        "Minute Maid Park",
        "Miller Park",
        "T-Mobile Park",
    }
)
PITCHER_CONTEXT_RATE_STAT_FIELDS = (
    "season_era",
    "season_whip",
    "season_avg_allowed",
    "season_runs_per_9",
    "season_strike_pct",
    "season_win_pct",
)
PITCHER_CONTEXT_PROVENANCE_FIELDS = (
    "stats_source",
    "stats_as_of_date",
    "season_stats_scope",
    "season_stats_leakage_risk",
)
REBUILD_STAGE_BASE = "base"
REBUILD_STAGE_TEAM_STATS = "team-stats"
REBUILD_STAGE_PITCHER_CONTEXT = "pitcher-context"
REBUILD_STAGE_PITCHER_APPEARANCES = "pitcher-appearances"
REBUILD_STAGE_BULLPEN_SUPPORT = "bullpen-support"
REBUILD_STAGE_LINEUP_SUPPORT = "lineup-support"
REBUILD_STAGE_VENUES = "venues"
REBUILD_STAGE_WEATHER = "weather"
REBUILD_STAGE_FEATURE_ROWS = "feature-rows"
REBUILD_ALL_STAGES = "all"
REBUILD_STAGE_ORDER = (
    REBUILD_STAGE_BASE,
    REBUILD_STAGE_TEAM_STATS,
    REBUILD_STAGE_PITCHER_CONTEXT,
    REBUILD_STAGE_PITCHER_APPEARANCES,
    REBUILD_STAGE_BULLPEN_SUPPORT,
    REBUILD_STAGE_LINEUP_SUPPORT,
    REBUILD_STAGE_VENUES,
    REBUILD_STAGE_WEATHER,
    REBUILD_STAGE_FEATURE_ROWS,
)


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


def pitcher_appearances_job_name(season: int) -> str:
    return f"pitcher-appearances-{season}"


def bullpen_support_job_name(season: int) -> str:
    return f"bullpen-support-{season}"


def lineup_support_job_name(season: int) -> str:
    return f"lineup-support-{season}"


def feature_rows_job_name(season: int, feature_version: str) -> str:
    return f"feature-rows-{feature_version}-{season}"


def venue_dim_job_name(partition: str) -> str:
    return f"venue-dim-{partition}"


def weather_backfill_job_name(season: int) -> str:
    return f"weather-backfill-{season}"


def weather_forecast_job_name(target_date: str) -> str:
    return f"weather-forecast-{target_date}"


def game_metadata_backfill_job_name(partition: str) -> str:
    return f"game-metadata-{partition}"


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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS venue_dim (
          venue_id INTEGER PRIMARY KEY,
          venue_name TEXT NOT NULL,
          city TEXT,
          state TEXT,
          country TEXT DEFAULT 'USA',
          timezone TEXT NOT NULL,
          latitude REAL NOT NULL,
          longitude REAL NOT NULL,
          roof_type TEXT NOT NULL,
          weather_exposure_default INTEGER NOT NULL DEFAULT 1 CHECK(weather_exposure_default IN (0, 1)),
          statsapi_venue_name TEXT,
          source_updated_at TEXT,
          ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS game_weather_snapshots (
          game_id INTEGER NOT NULL,
          venue_id INTEGER NOT NULL,
          as_of_ts TEXT NOT NULL,
          target_game_ts TEXT NOT NULL,
          snapshot_type TEXT NOT NULL,
          source TEXT NOT NULL,
          source_priority INTEGER NOT NULL DEFAULT 1,
          hour_offset_from_first_pitch REAL,
          temperature_f REAL,
          humidity_pct REAL,
          pressure_hpa REAL,
          precipitation_mm REAL,
          precipitation_probability REAL,
          wind_speed_mph REAL,
          wind_gust_mph REAL,
          wind_direction_deg REAL,
          weather_code INTEGER,
          cloud_cover_pct REAL,
          is_day INTEGER CHECK(is_day IN (0, 1)),
          day_night_source TEXT,
          weather_exposure_flag INTEGER CHECK(weather_exposure_flag IN (0, 1)),
          statsapi_weather_condition_text TEXT,
          statsapi_wind_text TEXT,
          source_updated_at TEXT,
          ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
          PRIMARY KEY (game_id, as_of_ts, snapshot_type, source),
          FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE,
          FOREIGN KEY (venue_id) REFERENCES venue_dim(venue_id)
        )
        """
    )

    game_columns = {row["name"] for row in conn.execute("PRAGMA table_info(games)")}
    game_migrations = {
        "venue_id": "INTEGER",
        "day_night": "TEXT",
    }
    for col, col_type in game_migrations.items():
        if col not in game_columns:
            conn.execute(f"ALTER TABLE games ADD COLUMN {col} {col_type}")

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

    appearance_columns = {row["name"] for row in conn.execute("PRAGMA table_info(game_pitcher_appearances)")}
    appearance_migrations = {
        "pitcher_name": "TEXT",
        "appearance_order": "INTEGER",
        "is_starter": "INTEGER NOT NULL DEFAULT 0 CHECK(is_starter IN (0, 1))",
        "is_reliever": "INTEGER NOT NULL DEFAULT 1 CHECK(is_reliever IN (0, 1))",
        "outs_recorded": "INTEGER",
        "innings_pitched": "REAL",
        "batters_faced": "INTEGER",
        "pitches": "INTEGER",
        "strikes": "INTEGER",
        "hits": "INTEGER",
        "walks": "INTEGER",
        "strikeouts": "INTEGER",
        "runs": "INTEGER",
        "earned_runs": "INTEGER",
        "home_runs": "INTEGER",
        "holds": "INTEGER",
        "save_flag": "INTEGER",
        "blown_save_flag": "INTEGER",
        "inherited_runners": "INTEGER",
        "inherited_runners_scored": "INTEGER",
        "source_updated_at": "TEXT",
    }
    for col, col_type in appearance_migrations.items():
        if col not in appearance_columns:
            conn.execute(f"ALTER TABLE game_pitcher_appearances ADD COLUMN {col} {col_type}")

    bullpen_state_columns = {row["name"] for row in conn.execute("PRAGMA table_info(team_bullpen_game_state)")}
    bullpen_state_migrations = {
        "stats_scope": "TEXT NOT NULL DEFAULT 'prior_completed_games_only'",
        "freshness_method": "TEXT",
        "season_games_in_sample": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_pitchers_in_sample": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_appearances_season": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_outs_season": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_era_season": "REAL",
        "bullpen_whip_season": "REAL",
        "bullpen_runs_per_9_season": "REAL",
        "bullpen_k_rate_season": "REAL",
        "bullpen_bb_rate_season": "REAL",
        "bullpen_k_minus_bb_rate_season": "REAL",
        "bullpen_hr_rate_season": "REAL",
        "bullpen_outs_last1d": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_outs_last3d": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_outs_last5d": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_outs_last7d": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_pitches_last1d": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_pitches_last3d": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_pitches_last5d": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_appearances_last3d": "INTEGER NOT NULL DEFAULT 0",
        "bullpen_appearances_last5d": "INTEGER NOT NULL DEFAULT 0",
        "relievers_used_yesterday_count": "INTEGER NOT NULL DEFAULT 0",
        "relievers_used_last3d_count": "INTEGER NOT NULL DEFAULT 0",
        "relievers_back_to_back_count": "INTEGER NOT NULL DEFAULT 0",
        "relievers_2_of_last3_count": "INTEGER NOT NULL DEFAULT 0",
        "high_usage_relievers_last3d_count": "INTEGER NOT NULL DEFAULT 0",
        "freshness_score": "REAL",
        "source_updated_at": "TEXT",
    }
    for col, col_type in bullpen_state_migrations.items():
        if col not in bullpen_state_columns:
            conn.execute(f"ALTER TABLE team_bullpen_game_state ADD COLUMN {col} {col_type}")
    if bullpen_state_columns and "freshness_method" in bullpen_state_columns:
        conn.execute(
            """
            UPDATE team_bullpen_game_state
            SET freshness_method = ?
            WHERE freshness_method IS NULL
            """,
            (BULLPEN_FRESHNESS_METHOD_V1,),
        )

    bullpen_top_columns = {row["name"] for row in conn.execute("PRAGMA table_info(team_bullpen_top_relievers)")}
    bullpen_top_migrations = {
        "stats_scope": "TEXT NOT NULL DEFAULT 'prior_completed_games_only'",
        "ranking_method": "TEXT",
        "top_n": "INTEGER",
        "n_available": "INTEGER NOT NULL DEFAULT 0",
        "selected_pitcher_ids_json": "TEXT NOT NULL DEFAULT '[]'",
        "topn_appearances_season": "INTEGER NOT NULL DEFAULT 0",
        "topn_outs_season": "INTEGER NOT NULL DEFAULT 0",
        "topn_era_season": "REAL",
        "topn_whip_season": "REAL",
        "topn_runs_per_9_season": "REAL",
        "topn_k_rate_season": "REAL",
        "topn_bb_rate_season": "REAL",
        "topn_k_minus_bb_rate_season": "REAL",
        "topn_outs_last3d": "INTEGER NOT NULL DEFAULT 0",
        "topn_pitches_last3d": "INTEGER NOT NULL DEFAULT 0",
        "topn_appearances_last3d": "INTEGER NOT NULL DEFAULT 0",
        "topn_back_to_back_count": "INTEGER NOT NULL DEFAULT 0",
        "topn_freshness_score": "REAL",
        "quality_dropoff_vs_team": "REAL",
        "source_updated_at": "TEXT",
    }
    for col, col_type in bullpen_top_migrations.items():
        if col not in bullpen_top_columns:
            conn.execute(f"ALTER TABLE team_bullpen_top_relievers ADD COLUMN {col} {col_type}")
    if bullpen_top_columns and "ranking_method" in bullpen_top_columns:
        conn.execute(
            """
            UPDATE team_bullpen_top_relievers
            SET ranking_method = ?
            WHERE ranking_method IS NULL
            """,
            (BULLPEN_TOP_RELIEVER_RANKING_METHOD_V1,),
        )

    handedness_columns = {row["name"] for row in conn.execute("PRAGMA table_info(player_handedness_dim)")}
    handedness_migrations = {
        "player_name": "TEXT",
        "bat_side": "TEXT CHECK(bat_side IN ('L', 'R', 'S'))",
        "pitch_hand": "TEXT CHECK(pitch_hand IN ('L', 'R', 'S'))",
        "primary_position_code": "TEXT",
        "source_updated_at": "TEXT",
    }
    for col, col_type in handedness_migrations.items():
        if handedness_columns and col not in handedness_columns:
            conn.execute(f"ALTER TABLE player_handedness_dim ADD COLUMN {col} {col_type}")

    lineup_snapshot_columns = {row["name"] for row in conn.execute("PRAGMA table_info(game_lineup_snapshots)")}
    lineup_snapshot_migrations = {
        "snapshot_type": "TEXT",
        "lineup_status": "TEXT",
        "player_name": "TEXT",
        "position_code": "TEXT",
        "bat_side": "TEXT CHECK(bat_side IN ('L', 'R', 'S'))",
        "pitch_hand": "TEXT CHECK(pitch_hand IN ('L', 'R', 'S'))",
        "source_updated_at": "TEXT",
    }
    for col, col_type in lineup_snapshot_migrations.items():
        if lineup_snapshot_columns and col not in lineup_snapshot_columns:
            conn.execute(f"ALTER TABLE game_lineup_snapshots ADD COLUMN {col} {col_type}")

    lineup_state_columns = {row["name"] for row in conn.execute("PRAGMA table_info(team_lineup_game_state)")}
    lineup_state_migrations = {
        "snapshot_type": "TEXT",
        "lineup_status": "TEXT",
        "lineup_known_flag": "INTEGER NOT NULL DEFAULT 0 CHECK(lineup_known_flag IN (0, 1))",
        "announced_lineup_count": "INTEGER NOT NULL DEFAULT 0",
        "lineup_l_count": "INTEGER",
        "lineup_r_count": "INTEGER",
        "lineup_s_count": "INTEGER",
        "top3_l_count": "INTEGER",
        "top3_r_count": "INTEGER",
        "top3_s_count": "INTEGER",
        "top5_l_count": "INTEGER",
        "top5_r_count": "INTEGER",
        "top5_s_count": "INTEGER",
        "lineup_lefty_pa_share_proxy": "REAL",
        "lineup_righty_pa_share_proxy": "REAL",
        "lineup_switch_pa_share_proxy": "REAL",
        "lineup_balance_score": "REAL",
        "lineup_quality_metric": "TEXT",
        "lineup_quality_mean": "REAL",
        "top3_lineup_quality_mean": "REAL",
        "top5_lineup_quality_mean": "REAL",
        "lineup_vs_rhp_quality": "REAL",
        "lineup_vs_lhp_quality": "REAL",
        "source_updated_at": "TEXT",
    }
    for col, col_type in lineup_state_migrations.items():
        if lineup_state_columns and col not in lineup_state_columns:
            conn.execute(f"ALTER TABLE team_lineup_game_state ADD COLUMN {col} {col_type}")

    platoon_columns = {row["name"] for row in conn.execute("PRAGMA table_info(team_platoon_splits)")}
    platoon_migrations = {
        "stats_scope": f"TEXT NOT NULL DEFAULT '{PLATOON_STATS_SCOPE}'",
        "games_in_sample": "INTEGER NOT NULL DEFAULT 0",
        "plate_appearances": "INTEGER NOT NULL DEFAULT 0",
        "batting_avg": "REAL",
        "obp": "REAL",
        "slg": "REAL",
        "ops": "REAL",
        "runs_per_game": "REAL",
        "strikeout_rate": "REAL",
        "walk_rate": "REAL",
        "source_updated_at": "TEXT",
    }
    for col, col_type in platoon_migrations.items():
        if platoon_columns and col not in platoon_columns:
            conn.execute(f"ALTER TABLE team_platoon_splits ADD COLUMN {col} {col_type}")


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
          game_id, season, game_date, game_type, status, scheduled_datetime, venue_id, day_night,
          home_team_id, away_team_id, home_score, away_score, winning_team_id, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id) DO UPDATE SET
          season = excluded.season,
          game_date = excluded.game_date,
          game_type = excluded.game_type,
          status = excluded.status,
          scheduled_datetime = excluded.scheduled_datetime,
          venue_id = COALESCE(excluded.venue_id, games.venue_id),
          day_night = COALESCE(excluded.day_night, games.day_night),
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
            row.get("venue_id"),
            row.get("day_night"),
            row.get("home_team_id"),
            row.get("away_team_id"),
            row.get("home_score"),
            row.get("away_score"),
            row.get("winning_team_id"),
            row.get("source_updated_at"),
        ),
    )
    conn.commit()


def _normalize_day_night(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"day", "night"}:
        return text
    if text in {"d", "n"}:
        return "day" if text == "d" else "night"
    return None


def _normalize_venue_roof_type(venue_name: str | None, roof_type: Any) -> str:
    text = str(roof_type or "").strip().lower().replace(" ", "_")
    if text in {"open", "retractable", "fixed_dome", "unknown"}:
        return text
    canonical_name = (venue_name or "").strip()
    if canonical_name in FIXED_DOME_VENUE_NAMES:
        return "fixed_dome"
    if canonical_name in RETRACTABLE_ROOF_VENUE_NAMES:
        return "retractable"
    return "unknown"


def _weather_exposure_for_roof(roof_type: str) -> int:
    return 0 if roof_type == "fixed_dome" else 1


def _extract_schedule_venue(entry: dict[str, Any]) -> dict[str, Any] | None:
    venue_id = _to_int(entry.get("venue_id") or entry.get("venueId"))
    if venue_id is None:
        venue = entry.get("venue")
        if isinstance(venue, dict):
            venue_id = _to_int(venue.get("id"))
    if venue_id is None:
        return None
    venue = entry.get("venue")
    venue_name = entry.get("venue_name") or entry.get("venueName")
    if not venue_name and isinstance(venue, dict):
        venue_name = venue.get("name")
    latitude = _to_float(entry.get("venue_latitude") or entry.get("venueLatitude"))
    longitude = _to_float(entry.get("venue_longitude") or entry.get("venueLongitude"))
    timezone_name = entry.get("venue_timezone") or entry.get("venueTimezone")
    city = entry.get("venue_city") or entry.get("venueCity")
    state = entry.get("venue_state") or entry.get("venueState")
    country = entry.get("venue_country") or entry.get("venueCountry")
    roof_type = entry.get("venue_roof_type") or entry.get("venueRoofType")
    if isinstance(venue, dict):
        location = venue.get("location") if isinstance(venue.get("location"), dict) else {}
        if not venue_name:
            venue_name = venue.get("name")
        if city is None:
            city = location.get("city")
        if state is None:
            state = location.get("stateAbbrev") or location.get("state")
        if country is None:
            country = location.get("country")
        coordinates = location.get("defaultCoordinates") if isinstance(location.get("defaultCoordinates"), dict) else {}
        if latitude is None:
            latitude = _to_float(coordinates.get("latitude"))
        if longitude is None:
            longitude = _to_float(coordinates.get("longitude"))
        time_zone = location.get("timeZone") if isinstance(location.get("timeZone"), dict) else {}
        if timezone_name is None:
            timezone_name = time_zone.get("id")
    return {
        "venue_id": venue_id,
        "venue_name": venue_name,
        "city": city,
        "state": state,
        "country": country,
        "timezone": timezone_name,
        "latitude": latitude,
        "longitude": longitude,
        "roof_type": roof_type,
        "statsapi_venue_name": venue_name,
        "source_updated_at": utc_now(),
    }


def upsert_venue_dim(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    roof_type = _normalize_venue_roof_type(row.get("venue_name"), row.get("roof_type"))
    conn.execute(
        """
        INSERT INTO venue_dim (
          venue_id, venue_name, city, state, country, timezone, latitude, longitude,
          roof_type, weather_exposure_default, statsapi_venue_name, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(venue_id) DO UPDATE SET
          venue_name = excluded.venue_name,
          city = excluded.city,
          state = excluded.state,
          country = excluded.country,
          timezone = excluded.timezone,
          latitude = excluded.latitude,
          longitude = excluded.longitude,
          roof_type = excluded.roof_type,
          weather_exposure_default = excluded.weather_exposure_default,
          statsapi_venue_name = excluded.statsapi_venue_name,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["venue_id"],
            row["venue_name"],
            row.get("city"),
            row.get("state"),
            row.get("country") or "USA",
            row["timezone"],
            row["latitude"],
            row["longitude"],
            roof_type,
            _weather_exposure_for_roof(roof_type),
            row.get("statsapi_venue_name"),
            row.get("source_updated_at") or utc_now(),
        ),
    )
    conn.commit()


def upsert_game_weather_snapshot(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO game_weather_snapshots (
          game_id, venue_id, as_of_ts, target_game_ts, snapshot_type, source, source_priority,
          hour_offset_from_first_pitch, temperature_f, humidity_pct, pressure_hpa, precipitation_mm,
          precipitation_probability, wind_speed_mph, wind_gust_mph, wind_direction_deg, weather_code,
          cloud_cover_pct, is_day, day_night_source, weather_exposure_flag,
          statsapi_weather_condition_text, statsapi_wind_text, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, as_of_ts, snapshot_type, source) DO UPDATE SET
          venue_id = excluded.venue_id,
          target_game_ts = excluded.target_game_ts,
          source_priority = excluded.source_priority,
          hour_offset_from_first_pitch = excluded.hour_offset_from_first_pitch,
          temperature_f = excluded.temperature_f,
          humidity_pct = excluded.humidity_pct,
          pressure_hpa = excluded.pressure_hpa,
          precipitation_mm = excluded.precipitation_mm,
          precipitation_probability = excluded.precipitation_probability,
          wind_speed_mph = excluded.wind_speed_mph,
          wind_gust_mph = excluded.wind_gust_mph,
          wind_direction_deg = excluded.wind_direction_deg,
          weather_code = excluded.weather_code,
          cloud_cover_pct = excluded.cloud_cover_pct,
          is_day = excluded.is_day,
          day_night_source = excluded.day_night_source,
          weather_exposure_flag = excluded.weather_exposure_flag,
          statsapi_weather_condition_text = excluded.statsapi_weather_condition_text,
          statsapi_wind_text = excluded.statsapi_wind_text,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["venue_id"],
            row["as_of_ts"],
            row["target_game_ts"],
            row["snapshot_type"],
            row["source"],
            row.get("source_priority", WEATHER_SOURCE_PRIORITY_DEFAULT),
            row.get("hour_offset_from_first_pitch"),
            row.get("temperature_f"),
            row.get("humidity_pct"),
            row.get("pressure_hpa"),
            row.get("precipitation_mm"),
            row.get("precipitation_probability"),
            row.get("wind_speed_mph"),
            row.get("wind_gust_mph"),
            row.get("wind_direction_deg"),
            row.get("weather_code"),
            row.get("cloud_cover_pct"),
            row.get("is_day"),
            row.get("day_night_source"),
            row.get("weather_exposure_flag"),
            row.get("statsapi_weather_condition_text"),
            row.get("statsapi_wind_text"),
            row.get("source_updated_at") or utc_now(),
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


def _pitcher_context_row_value(row: dict[str, Any] | sqlite3.Row, field: str) -> Any:
    try:
        return row[field]
    except (KeyError, IndexError):
        return None


def _pitcher_context_stat_count(row: dict[str, Any] | sqlite3.Row) -> int:
    return sum(1 for field in PITCHER_CONTEXT_RATE_STAT_FIELDS if _pitcher_context_row_value(row, field) is not None)


def _pitcher_context_probable_known(row: dict[str, Any] | sqlite3.Row) -> bool:
    return bool(int(_pitcher_context_row_value(row, "probable_pitcher_known") or 0))


def _pitcher_context_missing_stats_with_known_probable(row: dict[str, Any] | sqlite3.Row) -> bool:
    return _pitcher_context_probable_known(row) and _pitcher_context_stat_count(row) == 0


def _pitcher_context_is_null_safe_fallback(row: dict[str, Any] | sqlite3.Row) -> bool:
    source = str(_pitcher_context_row_value(row, "stats_source") or "")
    return _pitcher_context_missing_stats_with_known_probable(row) and "leakage_safe_null_fallback" in source


def _should_preserve_existing_pitcher_context(existing_row: dict[str, Any], incoming_row: dict[str, Any]) -> bool:
    existing_stat_count = _pitcher_context_stat_count(existing_row)
    incoming_stat_count = _pitcher_context_stat_count(incoming_row)
    if existing_stat_count == 0 or incoming_stat_count >= existing_stat_count:
        return False
    if _pitcher_context_is_null_safe_fallback(incoming_row):
        return True
    return int(existing_row.get("season_stats_leakage_risk") or 0) <= int(incoming_row.get("season_stats_leakage_risk") or 0)


def _merge_pitcher_context_for_safe_write(existing_row: dict[str, Any], incoming_row: dict[str, Any]) -> dict[str, Any]:
    merged = dict(incoming_row)
    if not _should_preserve_existing_pitcher_context(existing_row, incoming_row):
        return merged
    for field in PITCHER_CONTEXT_RATE_STAT_FIELDS + PITCHER_CONTEXT_PROVENANCE_FIELDS:
        merged[field] = existing_row.get(field)
    return merged


def _build_pitcher_context_quality_report(
    rows: list[dict[str, Any] | sqlite3.Row],
    *,
    season: int,
    schedule_fallback_used: bool = False,
    boxscore_fallback_used: bool = False,
    handedness_fallback_used: bool = False,
    max_null_safe_fallback_share: float = DEFAULT_MAX_NULL_SAFE_FALLBACK_SHARE,
    max_missing_probable_share: float = DEFAULT_MAX_MISSING_PROBABLE_SHARE,
) -> dict[str, Any]:
    total_rows = len(rows)
    probable_known_rows = sum(1 for row in rows if _pitcher_context_probable_known(row))
    rows_with_stats = sum(1 for row in rows if _pitcher_context_stat_count(row) > 0)
    null_safe_fallback_rows = sum(1 for row in rows if _pitcher_context_is_null_safe_fallback(row))
    missing_stats_with_known_probable_rows = sum(
        1 for row in rows if _pitcher_context_missing_stats_with_known_probable(row)
    )
    missing_probable_identity_rows = sum(1 for row in rows if not _pitcher_context_probable_known(row))
    rows_with_nonzero_leakage_risk = sum(
        1 for row in rows if int(_pitcher_context_row_value(row, "season_stats_leakage_risk") or 0) != 0
    )
    null_safe_fallback_share = (
        float(null_safe_fallback_rows / probable_known_rows) if probable_known_rows else 0.0
    )
    missing_probable_share = float(missing_probable_identity_rows / total_rows) if total_rows else 0.0
    safe_for_canonical_write = (
        null_safe_fallback_share <= max_null_safe_fallback_share
        and missing_probable_share <= max_missing_probable_share
        and not schedule_fallback_used
    )
    return {
        "season": season,
        "total_rows": total_rows,
        "probable_known_rows": probable_known_rows,
        "rows_with_stats": rows_with_stats,
        "missing_stats_with_known_probable_rows": missing_stats_with_known_probable_rows,
        "missing_probable_identity_rows": missing_probable_identity_rows,
        "null_safe_fallback_rows": null_safe_fallback_rows,
        "null_safe_fallback_share": round(null_safe_fallback_share, 6),
        "missing_probable_share": round(missing_probable_share, 6),
        "rows_with_nonzero_leakage_risk": rows_with_nonzero_leakage_risk,
        "schedule_fallback_used": schedule_fallback_used,
        "boxscore_fallback_used": boxscore_fallback_used,
        "handedness_fallback_used": handedness_fallback_used,
        "max_null_safe_fallback_share": max_null_safe_fallback_share,
        "max_missing_probable_share": max_missing_probable_share,
        "safe_for_canonical_write": safe_for_canonical_write,
    }


def upsert_game_pitcher_context(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    existing_row = conn.execute(
        """
        SELECT *
        FROM game_pitcher_context
        WHERE game_id = ? AND side = ?
        """,
        (row["game_id"], row["side"]),
    ).fetchone()
    write_row = row
    if existing_row is not None:
        write_row = _merge_pitcher_context_for_safe_write(dict(existing_row), row)
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
            write_row["game_id"],
            write_row["side"],
            write_row.get("pitcher_id"),
            write_row.get("pitcher_name"),
            write_row.get("probable_pitcher_id"),
            write_row.get("probable_pitcher_name"),
            write_row.get("probable_pitcher_known", 0),
            write_row.get("season_era"),
            write_row.get("season_whip"),
            write_row.get("season_avg_allowed"),
            write_row.get("season_runs_per_9"),
            write_row.get("season_strike_pct"),
            write_row.get("season_win_pct"),
            write_row.get("career_era"),
            write_row.get("stats_source"),
            write_row.get("stats_as_of_date"),
            write_row.get("season_stats_scope"),
            write_row.get("season_stats_leakage_risk", 1),
            write_row.get("source_updated_at") or utc_now(),
        ),
    )
    conn.commit()


def upsert_game_pitcher_appearance(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO game_pitcher_appearances (
          game_id, team_id, side, pitcher_id, pitcher_name, appearance_order,
          is_starter, is_reliever, outs_recorded, innings_pitched, batters_faced,
          pitches, strikes, hits, walks, strikeouts, runs, earned_runs, home_runs,
          holds, save_flag, blown_save_flag, inherited_runners, inherited_runners_scored,
          source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, pitcher_id) DO UPDATE SET
          team_id = excluded.team_id,
          side = excluded.side,
          pitcher_name = excluded.pitcher_name,
          appearance_order = excluded.appearance_order,
          is_starter = excluded.is_starter,
          is_reliever = excluded.is_reliever,
          outs_recorded = excluded.outs_recorded,
          innings_pitched = excluded.innings_pitched,
          batters_faced = excluded.batters_faced,
          pitches = excluded.pitches,
          strikes = excluded.strikes,
          hits = excluded.hits,
          walks = excluded.walks,
          strikeouts = excluded.strikeouts,
          runs = excluded.runs,
          earned_runs = excluded.earned_runs,
          home_runs = excluded.home_runs,
          holds = excluded.holds,
          save_flag = excluded.save_flag,
          blown_save_flag = excluded.blown_save_flag,
          inherited_runners = excluded.inherited_runners,
          inherited_runners_scored = excluded.inherited_runners_scored,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["team_id"],
            row["side"],
            row["pitcher_id"],
            row.get("pitcher_name"),
            row.get("appearance_order"),
            row.get("is_starter", 0),
            row.get("is_reliever", 1),
            row.get("outs_recorded"),
            row.get("innings_pitched"),
            row.get("batters_faced"),
            row.get("pitches"),
            row.get("strikes"),
            row.get("hits"),
            row.get("walks"),
            row.get("strikeouts"),
            row.get("runs"),
            row.get("earned_runs"),
            row.get("home_runs"),
            row.get("holds"),
            row.get("save_flag"),
            row.get("blown_save_flag"),
            row.get("inherited_runners"),
            row.get("inherited_runners_scored"),
            row.get("source_updated_at") or utc_now(),
        ),
    )
    conn.commit()


def upsert_team_bullpen_game_state(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        DELETE FROM team_bullpen_game_state
        WHERE game_id = ? AND side = ? AND as_of_ts <> ?
        """,
        (row["game_id"], row["side"], row["as_of_ts"]),
    )
    conn.execute(
        """
        INSERT INTO team_bullpen_game_state (
          game_id, team_id, side, as_of_ts, stats_scope, freshness_method,
          season_games_in_sample, bullpen_pitchers_in_sample, bullpen_appearances_season, bullpen_outs_season,
          bullpen_era_season, bullpen_whip_season, bullpen_runs_per_9_season,
          bullpen_k_rate_season, bullpen_bb_rate_season, bullpen_k_minus_bb_rate_season, bullpen_hr_rate_season,
          bullpen_outs_last1d, bullpen_outs_last3d, bullpen_outs_last5d, bullpen_outs_last7d,
          bullpen_pitches_last1d, bullpen_pitches_last3d, bullpen_pitches_last5d,
          bullpen_appearances_last3d, bullpen_appearances_last5d,
          relievers_used_yesterday_count, relievers_used_last3d_count, relievers_back_to_back_count,
          relievers_2_of_last3_count, high_usage_relievers_last3d_count, freshness_score,
          source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, side, as_of_ts) DO UPDATE SET
          team_id = excluded.team_id,
          stats_scope = excluded.stats_scope,
          freshness_method = excluded.freshness_method,
          season_games_in_sample = excluded.season_games_in_sample,
          bullpen_pitchers_in_sample = excluded.bullpen_pitchers_in_sample,
          bullpen_appearances_season = excluded.bullpen_appearances_season,
          bullpen_outs_season = excluded.bullpen_outs_season,
          bullpen_era_season = excluded.bullpen_era_season,
          bullpen_whip_season = excluded.bullpen_whip_season,
          bullpen_runs_per_9_season = excluded.bullpen_runs_per_9_season,
          bullpen_k_rate_season = excluded.bullpen_k_rate_season,
          bullpen_bb_rate_season = excluded.bullpen_bb_rate_season,
          bullpen_k_minus_bb_rate_season = excluded.bullpen_k_minus_bb_rate_season,
          bullpen_hr_rate_season = excluded.bullpen_hr_rate_season,
          bullpen_outs_last1d = excluded.bullpen_outs_last1d,
          bullpen_outs_last3d = excluded.bullpen_outs_last3d,
          bullpen_outs_last5d = excluded.bullpen_outs_last5d,
          bullpen_outs_last7d = excluded.bullpen_outs_last7d,
          bullpen_pitches_last1d = excluded.bullpen_pitches_last1d,
          bullpen_pitches_last3d = excluded.bullpen_pitches_last3d,
          bullpen_pitches_last5d = excluded.bullpen_pitches_last5d,
          bullpen_appearances_last3d = excluded.bullpen_appearances_last3d,
          bullpen_appearances_last5d = excluded.bullpen_appearances_last5d,
          relievers_used_yesterday_count = excluded.relievers_used_yesterday_count,
          relievers_used_last3d_count = excluded.relievers_used_last3d_count,
          relievers_back_to_back_count = excluded.relievers_back_to_back_count,
          relievers_2_of_last3_count = excluded.relievers_2_of_last3_count,
          high_usage_relievers_last3d_count = excluded.high_usage_relievers_last3d_count,
          freshness_score = excluded.freshness_score,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["team_id"],
            row["side"],
            row["as_of_ts"],
            row.get("stats_scope", BULLPEN_STATS_SCOPE),
            row.get("freshness_method", BULLPEN_FRESHNESS_METHOD_V1),
            row.get("season_games_in_sample", 0),
            row.get("bullpen_pitchers_in_sample", 0),
            row.get("bullpen_appearances_season", 0),
            row.get("bullpen_outs_season", 0),
            row.get("bullpen_era_season"),
            row.get("bullpen_whip_season"),
            row.get("bullpen_runs_per_9_season"),
            row.get("bullpen_k_rate_season"),
            row.get("bullpen_bb_rate_season"),
            row.get("bullpen_k_minus_bb_rate_season"),
            row.get("bullpen_hr_rate_season"),
            row.get("bullpen_outs_last1d", 0),
            row.get("bullpen_outs_last3d", 0),
            row.get("bullpen_outs_last5d", 0),
            row.get("bullpen_outs_last7d", 0),
            row.get("bullpen_pitches_last1d", 0),
            row.get("bullpen_pitches_last3d", 0),
            row.get("bullpen_pitches_last5d", 0),
            row.get("bullpen_appearances_last3d", 0),
            row.get("bullpen_appearances_last5d", 0),
            row.get("relievers_used_yesterday_count", 0),
            row.get("relievers_used_last3d_count", 0),
            row.get("relievers_back_to_back_count", 0),
            row.get("relievers_2_of_last3_count", 0),
            row.get("high_usage_relievers_last3d_count", 0),
            row.get("freshness_score"),
            row.get("source_updated_at") or utc_now(),
        ),
    )
    conn.commit()


def upsert_team_bullpen_top_relievers(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        DELETE FROM team_bullpen_top_relievers
        WHERE game_id = ? AND side = ? AND top_n = ? AND as_of_ts <> ?
        """,
        (row["game_id"], row["side"], row["top_n"], row["as_of_ts"]),
    )
    conn.execute(
        """
        INSERT INTO team_bullpen_top_relievers (
          game_id, team_id, side, as_of_ts, stats_scope, ranking_method, top_n, n_available,
          selected_pitcher_ids_json, topn_appearances_season, topn_outs_season,
          topn_era_season, topn_whip_season, topn_runs_per_9_season,
          topn_k_rate_season, topn_bb_rate_season, topn_k_minus_bb_rate_season,
          topn_outs_last3d, topn_pitches_last3d, topn_appearances_last3d, topn_back_to_back_count,
          topn_freshness_score, quality_dropoff_vs_team, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, side, as_of_ts, top_n) DO UPDATE SET
          team_id = excluded.team_id,
          stats_scope = excluded.stats_scope,
          ranking_method = excluded.ranking_method,
          n_available = excluded.n_available,
          selected_pitcher_ids_json = excluded.selected_pitcher_ids_json,
          topn_appearances_season = excluded.topn_appearances_season,
          topn_outs_season = excluded.topn_outs_season,
          topn_era_season = excluded.topn_era_season,
          topn_whip_season = excluded.topn_whip_season,
          topn_runs_per_9_season = excluded.topn_runs_per_9_season,
          topn_k_rate_season = excluded.topn_k_rate_season,
          topn_bb_rate_season = excluded.topn_bb_rate_season,
          topn_k_minus_bb_rate_season = excluded.topn_k_minus_bb_rate_season,
          topn_outs_last3d = excluded.topn_outs_last3d,
          topn_pitches_last3d = excluded.topn_pitches_last3d,
          topn_appearances_last3d = excluded.topn_appearances_last3d,
          topn_back_to_back_count = excluded.topn_back_to_back_count,
          topn_freshness_score = excluded.topn_freshness_score,
          quality_dropoff_vs_team = excluded.quality_dropoff_vs_team,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["team_id"],
            row["side"],
            row["as_of_ts"],
            row.get("stats_scope", BULLPEN_STATS_SCOPE),
            row.get("ranking_method", BULLPEN_TOP_RELIEVER_RANKING_METHOD_V1),
            row["top_n"],
            row.get("n_available", 0),
            row.get("selected_pitcher_ids_json", "[]"),
            row.get("topn_appearances_season", 0),
            row.get("topn_outs_season", 0),
            row.get("topn_era_season"),
            row.get("topn_whip_season"),
            row.get("topn_runs_per_9_season"),
            row.get("topn_k_rate_season"),
            row.get("topn_bb_rate_season"),
            row.get("topn_k_minus_bb_rate_season"),
            row.get("topn_outs_last3d", 0),
            row.get("topn_pitches_last3d", 0),
            row.get("topn_appearances_last3d", 0),
            row.get("topn_back_to_back_count", 0),
            row.get("topn_freshness_score"),
            row.get("quality_dropoff_vs_team"),
            row.get("source_updated_at") or utc_now(),
        ),
    )
    conn.commit()


def upsert_player_handedness(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO player_handedness_dim (
          player_id, player_name, bat_side, pitch_hand, primary_position_code, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id) DO UPDATE SET
          player_name = COALESCE(excluded.player_name, player_handedness_dim.player_name),
          bat_side = COALESCE(excluded.bat_side, player_handedness_dim.bat_side),
          pitch_hand = COALESCE(excluded.pitch_hand, player_handedness_dim.pitch_hand),
          primary_position_code = COALESCE(excluded.primary_position_code, player_handedness_dim.primary_position_code),
          source_updated_at = COALESCE(excluded.source_updated_at, player_handedness_dim.source_updated_at),
          ingested_at = datetime('now')
        """,
        (
            row["player_id"],
            row.get("player_name"),
            row.get("bat_side"),
            row.get("pitch_hand"),
            row.get("primary_position_code"),
            row.get("source_updated_at") or utc_now(),
        ),
    )
    conn.commit()


def upsert_game_lineup_snapshot(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO game_lineup_snapshots (
          game_id, team_id, side, as_of_ts, snapshot_type, lineup_status, player_id,
          player_name, batting_order, position_code, bat_side, pitch_hand, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, side, as_of_ts, batting_order) DO UPDATE SET
          team_id = excluded.team_id,
          snapshot_type = excluded.snapshot_type,
          lineup_status = excluded.lineup_status,
          player_id = excluded.player_id,
          player_name = excluded.player_name,
          position_code = excluded.position_code,
          bat_side = COALESCE(excluded.bat_side, game_lineup_snapshots.bat_side),
          pitch_hand = COALESCE(excluded.pitch_hand, game_lineup_snapshots.pitch_hand),
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["team_id"],
            row["side"],
            row["as_of_ts"],
            row["snapshot_type"],
            row["lineup_status"],
            row["player_id"],
            row.get("player_name"),
            row.get("batting_order"),
            row.get("position_code"),
            row.get("bat_side"),
            row.get("pitch_hand"),
            row.get("source_updated_at") or utc_now(),
        ),
    )
    conn.commit()


def upsert_team_lineup_game_state(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        DELETE FROM team_lineup_game_state
        WHERE game_id = ? AND side = ? AND as_of_ts <> ?
        """,
        (row["game_id"], row["side"], row["as_of_ts"]),
    )
    conn.execute(
        """
        INSERT INTO team_lineup_game_state (
          game_id, team_id, side, as_of_ts, snapshot_type, lineup_status, lineup_known_flag,
          announced_lineup_count, lineup_l_count, lineup_r_count, lineup_s_count,
          top3_l_count, top3_r_count, top3_s_count, top5_l_count, top5_r_count, top5_s_count,
          lineup_lefty_pa_share_proxy, lineup_righty_pa_share_proxy, lineup_switch_pa_share_proxy,
          lineup_balance_score, lineup_quality_metric, lineup_quality_mean,
          top3_lineup_quality_mean, top5_lineup_quality_mean, lineup_vs_rhp_quality,
          lineup_vs_lhp_quality, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, side, as_of_ts) DO UPDATE SET
          team_id = excluded.team_id,
          snapshot_type = excluded.snapshot_type,
          lineup_status = excluded.lineup_status,
          lineup_known_flag = excluded.lineup_known_flag,
          announced_lineup_count = excluded.announced_lineup_count,
          lineup_l_count = excluded.lineup_l_count,
          lineup_r_count = excluded.lineup_r_count,
          lineup_s_count = excluded.lineup_s_count,
          top3_l_count = excluded.top3_l_count,
          top3_r_count = excluded.top3_r_count,
          top3_s_count = excluded.top3_s_count,
          top5_l_count = excluded.top5_l_count,
          top5_r_count = excluded.top5_r_count,
          top5_s_count = excluded.top5_s_count,
          lineup_lefty_pa_share_proxy = excluded.lineup_lefty_pa_share_proxy,
          lineup_righty_pa_share_proxy = excluded.lineup_righty_pa_share_proxy,
          lineup_switch_pa_share_proxy = excluded.lineup_switch_pa_share_proxy,
          lineup_balance_score = excluded.lineup_balance_score,
          lineup_quality_metric = excluded.lineup_quality_metric,
          lineup_quality_mean = excluded.lineup_quality_mean,
          top3_lineup_quality_mean = excluded.top3_lineup_quality_mean,
          top5_lineup_quality_mean = excluded.top5_lineup_quality_mean,
          lineup_vs_rhp_quality = excluded.lineup_vs_rhp_quality,
          lineup_vs_lhp_quality = excluded.lineup_vs_lhp_quality,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["team_id"],
            row["side"],
            row["as_of_ts"],
            row["snapshot_type"],
            row["lineup_status"],
            row.get("lineup_known_flag", 0),
            row.get("announced_lineup_count", 0),
            row.get("lineup_l_count"),
            row.get("lineup_r_count"),
            row.get("lineup_s_count"),
            row.get("top3_l_count"),
            row.get("top3_r_count"),
            row.get("top3_s_count"),
            row.get("top5_l_count"),
            row.get("top5_r_count"),
            row.get("top5_s_count"),
            row.get("lineup_lefty_pa_share_proxy"),
            row.get("lineup_righty_pa_share_proxy"),
            row.get("lineup_switch_pa_share_proxy"),
            row.get("lineup_balance_score"),
            row.get("lineup_quality_metric"),
            row.get("lineup_quality_mean"),
            row.get("top3_lineup_quality_mean"),
            row.get("top5_lineup_quality_mean"),
            row.get("lineup_vs_rhp_quality"),
            row.get("lineup_vs_lhp_quality"),
            row.get("source_updated_at") or utc_now(),
        ),
    )
    conn.commit()


def upsert_team_platoon_split(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        DELETE FROM team_platoon_splits
        WHERE game_id = ? AND side = ? AND vs_pitch_hand = ? AND as_of_ts <> ?
        """,
        (row["game_id"], row["side"], row["vs_pitch_hand"], row["as_of_ts"]),
    )
    conn.execute(
        """
        INSERT INTO team_platoon_splits (
          game_id, team_id, side, as_of_ts, vs_pitch_hand, stats_scope, games_in_sample, plate_appearances,
          batting_avg, obp, slg, ops, runs_per_game, strikeout_rate, walk_rate, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, side, as_of_ts, vs_pitch_hand) DO UPDATE SET
          team_id = excluded.team_id,
          stats_scope = excluded.stats_scope,
          games_in_sample = excluded.games_in_sample,
          plate_appearances = excluded.plate_appearances,
          batting_avg = excluded.batting_avg,
          obp = excluded.obp,
          slg = excluded.slg,
          ops = excluded.ops,
          runs_per_game = excluded.runs_per_game,
          strikeout_rate = excluded.strikeout_rate,
          walk_rate = excluded.walk_rate,
          source_updated_at = excluded.source_updated_at,
          ingested_at = datetime('now')
        """,
        (
            row["game_id"],
            row["team_id"],
            row["side"],
            row["as_of_ts"],
            row["vs_pitch_hand"],
            row.get("stats_scope", PLATOON_STATS_SCOPE),
            row.get("games_in_sample", 0),
            row.get("plate_appearances", 0),
            row.get("batting_avg"),
            row.get("obp"),
            row.get("slg"),
            row.get("ops"),
            row.get("runs_per_game"),
            row.get("strikeout_rate"),
            row.get("walk_rate"),
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


def _existing_game_pitcher_appearance_keys(conn: sqlite3.Connection, game_ids: set[int]) -> set[tuple[int, int]]:
    if not game_ids:
        return set()
    keys: set[tuple[int, int]] = set()
    for chunk in _chunked(sorted(game_ids)):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT game_id, pitcher_id FROM game_pitcher_appearances WHERE game_id IN ({placeholders})",
            chunk,
        ).fetchall()
        keys.update((int(row["game_id"]), int(row["pitcher_id"])) for row in rows)
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


def _statsapi_schedule_hydrate(*, date_value: str | None = None, start_date: str | None = None, end_date: str | None = None, include_series_status: bool = True) -> str:
    hydrate = "decisions,probablePitcher(note),linescore,broadcasts,game(content(media(epg)))"
    if not include_series_status:
        return hydrate
    if date_value == "2014-03-11" or (str(start_date) <= "2014-03-11" <= str(end_date)):
        return hydrate
    return hydrate + ",seriesStatus"


def _schedule_game_row_from_statsapi_payload(game: dict[str, Any], game_date: str) -> dict[str, Any]:
    away_team = ((game.get("teams") or {}).get("away") or {})
    home_team = ((game.get("teams") or {}).get("home") or {})
    away_probable = away_team.get("probablePitcher") if isinstance(away_team.get("probablePitcher"), dict) else {}
    home_probable = home_team.get("probablePitcher") if isinstance(home_team.get("probablePitcher"), dict) else {}
    game_info: dict[str, Any] = {
        "game_id": game.get("gamePk"),
        "game_datetime": game.get("gameDate"),
        "game_date": game_date,
        "season": _to_int(game.get("season")),
        "game_type": game.get("gameType"),
        "status": ((game.get("status") or {}).get("detailedState")),
        "away_name": (((away_team.get("team") or {}).get("name")) or "???"),
        "home_name": (((home_team.get("team") or {}).get("name")) or "???"),
        "away_id": ((away_team.get("team") or {}).get("id")),
        "home_id": ((home_team.get("team") or {}).get("id")),
        "doubleheader": game.get("doubleHeader"),
        "game_num": game.get("gameNumber"),
        "home_probable_pitcher": home_probable.get("fullName", ""),
        "away_probable_pitcher": away_probable.get("fullName", ""),
        "home_probable_pitcher_id": home_probable.get("id"),
        "away_probable_pitcher_id": away_probable.get("id"),
        "home_pitcher_note": home_probable.get("note", ""),
        "away_pitcher_note": away_probable.get("note", ""),
        "away_score": away_team.get("score", "0"),
        "home_score": home_team.get("score", "0"),
        "current_inning": ((game.get("linescore") or {}).get("currentInning", "")),
        "inning_state": ((game.get("linescore") or {}).get("inningState", "")),
        "venue_id": ((game.get("venue") or {}).get("id")),
        "venue_name": ((game.get("venue") or {}).get("name")),
        "venue": game.get("venue"),
        "dayNight": game.get("dayNight"),
        "national_broadcasts": list(
            {
                broadcast.get("name")
                for broadcast in (game.get("broadcasts") or [])
                if isinstance(broadcast, dict) and broadcast.get("isNational", False) and broadcast.get("name")
            }
        ),
        "series_status": ((game.get("seriesStatus") or {}).get("result")),
    }
    content = game.get("content") if isinstance(game.get("content"), dict) else {}
    media = content.get("media") if isinstance(content.get("media"), dict) else {}
    if media.get("freeGame", False):
        game_info["national_broadcasts"].append("MLB.tv Free Game")
    status = str(game_info["status"] or "")
    if status in {"Final", "Game Over"}:
        if game.get("isTie"):
            game_info.update({"winning_team": "Tie", "losing_Team": "Tie"})
        else:
            away_is_winner = bool(away_team.get("isWinner"))
            game_info.update(
                {
                    "winning_team": game_info["away_name"] if away_is_winner else game_info["home_name"],
                    "losing_team": game_info["home_name"] if away_is_winner else game_info["away_name"],
                    "winning_pitcher": ((game.get("decisions") or {}).get("winner") or {}).get("fullName", ""),
                    "losing_pitcher": ((game.get("decisions") or {}).get("loser") or {}).get("fullName", ""),
                    "save_pitcher": ((game.get("decisions") or {}).get("save") or {}).get("fullName"),
                }
            )
        game_info["summary"] = (
            f"{game_date} - {game_info['away_name']} ({away_team.get('score', '')}) @ "
            f"{game_info['home_name']} ({home_team.get('score', '')}) ({status})"
        )
    elif status == "In Progress":
        current_inning_ordinal = ((game.get("linescore") or {}).get("currentInningOrdinal")) or ""
        game_info["summary"] = (
            f"{game_date} - {game_info['away_name']} ({away_team.get('score', '0')}) @ "
            f"{game_info['home_name']} ({home_team.get('score', '0')}) "
            f"({game_info['inning_state']} of the {current_inning_ordinal})"
        )
    else:
        game_info["summary"] = f"{game_date} - {game_info['away_name']} @ {game_info['home_name']} ({status})"
    return game_info


def _parse_statsapi_schedule_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    dates = payload.get("dates")
    if not isinstance(dates, list):
        return []
    games: list[dict[str, Any]] = []
    for date_entry in dates:
        if not isinstance(date_entry, dict):
            continue
        game_date = str(date_entry.get("date") or "")
        for game in date_entry.get("games") or []:
            if isinstance(game, dict):
                games.append(_schedule_game_row_from_statsapi_payload(game, game_date))
    return games


def fetch_schedule_bounded(policy: RequestPolicy, budget: RequestBudget, **kwargs: Any) -> list[dict[str, Any]]:
    _require_statsapi_available()
    date_value = kwargs.get("date")
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    params: dict[str, Any] = {}
    if end_date and not start_date:
        date_value = end_date
        end_date = None
    if start_date and not end_date:
        date_value = start_date
        start_date = None
    if date_value:
        params["date"] = date_value
    elif start_date and end_date:
        params["startDate"] = start_date
        params["endDate"] = end_date
    if kwargs.get("team", "") != "":
        params["teamId"] = str(kwargs["team"])
    if kwargs.get("opponent", "") != "":
        params["opponentId"] = str(kwargs["opponent"])
    if kwargs.get("game_id"):
        params["gamePks"] = kwargs["game_id"]
    if kwargs.get("leagueId"):
        params["leagueId"] = kwargs["leagueId"]
    if kwargs.get("season") is not None:
        params["season"] = kwargs["season"]
    params["sportId"] = str(kwargs.get("sportId", 1))
    params["hydrate"] = _statsapi_schedule_hydrate(
        date_value=date_value,
        start_date=start_date,
        end_date=end_date,
        include_series_status=bool(kwargs.get("include_series_status", True)),
    )
    result = run_with_bounded_retries(lambda: statsapi.get("schedule", params), policy=policy, budget=budget)
    return _parse_statsapi_schedule_payload(result if isinstance(result, dict) else {})


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


def fetch_pitcher_appearances_boxscore_bounded(game_id: int, policy: RequestPolicy, budget: RequestBudget) -> dict[str, Any]:
    _require_statsapi_available()
    result = run_with_bounded_retries(
        lambda: statsapi.get("game_boxscore", {"gamePk": game_id}),
        policy=policy,
        budget=budget,
    )
    if not isinstance(result, dict):
        return {}
    return result


def fetch_game_boxscore_bounded(game_id: int, policy: RequestPolicy, budget: RequestBudget) -> dict[str, Any]:
    return fetch_pitcher_appearances_boxscore_bounded(game_id, policy, budget)


def fetch_people_bounded(player_ids: list[int], policy: RequestPolicy, budget: RequestBudget) -> dict[str, Any]:
    _require_statsapi_available()
    unique_ids = sorted({int(player_id) for player_id in player_ids if _to_int(player_id) is not None})
    if not unique_ids:
        return {}
    result = run_with_bounded_retries(
        lambda: statsapi.get(
            "people",
            {
                "personIds": ",".join(str(player_id) for player_id in unique_ids),
            },
        ),
        policy=policy,
        budget=budget,
    )
    return result if isinstance(result, dict) else {}


def fetch_json_url_bounded(url: str, policy: RequestPolicy, budget: RequestBudget) -> dict[str, Any]:
    def _fetch() -> dict[str, Any]:
        with urlopen(url, timeout=policy.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload if isinstance(payload, dict) else {}

    result = run_with_bounded_retries(_fetch, policy=policy, budget=budget)
    return result if isinstance(result, dict) else {}


def _parse_statsapi_venue_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    venues = payload.get("venues")
    venue = venues[0] if isinstance(venues, list) and venues else payload
    if not isinstance(venue, dict):
        return None
    venue_id = _to_int(venue.get("id"))
    venue_name = venue.get("name")
    location = venue.get("location") if isinstance(venue.get("location"), dict) else {}
    coordinates = location.get("defaultCoordinates") if isinstance(location.get("defaultCoordinates"), dict) else {}
    time_zone = venue.get("timeZone") if isinstance(venue.get("timeZone"), dict) else {}
    if not time_zone:
        time_zone = location.get("timeZone") if isinstance(location.get("timeZone"), dict) else {}
    field_info = venue.get("fieldInfo") if isinstance(venue.get("fieldInfo"), dict) else {}
    latitude = _to_float(coordinates.get("latitude"))
    longitude = _to_float(coordinates.get("longitude"))
    timezone_name = time_zone.get("id")
    if venue_id is None or not venue_name or timezone_name is None or latitude is None or longitude is None:
        return None
    return {
        "venue_id": venue_id,
        "venue_name": venue_name,
        "city": location.get("city"),
        "state": location.get("stateAbbrev") or location.get("state"),
        "country": location.get("country") or "USA",
        "timezone": timezone_name,
        "latitude": latitude,
        "longitude": longitude,
        "roof_type": _normalize_venue_roof_type(str(venue_name), field_info.get("roofType") or venue.get("roofType")),
        "statsapi_venue_name": venue_name,
        "source_updated_at": utc_now(),
    }


def fetch_statsapi_venue_details_bounded(venue_id: int, policy: RequestPolicy, budget: RequestBudget) -> dict[str, Any] | None:
    url = "https://statsapi.mlb.com/api/v1/venues/{venue_id}?{query}".format(
        venue_id=venue_id,
        query=urlencode({"hydrate": "location,fieldInfo,timezone"}),
    )
    return _parse_statsapi_venue_payload(fetch_json_url_bounded(url, policy=policy, budget=budget))


def fetch_open_meteo_hourly_bounded(
    base_url: str,
    *,
    latitude: float,
    longitude: float,
    timezone_name: str,
    start_date: str,
    end_date: str,
    hourly_fields: tuple[str, ...],
    policy: RequestPolicy,
    budget: RequestBudget,
) -> dict[str, Any]:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": timezone_name,
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "mm",
        "hourly": ",".join(hourly_fields),
    }
    return fetch_json_url_bounded(f"{base_url}?{urlencode(params)}", policy=policy, budget=budget)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _target_local_window(target_game_ts: str, timezone_name: str) -> tuple[datetime, str, str]:
    target_utc = _parse_iso_datetime(target_game_ts)
    if target_utc is None:
        raise ValueError(f"invalid target_game_ts={target_game_ts}")
    target_local = target_utc.astimezone(ZoneInfo(timezone_name))
    start_date = (target_local.date() - timedelta(days=1)).isoformat()
    end_date = (target_local.date() + timedelta(days=1)).isoformat()
    return target_local, start_date, end_date


def _select_open_meteo_hour(payload: dict[str, Any], target_local: datetime) -> dict[str, Any] | None:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        return None
    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        return None
    best_index: int | None = None
    best_abs_offset: float | None = None
    for index, raw_value in enumerate(times):
        if not isinstance(raw_value, str):
            continue
        try:
            candidate = datetime.fromisoformat(raw_value).replace(tzinfo=target_local.tzinfo)
        except ValueError:
            continue
        offset_hours = (candidate - target_local).total_seconds() / 3600.0
        abs_offset = abs(offset_hours)
        if best_abs_offset is None or abs_offset < best_abs_offset:
            best_index = index
            best_abs_offset = abs_offset
    if best_index is None or best_abs_offset is None or best_abs_offset > WEATHER_ALIGNMENT_WINDOW_HOURS:
        return None
    row: dict[str, Any] = {
        "selected_time_local": times[best_index],
        "hour_offset_from_first_pitch": round(
            (
                datetime.fromisoformat(str(times[best_index])).replace(tzinfo=target_local.tzinfo) - target_local
            ).total_seconds()
            / 3600.0,
            3,
        ),
    }
    for field in OPEN_METEO_HOURLY_FIELDS_FORECAST:
        values = hourly.get(field)
        row[field] = values[best_index] if isinstance(values, list) and best_index < len(values) else None
    return row


def _precipitation_probability_for_source(selected: dict[str, Any], source: str) -> float | None:
    if source != WEATHER_SOURCE_FORECAST:
        return None
    return _to_float(selected.get("precipitation_probability"))


def _derive_day_night(game_day_night: str | None, is_day_value: Any, target_local: datetime) -> tuple[str | None, str]:
    normalized = _normalize_day_night(game_day_night)
    if normalized is not None:
        return normalized, "games.day_night"
    is_day = _to_int(is_day_value)
    if is_day in {0, 1}:
        return ("day" if is_day == 1 else "night"), "open_meteo.is_day"
    return ("day" if 6 <= target_local.hour < 18 else "night"), "local_schedule_fallback"

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
        "venue_id": _to_int(entry.get("venue_id") or entry.get("venueId") or ((entry.get("venue") or {}).get("id") if isinstance(entry.get("venue"), dict) else None)),
        "day_night": _normalize_day_night(entry.get("day_night") or entry.get("dayNight")),
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
    filtered_by_game_id = {
        game_id: row for row in filtered_rows if (game_id := _to_int(row.get("game_id"))) is not None
    }
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
        venue_row = _extract_schedule_venue(filtered_by_game_id.get(int(game["game_id"]), {}))
        if venue_row is not None and all(
            venue_row.get(field) is not None for field in ("venue_name", "timezone", "latitude", "longitude")
        ):
            upsert_venue_dim(conn, venue_row)
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


def _boxscore_side_payload(boxscore: dict[str, Any], side: str) -> dict[str, Any] | None:
    side_payload = boxscore.get(side)
    if isinstance(side_payload, dict):
        return side_payload
    teams = boxscore.get("teams")
    if isinstance(teams, dict):
        nested_side = teams.get(side)
        if isinstance(nested_side, dict):
            return nested_side
    return None


def _iter_boxscore_pitching_lines(boxscore: dict[str, Any]) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    for side in ("home", "away"):
        side_payload = _boxscore_side_payload(boxscore, side)
        if not isinstance(side_payload, dict):
            continue
        team = side_payload.get("team") if isinstance(side_payload.get("team"), dict) else {}
        team_id = _to_int(team.get("id"))
        players = side_payload.get("players")
        if not isinstance(players, dict):
            continue
        pitcher_ids: list[int] = []
        seen_pitcher_ids: set[int] = set()
        raw_pitcher_ids = side_payload.get("pitchers")
        if isinstance(raw_pitcher_ids, list):
            for pitcher_id in raw_pitcher_ids:
                parsed_pitcher_id = _to_int(pitcher_id)
                if parsed_pitcher_id is None or parsed_pitcher_id in seen_pitcher_ids:
                    continue
                pitcher_ids.append(parsed_pitcher_id)
                seen_pitcher_ids.add(parsed_pitcher_id)
        if not pitcher_ids:
            for player_payload in players.values():
                if not isinstance(player_payload, dict):
                    continue
                person = player_payload.get("person") if isinstance(player_payload.get("person"), dict) else {}
                pitcher_id = _to_int(person.get("id")) or _to_int(player_payload.get("id"))
                if pitcher_id is None or pitcher_id in seen_pitcher_ids:
                    continue
                pitcher_ids.append(pitcher_id)
                seen_pitcher_ids.add(pitcher_id)
        appearance_order = 0
        for pitcher_id in pitcher_ids:
            player_payload = players.get(f"ID{pitcher_id}")
            if not isinstance(player_payload, dict):
                continue
            person = player_payload.get("person") if isinstance(player_payload.get("person"), dict) else {}
            stats = player_payload.get("stats") if isinstance(player_payload.get("stats"), dict) else {}
            pitching = stats.get("pitching") if isinstance(stats.get("pitching"), dict) else {}
            resolved_pitcher_id = _to_int(person.get("id")) or _to_int(player_payload.get("id")) or pitcher_id
            outs = _innings_to_outs(pitching.get("inningsPitched"))
            if resolved_pitcher_id is None or (not pitching and outs == 0):
                continue
            appearance_order += 1
            innings_pitched = _to_float(pitching.get("inningsPitched"))
            lines.append(
                {
                    "game_id": _to_int(boxscore.get("gamePk")) or _to_int(boxscore.get("gameId")),
                    "team_id": team_id,
                    "side": side,
                    "pitcher_id": resolved_pitcher_id,
                    "pitcher_name": person.get("fullName") or player_payload.get("name"),
                    "appearance_order": appearance_order,
                    "is_starter": 1 if appearance_order == 1 else 0,
                    "is_reliever": 0 if appearance_order == 1 else 1,
                    "outs_recorded": outs,
                    "innings_pitched": innings_pitched,
                    "batters_faced": _extract_int(pitching, "battersFaced", "batters_faced"),
                    "outs": outs,
                    "hits": _to_int(pitching.get("hits")) or 0,
                    "walks": _to_int(pitching.get("baseOnBalls")) or _to_int(pitching.get("walks")) or 0,
                    "strikeouts": _extract_int(pitching, "strikeOuts", "strikeouts") or 0,
                    "earned_runs": _to_int(pitching.get("earnedRuns")) or 0,
                    "runs": _to_int(pitching.get("runs")) or 0,
                    "home_runs": _extract_int(pitching, "homeRuns", "homeRunsAllowed") or 0,
                    "at_bats": _to_int(pitching.get("atBats")) or 0,
                    "strikes": _to_int(pitching.get("strikes")) or 0,
                    "pitches": _to_int(pitching.get("numberOfPitches")) or _to_int(pitching.get("pitches")) or 0,
                    "holds": _extract_int(pitching, "holds") or 0,
                    "save_flag": 1 if (_extract_int(pitching, "saves", "save") or 0) > 0 else 0,
                    "blown_save_flag": 1 if (_extract_int(pitching, "blownSaves", "blownSave") or 0) > 0 else 0,
                    "inherited_runners": _extract_int(pitching, "inheritedRunners") or 0,
                    "inherited_runners_scored": _extract_int(pitching, "inheritedRunnersScored") or 0,
                    "source_updated_at": utc_now(),
                }
            )
    return lines


def build_game_pitcher_appearance_rows(game_id: int, boxscore: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in _iter_boxscore_pitching_lines(boxscore):
        if _to_int(line.get("team_id")) is None:
            continue
        row = dict(line)
        row["game_id"] = game_id
        row.pop("outs", None)
        rows.append(row)
    return rows


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


def _parse_top_n_values(raw_value: str | None) -> list[int]:
    if raw_value is None:
        return list(BULLPEN_TOP_N_DEFAULTS)
    top_n_values: list[int] = []
    seen: set[int] = set()
    for token in raw_value.split(","):
        parsed = _to_int(token.strip())
        if parsed is None or parsed <= 0:
            raise ValueError(f"invalid top_n value: {token!r}")
        if parsed not in seen:
            top_n_values.append(parsed)
            seen.add(parsed)
    if not top_n_values:
        raise ValueError("at least one top_n value is required")
    return sorted(top_n_values)


def _normalize_hand(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if text.startswith("L"):
        return "L"
    if text.startswith("R"):
        return "R"
    if text.startswith("S"):
        return "S"
    return None


def _player_payload_hand(payload: dict[str, Any], key: str) -> str | None:
    value: Any = None
    for container in (payload, payload.get("person")):
        if not isinstance(container, dict):
            continue
        value = container.get(key)
        if isinstance(value, dict):
            for nested_key in ("code", "description"):
                normalized = _normalize_hand(value.get(nested_key))
                if normalized is not None:
                    return normalized
        normalized = _normalize_hand(value)
        if normalized is not None:
            return normalized
    return _normalize_hand(value)


def _player_payload_position_code(payload: dict[str, Any]) -> str | None:
    position = payload.get("position")
    if isinstance(position, dict):
        value = position.get("abbreviation") or position.get("code") or position.get("type")
        return str(value).strip() if value else None
    value = payload.get("primaryPosition")
    if isinstance(value, dict):
        code = value.get("abbreviation") or value.get("code") or value.get("type")
        return str(code).strip() if code else None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _player_batting_order_sort_key(player_payload: dict[str, Any]) -> int | None:
    for candidate in (
        player_payload.get("battingOrder"),
        player_payload.get("batOrder"),
        (player_payload.get("stats") or {}).get("battingOrder") if isinstance(player_payload.get("stats"), dict) else None,
    ):
        parsed = _to_int(candidate)
        if parsed is not None:
            return parsed // 100 if parsed >= 100 else parsed
    return None


def _boxscore_side_batting_order(side_payload: dict[str, Any]) -> list[int]:
    raw = side_payload.get("battingOrder") or side_payload.get("batters")
    ordered: list[int] = []
    seen: set[int] = set()
    if isinstance(raw, list):
        for item in raw:
            player_id = _to_int(item)
            if player_id is None or player_id in seen:
                continue
            ordered.append(player_id)
            seen.add(player_id)
    if ordered:
        return ordered
    players = side_payload.get("players")
    if not isinstance(players, dict):
        return []
    sortable: list[tuple[int, int]] = []
    for player_key, player_payload in players.items():
        if not isinstance(player_payload, dict):
            continue
        player_id = _to_int(player_payload.get("id"))
        if player_id is None and player_key.startswith("ID"):
            player_id = _to_int(player_key[2:])
        sort_key = _player_batting_order_sort_key(player_payload)
        if player_id is None or sort_key is None:
            continue
        sortable.append((sort_key, player_id))
    sortable.sort()
    return [player_id for _, player_id in sortable]


def _extract_player_handedness_rows_from_boxscore(boxscore: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_player_ids: set[int] = set()
    for side in ("home", "away"):
        side_payload = _boxscore_side_payload(boxscore, side)
        if not isinstance(side_payload, dict):
            continue
        players = side_payload.get("players")
        if not isinstance(players, dict):
            continue
        for player_key, player_payload in players.items():
            if not isinstance(player_payload, dict):
                continue
            person = player_payload.get("person") if isinstance(player_payload.get("person"), dict) else {}
            player_id = _to_int(person.get("id")) or _to_int(player_payload.get("id"))
            if player_id is None and player_key.startswith("ID"):
                player_id = _to_int(player_key[2:])
            if player_id is None or player_id in seen_player_ids:
                continue
            seen_player_ids.add(player_id)
            rows.append(
                {
                    "player_id": player_id,
                    "player_name": person.get("fullName") or player_payload.get("name"),
                    "bat_side": _player_payload_hand(player_payload, "batSide"),
                    "pitch_hand": _player_payload_hand(player_payload, "pitchHand"),
                    "primary_position_code": _player_payload_position_code(player_payload),
                    "source_updated_at": utc_now(),
                }
            )
    return rows


def _extract_player_handedness_rows_from_people(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    people = payload.get("people")
    if not isinstance(people, list):
        return rows
    for person_payload in people:
        if not isinstance(person_payload, dict):
            continue
        player_id = _to_int(person_payload.get("id"))
        if player_id is None:
            continue
        rows.append(
            {
                "player_id": player_id,
                "player_name": person_payload.get("fullName"),
                "bat_side": _player_payload_hand(person_payload, "batSide"),
                "pitch_hand": _player_payload_hand(person_payload, "pitchHand"),
                "primary_position_code": _player_payload_position_code(person_payload),
                "source_updated_at": utc_now(),
            }
        )
    return rows


def _player_needs_handedness_refresh(handedness_row: sqlite3.Row | dict[str, Any] | None) -> bool:
    if handedness_row is None:
        return True
    return _normalize_hand(handedness_row["bat_side"]) is None or _normalize_hand(handedness_row["pitch_hand"]) is None


def _merge_handedness_into_snapshot_row(
    snapshot_row: dict[str, Any],
    handedness_by_player: dict[int, sqlite3.Row | dict[str, Any]] | None,
) -> dict[str, Any]:
    if handedness_by_player is None:
        return snapshot_row
    player_id = _to_int(snapshot_row.get("player_id"))
    if player_id is None:
        return snapshot_row
    handedness_row = handedness_by_player.get(player_id)
    if handedness_row is None:
        return snapshot_row
    merged = dict(snapshot_row)
    if _normalize_hand(merged.get("bat_side")) is None:
        merged["bat_side"] = _normalize_hand(handedness_row["bat_side"])
    if _normalize_hand(merged.get("pitch_hand")) is None:
        merged["pitch_hand"] = _normalize_hand(handedness_row["pitch_hand"])
    return merged


def _refresh_player_handedness_from_people(
    conn: sqlite3.Connection,
    boxscore: dict[str, Any],
    handedness_by_player: dict[int, sqlite3.Row | dict[str, Any]],
    policy: RequestPolicy,
    budget: RequestBudget,
) -> int:
    rows_upserted = 0
    for handedness_row in _extract_player_handedness_rows_from_boxscore(boxscore):
        upsert_player_handedness(conn, handedness_row)
        handedness_by_player[int(handedness_row["player_id"])] = handedness_row
        rows_upserted += 1

    player_ids_to_refresh = sorted(
        {
            int(row["player_id"])
            for row in _extract_player_handedness_rows_from_boxscore(boxscore)
            if _to_int(row.get("player_id")) is not None
            and _player_needs_handedness_refresh(handedness_by_player.get(int(row["player_id"])))
        }
    )
    if not player_ids_to_refresh:
        return rows_upserted

    for batch_start in range(0, len(player_ids_to_refresh), PEOPLE_LOOKUP_BATCH_SIZE):
        batch_ids = player_ids_to_refresh[batch_start : batch_start + PEOPLE_LOOKUP_BATCH_SIZE]
        people_payload = fetch_people_bounded(batch_ids, policy, budget)
        for handedness_row in _extract_player_handedness_rows_from_people(people_payload):
            upsert_player_handedness(conn, handedness_row)
            handedness_by_player[int(handedness_row["player_id"])] = handedness_row
            rows_upserted += 1
    return rows_upserted


def _refresh_pitcher_handedness_by_ids(
    conn: sqlite3.Connection,
    pitcher_ids: list[int],
    handedness_by_player: dict[int, sqlite3.Row | dict[str, Any]],
    policy: RequestPolicy,
    budget: RequestBudget,
) -> int:
    player_ids_to_refresh = sorted(
        {
            int(player_id)
            for player_id in pitcher_ids
            if _to_int(player_id) is not None
            and _player_needs_handedness_refresh(handedness_by_player.get(int(player_id)))
        }
    )
    if not player_ids_to_refresh:
        return 0

    rows_upserted = 0
    for batch_start in range(0, len(player_ids_to_refresh), PEOPLE_LOOKUP_BATCH_SIZE):
        batch_ids = player_ids_to_refresh[batch_start : batch_start + PEOPLE_LOOKUP_BATCH_SIZE]
        people_payload = fetch_people_bounded(batch_ids, policy, budget)
        for handedness_row in _extract_player_handedness_rows_from_people(people_payload):
            upsert_player_handedness(conn, handedness_row)
            handedness_by_player[int(handedness_row["player_id"])] = handedness_row
            rows_upserted += 1
    return rows_upserted


def build_game_lineup_snapshot_rows(
    game_row: sqlite3.Row | dict[str, Any],
    boxscore: dict[str, Any],
    *,
    snapshot_type: str,
    handedness_by_player: dict[int, sqlite3.Row | dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    as_of_ts = _feature_as_of_ts(game_row)
    for side in ("home", "away"):
        side_payload = _boxscore_side_payload(boxscore, side)
        if not isinstance(side_payload, dict):
            continue
        team_id = _to_int(game_row[f"{side}_team_id"] if isinstance(game_row, sqlite3.Row) else game_row.get(f"{side}_team_id"))
        if team_id is None:
            continue
        players = side_payload.get("players")
        if not isinstance(players, dict):
            continue
        batting_order = _boxscore_side_batting_order(side_payload)
        for order_index, player_id in enumerate(batting_order[:9], start=1):
            player_payload = players.get(f"ID{player_id}")
            if not isinstance(player_payload, dict):
                continue
            person = player_payload.get("person") if isinstance(player_payload.get("person"), dict) else {}
            rows.append(
                _merge_handedness_into_snapshot_row(
                    {
                        "game_id": int(game_row["game_id"] if isinstance(game_row, sqlite3.Row) else game_row.get("game_id")),
                        "team_id": team_id,
                        "side": side,
                        "as_of_ts": as_of_ts,
                        "snapshot_type": snapshot_type,
                        "lineup_status": "full" if len(batting_order) >= 9 else "partial",
                        "player_id": player_id,
                        "player_name": person.get("fullName") or player_payload.get("name"),
                        "batting_order": order_index,
                        "position_code": _player_payload_position_code(player_payload),
                        "bat_side": _player_payload_hand(player_payload, "batSide"),
                        "pitch_hand": _player_payload_hand(player_payload, "pitchHand"),
                        "source_updated_at": utc_now(),
                    },
                    handedness_by_player,
                )
            )
    return rows


def _lineup_snapshot_type_for_game(game_row: sqlite3.Row | dict[str, Any], *, daily_mode: bool) -> str:
    status = str(game_row["status"] if isinstance(game_row, sqlite3.Row) else game_row.get("status") or "").strip()
    if daily_mode and status in {"Scheduled", "Pre-Game", "Warmup", "In Progress"}:
        return "announced"
    return "fallback" if _is_completed_game(status) else "announced"


def _resolve_lineup_bat_side(snapshot_row: sqlite3.Row | dict[str, Any], handedness_by_player: dict[int, sqlite3.Row]) -> str | None:
    snapshot_side = snapshot_row["bat_side"] if isinstance(snapshot_row, sqlite3.Row) else snapshot_row.get("bat_side")
    normalized = _normalize_hand(snapshot_side)
    if normalized is not None:
        return normalized
    player_id = _to_int(snapshot_row["player_id"] if isinstance(snapshot_row, sqlite3.Row) else snapshot_row.get("player_id"))
    if player_id is None:
        return None
    handedness_row = handedness_by_player.get(player_id)
    if handedness_row is None:
        return None
    return _normalize_hand(handedness_row["bat_side"])


def _safe_share(count: int, total: int) -> float | None:
    return _safe_round(_safe_div(float(count), float(total)), 3) if total > 0 else None


def _build_team_lineup_game_state_row(
    game_row: sqlite3.Row | dict[str, Any],
    side: str,
    snapshot_rows: list[sqlite3.Row],
    handedness_by_player: dict[int, sqlite3.Row],
) -> dict[str, Any]:
    team_id = _to_int(game_row[f"{side}_team_id"] if isinstance(game_row, sqlite3.Row) else game_row.get(f"{side}_team_id"))
    if team_id is None:
        raise ValueError(f"missing {side}_team_id for lineup state")
    if not snapshot_rows:
        return {
            "game_id": int(game_row["game_id"] if isinstance(game_row, sqlite3.Row) else game_row.get("game_id")),
            "team_id": team_id,
            "side": side,
            "as_of_ts": _feature_as_of_ts(game_row),
            "snapshot_type": "fallback",
            "lineup_status": "missing",
            "lineup_known_flag": 0,
            "announced_lineup_count": 0,
            "lineup_quality_metric": LINEUP_QUALITY_METRIC_UNAVAILABLE,
            "source_updated_at": utc_now(),
        }

    counts = {"L": 0, "R": 0, "S": 0}
    top3_counts = {"L": 0, "R": 0, "S": 0}
    top5_counts = {"L": 0, "R": 0, "S": 0}
    for row in snapshot_rows:
        bat_side = _resolve_lineup_bat_side(row, handedness_by_player)
        batting_order = _to_int(row["batting_order"])
        if bat_side is None:
            continue
        counts[bat_side] += 1
        if batting_order is not None and batting_order <= 3:
            top3_counts[bat_side] += 1
        if batting_order is not None and batting_order <= 5:
            top5_counts[bat_side] += 1

    announced_count = len(snapshot_rows)
    known_count = counts["L"] + counts["R"] + counts["S"]
    non_switch_count = counts["L"] + counts["R"]
    lineup_lefty_share = _safe_share(counts["L"], announced_count)
    lineup_righty_share = _safe_share(counts["R"], announced_count)
    lineup_switch_share = _safe_share(counts["S"], announced_count)
    lineup_balance_score = None
    if non_switch_count > 0:
        lineup_balance_score = _safe_round(1.0 - abs(counts["L"] - counts["R"]) / float(non_switch_count), 3)
    lineup_vs_rhp_quality = None
    lineup_vs_lhp_quality = None
    if known_count > 0:
        lineup_vs_rhp_quality = _safe_round((counts["L"] + 0.5 * counts["S"]) / float(known_count), 3)
        lineup_vs_lhp_quality = _safe_round((counts["R"] + 0.5 * counts["S"]) / float(known_count), 3)

    quality_metric = (
        LINEUP_QUALITY_METRIC_HAND_AFFINITY_PROXY_V1 if known_count > 0 else LINEUP_QUALITY_METRIC_UNAVAILABLE
    )
    first_row = snapshot_rows[0]
    return {
        "game_id": int(game_row["game_id"] if isinstance(game_row, sqlite3.Row) else game_row.get("game_id")),
        "team_id": team_id,
        "side": side,
        "as_of_ts": _feature_as_of_ts(game_row),
        "snapshot_type": first_row["snapshot_type"],
        "lineup_status": first_row["lineup_status"],
        "lineup_known_flag": 1 if announced_count > 0 else 0,
        "announced_lineup_count": announced_count,
        "lineup_l_count": counts["L"],
        "lineup_r_count": counts["R"],
        "lineup_s_count": counts["S"],
        "top3_l_count": top3_counts["L"],
        "top3_r_count": top3_counts["R"],
        "top3_s_count": top3_counts["S"],
        "top5_l_count": top5_counts["L"],
        "top5_r_count": top5_counts["R"],
        "top5_s_count": top5_counts["S"],
        "lineup_lefty_pa_share_proxy": lineup_lefty_share,
        "lineup_righty_pa_share_proxy": lineup_righty_share,
        "lineup_switch_pa_share_proxy": lineup_switch_share,
        "lineup_balance_score": lineup_balance_score,
        "lineup_quality_metric": quality_metric,
        "lineup_quality_mean": None,
        "top3_lineup_quality_mean": None,
        "top5_lineup_quality_mean": None,
        "lineup_vs_rhp_quality": lineup_vs_rhp_quality,
        "lineup_vs_lhp_quality": lineup_vs_lhp_quality,
        "source_updated_at": utc_now(),
    }


def _empty_platoon_split_state() -> dict[str, Any]:
    return {
        "games": 0,
        "estimated_ab": 0.0,
        "estimated_pa": 0.0,
        "hits": 0.0,
        "walks": 0.0,
        "strikeouts": 0.0,
        "runs": 0.0,
        "weighted_obp": 0.0,
        "weighted_slg": 0.0,
        "weighted_ops": 0.0,
    }


def _estimate_team_at_bats(team_stats_row: sqlite3.Row | dict[str, Any]) -> float:
    hits = _to_float(team_stats_row["hits"] if isinstance(team_stats_row, sqlite3.Row) else team_stats_row.get("hits")) or 0.0
    batting_avg = _to_float(
        team_stats_row["batting_avg"] if isinstance(team_stats_row, sqlite3.Row) else team_stats_row.get("batting_avg")
    )
    if batting_avg is None or batting_avg <= 0:
        return 0.0
    return max(0.0, round(hits / batting_avg, 3))


def _update_platoon_split_state(split_state: dict[str, Any], team_stats_row: sqlite3.Row) -> None:
    estimated_ab = _estimate_team_at_bats(team_stats_row)
    walks = _to_float(team_stats_row["walks"]) or 0.0
    estimated_pa = estimated_ab + walks
    split_state["games"] += 1
    split_state["estimated_ab"] += estimated_ab
    split_state["estimated_pa"] += estimated_pa
    split_state["hits"] += _to_float(team_stats_row["hits"]) or 0.0
    split_state["walks"] += walks
    split_state["strikeouts"] += _to_float(team_stats_row["strikeouts"]) or 0.0
    split_state["runs"] += _to_float(team_stats_row["runs"]) or 0.0
    split_state["weighted_obp"] += (_to_float(team_stats_row["obp"]) or 0.0) * estimated_pa
    split_state["weighted_slg"] += (_to_float(team_stats_row["slg"]) or 0.0) * estimated_ab
    split_state["weighted_ops"] += (_to_float(team_stats_row["ops"]) or 0.0) * estimated_pa


def _opponent_pitcher_hand_for_team_side(
    pitcher_context_by_game_side: dict[tuple[int, str], sqlite3.Row],
    handedness_by_player: dict[int, sqlite3.Row],
    game_id: int,
    offense_side: str,
) -> str | None:
    defense_side = "away" if offense_side == "home" else "home"
    pitcher_row = pitcher_context_by_game_side.get((game_id, defense_side))
    if pitcher_row is None:
        return None
    pitcher_id = _to_int(pitcher_row["pitcher_id"]) or _to_int(pitcher_row["probable_pitcher_id"])
    if pitcher_id is None:
        return None
    handedness_row = handedness_by_player.get(pitcher_id)
    if handedness_row is None:
        return None
    return _normalize_hand(handedness_row["pitch_hand"])


def _build_team_platoon_split_row(
    game_row: sqlite3.Row | dict[str, Any],
    side: str,
    vs_pitch_hand: str,
    split_state: dict[str, Any] | None,
) -> dict[str, Any]:
    team_id = _to_int(game_row[f"{side}_team_id"] if isinstance(game_row, sqlite3.Row) else game_row.get(f"{side}_team_id"))
    if team_id is None:
        raise ValueError(f"missing {side}_team_id for platoon split")
    state = split_state or _empty_platoon_split_state()
    estimated_ab = float(state.get("estimated_ab", 0.0) or 0.0)
    estimated_pa = float(state.get("estimated_pa", 0.0) or 0.0)
    batting_avg = _safe_round(_safe_div(float(state.get("hits", 0.0) or 0.0), estimated_ab), 3) if estimated_ab > 0 else None
    obp = _safe_round(_safe_div(float(state.get("weighted_obp", 0.0) or 0.0), estimated_pa), 3) if estimated_pa > 0 else None
    slg = _safe_round(_safe_div(float(state.get("weighted_slg", 0.0) or 0.0), estimated_ab), 3) if estimated_ab > 0 else None
    ops = _safe_round(_safe_div(float(state.get("weighted_ops", 0.0) or 0.0), estimated_pa), 3) if estimated_pa > 0 else None
    games_in_sample = int(state.get("games", 0) or 0)
    return {
        "game_id": int(game_row["game_id"] if isinstance(game_row, sqlite3.Row) else game_row.get("game_id")),
        "team_id": team_id,
        "side": side,
        "as_of_ts": _feature_as_of_ts(game_row),
        "vs_pitch_hand": vs_pitch_hand,
        "stats_scope": PLATOON_STATS_SCOPE,
        "games_in_sample": games_in_sample,
        "plate_appearances": int(round(estimated_pa)),
        "batting_avg": batting_avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "runs_per_game": _safe_round(_safe_div(float(state.get("runs", 0.0) or 0.0), games_in_sample), 3)
        if games_in_sample > 0
        else None,
        "strikeout_rate": _safe_round(_safe_div(float(state.get("strikeouts", 0.0) or 0.0), estimated_pa), 3)
        if estimated_pa > 0
        else None,
        "walk_rate": _safe_round(_safe_div(float(state.get("walks", 0.0) or 0.0), estimated_pa), 3)
        if estimated_pa > 0
        else None,
        "source_updated_at": utc_now(),
    }


def _empty_bullpen_team_state() -> dict[str, Any]:
    return {
        "game_ids_in_sample": set(),
        "pitchers": {},
        "season_totals": {
            "appearances": 0,
            "outs": 0,
            "hits": 0,
            "walks": 0,
            "strikeouts": 0,
            "runs": 0,
            "earned_runs": 0,
            "home_runs": 0,
            "batters_faced": 0,
            "pitches": 0,
        },
    }


def _empty_bullpen_pitcher_state(pitcher_id: int, pitcher_name: str | None) -> dict[str, Any]:
    return {
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "appearances": 0,
        "outs": 0,
        "hits": 0,
        "walks": 0,
        "strikeouts": 0,
        "runs": 0,
        "earned_runs": 0,
        "home_runs": 0,
        "batters_faced": 0,
        "pitches": 0,
        "by_date": {},
    }


def _update_bullpen_team_state(team_state: dict[str, Any], appearance_row: sqlite3.Row | dict[str, Any], game_date: str) -> None:
    pitcher_id = _to_int(appearance_row["pitcher_id"] if isinstance(appearance_row, sqlite3.Row) else appearance_row.get("pitcher_id"))
    if pitcher_id is None:
        return
    pitcher_name = appearance_row["pitcher_name"] if isinstance(appearance_row, sqlite3.Row) else appearance_row.get("pitcher_name")
    pitchers = team_state["pitchers"]
    pitcher_state = pitchers.setdefault(pitcher_id, _empty_bullpen_pitcher_state(pitcher_id, pitcher_name))
    pitcher_state["pitcher_name"] = pitcher_state.get("pitcher_name") or pitcher_name
    pitcher_state["appearances"] += 1
    for key in ("outs_recorded", "hits", "walks", "strikeouts", "runs", "earned_runs", "home_runs", "batters_faced", "pitches"):
        target_key = "outs" if key == "outs_recorded" else key
        value = appearance_row[key] if isinstance(appearance_row, sqlite3.Row) else appearance_row.get(key)
        pitcher_state[target_key] += int(value or 0)
        team_state["season_totals"][target_key] += int(value or 0)
    team_state["season_totals"]["appearances"] += 1
    team_state["game_ids_in_sample"].add(int(appearance_row["game_id"] if isinstance(appearance_row, sqlite3.Row) else appearance_row.get("game_id")))
    date_bucket = pitcher_state["by_date"].setdefault(
        game_date,
        {"outs": 0, "pitches": 0, "appearances": 0},
    )
    date_bucket["outs"] += int(appearance_row["outs_recorded"] if isinstance(appearance_row, sqlite3.Row) else appearance_row.get("outs_recorded") or 0)
    date_bucket["pitches"] += int(appearance_row["pitches"] if isinstance(appearance_row, sqlite3.Row) else appearance_row.get("pitches") or 0)
    date_bucket["appearances"] += 1


def _bullpen_rate_metrics_from_totals(totals: dict[str, Any]) -> dict[str, float | None]:
    outs = int(totals.get("outs", 0) or 0)
    innings = outs / 3.0
    batters_faced = float(totals.get("batters_faced", 0) or 0)
    hits = float(totals.get("hits", 0) or 0)
    walks = float(totals.get("walks", 0) or 0)
    strikeouts = float(totals.get("strikeouts", 0) or 0)
    runs = float(totals.get("runs", 0) or 0)
    earned_runs = float(totals.get("earned_runs", 0) or 0)
    home_runs = float(totals.get("home_runs", 0) or 0)
    k_rate = _safe_div(strikeouts, batters_faced)
    bb_rate = _safe_div(walks, batters_faced)
    return {
        "era": _safe_round(_safe_div(earned_runs * 9.0, innings), 3),
        "whip": _safe_round(_safe_div(hits + walks, innings), 3),
        "runs_per_9": _safe_round(_safe_div(runs * 9.0, innings), 3),
        "k_rate": _safe_round(k_rate, 3),
        "bb_rate": _safe_round(bb_rate, 3),
        "k_minus_bb_rate": _safe_round((k_rate - bb_rate) if k_rate is not None and bb_rate is not None else None, 3),
        "hr_rate": _safe_round(_safe_div(home_runs, innings), 3),
    }


def _bullpen_quality_score_from_metrics(metrics: dict[str, float | None]) -> float | None:
    k_minus_bb_rate = metrics.get("k_minus_bb_rate")
    whip = metrics.get("whip")
    runs_per_9 = metrics.get("runs_per_9")
    if k_minus_bb_rate is None and whip is None and runs_per_9 is None:
        return None
    return _safe_round((k_minus_bb_rate or 0.0) * 100.0 - (whip or 0.0) * 10.0 - (runs_per_9 or 0.0) * 2.0, 3)


def _pitcher_recent_window_summary(pitcher_state: dict[str, Any], target_game_date: str) -> dict[str, int]:
    target_date = _parse_iso_date(target_game_date)
    summary = {
        "outs_last1d": 0,
        "outs_last3d": 0,
        "outs_last5d": 0,
        "outs_last7d": 0,
        "pitches_last1d": 0,
        "pitches_last3d": 0,
        "pitches_last5d": 0,
        "appearances_last3d": 0,
        "appearances_last5d": 0,
        "used_yesterday": 0,
        "used_last3d": 0,
        "back_to_back": 0,
        "used_2_of_last3": 0,
        "high_usage_last3d": 0,
    }
    if target_date is None:
        return summary
    days_used_last3: set[int] = set()
    for appearance_date, day_stats in pitcher_state.get("by_date", {}).items():
        parsed_date = _parse_iso_date(appearance_date)
        if parsed_date is None:
            continue
        delta_days = (target_date - parsed_date).days
        if delta_days <= 0:
            continue
        if delta_days <= 1:
            summary["outs_last1d"] += int(day_stats.get("outs", 0) or 0)
            summary["pitches_last1d"] += int(day_stats.get("pitches", 0) or 0)
        if delta_days <= 3:
            summary["outs_last3d"] += int(day_stats.get("outs", 0) or 0)
            summary["pitches_last3d"] += int(day_stats.get("pitches", 0) or 0)
            summary["appearances_last3d"] += int(day_stats.get("appearances", 0) or 0)
            days_used_last3.add(delta_days)
        if delta_days <= 5:
            summary["outs_last5d"] += int(day_stats.get("outs", 0) or 0)
            summary["pitches_last5d"] += int(day_stats.get("pitches", 0) or 0)
            summary["appearances_last5d"] += int(day_stats.get("appearances", 0) or 0)
        if delta_days <= 7:
            summary["outs_last7d"] += int(day_stats.get("outs", 0) or 0)
    summary["used_yesterday"] = 1 if 1 in days_used_last3 else 0
    summary["used_last3d"] = 1 if days_used_last3 else 0
    summary["back_to_back"] = 1 if {1, 2}.issubset(days_used_last3) else 0
    summary["used_2_of_last3"] = 1 if len(days_used_last3) >= 2 else 0
    summary["high_usage_last3d"] = 1 if (
        summary["pitches_last1d"] >= BULLPEN_HIGH_USAGE_PITCHES_LAST1D
        or summary["pitches_last3d"] >= BULLPEN_HIGH_USAGE_PITCHES_LAST3D
    ) else 0
    return summary


def _bullpen_freshness_score(team_recent: dict[str, int], season_pitcher_count: int) -> float | None:
    if season_pitcher_count <= 0:
        return None
    score = 100.0
    score -= float(team_recent.get("bullpen_outs_last3d", 0) or 0) * 1.0
    score -= float(team_recent.get("bullpen_pitches_last3d", 0) or 0) * 0.15
    score -= float(team_recent.get("relievers_used_yesterday_count", 0) or 0) * 3.0
    score -= float(team_recent.get("relievers_back_to_back_count", 0) or 0) * 6.0
    score -= float(team_recent.get("relievers_2_of_last3_count", 0) or 0) * 4.0
    score -= float(team_recent.get("high_usage_relievers_last3d_count", 0) or 0) * 8.0
    return _safe_round(max(0.0, min(100.0, score)), 3)


def _build_team_bullpen_game_state_row(
    game_row: sqlite3.Row | dict[str, Any],
    side: str,
    team_state: dict[str, Any] | None,
) -> dict[str, Any]:
    team_id = _to_int(game_row[f"{side}_team_id"] if isinstance(game_row, sqlite3.Row) else game_row.get(f"{side}_team_id"))
    if team_id is None:
        raise ValueError(f"missing {side}_team_id for bullpen game state")
    game_date = str(game_row["game_date"] if isinstance(game_row, sqlite3.Row) else game_row.get("game_date"))
    state = team_state or _empty_bullpen_team_state()
    season_totals = dict(state.get("season_totals", {}))
    season_pitcher_count = len(state.get("pitchers", {}))
    team_recent = {
        "bullpen_outs_last1d": 0,
        "bullpen_outs_last3d": 0,
        "bullpen_outs_last5d": 0,
        "bullpen_outs_last7d": 0,
        "bullpen_pitches_last1d": 0,
        "bullpen_pitches_last3d": 0,
        "bullpen_pitches_last5d": 0,
        "bullpen_appearances_last3d": 0,
        "bullpen_appearances_last5d": 0,
        "relievers_used_yesterday_count": 0,
        "relievers_used_last3d_count": 0,
        "relievers_back_to_back_count": 0,
        "relievers_2_of_last3_count": 0,
        "high_usage_relievers_last3d_count": 0,
    }
    for pitcher_state in state.get("pitchers", {}).values():
        recent = _pitcher_recent_window_summary(pitcher_state, game_date)
        team_recent["bullpen_outs_last1d"] += recent["outs_last1d"]
        team_recent["bullpen_outs_last3d"] += recent["outs_last3d"]
        team_recent["bullpen_outs_last5d"] += recent["outs_last5d"]
        team_recent["bullpen_outs_last7d"] += recent["outs_last7d"]
        team_recent["bullpen_pitches_last1d"] += recent["pitches_last1d"]
        team_recent["bullpen_pitches_last3d"] += recent["pitches_last3d"]
        team_recent["bullpen_pitches_last5d"] += recent["pitches_last5d"]
        team_recent["bullpen_appearances_last3d"] += recent["appearances_last3d"]
        team_recent["bullpen_appearances_last5d"] += recent["appearances_last5d"]
        team_recent["relievers_used_yesterday_count"] += recent["used_yesterday"]
        team_recent["relievers_used_last3d_count"] += recent["used_last3d"]
        team_recent["relievers_back_to_back_count"] += recent["back_to_back"]
        team_recent["relievers_2_of_last3_count"] += recent["used_2_of_last3"]
        team_recent["high_usage_relievers_last3d_count"] += recent["high_usage_last3d"]
    metrics = _bullpen_rate_metrics_from_totals(season_totals)
    return {
        "game_id": int(game_row["game_id"] if isinstance(game_row, sqlite3.Row) else game_row.get("game_id")),
        "team_id": team_id,
        "side": side,
        "as_of_ts": _feature_as_of_ts(game_row),
        "stats_scope": BULLPEN_STATS_SCOPE,
        "freshness_method": BULLPEN_FRESHNESS_METHOD_V1,
        "season_games_in_sample": len(state.get("game_ids_in_sample", set())),
        "bullpen_pitchers_in_sample": season_pitcher_count,
        "bullpen_appearances_season": int(season_totals.get("appearances", 0) or 0),
        "bullpen_outs_season": int(season_totals.get("outs", 0) or 0),
        "bullpen_era_season": metrics["era"],
        "bullpen_whip_season": metrics["whip"],
        "bullpen_runs_per_9_season": metrics["runs_per_9"],
        "bullpen_k_rate_season": metrics["k_rate"],
        "bullpen_bb_rate_season": metrics["bb_rate"],
        "bullpen_k_minus_bb_rate_season": metrics["k_minus_bb_rate"],
        "bullpen_hr_rate_season": metrics["hr_rate"],
        "bullpen_outs_last1d": team_recent["bullpen_outs_last1d"],
        "bullpen_outs_last3d": team_recent["bullpen_outs_last3d"],
        "bullpen_outs_last5d": team_recent["bullpen_outs_last5d"],
        "bullpen_outs_last7d": team_recent["bullpen_outs_last7d"],
        "bullpen_pitches_last1d": team_recent["bullpen_pitches_last1d"],
        "bullpen_pitches_last3d": team_recent["bullpen_pitches_last3d"],
        "bullpen_pitches_last5d": team_recent["bullpen_pitches_last5d"],
        "bullpen_appearances_last3d": team_recent["bullpen_appearances_last3d"],
        "bullpen_appearances_last5d": team_recent["bullpen_appearances_last5d"],
        "relievers_used_yesterday_count": team_recent["relievers_used_yesterday_count"],
        "relievers_used_last3d_count": team_recent["relievers_used_last3d_count"],
        "relievers_back_to_back_count": team_recent["relievers_back_to_back_count"],
        "relievers_2_of_last3_count": team_recent["relievers_2_of_last3_count"],
        "high_usage_relievers_last3d_count": team_recent["high_usage_relievers_last3d_count"],
        "freshness_score": _bullpen_freshness_score(team_recent, season_pitcher_count),
        "source_updated_at": utc_now(),
    }


def _reliever_quality_sort_key(pitcher_state: dict[str, Any]) -> tuple[int, float, int, int]:
    metrics = _bullpen_rate_metrics_from_totals(pitcher_state)
    eligible = 1 if (
        int(pitcher_state.get("appearances", 0) or 0) >= BULLPEN_TOP_RELIEVER_MIN_APPEARANCES
        or int(pitcher_state.get("outs", 0) or 0) >= BULLPEN_TOP_RELIEVER_MIN_OUTS
    ) else 0
    quality_score = (metrics.get("k_minus_bb_rate") or 0.0) * 100.0
    quality_score -= (metrics.get("whip") or 0.0) * 10.0
    quality_score -= (metrics.get("runs_per_9") or 0.0) * 2.0
    quality_score += min(int(pitcher_state.get("outs", 0) or 0), 60) * 0.1
    return (
        eligible,
        round(quality_score, 6),
        int(pitcher_state.get("outs", 0) or 0),
        -int(pitcher_state.get("pitcher_id", 0) or 0),
    )


def _build_team_bullpen_top_reliever_rows(
    game_row: sqlite3.Row | dict[str, Any],
    side: str,
    team_state: dict[str, Any] | None,
    team_game_state_row: dict[str, Any],
    top_n_values: list[int],
) -> list[dict[str, Any]]:
    team_id = _to_int(game_row[f"{side}_team_id"] if isinstance(game_row, sqlite3.Row) else game_row.get(f"{side}_team_id"))
    if team_id is None:
        raise ValueError(f"missing {side}_team_id for bullpen top relievers")
    state = team_state or _empty_bullpen_team_state()
    game_date = str(game_row["game_date"] if isinstance(game_row, sqlite3.Row) else game_row.get("game_date"))
    ranked_pitchers = sorted(
        state.get("pitchers", {}).values(),
        key=_reliever_quality_sort_key,
        reverse=True,
    )
    team_quality_score = _bullpen_quality_score_from_metrics(
        {
            "k_minus_bb_rate": team_game_state_row.get("bullpen_k_minus_bb_rate_season"),
            "whip": team_game_state_row.get("bullpen_whip_season"),
            "runs_per_9": team_game_state_row.get("bullpen_runs_per_9_season"),
        }
    )
    rows: list[dict[str, Any]] = []
    for top_n in top_n_values:
        selected_pitchers = ranked_pitchers[:top_n]
        aggregate = {
            "appearances": 0,
            "outs": 0,
            "hits": 0,
            "walks": 0,
            "strikeouts": 0,
            "runs": 0,
            "earned_runs": 0,
            "home_runs": 0,
            "batters_faced": 0,
            "pitches": 0,
        }
        topn_recent = {
            "outs_last3d": 0,
            "pitches_last3d": 0,
            "appearances_last3d": 0,
            "back_to_back_count": 0,
        }
        for pitcher_state in selected_pitchers:
            for key in aggregate:
                aggregate[key] += int(pitcher_state.get(key, 0) or 0)
            recent = _pitcher_recent_window_summary(pitcher_state, game_date)
            topn_recent["outs_last3d"] += recent["outs_last3d"]
            topn_recent["pitches_last3d"] += recent["pitches_last3d"]
            topn_recent["appearances_last3d"] += recent["appearances_last3d"]
            topn_recent["back_to_back_count"] += recent["back_to_back"]
        metrics = _bullpen_rate_metrics_from_totals(aggregate)
        topn_quality_score = _bullpen_quality_score_from_metrics(metrics)
        rows.append(
            {
                "game_id": int(game_row["game_id"] if isinstance(game_row, sqlite3.Row) else game_row.get("game_id")),
                "team_id": team_id,
                "side": side,
                "as_of_ts": _feature_as_of_ts(game_row),
                "stats_scope": BULLPEN_STATS_SCOPE,
                "ranking_method": BULLPEN_TOP_RELIEVER_RANKING_METHOD_V1,
                "top_n": top_n,
                "n_available": len(selected_pitchers),
                "selected_pitcher_ids_json": json.dumps(
                    [int(pitcher_state["pitcher_id"]) for pitcher_state in selected_pitchers],
                    sort_keys=True,
                ),
                "topn_appearances_season": aggregate["appearances"],
                "topn_outs_season": aggregate["outs"],
                "topn_era_season": metrics["era"],
                "topn_whip_season": metrics["whip"],
                "topn_runs_per_9_season": metrics["runs_per_9"],
                "topn_k_rate_season": metrics["k_rate"],
                "topn_bb_rate_season": metrics["bb_rate"],
                "topn_k_minus_bb_rate_season": metrics["k_minus_bb_rate"],
                "topn_outs_last3d": topn_recent["outs_last3d"],
                "topn_pitches_last3d": topn_recent["pitches_last3d"],
                "topn_appearances_last3d": topn_recent["appearances_last3d"],
                "topn_back_to_back_count": topn_recent["back_to_back_count"],
                "topn_freshness_score": _bullpen_freshness_score(
                    {
                        "bullpen_outs_last3d": topn_recent["outs_last3d"],
                        "bullpen_pitches_last3d": topn_recent["pitches_last3d"],
                        "relievers_used_yesterday_count": 0,
                        "relievers_back_to_back_count": topn_recent["back_to_back_count"],
                        "relievers_2_of_last3_count": 0,
                        "high_usage_relievers_last3d_count": 0,
                    },
                    len(selected_pitchers),
                ),
                "quality_dropoff_vs_team": _safe_round(
                    (topn_quality_score - team_quality_score)
                    if topn_quality_score is not None and team_quality_score is not None
                    else None,
                    3,
                ),
                "source_updated_at": utc_now(),
            }
        )
    return rows


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
            handedness_by_player: dict[int, sqlite3.Row | dict[str, Any]] = _load_handedness_by_player(conn)
            boxscore_fallback_used = False
            handedness_fallback_used = False
            rows_upserted = 0
            candidate_rows: list[dict[str, Any]] = []
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
                try:
                    _refresh_pitcher_handedness_by_ids(
                        conn,
                        [
                            int(probable_pitcher_id)
                            for probable_pitcher_id in (
                                row.get("probable_pitcher_id") for row in context_rows if isinstance(row, dict)
                            )
                            if _to_int(probable_pitcher_id) is not None
                        ],
                        handedness_by_player,
                        config.request_policy,
                        budget,
                    )
                except Exception:
                    handedness_fallback_used = True
                for row in context_rows:
                    candidate_rows.append(row)
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

            safety_report = _build_pitcher_context_quality_report(
                candidate_rows,
                season=season,
                schedule_fallback_used=schedule_fallback_used,
                boxscore_fallback_used=boxscore_fallback_used,
                handedness_fallback_used=handedness_fallback_used,
                max_null_safe_fallback_share=args.max_null_safe_fallback_share,
                max_missing_probable_share=args.max_missing_probable_share,
            )
            if args.repair_mode and not safety_report["safe_for_canonical_write"]:
                raise RuntimeError(
                    "repair-mode pitcher-context run aborted: "
                    f"null_safe_fallback_share={safety_report['null_safe_fallback_share']:.3f}, "
                    f"missing_probable_share={safety_report['missing_probable_share']:.3f}, "
                    f"schedule_fallback_used={safety_report['schedule_fallback_used']}"
                )

            rows_upserted = 0
            richer_rows_preserved = 0
            for row in candidate_rows:
                existing_row = conn.execute(
                    "SELECT * FROM game_pitcher_context WHERE game_id = ? AND side = ?",
                    (row["game_id"], row["side"]),
                ).fetchone()
                if existing_row is not None and _should_preserve_existing_pitcher_context(dict(existing_row), row):
                    richer_rows_preserved += 1
                upsert_game_pitcher_context(conn, row)
                rows_upserted += 1

            cursor = {
                "season": season,
                "games_seen": len(games_for_season),
                "rows_upserted": rows_upserted,
                "distinct_pitchers_cached": len(prior_pitcher_aggregates),
                "richer_rows_preserved": richer_rows_preserved,
                **safety_report,
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


def cmd_backfill_pitcher_appearances(args: argparse.Namespace) -> None:
    season = validate_supported_season(args.season)
    job_name = pitcher_appearances_job_name(season)
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = f"season={season}"

    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=f"{job_name}:{partition_key}", config=config)
        processed_games = 0
        rows_inserted = 0
        rows_updated = 0
        total_rows_upserted = 0
        last_game_id: int | None = None
        try:
            game_ids = _completed_game_ids_for_season(conn, season, limit=args.max_games)
            existing_keys = _existing_game_pitcher_appearance_keys(conn, set(game_ids))

            for game_id in game_ids:
                boxscore = fetch_pitcher_appearances_boxscore_bounded(game_id, config.request_policy, budget)
                rows = build_game_pitcher_appearance_rows(game_id, boxscore)
                for row in rows:
                    key = (int(row["game_id"]), int(row["pitcher_id"]))
                    if key in existing_keys:
                        rows_updated += 1
                    else:
                        rows_inserted += 1
                        existing_keys.add(key)
                    upsert_game_pitcher_appearance(conn, row)
                    total_rows_upserted += 1

                processed_games += 1
                last_game_id = game_id
                if config.checkpoint_every > 0 and processed_games % config.checkpoint_every == 0:
                    upsert_checkpoint(
                        conn,
                        job_name=job_name,
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
                "distinct_pitchers": len({key[1] for key in existing_keys}),
            }
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor=run_stats,
                status="success",
                last_game_id=last_game_id,
            )
            finish_run(conn, run_id, "success", note=format_run_observability(run_stats), request_count=budget.used)
            print(
                f"Pitcher appearances backfill complete for {partition_key}: "
                f"{format_run_observability({**run_stats, 'request_count': budget.used})}"
            )
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor={"season": season, "games_processed": processed_games},
                status="failed",
                last_game_id=last_game_id,
                last_error=error,
            )
            finish_run(conn, run_id, "failed", note=error, request_count=budget.used)
            raise


def cmd_backfill_bullpen_support(args: argparse.Namespace) -> None:
    season = validate_supported_season(args.season)
    job_name = bullpen_support_job_name(season)
    top_n_values = _parse_top_n_values(getattr(args, "top_n_values", None))
    config = build_config(args)
    partition_key = f"season={season}"

    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=f"{job_name}:{partition_key}", config=config)
        last_game_id: int | None = None
        game_state_rows_upserted = 0
        top_reliever_rows_upserted = 0
        try:
            games = conn.execute(
                """
                SELECT game_id, season, game_date, scheduled_datetime, status, home_team_id, away_team_id
                FROM games
                WHERE season = ?
                ORDER BY game_date, scheduled_datetime, game_id
                """,
                (season,),
            ).fetchall()
            appearance_rows = conn.execute(
                """
                SELECT game_pitcher_appearances.*, games.game_date
                FROM game_pitcher_appearances
                INNER JOIN games ON games.game_id = game_pitcher_appearances.game_id
                WHERE games.season = ? AND game_pitcher_appearances.is_reliever = 1
                ORDER BY games.game_date, games.scheduled_datetime, game_pitcher_appearances.game_id,
                         game_pitcher_appearances.side, game_pitcher_appearances.appearance_order, game_pitcher_appearances.pitcher_id
                """,
                (season,),
            ).fetchall()
            appearances_by_game_side: dict[tuple[int, str], list[sqlite3.Row]] = {}
            for row in appearance_rows:
                appearances_by_game_side.setdefault((int(row["game_id"]), str(row["side"])), []).append(row)

            bullpen_states: dict[int, dict[str, Any]] = {}
            for idx, game in enumerate(games, start=1):
                for side in ("home", "away"):
                    team_id = _to_int(game[f"{side}_team_id"])
                    if team_id is None:
                        continue
                    team_state = bullpen_states.get(team_id)
                    game_state_row = _build_team_bullpen_game_state_row(game, side, team_state)
                    upsert_team_bullpen_game_state(conn, game_state_row)
                    game_state_rows_upserted += 1
                    for top_row in _build_team_bullpen_top_reliever_rows(
                        game,
                        side,
                        team_state,
                        game_state_row,
                        top_n_values,
                    ):
                        upsert_team_bullpen_top_relievers(conn, top_row)
                        top_reliever_rows_upserted += 1

                if _is_completed_game(game["status"]):
                    for side in ("home", "away"):
                        team_id = _to_int(game[f"{side}_team_id"])
                        if team_id is None:
                            continue
                        team_state = bullpen_states.setdefault(team_id, _empty_bullpen_team_state())
                        for appearance_row in appearances_by_game_side.get((int(game["game_id"]), side), []):
                            _update_bullpen_team_state(team_state, appearance_row, str(game["game_date"]))

                last_game_id = int(game["game_id"])
                if config.checkpoint_every > 0 and idx % config.checkpoint_every == 0:
                    upsert_checkpoint(
                        conn,
                        job_name=job_name,
                        partition_key=partition_key,
                        cursor={
                            "season": season,
                            "games_seen": idx,
                            "game_state_rows_upserted": game_state_rows_upserted,
                            "top_reliever_rows_upserted": top_reliever_rows_upserted,
                            "top_n_values": top_n_values,
                        },
                        status="running",
                        last_game_id=last_game_id,
                    )

            run_stats = {
                "season": season,
                "games_seen": len(games),
                "game_state_rows_upserted": game_state_rows_upserted,
                "top_reliever_rows_upserted": top_reliever_rows_upserted,
                "teams_with_relief_history": len([team_id for team_id, state in bullpen_states.items() if state["pitchers"]]),
                "top_n_values": top_n_values,
            }
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor=run_stats,
                status="success",
                last_game_id=last_game_id,
            )
            finish_run(conn, run_id, "success", note=format_run_observability(run_stats), request_count=0)
            print(f"Bullpen support backfill complete for {partition_key}: {format_run_observability(run_stats)}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor={"season": season, "games_seen": 0, "top_n_values": top_n_values},
                status="failed",
                last_game_id=last_game_id,
                last_error=error,
            )
            finish_run(conn, run_id, "failed", note=error, request_count=0)
            raise


def _load_handedness_by_player(conn: sqlite3.Connection) -> dict[int, sqlite3.Row]:
    return {
        int(row["player_id"]): row
        for row in conn.execute("SELECT * FROM player_handedness_dim").fetchall()
    }


def _rebuild_lineup_and_platoon_support_rows(
    conn: sqlite3.Connection,
    *,
    games: list[sqlite3.Row],
    target_game_ids: set[int],
) -> tuple[int, int]:
    if not games or not target_game_ids:
        return 0, 0
    season_ids = sorted({int(game["season"]) for game in games})
    placeholders = ",".join("?" for _ in season_ids)
    team_stats_rows = conn.execute(
        f"""
        SELECT game_team_stats.*, games.game_date, games.season
        FROM game_team_stats
        INNER JOIN games ON games.game_id = game_team_stats.game_id
        WHERE games.season IN ({placeholders})
        """,
        season_ids,
    ).fetchall()
    team_stats_by_game_side = {
        (int(row["game_id"]), str(row["side"])): row for row in team_stats_rows
    }
    pitcher_context_rows = conn.execute(
        f"""
        SELECT game_pitcher_context.*, games.season
        FROM game_pitcher_context
        INNER JOIN games ON games.game_id = game_pitcher_context.game_id
        WHERE games.season IN ({placeholders})
        """,
        season_ids,
    ).fetchall()
    pitcher_context_by_game_side = {
        (int(row["game_id"]), str(row["side"])): row for row in pitcher_context_rows
    }
    snapshot_rows = conn.execute(
        f"""
        SELECT game_lineup_snapshots.*
        FROM game_lineup_snapshots
        INNER JOIN games ON games.game_id = game_lineup_snapshots.game_id
        WHERE games.season IN ({placeholders})
        ORDER BY game_id, side, batting_order
        """,
        season_ids,
    ).fetchall()
    snapshot_rows_by_game_side: dict[tuple[int, str], list[sqlite3.Row]] = {}
    for row in snapshot_rows:
        snapshot_rows_by_game_side.setdefault((int(row["game_id"]), str(row["side"])), []).append(row)
    handedness_by_player = _load_handedness_by_player(conn)

    platoon_states: dict[int, dict[str, dict[str, Any]]] = {}
    lineup_rows_upserted = 0
    platoon_rows_upserted = 0

    for game in games:
        game_id = int(game["game_id"])
        for side in ("home", "away"):
            if game_id in target_game_ids:
                lineup_state_row = _build_team_lineup_game_state_row(
                    game,
                    side,
                    snapshot_rows_by_game_side.get((game_id, side), []),
                    handedness_by_player,
                )
                upsert_team_lineup_game_state(conn, lineup_state_row)
                lineup_rows_upserted += 1

                team_id = _to_int(game[f"{side}_team_id"])
                split_bucket = platoon_states.get(team_id, {}) if team_id is not None else {}
                for vs_pitch_hand in ("L", "R"):
                    platoon_row = _build_team_platoon_split_row(
                        game,
                        side,
                        vs_pitch_hand,
                        split_bucket.get(vs_pitch_hand),
                    )
                    upsert_team_platoon_split(conn, platoon_row)
                    platoon_rows_upserted += 1

        if _is_completed_game(game["status"]):
            for offense_side in ("home", "away"):
                team_stats_row = team_stats_by_game_side.get((game_id, offense_side))
                if team_stats_row is None:
                    continue
                team_id = _to_int(team_stats_row["team_id"])
                opponent_hand = _opponent_pitcher_hand_for_team_side(
                    pitcher_context_by_game_side,
                    handedness_by_player,
                    game_id,
                    offense_side,
                )
                if team_id is None or opponent_hand not in {"L", "R"}:
                    continue
                split_bucket = platoon_states.setdefault(team_id, {"L": _empty_platoon_split_state(), "R": _empty_platoon_split_state()})
                _update_platoon_split_state(split_bucket[opponent_hand], team_stats_row)

    return lineup_rows_upserted, platoon_rows_upserted


def cmd_backfill_lineup_support(args: argparse.Namespace) -> None:
    season = validate_supported_season(args.season)
    job_name = lineup_support_job_name(season)
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = f"season={season}"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=f"{job_name}:{partition_key}", config=config)
        last_game_id: int | None = None
        handedness_rows_upserted = 0
        lineup_snapshot_rows_upserted = 0
        try:
            handedness_by_player: dict[int, sqlite3.Row | dict[str, Any]] = _load_handedness_by_player(conn)
            games = conn.execute(
                """
                SELECT game_id, season, game_date, scheduled_datetime, status, home_team_id, away_team_id
                FROM games
                WHERE season = ?
                ORDER BY game_date, scheduled_datetime, game_id
                """,
                (season,),
            ).fetchall()
            if args.max_games is not None:
                games = games[: max(0, args.max_games)]

            for index, game in enumerate(games, start=1):
                boxscore = fetch_game_boxscore_bounded(int(game["game_id"]), config.request_policy, budget)
                handedness_rows_upserted += _refresh_player_handedness_from_people(
                    conn,
                    boxscore,
                    handedness_by_player,
                    config.request_policy,
                    budget,
                )
                snapshot_type = _lineup_snapshot_type_for_game(game, daily_mode=False)
                for snapshot_row in build_game_lineup_snapshot_rows(
                    game,
                    boxscore,
                    snapshot_type=snapshot_type,
                    handedness_by_player=handedness_by_player,
                ):
                    upsert_game_lineup_snapshot(conn, snapshot_row)
                    lineup_snapshot_rows_upserted += 1
                last_game_id = int(game["game_id"])
                if config.checkpoint_every > 0 and index % config.checkpoint_every == 0:
                    upsert_checkpoint(
                        conn,
                        job_name=job_name,
                        partition_key=partition_key,
                        cursor={
                            "season": season,
                            "games_processed": index,
                            "handedness_rows_upserted": handedness_rows_upserted,
                            "lineup_snapshot_rows_upserted": lineup_snapshot_rows_upserted,
                        },
                        status="running",
                        last_game_id=last_game_id,
                    )

            lineup_rows_upserted, platoon_rows_upserted = _rebuild_lineup_and_platoon_support_rows(
                conn,
                games=games,
                target_game_ids={int(game["game_id"]) for game in games},
            )
            run_stats = {
                "season": season,
                "games_processed": len(games),
                "handedness_rows_upserted": handedness_rows_upserted,
                "lineup_snapshot_rows_upserted": lineup_snapshot_rows_upserted,
                "team_lineup_game_state_rows_upserted": lineup_rows_upserted,
                "team_platoon_splits_rows_upserted": platoon_rows_upserted,
            }
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor=run_stats,
                status="success",
                last_game_id=last_game_id,
            )
            finish_run(conn, run_id, "success", note=format_run_observability(run_stats), request_count=budget.used)
            print(f"Lineup support backfill complete for {partition_key}: {format_run_observability({**run_stats, 'request_count': budget.used})}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor={"season": season, "games_processed": 0},
                status="failed",
                last_game_id=last_game_id,
                last_error=error,
            )
            finish_run(conn, run_id, "failed", note=error, request_count=budget.used)
            raise


def cmd_update_lineup_support(args: argparse.Namespace) -> None:
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    target_date = args.date or datetime.now().date().isoformat()
    partition_key = f"date={target_date}"
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "incremental", partition_key=f"lineup-support:{partition_key}", config=config)
        last_game_id: int | None = None
        try:
            games = conn.execute(
                """
                SELECT game_id, season, game_date, scheduled_datetime, status, home_team_id, away_team_id
                FROM games
                WHERE game_date = ?
                ORDER BY scheduled_datetime, game_id
                """,
                (target_date,),
            ).fetchall()
            handedness_rows_upserted = 0
            lineup_snapshot_rows_upserted = 0
            handedness_by_player: dict[int, sqlite3.Row | dict[str, Any]] = _load_handedness_by_player(conn)
            for game in games:
                boxscore = fetch_game_boxscore_bounded(int(game["game_id"]), config.request_policy, budget)
                handedness_rows_upserted += _refresh_player_handedness_from_people(
                    conn,
                    boxscore,
                    handedness_by_player,
                    config.request_policy,
                    budget,
                )
                snapshot_type = _lineup_snapshot_type_for_game(game, daily_mode=True)
                for snapshot_row in build_game_lineup_snapshot_rows(
                    game,
                    boxscore,
                    snapshot_type=snapshot_type,
                    handedness_by_player=handedness_by_player,
                ):
                    upsert_game_lineup_snapshot(conn, snapshot_row)
                    lineup_snapshot_rows_upserted += 1
                last_game_id = int(game["game_id"])

            seasons = sorted({int(game["season"]) for game in games})
            season_games: list[sqlite3.Row] = []
            if seasons:
                placeholders = ",".join("?" for _ in seasons)
                season_games = conn.execute(
                    f"""
                    SELECT game_id, season, game_date, scheduled_datetime, status, home_team_id, away_team_id
                    FROM games
                    WHERE season IN ({placeholders})
                    ORDER BY season, game_date, scheduled_datetime, game_id
                    """,
                    seasons,
                ).fetchall()
            lineup_rows_upserted, platoon_rows_upserted = _rebuild_lineup_and_platoon_support_rows(
                conn,
                games=season_games,
                target_game_ids={int(game["game_id"]) for game in games},
            )
            run_stats = {
                "date": target_date,
                "games_processed": len(games),
                "handedness_rows_upserted": handedness_rows_upserted,
                "lineup_snapshot_rows_upserted": lineup_snapshot_rows_upserted,
                "team_lineup_game_state_rows_upserted": lineup_rows_upserted,
                "team_platoon_splits_rows_upserted": platoon_rows_upserted,
            }
            upsert_checkpoint(
                conn,
                job_name="lineup-support-incremental",
                partition_key=partition_key,
                cursor=run_stats,
                status="success",
                last_game_id=last_game_id,
            )
            finish_run(conn, run_id, "success", note=format_run_observability(run_stats), request_count=budget.used)
            print(f"Lineup support update complete for {partition_key}: {format_run_observability({**run_stats, 'request_count': budget.used})}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name="lineup-support-incremental",
                partition_key=partition_key,
                cursor={"date": target_date},
                status="failed",
                last_game_id=last_game_id,
                last_error=error,
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


def _build_pitcher_feature_block(
    row: sqlite3.Row | None,
    *,
    degrade_on_missing_stats: bool = True,
) -> tuple[dict[str, Any], list[str]]:
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
    if degrade_on_missing_stats and int(row["probable_pitcher_known"] or 0) and not stats_available:
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


def _safe_delta(home_value: Any, away_value: Any, *, digits: int = 3) -> float | None:
    home_float = _to_float(home_value)
    away_float = _to_float(away_value)
    if home_float is None or away_float is None:
        return None
    return round(home_float - away_float, digits)


def _safe_reverse_delta(home_value: Any, away_value: Any, *, digits: int = 3) -> float | None:
    delta = _safe_delta(home_value, away_value, digits=digits)
    return None if delta is None else round(-delta, digits)


def _normalized_issue_token(value: str) -> str:
    return value.lower().replace(" ", "_").replace("-", "_")


def _resolve_pitcher_hand(
    pitcher_row: sqlite3.Row | None,
    handedness_by_player: dict[int, sqlite3.Row],
) -> str | None:
    if pitcher_row is None:
        return None
    pitcher_id = _to_int(pitcher_row["pitcher_id"]) or _to_int(pitcher_row["probable_pitcher_id"])
    if pitcher_id is None:
        return None
    handedness_row = handedness_by_player.get(pitcher_id)
    if handedness_row is None:
        return None
    return _normalize_hand(handedness_row["pitch_hand"])


def _lineup_vs_opposing_hand_quality(lineup_row: sqlite3.Row | None, opposing_hand: str | None) -> float | None:
    if lineup_row is None or opposing_hand not in {"L", "R"}:
        return None
    quality_key = "lineup_vs_lhp_quality" if opposing_hand == "L" else "lineup_vs_rhp_quality"
    return _to_float(lineup_row[quality_key])


def _roof_closed_or_fixed_flag(roof_type: str | None, weather_exposure_default: Any) -> int:
    roof_text = str(roof_type or "").strip().lower()
    if roof_text in {"dome", "fixed", "fixed_roof", "closed"}:
        return 1
    weather_exposed = _to_int(weather_exposure_default)
    return 1 if weather_exposed == 0 else 0


def _build_v1_feature_payload(
    game: sqlite3.Row,
    *,
    home_state: dict[str, Any] | None,
    away_state: dict[str, Any] | None,
    home_pitcher_row: sqlite3.Row | None,
    away_pitcher_row: sqlite3.Row | None,
    degrade_on_missing_starter_stats: bool,
) -> tuple[dict[str, Any], list[str]]:
    issues: list[str] = []
    home_team_id = int(game["home_team_id"]) if game["home_team_id"] is not None else None
    away_team_id = int(game["away_team_id"]) if game["away_team_id"] is not None else None
    home_pitcher, home_pitcher_issues = _build_pitcher_feature_block(
        home_pitcher_row,
        degrade_on_missing_stats=degrade_on_missing_starter_stats,
    )
    away_pitcher, away_pitcher_issues = _build_pitcher_feature_block(
        away_pitcher_row,
        degrade_on_missing_stats=degrade_on_missing_starter_stats,
    )
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
    return payload, issues


def _build_v2_phase1_side_block(
    *,
    prefix: str,
    bullpen_row: sqlite3.Row | None,
    top3_row: sqlite3.Row | None,
    lineup_row: sqlite3.Row | None,
    platoon_row: sqlite3.Row | None,
    opposing_starter_hand: str | None,
) -> tuple[dict[str, Any], list[str]]:
    issues: list[str] = []
    block: dict[str, Any] = {}

    bullpen_available = 1 if bullpen_row is not None else 0
    top3_available = 1 if top3_row is not None else 0
    block[f"{prefix}_bullpen_available_flag"] = bullpen_available
    if bullpen_row is None:
        issues.append(f"{prefix}_bullpen_support_missing")
        bullpen_values = {
            "season_games_in_sample": 0,
            "bullpen_appearances_season": 0,
            "bullpen_era_season": None,
            "bullpen_whip_season": None,
            "bullpen_k_minus_bb_rate_season": None,
            "bullpen_hr_rate_season": None,
            "freshness_score": None,
            "bullpen_outs_last3d": None,
            "bullpen_pitches_last3d": None,
            "relievers_back_to_back_count": None,
            "high_usage_relievers_last3d_count": None,
        }
    else:
        bullpen_values = {
            "season_games_in_sample": int(bullpen_row["season_games_in_sample"] or 0),
            "bullpen_appearances_season": int(bullpen_row["bullpen_appearances_season"] or 0),
            "bullpen_era_season": _to_float(bullpen_row["bullpen_era_season"]),
            "bullpen_whip_season": _to_float(bullpen_row["bullpen_whip_season"]),
            "bullpen_k_minus_bb_rate_season": _to_float(bullpen_row["bullpen_k_minus_bb_rate_season"]),
            "bullpen_hr_rate_season": _to_float(bullpen_row["bullpen_hr_rate_season"]),
            "freshness_score": _to_float(bullpen_row["freshness_score"]),
            "bullpen_outs_last3d": _to_float(bullpen_row["bullpen_outs_last3d"]),
            "bullpen_pitches_last3d": _to_float(bullpen_row["bullpen_pitches_last3d"]),
            "relievers_back_to_back_count": _to_float(bullpen_row["relievers_back_to_back_count"]),
            "high_usage_relievers_last3d_count": _to_float(bullpen_row["high_usage_relievers_last3d_count"]),
        }
    block.update({f"{prefix}_{key}": value for key, value in bullpen_values.items()})
    bullpen_appearances = int(bullpen_values["bullpen_appearances_season"] or 0)
    freshness_score = _to_float(bullpen_values["freshness_score"])
    relievers_back_to_back = int(bullpen_values["relievers_back_to_back_count"] or 0) if bullpen_values["relievers_back_to_back_count"] is not None else 0
    block[f"{prefix}_bullpen_low_sample_flag"] = 1 if bullpen_available and bullpen_appearances < 15 else 0
    block[f"{prefix}_bullpen_fatigue_flag"] = 1 if freshness_score is not None and (freshness_score < 0.4 or relievers_back_to_back >= 2) else 0

    block[f"{prefix}_top3_availability_flag"] = top3_available
    if top3_row is None:
        block[f"{prefix}_top3_n_available"] = 0
        block[f"{prefix}_top3_freshness_score"] = None
        block[f"{prefix}_top3_k_minus_bb_rate_season"] = None
        block[f"{prefix}_top3_quality_dropoff_vs_team"] = None
        block[f"{prefix}_top3_availability_low_flag"] = 1
    else:
        n_available = int(top3_row["n_available"] or 0)
        block[f"{prefix}_top3_n_available"] = n_available
        block[f"{prefix}_top3_freshness_score"] = _to_float(top3_row["topn_freshness_score"])
        block[f"{prefix}_top3_k_minus_bb_rate_season"] = _to_float(top3_row["topn_k_minus_bb_rate_season"])
        block[f"{prefix}_top3_quality_dropoff_vs_team"] = _to_float(top3_row["quality_dropoff_vs_team"])
        block[f"{prefix}_top3_availability_low_flag"] = 1 if n_available < 3 else 0

    lineup_known_flag = int(lineup_row["lineup_known_flag"] or 0) if lineup_row is not None else 0
    lineup_status = str(lineup_row["lineup_status"]) if lineup_row is not None and lineup_row["lineup_status"] else "missing"
    block[f"{prefix}_lineup_known_flag"] = lineup_known_flag
    block[f"{prefix}_lineup_partial_flag"] = 1 if lineup_status == "partial" else 0
    block[f"{prefix}_lineup_quality_available_flag"] = (
        1
        if lineup_row is not None
        and any(
            _to_float(lineup_row[column]) is not None
            for column in ("lineup_quality_mean", "top3_lineup_quality_mean", "lineup_vs_rhp_quality", "lineup_vs_lhp_quality")
        )
        else 0
    )
    block[f"{prefix}_lineup_status_missing_flag"] = 1 if lineup_status == "missing" else 0
    block[f"{prefix}_announced_lineup_count"] = int(lineup_row["announced_lineup_count"] or 0) if lineup_row is not None else 0
    for column in (
        "lineup_lefty_pa_share_proxy",
        "lineup_righty_pa_share_proxy",
        "lineup_switch_pa_share_proxy",
        "lineup_balance_score",
        "lineup_quality_mean",
        "top3_lineup_quality_mean",
        "top3_l_count",
        "top3_r_count",
        "top3_s_count",
    ):
        block[f"{prefix}_{column}"] = _to_float(lineup_row[column]) if lineup_row is not None else None
    block[f"{prefix}_lineup_vs_opp_starter_hand_quality"] = _lineup_vs_opposing_hand_quality(lineup_row, opposing_starter_hand)

    starter_hand_known = 1 if opposing_starter_hand in {"L", "R"} else 0
    block[f"opposing_starter_hand_known_flag_{prefix}_offense"] = starter_hand_known
    block[f"{prefix}_opposing_starter_pitch_hand_l_flag"] = 1 if opposing_starter_hand == "L" else 0
    block[f"{prefix}_opposing_starter_pitch_hand_r_flag"] = 1 if opposing_starter_hand == "R" else 0

    platoon_available = 1 if platoon_row is not None else 0
    block[f"{prefix}_platoon_available_flag"] = platoon_available
    if platoon_row is None:
        if lineup_known_flag == 0:
            issues.append(f"{prefix}_lineup_platoon_support_missing")
        elif starter_hand_known == 0:
            issues.append(f"{prefix}_opposing_starter_hand_unknown")
        platoon_values = {
            "platoon_ops": None,
            "platoon_runs_per_game": None,
            "platoon_strikeout_rate": None,
            "platoon_walk_rate": None,
            "platoon_games_in_sample": 0,
            "platoon_plate_appearances": 0,
        }
    else:
        platoon_values = {
            "platoon_ops": _to_float(platoon_row["ops"]),
            "platoon_runs_per_game": _to_float(platoon_row["runs_per_game"]),
            "platoon_strikeout_rate": _to_float(platoon_row["strikeout_rate"]),
            "platoon_walk_rate": _to_float(platoon_row["walk_rate"]),
            "platoon_games_in_sample": int(platoon_row["games_in_sample"] or 0),
            "platoon_plate_appearances": int(platoon_row["plate_appearances"] or 0),
        }
    block.update({f"{prefix}_{key}": value for key, value in platoon_values.items()})
    block[f"{prefix}_platoon_low_sample_flag"] = 1 if int(platoon_values["platoon_plate_appearances"] or 0) < 80 else 0

    return block, issues


def _select_weather_snapshot(
    candidate_rows: list[sqlite3.Row],
    feature_as_of_ts: str,
) -> sqlite3.Row | None:
    feature_ts = _parse_iso_datetime(feature_as_of_ts)
    if feature_ts is None:
        return None
    best_row: sqlite3.Row | None = None
    best_key: tuple[datetime, int, int] | None = None
    for row in candidate_rows:
        row_ts = _parse_iso_datetime(str(row["as_of_ts"]))
        if row_ts is None or row_ts > feature_ts:
            continue
        row_priority = int(row["source_priority"] or WEATHER_SOURCE_PRIORITY_DEFAULT)
        snapshot_rank = 1 if str(row["snapshot_type"]) == WEATHER_SNAPSHOT_OBSERVED else 0
        key = (row_ts, row_priority, snapshot_rank)
        if best_key is None or key > best_key:
            best_row = row
            best_key = key
    return best_row


def _build_v2_phase1_weather_block(
    venue_row: sqlite3.Row | None,
    weather_row: sqlite3.Row | None,
) -> dict[str, Any]:
    roof_type = str(venue_row["roof_type"]) if venue_row is not None and venue_row["roof_type"] else None
    weather_exposure_default = venue_row["weather_exposure_default"] if venue_row is not None else None
    weather_available = 1 if weather_row is not None else 0
    weather_exposed_flag = (
        int(weather_row["weather_exposure_flag"])
        if weather_row is not None and weather_row["weather_exposure_flag"] is not None
        else int(weather_exposure_default)
        if weather_exposure_default is not None
        else 0
    )
    temperature_f = _to_float(weather_row["temperature_f"]) if weather_row is not None else None
    wind_speed_mph = _to_float(weather_row["wind_speed_mph"]) if weather_row is not None else None
    return {
        "roof_type": roof_type,
        "weather_available_flag": weather_available,
        "weather_forecast_flag": 1 if weather_row is not None and str(weather_row["snapshot_type"]) == WEATHER_SNAPSHOT_FORECAST else 0,
        "weather_observed_archive_flag": 1 if weather_row is not None and str(weather_row["snapshot_type"]) == WEATHER_SNAPSHOT_OBSERVED else 0,
        "roof_closed_or_fixed_flag": _roof_closed_or_fixed_flag(roof_type, weather_exposure_default),
        "weather_exposed_flag": weather_exposed_flag,
        "temperature_f": temperature_f,
        "wind_speed_mph": wind_speed_mph,
        "wind_gust_mph": _to_float(weather_row["wind_gust_mph"]) if weather_row is not None else None,
        "wind_direction_deg": _to_float(weather_row["wind_direction_deg"]) if weather_row is not None else None,
        "precipitation_mm": _to_float(weather_row["precipitation_mm"]) if weather_row is not None else None,
        "humidity_pct": _to_float(weather_row["humidity_pct"]) if weather_row is not None else None,
        "pressure_hpa": _to_float(weather_row["pressure_hpa"]) if weather_row is not None else None,
        "cloud_cover_pct": _to_float(weather_row["cloud_cover_pct"]) if weather_row is not None else None,
        "is_day": _to_int(weather_row["is_day"]) if weather_row is not None else None,
        "hour_offset_from_first_pitch": _to_float(weather_row["hour_offset_from_first_pitch"]) if weather_row is not None else None,
        "windy_flag": 1 if wind_speed_mph is not None and wind_speed_mph >= 12.0 else 0,
        "extreme_temp_flag": 1 if temperature_f is not None and (temperature_f < 45.0 or temperature_f > 85.0) else 0,
    }


def _build_v2_phase1_feature_payload(
    game: sqlite3.Row,
    *,
    v1_payload: dict[str, Any],
    v1_issues: list[str],
    bullpen_by_key: dict[tuple[int, str], sqlite3.Row],
    top3_by_key: dict[tuple[int, str], sqlite3.Row],
    lineup_by_key: dict[tuple[int, str], sqlite3.Row],
    platoon_by_key: dict[tuple[int, str, str], sqlite3.Row],
    pitcher_by_key: dict[tuple[int, str], sqlite3.Row],
    handedness_by_player: dict[int, sqlite3.Row],
    venue_by_id: dict[int, sqlite3.Row],
    weather_by_game_id: dict[int, list[sqlite3.Row]],
) -> tuple[dict[str, Any], list[str]]:
    payload = dict(v1_payload)
    issues = list(v1_issues)
    game_id = int(game["game_id"])
    feature_as_of_ts = _feature_as_of_ts(game)

    opposing_hands = {
        "home": _resolve_pitcher_hand(pitcher_by_key.get((game_id, "away")), handedness_by_player),
        "away": _resolve_pitcher_hand(pitcher_by_key.get((game_id, "home")), handedness_by_player),
    }
    side_blocks: dict[str, dict[str, Any]] = {}
    for side in ("home", "away"):
        side_block, side_issues = _build_v2_phase1_side_block(
            prefix=side,
            bullpen_row=bullpen_by_key.get((game_id, side)),
            top3_row=top3_by_key.get((game_id, side)),
            lineup_row=lineup_by_key.get((game_id, side)),
            platoon_row=platoon_by_key.get((game_id, side, opposing_hands[side])) if opposing_hands[side] in {"L", "R"} else None,
            opposing_starter_hand=opposing_hands[side],
        )
        payload.update(side_block)
        side_blocks[side] = side_block
        issues.extend(side_issues)

    payload.update(
        {
            "bullpen_era_delta": _safe_reverse_delta(
                side_blocks["home"]["home_bullpen_era_season"],
                side_blocks["away"]["away_bullpen_era_season"],
            ),
            "bullpen_whip_delta": _safe_reverse_delta(
                side_blocks["home"]["home_bullpen_whip_season"],
                side_blocks["away"]["away_bullpen_whip_season"],
            ),
            "bullpen_k_minus_bb_rate_delta": _safe_delta(
                side_blocks["home"]["home_bullpen_k_minus_bb_rate_season"],
                side_blocks["away"]["away_bullpen_k_minus_bb_rate_season"],
            ),
            "bullpen_hr_rate_delta": _safe_reverse_delta(
                side_blocks["home"]["home_bullpen_hr_rate_season"],
                side_blocks["away"]["away_bullpen_hr_rate_season"],
            ),
            "bullpen_freshness_delta": _safe_delta(
                side_blocks["home"]["home_freshness_score"],
                side_blocks["away"]["away_freshness_score"],
            ),
            "bullpen_outs_last3d_delta": _safe_reverse_delta(
                side_blocks["home"]["home_bullpen_outs_last3d"],
                side_blocks["away"]["away_bullpen_outs_last3d"],
            ),
            "bullpen_pitches_last3d_delta": _safe_reverse_delta(
                side_blocks["home"]["home_bullpen_pitches_last3d"],
                side_blocks["away"]["away_bullpen_pitches_last3d"],
            ),
            "bullpen_back_to_back_delta": _safe_reverse_delta(
                side_blocks["home"]["home_relievers_back_to_back_count"],
                side_blocks["away"]["away_relievers_back_to_back_count"],
            ),
            "bullpen_high_usage_delta": _safe_reverse_delta(
                side_blocks["home"]["home_high_usage_relievers_last3d_count"],
                side_blocks["away"]["away_high_usage_relievers_last3d_count"],
            ),
            "top3_freshness_delta": _safe_delta(
                side_blocks["home"]["home_top3_freshness_score"],
                side_blocks["away"]["away_top3_freshness_score"],
            ),
            "top3_quality_delta": _safe_delta(
                side_blocks["home"]["home_top3_k_minus_bb_rate_season"],
                side_blocks["away"]["away_top3_k_minus_bb_rate_season"],
            ),
            "lineup_balance_delta": _safe_delta(
                side_blocks["home"]["home_lineup_balance_score"],
                side_blocks["away"]["away_lineup_balance_score"],
            ),
            "lineup_lefty_share_delta": _safe_delta(
                side_blocks["home"]["home_lineup_lefty_pa_share_proxy"],
                side_blocks["away"]["away_lineup_lefty_pa_share_proxy"],
            ),
            "lineup_righty_share_delta": _safe_delta(
                side_blocks["home"]["home_lineup_righty_pa_share_proxy"],
                side_blocks["away"]["away_lineup_righty_pa_share_proxy"],
            ),
            "top3_lefty_count_delta": _safe_delta(
                side_blocks["home"]["home_top3_l_count"],
                side_blocks["away"]["away_top3_l_count"],
            ),
            "lineup_vs_opp_hand_ops_delta": _safe_delta(
                side_blocks["home"]["home_platoon_ops"],
                side_blocks["away"]["away_platoon_ops"],
            ),
            "lineup_vs_opp_hand_runs_per_game_delta": _safe_delta(
                side_blocks["home"]["home_platoon_runs_per_game"],
                side_blocks["away"]["away_platoon_runs_per_game"],
            ),
            "lineup_vs_opp_hand_walk_rate_delta": _safe_delta(
                side_blocks["home"]["home_platoon_walk_rate"],
                side_blocks["away"]["away_platoon_walk_rate"],
            ),
            "lineup_vs_opp_hand_strikeout_rate_delta": _safe_delta(
                side_blocks["home"]["home_platoon_strikeout_rate"],
                side_blocks["away"]["away_platoon_strikeout_rate"],
            ),
            "lineup_quality_delta": _safe_delta(
                side_blocks["home"]["home_lineup_quality_mean"],
                side_blocks["away"]["away_lineup_quality_mean"],
            ),
            "top3_lineup_quality_delta": _safe_delta(
                side_blocks["home"]["home_top3_lineup_quality_mean"],
                side_blocks["away"]["away_top3_lineup_quality_mean"],
            ),
        }
    )

    venue_id = _to_int(game["venue_id"]) if "venue_id" in game.keys() else None
    venue_row = venue_by_id.get(venue_id) if venue_id is not None else None
    weather_row = _select_weather_snapshot(weather_by_game_id.get(game_id, []), feature_as_of_ts)
    payload.update(_build_v2_phase1_weather_block(venue_row, weather_row))
    if venue_id is not None and venue_row is None:
        payload["weather_exposed_flag"] = 0
    return payload, sorted({_normalized_issue_token(issue) for issue in issues})


def _ensure_venue_dimensions(
    conn: sqlite3.Connection,
    *,
    venue_ids: list[int],
    policy: RequestPolicy,
    budget: RequestBudget,
) -> tuple[int, int]:
    if not venue_ids:
        return 0, 0
    placeholders = ",".join("?" for _ in venue_ids)
    existing = {
        int(row["venue_id"])
        for row in conn.execute(f"SELECT venue_id FROM venue_dim WHERE venue_id IN ({placeholders})", venue_ids).fetchall()
    }
    inserted = 0
    missing = 0
    for venue_id in venue_ids:
        if venue_id in existing:
            continue
        override = KNOWN_VENUE_METADATA.get(venue_id)
        venue_row = {**override, "source_updated_at": utc_now()} if override is not None else None
        if venue_row is None:
            venue_row = fetch_statsapi_venue_details_bounded(venue_id, policy=policy, budget=budget)
        if venue_row is None:
            missing += 1
            continue
        upsert_venue_dim(conn, venue_row)
        existing.add(venue_id)
        inserted += 1
    return inserted, missing


def _weather_snapshot_row_from_open_meteo(
    game_row: sqlite3.Row,
    venue_row: sqlite3.Row,
    *,
    open_meteo_payload: dict[str, Any],
    as_of_ts: str,
    snapshot_type: str,
    source: str,
) -> dict[str, Any] | None:
    target_game_ts = str(game_row["scheduled_datetime"] or "")
    if not target_game_ts:
        return None
    target_local, _start_date, _end_date = _target_local_window(target_game_ts, str(venue_row["timezone"]))
    selected = _select_open_meteo_hour(open_meteo_payload, target_local)
    if selected is None:
        return None
    _day_night, day_night_source = _derive_day_night(game_row["day_night"], selected.get("is_day"), target_local)
    return {
        "game_id": int(game_row["game_id"]),
        "venue_id": int(venue_row["venue_id"]),
        "as_of_ts": as_of_ts,
        "target_game_ts": target_game_ts,
        "snapshot_type": snapshot_type,
        "source": source,
        "source_priority": WEATHER_SOURCE_PRIORITY_DEFAULT,
        "hour_offset_from_first_pitch": selected.get("hour_offset_from_first_pitch"),
        "temperature_f": _to_float(selected.get("temperature_2m")),
        "humidity_pct": _to_float(selected.get("relative_humidity_2m")),
        "pressure_hpa": _to_float(selected.get("surface_pressure")),
        "precipitation_mm": _to_float(selected.get("precipitation")),
        "precipitation_probability": _precipitation_probability_for_source(selected, source),
        "wind_speed_mph": _to_float(selected.get("wind_speed_10m")),
        "wind_gust_mph": _to_float(selected.get("wind_gusts_10m")),
        "wind_direction_deg": _to_float(selected.get("wind_direction_10m")),
        "weather_code": _to_int(selected.get("weather_code")),
        "cloud_cover_pct": _to_float(selected.get("cloud_cover")),
        "is_day": _to_int(selected.get("is_day")),
        "day_night_source": day_night_source,
        "weather_exposure_flag": int(venue_row["weather_exposure_default"]),
        "statsapi_weather_condition_text": None,
        "statsapi_wind_text": None,
        "source_updated_at": utc_now(),
    }


def cmd_sync_venues(args: argparse.Namespace) -> None:
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = f"season={args.season}" if args.season is not None else f"date={args.date}" if args.date else "all"
    job_name = venue_dim_job_name(partition_key)
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=partition_key, config=config)
        try:
            where = []
            params: list[Any] = []
            if args.season is not None:
                where.append("season = ?")
                params.append(args.season)
            if args.date:
                where.append("game_date = ?")
                params.append(args.date)
            where_sql = f"WHERE {' AND '.join(where)}" if where else "WHERE 1=1"
            venue_ids = [
                int(row["venue_id"])
                for row in conn.execute(
                    f"SELECT DISTINCT venue_id FROM games {where_sql} AND venue_id IS NOT NULL",
                    params,
                ).fetchall()
            ]
            inserted, missing = _ensure_venue_dimensions(
                conn,
                venue_ids=venue_ids,
                policy=config.request_policy,
                budget=budget,
            )
            stats = {
                "job": job_name,
                "venues_considered": len(venue_ids),
                "venues_inserted": inserted,
                "venues_missing_metadata": missing,
            }
            upsert_checkpoint(conn, job_name=job_name, partition_key=partition_key, cursor=stats, status="success")
            finish_run(conn, run_id, "success", note=format_run_observability(stats), request_count=budget.used)
            print(f"Venue sync complete for {partition_key}: {format_run_observability({**stats, 'request_count': budget.used})}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(conn, job_name=job_name, partition_key=partition_key, cursor={"job": job_name}, status="failed", last_error=error)
            finish_run(conn, run_id, "failed", note=error, request_count=budget.used)
            raise


def cmd_backfill_game_weather(args: argparse.Namespace) -> None:
    season = validate_supported_season(args.season)
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    partition_key = f"weather-season={season}"
    job_name = weather_backfill_job_name(season)
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=partition_key, config=config)
        rows_upserted = 0
        skipped_missing_schedule = 0
        skipped_missing_venue = 0
        last_game_id: int | None = None
        requested_game_ids = [int(game_id) for game_id in (args.game_id or [])]
        try:
            venue_ids = [
                int(row["venue_id"])
                for row in conn.execute(
                    "SELECT DISTINCT venue_id FROM games WHERE season = ? AND venue_id IS NOT NULL",
                    (season,),
                ).fetchall()
            ]
            _ensure_venue_dimensions(conn, venue_ids=venue_ids, policy=config.request_policy, budget=budget)
            sql = """
                SELECT games.game_id, games.scheduled_datetime, games.day_night, games.venue_id,
                       venue_dim.venue_id, venue_dim.timezone, venue_dim.latitude, venue_dim.longitude,
                       venue_dim.weather_exposure_default
                FROM games
                INNER JOIN venue_dim ON venue_dim.venue_id = games.venue_id
                WHERE games.season = ? AND games.status IN (?, ?, ?) AND games.venue_id IS NOT NULL
            """
            params: list[Any] = [season, *sorted(FINAL_STATUSES)]
            if requested_game_ids:
                placeholders = ",".join("?" for _ in requested_game_ids)
                sql += f" AND games.game_id IN ({placeholders})"
                params.extend(requested_game_ids)
            sql += " ORDER BY games.game_date, games.scheduled_datetime, games.game_id"
            if args.max_games is not None:
                sql += " LIMIT ?"
                params.append(args.max_games)
            rows = conn.execute(sql, params).fetchall()
            for idx, game_row in enumerate(rows, start=1):
                last_game_id = int(game_row["game_id"])
                if not game_row["scheduled_datetime"]:
                    skipped_missing_schedule += 1
                    continue
                target_local, start_date, end_date = _target_local_window(
                    str(game_row["scheduled_datetime"]),
                    str(game_row["timezone"]),
                )
                payload = fetch_open_meteo_hourly_bounded(
                    OPEN_METEO_ARCHIVE_URL,
                    latitude=float(game_row["latitude"]),
                    longitude=float(game_row["longitude"]),
                    timezone_name=str(game_row["timezone"]),
                    start_date=start_date,
                    end_date=end_date,
                    hourly_fields=OPEN_METEO_HOURLY_FIELDS_ARCHIVE,
                    policy=config.request_policy,
                    budget=budget,
                )
                snapshot = _weather_snapshot_row_from_open_meteo(
                    game_row,
                    game_row,
                    open_meteo_payload=payload,
                    as_of_ts=str(game_row["scheduled_datetime"]),
                    snapshot_type=WEATHER_SNAPSHOT_OBSERVED,
                    source=WEATHER_SOURCE_ARCHIVE,
                )
                if snapshot is None:
                    skipped_missing_venue += 1
                    continue
                upsert_game_weather_snapshot(conn, snapshot)
                rows_upserted += 1
                if config.checkpoint_every > 0 and idx % config.checkpoint_every == 0:
                    upsert_checkpoint(
                        conn,
                        job_name=job_name,
                        partition_key=partition_key,
                        cursor={
                            "season": season,
                            "games_seen": idx,
                            "rows_upserted": rows_upserted,
                            "snapshot_type": WEATHER_SNAPSHOT_OBSERVED,
                            "source": WEATHER_SOURCE_ARCHIVE,
                        },
                        status="running",
                        last_game_id=last_game_id,
                    )
            stats = {
                "season": season,
                "rows_upserted": rows_upserted,
                "skipped_missing_schedule": skipped_missing_schedule,
                "skipped_missing_venue": skipped_missing_venue,
                "requested_game_ids": requested_game_ids or None,
                "weather_source_path": f"{WEATHER_SOURCE_ARCHIVE}.hourly_nearest_first_pitch",
            }
            upsert_checkpoint(conn, job_name=job_name, partition_key=partition_key, cursor=stats, status="success", last_game_id=last_game_id)
            finish_run(conn, run_id, "success", note=format_run_observability(stats), request_count=budget.used)
            print(f"Weather backfill complete for {partition_key}: {format_run_observability({**stats, 'request_count': budget.used})}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(conn, job_name=job_name, partition_key=partition_key, cursor={"season": season}, status="failed", last_game_id=last_game_id, last_error=error)
            finish_run(conn, run_id, "failed", note=error, request_count=budget.used)
            raise


def cmd_update_game_weather_forecasts(args: argparse.Namespace) -> None:
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    target_date = args.date or datetime.now().date().isoformat()
    as_of_ts = args.as_of_ts or utc_now()
    as_of_dt = _parse_iso_datetime(as_of_ts)
    if as_of_dt is None:
        raise ValueError(f"invalid --as-of-ts={as_of_ts}")
    partition_key = f"weather-date={target_date}"
    job_name = weather_forecast_job_name(target_date)
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "incremental", partition_key=partition_key, config=config)
        rows_upserted = 0
        skipped_started_games = 0
        skipped_missing_schedule = 0
        last_game_id: int | None = None
        try:
            venue_ids = [
                int(row["venue_id"])
                for row in conn.execute(
                    "SELECT DISTINCT venue_id FROM games WHERE game_date = ? AND venue_id IS NOT NULL",
                    (target_date,),
                ).fetchall()
            ]
            _ensure_venue_dimensions(conn, venue_ids=venue_ids, policy=config.request_policy, budget=budget)
            rows = conn.execute(
                """
                SELECT games.game_id, games.scheduled_datetime, games.day_night, games.venue_id,
                       venue_dim.venue_id, venue_dim.timezone, venue_dim.latitude, venue_dim.longitude,
                       venue_dim.weather_exposure_default
                FROM games
                INNER JOIN venue_dim ON venue_dim.venue_id = games.venue_id
                WHERE games.game_date = ? AND games.venue_id IS NOT NULL
                ORDER BY games.scheduled_datetime, games.game_id
                """,
                (target_date,),
            ).fetchall()
            for idx, game_row in enumerate(rows, start=1):
                last_game_id = int(game_row["game_id"])
                target_dt = _parse_iso_datetime(str(game_row["scheduled_datetime"])) if game_row["scheduled_datetime"] else None
                if target_dt is None:
                    skipped_missing_schedule += 1
                    continue
                if target_dt <= as_of_dt:
                    skipped_started_games += 1
                    continue
                target_local, start_date, end_date = _target_local_window(
                    str(game_row["scheduled_datetime"]),
                    str(game_row["timezone"]),
                )
                payload = fetch_open_meteo_hourly_bounded(
                    OPEN_METEO_FORECAST_URL,
                    latitude=float(game_row["latitude"]),
                    longitude=float(game_row["longitude"]),
                    timezone_name=str(game_row["timezone"]),
                    start_date=start_date,
                    end_date=end_date,
                    hourly_fields=OPEN_METEO_HOURLY_FIELDS_FORECAST,
                    policy=config.request_policy,
                    budget=budget,
                )
                snapshot = _weather_snapshot_row_from_open_meteo(
                    game_row,
                    game_row,
                    open_meteo_payload=payload,
                    as_of_ts=as_of_ts,
                    snapshot_type=WEATHER_SNAPSHOT_FORECAST,
                    source=WEATHER_SOURCE_FORECAST,
                )
                if snapshot is None:
                    skipped_missing_schedule += 1
                    continue
                upsert_game_weather_snapshot(conn, snapshot)
                rows_upserted += 1
                if config.checkpoint_every > 0 and idx % config.checkpoint_every == 0:
                    upsert_checkpoint(
                        conn,
                        job_name=job_name,
                        partition_key=partition_key,
                        cursor={
                            "date": target_date,
                            "as_of_ts": as_of_ts,
                            "games_seen": idx,
                            "rows_upserted": rows_upserted,
                            "snapshot_type": WEATHER_SNAPSHOT_FORECAST,
                            "source": WEATHER_SOURCE_FORECAST,
                        },
                        status="running",
                        last_game_id=last_game_id,
                    )
            stats = {
                "date": target_date,
                "as_of_ts": as_of_ts,
                "rows_upserted": rows_upserted,
                "skipped_started_games": skipped_started_games,
                "skipped_missing_schedule": skipped_missing_schedule,
                "weather_source_path": f"{WEATHER_SOURCE_FORECAST}.hourly_nearest_first_pitch",
            }
            upsert_checkpoint(conn, job_name=job_name, partition_key=partition_key, cursor=stats, status="success", last_game_id=last_game_id)
            finish_run(conn, run_id, "success", note=format_run_observability(stats), request_count=budget.used)
            print(f"Forecast weather update complete for {partition_key}: {format_run_observability({**stats, 'request_count': budget.used})}")
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(conn, job_name=job_name, partition_key=partition_key, cursor={"date": target_date, "as_of_ts": as_of_ts}, status="failed", last_game_id=last_game_id, last_error=error)
            finish_run(conn, run_id, "failed", note=error, request_count=budget.used)
            raise


def cmd_materialize_feature_rows(args: argparse.Namespace) -> None:
    season = validate_supported_season(args.season)
    job_name = feature_rows_job_name(season, args.feature_version)
    config = build_config(args)
    partition_key = f"feature-rows-season={season}:version={args.feature_version}"
    if args.feature_version not in {FEATURE_VERSION_V1, FEATURE_VERSION_V2_PHASE1}:
        raise ValueError(f"unsupported feature version: {args.feature_version}")
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=partition_key, config=config)
        last_game_id: int | None = None
        try:
            if args.feature_version == FEATURE_VERSION_V2_PHASE1 and not args.allow_unsafe_pitcher_context:
                pitcher_context_report = build_pitcher_context_quality_report(conn, season=season)
                if (
                    pitcher_context_report["null_safe_fallback_share"] > DEFAULT_MAX_NULL_SAFE_FALLBACK_SHARE
                    or pitcher_context_report["rows_with_nonzero_leakage_risk"] > 0
                ):
                    raise RuntimeError(
                        "unsafe pitcher context for v2_phase1 materialization; "
                        f"null_safe_fallback_share={pitcher_context_report['null_safe_fallback_share']:.3f}, "
                        f"rows_with_nonzero_leakage_risk={pitcher_context_report['rows_with_nonzero_leakage_risk']}. "
                        "Run audit-pitcher-context before canonical promotion or pass --allow-unsafe-pitcher-context."
                    )
            games = conn.execute(
                """
                SELECT game_id, season, game_date, scheduled_datetime, status, venue_id, home_team_id, away_team_id
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
            bullpen_by_key: dict[tuple[int, str], sqlite3.Row] = {}
            top3_by_key: dict[tuple[int, str], sqlite3.Row] = {}
            lineup_by_key: dict[tuple[int, str], sqlite3.Row] = {}
            platoon_by_key: dict[tuple[int, str, str], sqlite3.Row] = {}
            handedness_by_player: dict[int, sqlite3.Row] = {}
            venue_by_id: dict[int, sqlite3.Row] = {}
            weather_by_game_id: dict[int, list[sqlite3.Row]] = {}

            if args.feature_version == FEATURE_VERSION_V2_PHASE1:
                bullpen_rows = conn.execute(
                    """
                    SELECT team_bullpen_game_state.*
                    FROM team_bullpen_game_state
                    INNER JOIN games ON games.game_id = team_bullpen_game_state.game_id
                    WHERE games.season = ?
                    """,
                    (season,),
                ).fetchall()
                bullpen_by_key = {(int(row["game_id"]), str(row["side"])): row for row in bullpen_rows}
                top3_rows = conn.execute(
                    """
                    SELECT team_bullpen_top_relievers.*
                    FROM team_bullpen_top_relievers
                    INNER JOIN games ON games.game_id = team_bullpen_top_relievers.game_id
                    WHERE games.season = ? AND team_bullpen_top_relievers.top_n = 3
                    """,
                    (season,),
                ).fetchall()
                top3_by_key = {(int(row["game_id"]), str(row["side"])): row for row in top3_rows}
                lineup_rows = conn.execute(
                    """
                    SELECT team_lineup_game_state.*
                    FROM team_lineup_game_state
                    INNER JOIN games ON games.game_id = team_lineup_game_state.game_id
                    WHERE games.season = ?
                    """,
                    (season,),
                ).fetchall()
                lineup_by_key = {(int(row["game_id"]), str(row["side"])): row for row in lineup_rows}
                platoon_rows = conn.execute(
                    """
                    SELECT team_platoon_splits.*
                    FROM team_platoon_splits
                    INNER JOIN games ON games.game_id = team_platoon_splits.game_id
                    WHERE games.season = ?
                    """,
                    (season,),
                ).fetchall()
                platoon_by_key = {
                    (int(row["game_id"]), str(row["side"]), str(row["vs_pitch_hand"])): row for row in platoon_rows
                }
                handedness_by_player = _load_handedness_by_player(conn)
                venue_rows = conn.execute("SELECT * FROM venue_dim").fetchall()
                venue_by_id = {int(row["venue_id"]): row for row in venue_rows}
                weather_rows = conn.execute(
                    """
                    SELECT game_weather_snapshots.*
                    FROM game_weather_snapshots
                    INNER JOIN games ON games.game_id = game_weather_snapshots.game_id
                    WHERE games.season = ?
                    ORDER BY game_id, as_of_ts, source_priority
                    """,
                    (season,),
                ).fetchall()
                for row in weather_rows:
                    weather_by_game_id.setdefault(int(row["game_id"]), []).append(row)

            team_states: dict[int, dict[str, Any]] = {}
            rows_upserted = 0
            degrade_on_missing_starter_stats = season != 2020

            for idx, game in enumerate(games, start=1):
                home_team_id = int(game["home_team_id"]) if game["home_team_id"] is not None else None
                away_team_id = int(game["away_team_id"]) if game["away_team_id"] is not None else None
                home_state = team_states.get(home_team_id or -1)
                away_state = team_states.get(away_team_id or -1)
                payload, issues = _build_v1_feature_payload(
                    game,
                    home_state=home_state,
                    away_state=away_state,
                    home_pitcher_row=pitcher_by_key.get((int(game["game_id"]), "home")),
                    away_pitcher_row=pitcher_by_key.get((int(game["game_id"]), "away")),
                    degrade_on_missing_starter_stats=degrade_on_missing_starter_stats,
                )
                if args.feature_version == FEATURE_VERSION_V2_PHASE1:
                    payload, issues = _build_v2_phase1_feature_payload(
                        game,
                        v1_payload=payload,
                        v1_issues=issues,
                        bullpen_by_key=bullpen_by_key,
                        top3_by_key=top3_by_key,
                        lineup_by_key=lineup_by_key,
                        platoon_by_key=platoon_by_key,
                        pitcher_by_key=pitcher_by_key,
                        handedness_by_player=handedness_by_player,
                        venue_by_id=venue_by_id,
                        weather_by_game_id=weather_by_game_id,
                    )

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


def _supported_seasons_present(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute(
        """
        SELECT DISTINCT season
        FROM games
        WHERE season BETWEEN ? AND ?
        ORDER BY season
        """,
        (MIN_SUPPORTED_SEASON, MAX_SUPPORTED_SEASON),
    ).fetchall()
    return [int(row["season"]) for row in rows]


def _missing_support_reason(*, status: str | None, completed: bool, family: str, venue_id: int | None, venue_name: str | None) -> str:
    normalized_status = str(status or "").strip() or "unknown"
    if not completed:
        return f"non_completed_game_status_{_normalized_issue_token(normalized_status)}"
    if family == "weather":
        if venue_id is None:
            return "completed_game_missing_venue_id"
        if not venue_name:
            return "completed_game_missing_venue_dim"
        return "completed_game_missing_weather_snapshot"
    return "completed_game_missing_lineup_snapshot"


def build_pitcher_context_quality_report(conn: sqlite3.Connection, *, season: int) -> dict[str, Any]:
    validated_season = validate_supported_season(season)
    rows = conn.execute(
        """
        SELECT game_pitcher_context.*
        FROM game_pitcher_context
        INNER JOIN games ON games.game_id = game_pitcher_context.game_id
        WHERE games.season = ?
        ORDER BY game_pitcher_context.game_id, game_pitcher_context.side
        """,
        (validated_season,),
    ).fetchall()
    return _build_pitcher_context_quality_report(rows, season=validated_season)


def build_support_coverage_report(
    conn: sqlite3.Connection,
    *,
    seasons: list[int] | None = None,
    feature_version: str = FEATURE_VERSION_V2_PHASE1,
) -> dict[str, Any]:
    selected_seasons = seasons or _supported_seasons_present(conn)
    if not selected_seasons:
        return {"seasons": [], "by_season": [], "missing_games": {"weather": [], "lineup_snapshot": []}}

    validated_seasons = [validate_supported_season(int(season)) for season in selected_seasons]
    placeholders = ",".join("?" for _ in validated_seasons)
    season_params: tuple[Any, ...] = tuple(validated_seasons)
    completed_statuses = tuple(sorted(FINAL_STATUSES))
    completed_placeholders = ",".join("?" for _ in completed_statuses)
    season_query_params = completed_statuses + season_params

    season_rows = conn.execute(
        f"""
        WITH lineup_games AS (
          SELECT DISTINCT game_id FROM game_lineup_snapshots
        ),
        weather_games AS (
          SELECT DISTINCT game_id FROM game_weather_snapshots
        ),
        v2_games AS (
          SELECT DISTINCT game_id
          FROM feature_rows
          WHERE feature_version = ?
        )
        SELECT
          g.season,
          COUNT(*) AS games,
          SUM(CASE WHEN g.status IN ({completed_placeholders}) THEN 1 ELSE 0 END) AS completed_games,
          COUNT(DISTINCT CASE WHEN lg.game_id IS NOT NULL THEN g.game_id END) AS lineup_snapshot_games,
          COUNT(DISTINCT CASE WHEN wg.game_id IS NOT NULL THEN g.game_id END) AS weather_games,
          COUNT(DISTINCT CASE WHEN v2.game_id IS NOT NULL THEN g.game_id END) AS integrated_feature_games
        FROM games g
        LEFT JOIN lineup_games lg ON lg.game_id = g.game_id
        LEFT JOIN weather_games wg ON wg.game_id = g.game_id
        LEFT JOIN v2_games v2 ON v2.game_id = g.game_id
        WHERE g.season IN ({placeholders})
        GROUP BY g.season
        ORDER BY g.season
        """,
        (feature_version, *season_query_params),
    ).fetchall()

    missing_weather_rows = conn.execute(
        f"""
        WITH weather_games AS (
          SELECT DISTINCT game_id FROM game_weather_snapshots
        )
        SELECT
          g.game_id,
          g.season,
          g.game_date,
          g.status,
          g.scheduled_datetime,
          g.venue_id,
          v.venue_name,
          CASE WHEN g.status IN ({completed_placeholders}) THEN 1 ELSE 0 END AS completed_flag
        FROM games g
        LEFT JOIN weather_games wg ON wg.game_id = g.game_id
        LEFT JOIN venue_dim v ON v.venue_id = g.venue_id
        WHERE g.season IN ({placeholders})
          AND wg.game_id IS NULL
        ORDER BY g.season, g.game_date, g.game_id
        """,
        season_query_params,
    ).fetchall()
    missing_lineup_rows = conn.execute(
        f"""
        WITH lineup_games AS (
          SELECT DISTINCT game_id FROM game_lineup_snapshots
        )
        SELECT
          g.game_id,
          g.season,
          g.game_date,
          g.status,
          g.scheduled_datetime,
          g.venue_id,
          v.venue_name,
          CASE WHEN g.status IN ({completed_placeholders}) THEN 1 ELSE 0 END AS completed_flag,
          COUNT(tlgs.team_id) AS lineup_state_team_rows
        FROM games g
        LEFT JOIN lineup_games lg ON lg.game_id = g.game_id
        LEFT JOIN venue_dim v ON v.venue_id = g.venue_id
        LEFT JOIN team_lineup_game_state tlgs ON tlgs.game_id = g.game_id
        WHERE g.season IN ({placeholders})
          AND lg.game_id IS NULL
        GROUP BY g.game_id, g.season, g.game_date, g.status, g.scheduled_datetime, g.venue_id, v.venue_name
        ORDER BY g.season, g.game_date, g.game_id
        """,
        season_query_params,
    ).fetchall()

    def _serialize_gap(row: sqlite3.Row, *, family: str) -> dict[str, Any]:
        venue_id = _to_int(row["venue_id"])
        venue_name = str(row["venue_name"]) if row["venue_name"] is not None else None
        completed = bool(int(row["completed_flag"] or 0))
        payload: dict[str, Any] = {
            "game_id": int(row["game_id"]),
            "season": int(row["season"]),
            "game_date": str(row["game_date"]),
            "status": str(row["status"]) if row["status"] is not None else None,
            "scheduled_datetime": str(row["scheduled_datetime"]) if row["scheduled_datetime"] is not None else None,
            "venue_id": venue_id,
            "venue_name": venue_name,
            "completed_game": completed,
            "reason": _missing_support_reason(
                status=str(row["status"]) if row["status"] is not None else None,
                completed=completed,
                family=family,
                venue_id=venue_id,
                venue_name=venue_name,
            ),
        }
        if family == "lineup_snapshot":
            payload["lineup_state_team_rows"] = int(row["lineup_state_team_rows"] or 0)
        return payload

    by_season: list[dict[str, Any]] = []
    for row in season_rows:
        games = int(row["games"] or 0)
        completed_games = int(row["completed_games"] or 0)
        lineup_snapshot_games = int(row["lineup_snapshot_games"] or 0)
        weather_games = int(row["weather_games"] or 0)
        integrated_feature_games = int(row["integrated_feature_games"] or 0)
        by_season.append(
            {
                "season": int(row["season"]),
                "games": games,
                "completed_games": completed_games,
                "lineup_snapshot_games": lineup_snapshot_games,
                "lineup_snapshot_missing_games": games - lineup_snapshot_games,
                "lineup_snapshot_completed_coverage": (
                    (completed_games - sum(1 for gap in missing_lineup_rows if int(gap["season"]) == int(row["season"]) and int(gap["completed_flag"] or 0) == 1))
                    / completed_games
                    if completed_games
                    else 1.0
                ),
                "weather_games": weather_games,
                "weather_missing_games": games - weather_games,
                "weather_completed_coverage": (
                    (completed_games - sum(1 for gap in missing_weather_rows if int(gap["season"]) == int(row["season"]) and int(gap["completed_flag"] or 0) == 1))
                    / completed_games
                    if completed_games
                    else 1.0
                ),
                "integrated_feature_rows": integrated_feature_games,
                "integrated_feature_missing_games": games - integrated_feature_games,
            }
        )

    return {
        "seasons": validated_seasons,
        "feature_version": feature_version,
        "by_season": by_season,
        "missing_games": {
            "weather": [_serialize_gap(row, family="weather") for row in missing_weather_rows],
            "lineup_snapshot": [_serialize_gap(row, family="lineup_snapshot") for row in missing_lineup_rows],
        },
    }


def cmd_audit_support_coverage(args: argparse.Namespace) -> None:
    selected_seasons = [validate_supported_season(args.season)] if args.season is not None else None
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        report = build_support_coverage_report(
            conn,
            seasons=selected_seasons,
            feature_version=args.feature_version,
        )
    print(json.dumps(report, indent=2, sort_keys=True))


def cmd_audit_pitcher_context(args: argparse.Namespace) -> None:
    season = validate_supported_season(args.season)
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        report = build_pitcher_context_quality_report(conn, season=season)
    print(json.dumps(report, indent=2, sort_keys=True))


def _rebuild_selected_stages(raw_stages: list[str] | None) -> list[str]:
    if not raw_stages or raw_stages == [REBUILD_ALL_STAGES]:
        return list(REBUILD_STAGE_ORDER)
    seen: set[str] = set()
    selected: list[str] = []
    for stage in raw_stages:
        if stage == REBUILD_ALL_STAGES:
            raise ValueError(f"{REBUILD_ALL_STAGES} cannot be combined with explicit stages")
        if stage in seen:
            continue
        if stage not in REBUILD_STAGE_ORDER:
            raise ValueError(f"unsupported rebuild stage: {stage}")
        seen.add(stage)
        selected.append(stage)
    return [stage for stage in REBUILD_STAGE_ORDER if stage in seen]


def _rebuild_selected_seasons(args: argparse.Namespace) -> list[int]:
    if getattr(args, "season", None) is not None:
        return [validate_supported_season(int(args.season))]
    season_start = validate_supported_season(int(args.season_start))
    season_end = validate_supported_season(int(args.season_end))
    if season_start > season_end:
        raise ValueError("season-start must be less than or equal to season-end")
    return list(range(season_start, season_end + 1))


def _stage_args(args: argparse.Namespace, **overrides: Any) -> argparse.Namespace:
    payload = dict(vars(args))
    payload.update(overrides)
    return argparse.Namespace(**payload)


def _run_rebuild_stage(stage: str, args: argparse.Namespace, *, season: int) -> None:
    if stage == REBUILD_STAGE_BASE:
        cmd_backfill(_stage_args(args, season=season, season_start=season, season_end=season))
        return
    if stage == REBUILD_STAGE_TEAM_STATS:
        cmd_backfill_team_stats(_stage_args(args, season=season))
        return
    if stage == REBUILD_STAGE_PITCHER_CONTEXT:
        cmd_backfill_pitcher_context(_stage_args(args, season=season))
        return
    if stage == REBUILD_STAGE_PITCHER_APPEARANCES:
        cmd_backfill_pitcher_appearances(_stage_args(args, season=season))
        return
    if stage == REBUILD_STAGE_BULLPEN_SUPPORT:
        cmd_backfill_bullpen_support(_stage_args(args, season=season))
        return
    if stage == REBUILD_STAGE_LINEUP_SUPPORT:
        cmd_backfill_lineup_support(_stage_args(args, season=season))
        return
    if stage == REBUILD_STAGE_VENUES:
        cmd_sync_venues(_stage_args(args, season=season, date=None))
        return
    if stage == REBUILD_STAGE_WEATHER:
        cmd_backfill_game_weather(_stage_args(args, season=season))
        return
    if stage == REBUILD_STAGE_FEATURE_ROWS:
        cmd_materialize_feature_rows(_stage_args(args, season=season))
        return
    raise ValueError(f"unsupported rebuild stage: {stage}")


def cmd_rebuild_history(args: argparse.Namespace) -> None:
    """Orchestrate a safe-by-default historical rebuild without replacing the canonical DB."""
    selected_seasons = _rebuild_selected_seasons(args)
    selected_stages = _rebuild_selected_stages(args.stages)
    config = build_config(args)
    partition_key = (
        f"season={selected_seasons[0]}"
        if len(selected_seasons) == 1
        else f"range={selected_seasons[0]}-{selected_seasons[-1]}"
    )
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(
            conn,
            "backfill",
            partition_key=f"rebuild-history:{partition_key}",
            config=config,
        )
        try:
            for season in selected_seasons:
                for stage in selected_stages:
                    _run_rebuild_stage(stage, args, season=season)
            finish_run(
                conn,
                run_id,
                "success",
                note=format_run_observability(
                    {
                        "job": "rebuild-history",
                        "seasons": selected_seasons,
                        "stages": selected_stages,
                        "destructive_replace": "disabled",
                    }
                ),
            )
        except Exception as exc:
            finish_run(conn, run_id, "failed", note=str(exc))
            raise
    print(
        "Rebuild history complete for "
        f"{partition_key}: {format_run_observability({'seasons': selected_seasons, 'stages': selected_stages, 'destructive_replace': 'disabled'})}"
    )


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


def cmd_backfill_game_metadata(args: argparse.Namespace) -> None:
    config = build_config(args)
    budget = RequestBudget(limit=config.request_policy.request_budget_per_run)
    if args.season is not None:
        validate_supported_season(args.season)
    partition_key = f"season={args.season}" if args.season is not None else f"date={args.date}"
    job_name = game_metadata_backfill_job_name(partition_key)
    with connect_db(config.db_path) as conn:
        ensure_schema(conn)
        run_id = start_run(conn, "backfill", partition_key=partition_key, config=config)
        try:
            fetch_kwargs: dict[str, Any] = {"sportId": 1}
            default_season: int | None = None
            if args.season is not None:
                fetch_kwargs["season"] = args.season
                default_season = args.season
            if args.date:
                fetch_kwargs["date"] = args.date
                parsed_target_date = _parse_iso_date(args.date)
                if default_season is None and parsed_target_date is not None:
                    default_season = parsed_target_date.year
            schedule_rows = fetch_schedule_bounded(config.request_policy, budget, **fetch_kwargs)
            stats, last_game_id = ingest_schedule_partition(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                schedule_rows=schedule_rows,
                checkpoint_every=config.checkpoint_every,
                default_season=default_season,
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
            }
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor=run_stats,
                status="success",
                last_game_id=last_game_id,
            )
            finish_run(conn, run_id, "success", note=format_run_observability(run_stats), request_count=budget.used)
            print(
                f"Game metadata backfill complete for {partition_key}: "
                f"{format_run_observability({**run_stats, 'request_count': budget.used})}"
            )
        except Exception as exc:
            error = str(exc)
            upsert_checkpoint(
                conn,
                job_name=job_name,
                partition_key=partition_key,
                cursor={"partition": partition_key},
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
    parser = argparse.ArgumentParser(
        description="Historical MLB ingestion and rebuild CLI for statsapi/Open-Meteo -> SQLite"
    )
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to SQLite DB (default: data/mlb_history.db)")
    parser.add_argument(
        "--allow-canonical-writes",
        action="store_true",
        help="Required for mutating commands when --db points at the canonical local DB (data/mlb_history.db)",
    )
    parser.add_argument("--checkpoint-every", type=int, default=25)
    parser.add_argument("--timeout-seconds", type=int, default=25)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--initial-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--max-backoff-seconds", type=float, default=16.0)
    parser.add_argument("--jitter-seconds", type=float, default=0.4)
    parser.add_argument("--request-budget-per-run", type=int, default=2500)

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db", help="Initialize or update the historical schema")
    init_db.set_defaults(func=cmd_init_db)

    backfill = subparsers.add_parser("backfill", help="Backfill schedule/labels only for one season or season range")
    backfill.add_argument("--season", type=int, help="Single season override")
    backfill.add_argument("--season-start", type=int, default=MIN_SUPPORTED_SEASON)
    backfill.add_argument("--season-end", type=int, default=MAX_SUPPORTED_SEASON)
    backfill.set_defaults(func=cmd_backfill)

    rebuild = subparsers.add_parser(
        "rebuild-history",
        help="Safe-by-default rebuild orchestrator; initializes schema and runs selected seasonal stages without deleting or replacing the DB",
        description=(
            "Safe-by-default historical rebuild orchestrator. This command never deletes or replaces the target DB. "
            "Use a scratch --db path for validation, and pass --allow-canonical-writes only when intentionally mutating "
            "the canonical data/mlb_history.db. Prefer this command for reproducible multi-stage rebuilds."
        ),
    )
    rebuild.add_argument("--season", type=int, help="Single supported season override")
    rebuild.add_argument("--season-start", type=int, default=MIN_SUPPORTED_SEASON, help=f"Start season for ranged rebuilds ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})")
    rebuild.add_argument("--season-end", type=int, default=MAX_SUPPORTED_SEASON, help=f"End season for ranged rebuilds ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})")
    rebuild.add_argument(
        "--stages",
        nargs="+",
        choices=(REBUILD_ALL_STAGES, *REBUILD_STAGE_ORDER),
        default=[REBUILD_ALL_STAGES],
        help=(
            "Rebuild stages to run. Defaults to all supported stages. "
            "Explicit selections are normalized into canonical execution order. "
            "Use one or more of: "
            + ", ".join(REBUILD_STAGE_ORDER)
        ),
    )
    rebuild.add_argument("--max-games", type=int, help="Optional cap for validation runs; passed to stages that support it")
    rebuild.add_argument(
        "--top-n-values",
        default="3,5",
        help="Comma-separated bullpen top-N reliever summaries for the bullpen-support stage (default: 3,5)",
    )
    rebuild.add_argument(
        "--feature-version",
        default=FEATURE_VERSION_V1,
        help=f"Feature version for the feature-rows stage (default: {FEATURE_VERSION_V1})",
    )
    rebuild.add_argument(
        "--allow-unsafe-pitcher-context",
        action="store_true",
        help="Pass through to feature-rows when rebuilding integrated features that require pitcher-context promotion",
    )
    rebuild.add_argument(
        "--repair-mode",
        action="store_true",
        help="Pass through to pitcher-context stage to abort before writes when starter identity fallback rates are too broad",
    )
    rebuild.add_argument(
        "--max-null-safe-fallback-share",
        type=float,
        default=DEFAULT_MAX_NULL_SAFE_FALLBACK_SHARE,
        help="Maximum allowed share of null-safe fallback rows when --repair-mode reaches pitcher-context",
    )
    rebuild.add_argument(
        "--max-missing-probable-share",
        type=float,
        default=DEFAULT_MAX_MISSING_PROBABLE_SHARE,
        help="Maximum allowed share of missing probable rows when --repair-mode reaches pitcher-context",
    )
    rebuild.set_defaults(func=cmd_rebuild_history)

    incremental = subparsers.add_parser("incremental", help="Run daily incremental sync")
    incremental.add_argument("--date", help="YYYY-MM-DD; defaults to today")
    incremental.set_defaults(func=cmd_incremental)

    game_metadata = subparsers.add_parser(
        "backfill-game-metadata",
        help="Repair or populate schedule-derived game metadata such as games.venue_id and games.day_night",
    )
    game_metadata_scope = game_metadata.add_mutually_exclusive_group(required=True)
    game_metadata_scope.add_argument("--season", type=int, help=f"Supported season to repair ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})")
    game_metadata_scope.add_argument("--date", help="Single YYYY-MM-DD date to repair")
    game_metadata.set_defaults(func=cmd_backfill_game_metadata)

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

    pitcher_appearances = subparsers.add_parser(
        "backfill-pitcher-appearances",
        help="Backfill game_pitcher_appearances for one supported season",
    )
    pitcher_appearances.add_argument(
        "--season",
        type=int,
        default=MIN_SUPPORTED_SEASON,
        help=f"Season to process ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})",
    )
    pitcher_appearances.add_argument("--max-games", type=int, help="Optional cap for validation runs")
    pitcher_appearances.set_defaults(func=cmd_backfill_pitcher_appearances)

    bullpen_support = subparsers.add_parser(
        "backfill-bullpen-support",
        help="Build team bullpen support tables from existing local game_pitcher_appearances history",
    )
    bullpen_support.add_argument(
        "--season",
        type=int,
        default=MIN_SUPPORTED_SEASON,
        help=f"Season to process ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})",
    )
    bullpen_support.add_argument(
        "--top-n-values",
        default="3,5",
        help="Comma-separated top-N reliever summaries to materialize (default: 3,5)",
    )
    bullpen_support.set_defaults(func=cmd_backfill_bullpen_support)

    lineup_support = subparsers.add_parser(
        "backfill-lineup-support",
        help="Backfill player handedness, lineup snapshots, lineup state, and platoon splits for one supported season",
    )
    lineup_support.add_argument(
        "--season",
        type=int,
        default=MIN_SUPPORTED_SEASON,
        help=f"Season to process ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})",
    )
    lineup_support.add_argument("--max-games", type=int, help="Optional cap for validation runs")
    lineup_support.set_defaults(func=cmd_backfill_lineup_support)

    lineup_support_incremental = subparsers.add_parser(
        "update-lineup-support",
        help="Capture same-day lineups and rebuild lineup/platoon support rows for the target date",
    )
    lineup_support_incremental.add_argument("--date", help="YYYY-MM-DD; defaults to today")
    lineup_support_incremental.set_defaults(func=cmd_update_lineup_support)

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
    pitcher_context.add_argument(
        "--repair-mode",
        action="store_true",
        help="Abort before writes if fallback or missing-identity rates are broad enough to threaten canonical quality",
    )
    pitcher_context.add_argument(
        "--max-null-safe-fallback-share",
        type=float,
        default=DEFAULT_MAX_NULL_SAFE_FALLBACK_SHARE,
        help="Maximum allowed share of known probable rows using null-safe fallback in repair mode",
    )
    pitcher_context.add_argument(
        "--max-missing-probable-share",
        type=float,
        default=DEFAULT_MAX_MISSING_PROBABLE_SHARE,
        help="Maximum allowed share of rows missing probable starter identity in repair mode",
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
    feature_rows.add_argument(
        "--allow-unsafe-pitcher-context",
        action="store_true",
        help="Allow v2_phase1 materialization even when pitcher-context audit indicates broad null-safe fallback damage",
    )
    feature_rows.set_defaults(func=cmd_materialize_feature_rows)

    sync_venues = subparsers.add_parser(
        "sync-venues",
        help="Populate venue_dim metadata for venue_ids already present on games",
    )
    sync_venues.add_argument("--season", type=int, help="Optional supported season scope")
    sync_venues.add_argument("--date", help="Optional YYYY-MM-DD scope")
    sync_venues.set_defaults(func=cmd_sync_venues)

    weather_backfill = subparsers.add_parser(
        "backfill-game-weather",
        help="Backfill observed_archive weather snapshots from Open-Meteo archive for one supported season",
    )
    weather_backfill.add_argument(
        "--season",
        type=int,
        default=MIN_SUPPORTED_SEASON,
        help=f"Season to process ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})",
    )
    weather_backfill.add_argument(
        "--game-id",
        action="append",
        type=int,
        help="Optional specific game_id to backfill; repeat to target multiple games",
    )
    weather_backfill.add_argument("--max-games", type=int, help="Optional cap for validation runs")
    weather_backfill.set_defaults(func=cmd_backfill_game_weather)

    weather_forecasts = subparsers.add_parser(
        "update-game-weather-forecasts",
        help="Write forecast weather snapshots for not-yet-started games on one date",
    )
    weather_forecasts.add_argument("--date", help="YYYY-MM-DD; defaults to today")
    weather_forecasts.add_argument(
        "--as-of-ts",
        help="Explicit UTC snapshot timestamp; defaults to the command runtime",
    )
    weather_forecasts.set_defaults(func=cmd_update_game_weather_forecasts)

    audit_support = subparsers.add_parser(
        "audit-support-coverage",
        help="Report lineup/weather residual gaps and integrated feature materialization coverage",
    )
    audit_support.add_argument("--season", type=int, help=f"Optional supported season scope ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})")
    audit_support.add_argument(
        "--feature-version",
        default=FEATURE_VERSION_V2_PHASE1,
        help=f"Feature version to audit for integrated materialization coverage (default: {FEATURE_VERSION_V2_PHASE1})",
    )
    audit_support.set_defaults(func=cmd_audit_support_coverage)

    audit_pitcher = subparsers.add_parser(
        "audit-pitcher-context",
        help="Report starter-context quality metrics for a supported season before canonical promotion",
    )
    audit_pitcher.add_argument(
        "--season",
        type=int,
        required=True,
        help=f"Season to inspect ({MIN_SUPPORTED_SEASON}-{MAX_SUPPORTED_SEASON})",
    )
    audit_pitcher.set_defaults(func=cmd_audit_pitcher_context)

    dq = subparsers.add_parser("dq", help="Run data quality checks scaffold")
    dq.add_argument("--partition", help="Partition label, e.g. season=2024")
    dq.set_defaults(func=cmd_dq)

    return parser


def _is_canonical_db_path(db_path: str) -> bool:
    return Path(db_path).expanduser().resolve() == DEFAULT_DB_PATH.resolve()


def _command_mutates_db(command: str | None) -> bool:
    return command not in READ_ONLY_COMMANDS


def enforce_canonical_write_guard(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if not _command_mutates_db(getattr(args, "command", None)):
        return
    if not _is_canonical_db_path(args.db):
        return
    if getattr(args, "allow_canonical_writes", False):
        return
    parser.error(
        "mutating the canonical DB requires --allow-canonical-writes; use a scratch DB by default for validation or rebuild development"
    )


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
    enforce_canonical_write_guard(args, parser)
    args.func(args)


if __name__ == "__main__":
    main()
