"""SQLite phase 1 helpers (behavior-preserving, Excel remains source of truth).

This module is intentionally not wired into runtime prediction/tweet paths yet.
It provides schema initialization plus offline import/parity tooling.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Sequence

import pandas as pd  # type: ignore


PREDICTIONS_TABLE = "predictions"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    time TEXT,
    datetime TEXT,
    home TEXT NOT NULL,
    away TEXT NOT NULL,
    home_probable TEXT,
    away_probable TEXT,
    predicted_winner TEXT,
    predicted_winner_location TEXT,
    model TEXT,
    favorite TEXT,
    prediction_value REAL,
    prediction_accuracy REAL,
    home_odds TEXT,
    home_odds_bookmaker TEXT,
    away_odds TEXT,
    away_odds_bookmaker TEXT,
    odds_retrieval_time TEXT,
    prediction_generation_time TEXT,
    home_score INTEGER,
    away_score INTEGER,
    winning_pitcher TEXT,
    losing_pitcher TEXT,
    venue TEXT,
    series_status TEXT,
    national_broadcasts TEXT,
    summary TEXT,
    tweet TEXT,
    time_to_tweet TEXT,
    tweeted INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
""".strip()

INDEX_SQL: Sequence[str] = (
    "CREATE INDEX IF NOT EXISTS idx_predictions_game_id ON predictions(game_id);",
    "CREATE INDEX IF NOT EXISTS idx_predictions_date ON predictions(date);",
    "CREATE INDEX IF NOT EXISTS idx_predictions_tweeted_date ON predictions(tweeted, date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_predictions_game_date_model ON predictions(game_id, date, model);",
)

EXCEL_TO_DB_COLUMN_MAP = {
    "tweeted?": "tweeted",
}

DB_COLUMNS: Sequence[str] = (
    "game_id",
    "date",
    "time",
    "datetime",
    "home",
    "away",
    "home_probable",
    "away_probable",
    "predicted_winner",
    "predicted_winner_location",
    "model",
    "favorite",
    "prediction_value",
    "prediction_accuracy",
    "home_odds",
    "home_odds_bookmaker",
    "away_odds",
    "away_odds_bookmaker",
    "odds_retrieval_time",
    "prediction_generation_time",
    "home_score",
    "away_score",
    "winning_pitcher",
    "losing_pitcher",
    "venue",
    "series_status",
    "national_broadcasts",
    "summary",
    "tweet",
    "time_to_tweet",
    "tweeted",
)


@dataclass(frozen=True)
class ImportResult:
    imported_rows: int
    sqlite_total_rows: int


@dataclass(frozen=True)
class ParityResult:
    excel_row_count: int
    sqlite_row_count: int
    key_fields: Sequence[str]
    excel_key_count: int
    sqlite_key_count: int
    missing_in_sqlite: int
    missing_in_excel: int

    @property
    def matches(self) -> bool:
        return (
            self.excel_row_count == self.sqlite_row_count
            and self.missing_in_sqlite == 0
            and self.missing_in_excel == 0
        )


def ensure_predictions_schema(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(SCHEMA_SQL)
        for stmt in INDEX_SQL:
            conn.execute(stmt)
        conn.commit()


def _normalize_tweeted(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    s = str(value).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return 1
    return 0


def _normalize_scalar(value: object) -> object:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    return value


def _build_import_frame(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.rename(columns=EXCEL_TO_DB_COLUMN_MAP)
    for col in DB_COLUMNS:
        if col not in renamed.columns:
            renamed[col] = None
    out = renamed.loc[:, DB_COLUMNS].copy()

    if "tweeted" in out.columns:
        out["tweeted"] = out["tweeted"].map(_normalize_tweeted)

    for col in out.columns:
        if col == "tweeted":
            continue
        out[col] = out[col].map(_normalize_scalar)
    return out


def import_excel_to_sqlite(excel_path: str, db_path: str, replace: bool = True) -> ImportResult:
    ensure_predictions_schema(db_path)
    excel_df = pd.read_excel(excel_path)
    import_df = _build_import_frame(excel_df)

    with sqlite3.connect(db_path) as conn:
        if replace:
            conn.execute(f"DELETE FROM {PREDICTIONS_TABLE};")
        import_df.to_sql(PREDICTIONS_TABLE, conn, if_exists="append", index=False)
        total = int(
            conn.execute(f"SELECT COUNT(*) FROM {PREDICTIONS_TABLE};").fetchone()[0]
        )
        conn.commit()

    return ImportResult(imported_rows=len(import_df), sqlite_total_rows=total)


def _read_sqlite_columns(db_path: str, columns: Sequence[str]) -> pd.DataFrame:
    projection = ", ".join(columns)
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(f"SELECT {projection} FROM {PREDICTIONS_TABLE};", conn)


def _resolve_key_fields(
    excel_df: pd.DataFrame, sqlite_df: pd.DataFrame, requested: Sequence[str]
) -> Sequence[str]:
    return [
        field
        for field in requested
        if field in excel_df.columns and field in sqlite_df.columns
    ]


def _key_set(df: pd.DataFrame, key_fields: Sequence[str]) -> set[tuple[object, ...]]:
    if not key_fields:
        return set()

    rows: set[tuple[object, ...]] = set()
    for _, row in df.loc[:, key_fields].iterrows():
        rows.add(tuple(_normalize_scalar(v) for v in row.values.tolist()))
    return rows


def check_excel_sqlite_parity(
    excel_path: str,
    db_path: str,
    key_fields: Sequence[str] = ("game_id", "date", "home", "away", "model"),
) -> ParityResult:
    excel_df = _build_import_frame(pd.read_excel(excel_path))
    sqlite_df = _read_sqlite_columns(db_path, DB_COLUMNS)

    resolved_keys = _resolve_key_fields(excel_df, sqlite_df, key_fields)

    excel_keys = _key_set(excel_df, resolved_keys)
    sqlite_keys = _key_set(sqlite_df, resolved_keys)

    return ParityResult(
        excel_row_count=len(excel_df),
        sqlite_row_count=len(sqlite_df),
        key_fields=resolved_keys,
        excel_key_count=len(excel_keys),
        sqlite_key_count=len(sqlite_keys),
        missing_in_sqlite=len(excel_keys - sqlite_keys),
        missing_in_excel=len(sqlite_keys - excel_keys),
    )
