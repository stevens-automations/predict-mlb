"""SQLite-first prediction storage with one-time Excel bootstrap support."""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional, Protocol

import pandas as pd  # type: ignore

from paths import get_env_path, get_predictions_db_path
from sqlite_phase1 import DB_COLUMNS, import_excel_to_sqlite, ensure_predictions_schema


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


def _to_db_frame(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.rename(columns={"tweeted?": "tweeted"}).copy()
    for col in DB_COLUMNS:
        if col not in renamed.columns:
            renamed[col] = None
    out = renamed.loc[:, DB_COLUMNS].copy()
    out["tweeted"] = out["tweeted"].map(_normalize_tweeted)
    for col in out.columns:
        if col == "tweeted":
            continue
        out[col] = out[col].map(_normalize_scalar)
    return out


def _from_db_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "tweeted" in out.columns:
        out["tweeted?"] = out["tweeted"].map(lambda v: bool(_normalize_tweeted(v)))
        out = out.drop(columns=["tweeted"])
    return out


class PredictionStorage(Protocol):
    def bootstrap_if_needed(self) -> bool:
        ...

    def read_predictions(self) -> pd.DataFrame:
        ...

    def upsert_predictions(self, df: pd.DataFrame) -> tuple[int, int]:
        ...

    def replace_predictions(self, df: pd.DataFrame) -> tuple[int, int]:
        ...


@dataclass
class WriteStats:
    success: int = 0
    failure: int = 0

    def add(self, success: int, failure: int) -> None:
        self.success += success
        self.failure += failure


class SQLitePredictionStorage:
    def __init__(self, db_path: Optional[str] = None, excel_path: Optional[str] = None):
        self.db_path = db_path or get_predictions_db_path()
        self.excel_path = excel_path or get_env_path("DATA_SHEET_PATH", "data/predictions.xlsx")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def ensure_ready(self) -> None:
        ensure_predictions_schema(self.db_path)

    def _row_count(self) -> int:
        self.ensure_ready()
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM predictions;").fetchone()[0])

    def bootstrap_if_needed(self) -> bool:
        self.ensure_ready()
        try:
            count = self._row_count()
        except Exception:
            count = 0
        if count > 0:
            return False
        if not os.path.exists(self.excel_path):
            return False
        try:
            result = import_excel_to_sqlite(self.excel_path, self.db_path, replace=True)
            print(
                "[storage] SQLite bootstrap imported "
                f"{result.imported_rows} rows from Excel ({self.excel_path})."
            )
            return result.imported_rows > 0
        except Exception as exc:
            print(f"[warn] SQLite bootstrap failed; continuing with empty DB: {exc}")
            return False

    def read_predictions(self) -> pd.DataFrame:
        self.ensure_ready()
        with self._connect() as conn:
            df = pd.read_sql_query("SELECT * FROM predictions;", conn)
        if "id" in df.columns:
            df = df.drop(columns=["id"])
        return _from_db_frame(df)

    def _upsert_with_connection(self, conn: sqlite3.Connection, df: pd.DataFrame) -> tuple[int, int]:
        if df.empty:
            return (0, 0)

        import_df = _to_db_frame(df)
        columns = list(DB_COLUMNS)
        placeholders = ", ".join(["?" for _ in columns])
        update_columns = [c for c in columns if c not in {"game_id", "date", "model"}]
        update_sql = ", ".join([f"{c}=excluded.{c}" for c in update_columns])
        sql = (
            "INSERT INTO predictions ("
            + ", ".join(columns)
            + ") VALUES ("
            + placeholders
            + ") ON CONFLICT(game_id, date, model) DO UPDATE SET "
            + update_sql
            + ";"
        )

        success = 0
        failure = 0
        for row in import_df.itertuples(index=False, name=None):
            try:
                conn.execute(sql, row)
                success += 1
            except Exception as row_exc:
                failure += 1
                print(f"[warn] SQLite write failed for a row: {row_exc}")
        return (success, failure)

    def upsert_predictions(self, df: pd.DataFrame) -> tuple[int, int]:
        if df.empty:
            return (0, 0)
        self.ensure_ready()
        with self._connect() as conn:
            success, failure = self._upsert_with_connection(conn, df)
            conn.commit()
        return (success, failure)

    def replace_predictions(self, df: pd.DataFrame) -> tuple[int, int]:
        self.ensure_ready()
        with self._connect() as conn:
            try:
                conn.execute("BEGIN;")
                conn.execute("DELETE FROM predictions;")
                success, failure = self._upsert_with_connection(conn, df)
                if failure > 0:
                    raise RuntimeError(
                        f"replace_predictions aborted due to {failure} row write failure(s)"
                    )
                conn.commit()
                return (success, failure)
            except Exception as exc:
                conn.rollback()
                print(f"[warn] SQLite replace_predictions rolled back: {exc}")
                return (0, len(df))

    def mark_tweeted(self, lines: Iterable[str]) -> None:
        self.ensure_ready()
        with self._connect() as conn:
            for line in lines:
                conn.execute(
                    "UPDATE predictions SET tweeted = 1 WHERE tweet = ?;",
                    (line,),
                )
            conn.commit()


class NullShadowWriter:
    def mirror_predictions(self, _df):
        return (0, 0)


class CompatibilityShadowWriter:
    def mirror_predictions(self, df):
        return self.storage.upsert_predictions(df)

    def __init__(self, storage: SQLitePredictionStorage):
        self.storage = storage


def sqlite_shadow_enabled() -> bool:
    return os.getenv("SQLITE_SHADOW_WRITE", "").strip().lower() in {"1", "true", "yes", "on"}


def shadow_writer_from_env():
    if sqlite_shadow_enabled():
        return CompatibilityShadowWriter(get_primary_storage())
    return NullShadowWriter()


def get_primary_storage() -> SQLitePredictionStorage:
    db_path = get_predictions_db_path()
    excel_path = get_env_path("DATA_SHEET_PATH", "data/predictions.xlsx")
    return SQLitePredictionStorage(db_path=db_path, excel_path=excel_path)


# Backwards compatibility name used by prior tests/docs.
ShadowWriteStats = WriteStats
