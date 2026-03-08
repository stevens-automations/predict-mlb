"""Storage helpers for Excel source-of-truth + optional SQLite shadow writes."""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Protocol

import pandas as pd  # type: ignore

from sqlite_phase1 import DB_COLUMNS, ensure_predictions_schema


class PredictionStorage(Protocol):
    """Minimal storage contract for prediction persistence."""

    def read_predictions(self, path: str) -> pd.DataFrame:
        ...

    def write_predictions(self, path: str, df: pd.DataFrame) -> None:
        ...


class ExcelPredictionStorage:
    """Excel-backed storage implementation."""

    def read_predictions(self, path: str) -> pd.DataFrame:
        return pd.read_excel(path)

    def write_predictions(self, path: str, df: pd.DataFrame) -> None:
        df.to_excel(path, index=False)


@dataclass
class ShadowWriteStats:
    success: int = 0
    failure: int = 0

    def add(self, success: int, failure: int) -> None:
        self.success += success
        self.failure += failure


class SQLiteShadowWriter:
    """Best-effort SQLite mirror writer (fail-open)."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    @staticmethod
    def _normalize_value(value: object) -> object:
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime().isoformat()
        return value

    @staticmethod
    def _normalize_tweeted(value: object) -> int:
        if value is None:
            return 0
        if isinstance(value, bool):
            return int(value)
        s = str(value).strip().lower()
        if s in {"1", "true", "t", "yes", "y"}:
            return 1
        return 0

    def _to_db_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        renamed = df.rename(columns={"tweeted?": "tweeted"}).copy()
        for col in DB_COLUMNS:
            if col not in renamed.columns:
                renamed[col] = None

        out = renamed.loc[:, DB_COLUMNS].copy()
        if "tweeted" in out.columns:
            out["tweeted"] = out["tweeted"].map(self._normalize_tweeted)

        for col in out.columns:
            if col == "tweeted":
                continue
            out[col] = out[col].map(self._normalize_value)
        return out

    def mirror_predictions(self, df: pd.DataFrame) -> tuple[int, int]:
        if df.empty:
            return (0, 0)

        try:
            ensure_predictions_schema(self.db_path)
            import_df = self._to_db_frame(df)
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
            with sqlite3.connect(self.db_path) as conn:
                for row in import_df.itertuples(index=False, name=None):
                    try:
                        conn.execute(sql, row)
                        success += 1
                    except Exception as row_exc:
                        failure += 1
                        print(f"[warn] SQLite shadow write failed for a row: {row_exc}")
                conn.commit()
            return (success, failure)
        except Exception as exc:
            print(f"[warn] SQLite shadow write unavailable, continuing with Excel only: {exc}")
            return (0, len(df))


class NullShadowWriter:
    def mirror_predictions(self, df: pd.DataFrame) -> tuple[int, int]:
        return (0, 0)


def sqlite_shadow_enabled() -> bool:
    return os.getenv("SQLITE_SHADOW_WRITE", "").strip().lower() in {"1", "true", "yes", "on"}


def shadow_writer_from_env() -> SQLiteShadowWriter | NullShadowWriter:
    if not sqlite_shadow_enabled():
        return NullShadowWriter()
    db_path = os.getenv("SQLITE_DB_PATH", "data/predictions.db")
    return SQLiteShadowWriter(db_path=db_path)
