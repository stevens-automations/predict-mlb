from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

DEFAULT_DB_PATH = Path("data/mlb_history.db")
DEFAULT_LABEL_COLUMN = "did_home_win"
DEFAULT_DATE_COLUMN = "game_date"
DEFAULT_AS_OF_COLUMN = "as_of_ts"
DEFAULT_ID_COLUMN = "game_id"

NON_FEATURE_COLUMNS = {
    DEFAULT_ID_COLUMN,
    "season",
    DEFAULT_DATE_COLUMN,
    DEFAULT_AS_OF_COLUMN,
    DEFAULT_LABEL_COLUMN,
    "home_team_id",
    "away_team_id",
}


@dataclass(frozen=True)
class DatasetBundle:
    dataframe: pd.DataFrame
    feature_columns: list[str]
    label_column: str = DEFAULT_LABEL_COLUMN
    date_column: str = DEFAULT_DATE_COLUMN
    as_of_column: str = DEFAULT_AS_OF_COLUMN
    id_column: str = DEFAULT_ID_COLUMN

    @property
    def X(self) -> pd.DataFrame:
        return self.dataframe.loc[:, self.feature_columns].copy()

    @property
    def y(self) -> pd.Series:
        return self.dataframe[self.label_column].copy()


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _build_filters(
    seasons: Sequence[int] | None,
    start_date: str | None,
    end_date: str | None,
    feature_version: str,
    contract_statuses: Sequence[str] | None,
) -> tuple[str, list[Any]]:
    clauses = ["fr.feature_version = ?"]
    params: list[Any] = [feature_version]

    if seasons:
        placeholders = ",".join("?" for _ in seasons)
        clauses.append(f"g.season IN ({placeholders})")
        params.extend(int(season) for season in seasons)
    if start_date:
        clauses.append("g.game_date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("g.game_date <= ?")
        params.append(end_date)
    if contract_statuses:
        placeholders = ",".join("?" for _ in contract_statuses)
        clauses.append(f"fr.source_contract_status IN ({placeholders})")
        params.extend(contract_statuses)

    return " AND ".join(clauses), params


def _rows_to_dataframe(rows: Iterable[sqlite3.Row]) -> pd.DataFrame:
    flattened: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(row["feature_payload_json"])
        payload[DEFAULT_LABEL_COLUMN] = row[DEFAULT_LABEL_COLUMN]
        payload[DEFAULT_AS_OF_COLUMN] = row[DEFAULT_AS_OF_COLUMN]
        payload["source_contract_status"] = row["source_contract_status"]
        payload["source_contract_issues_json"] = row["source_contract_issues_json"]
        flattened.append(payload)

    if not flattened:
        return pd.DataFrame()
    return pd.DataFrame(flattened)


def _coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    coerced = df.copy()
    if DEFAULT_DATE_COLUMN in coerced.columns:
        coerced[DEFAULT_DATE_COLUMN] = pd.to_datetime(coerced[DEFAULT_DATE_COLUMN], errors="coerce").dt.date.astype("string")
    if DEFAULT_AS_OF_COLUMN in coerced.columns:
        coerced[DEFAULT_AS_OF_COLUMN] = pd.to_datetime(coerced[DEFAULT_AS_OF_COLUMN], errors="coerce", utc=True)
    for column in coerced.columns:
        if column in {DEFAULT_DATE_COLUMN, DEFAULT_AS_OF_COLUMN, "source_contract_status", "source_contract_issues_json"}:
            continue
        if column == DEFAULT_LABEL_COLUMN:
            coerced[column] = pd.to_numeric(coerced[column], errors="coerce").astype("Int64")
            continue
        coerced[column] = pd.to_numeric(coerced[column], errors="coerce")
    return coerced


def infer_feature_columns(df: pd.DataFrame, explicit_feature_columns: Sequence[str] | None = None) -> list[str]:
    if explicit_feature_columns:
        missing = [column for column in explicit_feature_columns if column not in df.columns]
        if missing:
            raise ValueError(f"Configured feature columns missing from dataset: {missing}")
        return list(explicit_feature_columns)

    feature_columns = [
        column
        for column in df.columns
        if column not in NON_FEATURE_COLUMNS
        and column not in {"source_contract_status", "source_contract_issues_json"}
        and pd.api.types.is_numeric_dtype(df[column])
    ]
    if not feature_columns:
        raise ValueError("No numeric training features found in loaded feature_rows payloads")
    return sorted(feature_columns)


def load_feature_rows(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    seasons: Sequence[int] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    feature_version: str = "v1",
    contract_statuses: Sequence[str] | None = ("valid", "degraded"),
    require_labels: bool = True,
    explicit_feature_columns: Sequence[str] | None = None,
) -> DatasetBundle:
    where_clause, params = _build_filters(
        seasons=seasons,
        start_date=start_date,
        end_date=end_date,
        feature_version=feature_version,
        contract_statuses=contract_statuses,
    )

    query = f"""
        SELECT
          fr.as_of_ts AS {DEFAULT_AS_OF_COLUMN},
          fr.feature_payload_json,
          fr.source_contract_status,
          fr.source_contract_issues_json,
          lb.did_home_win AS {DEFAULT_LABEL_COLUMN}
        FROM feature_rows fr
        INNER JOIN games g ON g.game_id = fr.game_id
        {"INNER JOIN" if require_labels else "LEFT JOIN"} labels lb ON lb.game_id = fr.game_id
        WHERE {where_clause}
        ORDER BY g.game_date ASC, fr.as_of_ts ASC, fr.game_id ASC
    """

    with _connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    df = _rows_to_dataframe(rows)
    if df.empty:
        raise ValueError(
            "No training rows found for requested filters. Check historical ingestion status and feature_version."
        )

    df = _coerce_columns(df)
    df = df.dropna(subset=[DEFAULT_LABEL_COLUMN, DEFAULT_DATE_COLUMN, DEFAULT_ID_COLUMN])
    df = df.sort_values([DEFAULT_DATE_COLUMN, DEFAULT_AS_OF_COLUMN, DEFAULT_ID_COLUMN]).reset_index(drop=True)
    feature_columns = infer_feature_columns(df, explicit_feature_columns=explicit_feature_columns)
    return DatasetBundle(dataframe=df, feature_columns=feature_columns)
