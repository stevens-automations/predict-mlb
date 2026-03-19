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
DEFAULT_SEASON_COLUMN = "season"
DEFAULT_FEATURE_CONTRACT = "raw"
CLEANED_COMPARATIVE_FEATURE_CONTRACT = "cleaned_comparative_v1"
HYBRID_COMPARATIVE_FEATURE_CONTRACT = "hybrid_comparative_v1"

FORBIDDEN_FEATURE_TOKENS = ("odds", "bookmaker")

NON_FEATURE_COLUMNS = {
    DEFAULT_ID_COLUMN,
    DEFAULT_SEASON_COLUMN,
    DEFAULT_DATE_COLUMN,
    DEFAULT_AS_OF_COLUMN,
    DEFAULT_LABEL_COLUMN,
    "home_team_id",
    "away_team_id",
}

REQUIRED_DATASET_COLUMNS = {
    DEFAULT_ID_COLUMN,
    DEFAULT_SEASON_COLUMN,
    DEFAULT_DATE_COLUMN,
    DEFAULT_AS_OF_COLUMN,
    DEFAULT_LABEL_COLUMN,
    "source_contract_status",
    "source_contract_issues_json",
}

CHALLENGER_SHARED_CONTEXT_COLUMNS = (
    "home_field_advantage",
    "hour_offset_from_first_pitch",
    "is_day",
    "temperature_f",
    "humidity_pct",
    "pressure_hpa",
    "precipitation_mm",
    "cloud_cover_pct",
    "wind_speed_mph",
    "wind_gust_mph",
    "wind_direction_deg",
    "windy_flag",
    "extreme_temp_flag",
    "roof_closed_or_fixed_flag",
    "weather_available_flag",
    "weather_forecast_flag",
    "weather_observed_archive_flag",
    "weather_exposed_flag",
)

CHALLENGER_RAW_ANCHOR_COLUMNS = (
    "home_starter_known",
    "away_starter_known",
    "home_starter_stats_available",
    "away_starter_stats_available",
    "home_team_strength_available",
    "away_team_strength_available",
    "home_team_rolling_available",
    "away_team_rolling_available",
    "home_team_season_games",
    "away_team_season_games",
    "home_team_rolling_games",
    "away_team_rolling_games",
    "home_team_days_rest",
    "away_team_days_rest",
    "home_team_doubleheader_flag",
    "away_team_doubleheader_flag",
    "home_bullpen_available_flag",
    "away_bullpen_available_flag",
    "home_bullpen_low_sample_flag",
    "away_bullpen_low_sample_flag",
    "home_bullpen_appearances_season",
    "away_bullpen_appearances_season",
    "home_season_games_in_sample",
    "away_season_games_in_sample",
    "home_top3_availability_flag",
    "away_top3_availability_flag",
    "home_top3_availability_low_flag",
    "away_top3_availability_low_flag",
    "home_top3_n_available",
    "away_top3_n_available",
    "home_lineup_known_flag",
    "away_lineup_known_flag",
    "home_lineup_partial_flag",
    "away_lineup_partial_flag",
    "home_lineup_status_missing_flag",
    "away_lineup_status_missing_flag",
    "home_announced_lineup_count",
    "away_announced_lineup_count",
    "home_platoon_available_flag",
    "away_platoon_available_flag",
    "home_platoon_low_sample_flag",
    "away_platoon_low_sample_flag",
    "home_platoon_games_in_sample",
    "away_platoon_games_in_sample",
    "home_platoon_plate_appearances",
    "away_platoon_plate_appearances",
    "opposing_starter_hand_known_flag_home_offense",
    "opposing_starter_hand_known_flag_away_offense",
)

HYBRID_RAW_QUALITY_ANCHOR_COLUMNS = (
    "home_starter_era",
    "away_starter_era",
    "home_starter_whip",
    "away_starter_whip",
    "home_starter_runs_per_9",
    "away_starter_runs_per_9",
    "home_starter_career_era",
    "away_starter_career_era",
    "home_bullpen_era_season",
    "away_bullpen_era_season",
    "home_bullpen_whip_season",
    "away_bullpen_whip_season",
    "home_top3_freshness_score",
    "away_top3_freshness_score",
    "home_team_season_win_pct",
    "away_team_season_win_pct",
    "home_team_season_run_diff_per_game",
    "away_team_season_run_diff_per_game",
    "home_team_rolling_last10_win_pct",
    "away_team_rolling_last10_win_pct",
    "home_team_rolling_last10_ops",
    "away_team_rolling_last10_ops",
)

PLACEHOLDER_GRADE_COLUMNS = {
    "home_lineup_quality_mean",
    "away_lineup_quality_mean",
    "home_top3_lineup_quality_mean",
    "away_top3_lineup_quality_mean",
    "home_lineup_vs_opp_starter_hand_quality",
    "away_lineup_vs_opp_starter_hand_quality",
    "lineup_quality_delta",
    "top3_lineup_quality_delta",
}

STARTER_ID_ABLATION_COLUMNS = {
    "home_starter_id",
    "away_starter_id",
    "home_probable_pitcher_id",
    "away_probable_pitcher_id",
}

WEATHER_ABLATION_COLUMNS = {
    "temperature_f",
    "humidity_pct",
    "pressure_hpa",
    "precipitation_mm",
    "cloud_cover_pct",
    "wind_speed_mph",
    "wind_gust_mph",
    "wind_direction_deg",
    "windy_flag",
    "extreme_temp_flag",
    "roof_closed_or_fixed_flag",
    "weather_available_flag",
    "weather_forecast_flag",
    "weather_observed_archive_flag",
    "weather_exposed_flag",
}

PLATOON_ABLATION_COLUMNS = {
    "home_platoon_available_flag",
    "away_platoon_available_flag",
    "home_platoon_low_sample_flag",
    "away_platoon_low_sample_flag",
    "home_platoon_games_in_sample",
    "away_platoon_games_in_sample",
    "home_platoon_plate_appearances",
    "away_platoon_plate_appearances",
    "home_platoon_ops",
    "away_platoon_ops",
    "home_platoon_runs_per_game",
    "away_platoon_runs_per_game",
    "home_platoon_strikeout_rate",
    "away_platoon_strikeout_rate",
    "home_platoon_walk_rate",
    "away_platoon_walk_rate",
    "home_lineup_lefty_pa_share_proxy",
    "away_lineup_lefty_pa_share_proxy",
    "home_lineup_righty_pa_share_proxy",
    "away_lineup_righty_pa_share_proxy",
    "home_lineup_switch_pa_share_proxy",
    "away_lineup_switch_pa_share_proxy",
    "home_lineup_balance_score",
    "away_lineup_balance_score",
    "home_top3_l_count",
    "away_top3_l_count",
    "home_top3_r_count",
    "away_top3_r_count",
    "home_top3_s_count",
    "away_top3_s_count",
    "home_lineup_vs_opp_starter_hand_quality",
    "away_lineup_vs_opp_starter_hand_quality",
    "home_opposing_starter_pitch_hand_l_flag",
    "away_opposing_starter_pitch_hand_l_flag",
    "home_opposing_starter_pitch_hand_r_flag",
    "away_opposing_starter_pitch_hand_r_flag",
    "opposing_starter_hand_known_flag_home_offense",
    "opposing_starter_hand_known_flag_away_offense",
}

FEATURE_ABLATION_GROUPS: dict[str, set[str]] = {
    "starter_ids": STARTER_ID_ABLATION_COLUMNS,
    "placeholder_lineup_quality": set(PLACEHOLDER_GRADE_COLUMNS),
    "weather": WEATHER_ABLATION_COLUMNS,
    "platoon": PLATOON_ABLATION_COLUMNS,
}

FORBIDDEN_IDENTITY_FEATURE_COLUMNS = {
    "home_team_id",
    "away_team_id",
    "home_starter_id",
    "away_starter_id",
    "home_probable_pitcher_id",
    "away_probable_pitcher_id",
    "venue_id",
}

COMPARATIVE_FEATURE_SPECS = (
    ("starter_era_home_edge", "home_starter_era", "away_starter_era", False),
    ("starter_whip_home_edge", "home_starter_whip", "away_starter_whip", False),
    ("starter_avg_allowed_home_edge", "home_starter_avg_allowed", "away_starter_avg_allowed", False),
    ("starter_runs_per_9_home_edge", "home_starter_runs_per_9", "away_starter_runs_per_9", False),
    ("starter_strike_pct_home_edge", "home_starter_strike_pct", "away_starter_strike_pct", True),
    ("starter_win_pct_home_edge", "home_starter_win_pct", "away_starter_win_pct", True),
    ("starter_career_era_home_edge", "home_starter_career_era", "away_starter_career_era", False),
    ("bullpen_era_home_edge", "home_bullpen_era_season", "away_bullpen_era_season", False),
    ("bullpen_whip_home_edge", "home_bullpen_whip_season", "away_bullpen_whip_season", False),
    (
        "bullpen_k_minus_bb_rate_home_edge",
        "home_bullpen_k_minus_bb_rate_season",
        "away_bullpen_k_minus_bb_rate_season",
        True,
    ),
    ("bullpen_hr_rate_home_edge", "home_bullpen_hr_rate_season", "away_bullpen_hr_rate_season", False),
    ("bullpen_freshness_home_edge", "home_freshness_score", "away_freshness_score", True),
    ("bullpen_outs_last3d_home_edge", "home_bullpen_outs_last3d", "away_bullpen_outs_last3d", False),
    (
        "bullpen_pitches_last3d_home_edge",
        "home_bullpen_pitches_last3d",
        "away_bullpen_pitches_last3d",
        False,
    ),
    (
        "bullpen_back_to_back_home_edge",
        "home_relievers_back_to_back_count",
        "away_relievers_back_to_back_count",
        False,
    ),
    (
        "bullpen_high_usage_home_edge",
        "home_high_usage_relievers_last3d_count",
        "away_high_usage_relievers_last3d_count",
        False,
    ),
    ("team_season_win_pct_home_edge", "home_team_season_win_pct", "away_team_season_win_pct", True),
    (
        "team_season_run_diff_per_game_home_edge",
        "home_team_season_run_diff_per_game",
        "away_team_season_run_diff_per_game",
        True,
    ),
    (
        "team_rolling_last10_win_pct_home_edge",
        "home_team_rolling_last10_win_pct",
        "away_team_rolling_last10_win_pct",
        True,
    ),
    (
        "team_rolling_last10_hits_per_game_home_edge",
        "home_team_rolling_last10_hits_per_game",
        "away_team_rolling_last10_hits_per_game",
        True,
    ),
    (
        "team_rolling_last10_batting_avg_home_edge",
        "home_team_rolling_last10_batting_avg",
        "away_team_rolling_last10_batting_avg",
        True,
    ),
    ("team_rolling_last10_obp_home_edge", "home_team_rolling_last10_obp", "away_team_rolling_last10_obp", True),
    ("team_rolling_last10_ops_home_edge", "home_team_rolling_last10_ops", "away_team_rolling_last10_ops", True),
    (
        "team_rolling_last10_runs_for_per_game_home_edge",
        "home_team_rolling_last10_runs_for_per_game",
        "away_team_rolling_last10_runs_for_per_game",
        True,
    ),
    (
        "team_rolling_last10_runs_against_per_game_home_edge",
        "home_team_rolling_last10_runs_against_per_game",
        "away_team_rolling_last10_runs_against_per_game",
        False,
    ),
    ("team_days_rest_home_edge", "home_team_days_rest", "away_team_days_rest", True),
    ("team_doubleheader_home_edge", "home_team_doubleheader_flag", "away_team_doubleheader_flag", False),
    ("top3_freshness_home_edge", "home_top3_freshness_score", "away_top3_freshness_score", True),
    (
        "top3_k_minus_bb_rate_home_edge",
        "home_top3_k_minus_bb_rate_season",
        "away_top3_k_minus_bb_rate_season",
        True,
    ),
    (
        "top3_quality_dropoff_vs_team_home_edge",
        "home_top3_quality_dropoff_vs_team",
        "away_top3_quality_dropoff_vs_team",
        False,
    ),
    ("top3_n_available_home_edge", "home_top3_n_available", "away_top3_n_available", True),
    ("platoon_ops_home_edge", "home_platoon_ops", "away_platoon_ops", True),
    ("platoon_runs_per_game_home_edge", "home_platoon_runs_per_game", "away_platoon_runs_per_game", True),
    (
        "platoon_strikeout_rate_home_edge",
        "home_platoon_strikeout_rate",
        "away_platoon_strikeout_rate",
        False,
    ),
    ("platoon_walk_rate_home_edge", "home_platoon_walk_rate", "away_platoon_walk_rate", True),
    (
        "lineup_lefty_share_home_edge",
        "home_lineup_lefty_pa_share_proxy",
        "away_lineup_lefty_pa_share_proxy",
        True,
    ),
    (
        "lineup_righty_share_home_edge",
        "home_lineup_righty_pa_share_proxy",
        "away_lineup_righty_pa_share_proxy",
        True,
    ),
    (
        "lineup_switch_share_home_edge",
        "home_lineup_switch_pa_share_proxy",
        "away_lineup_switch_pa_share_proxy",
        True,
    ),
    ("lineup_balance_home_edge", "home_lineup_balance_score", "away_lineup_balance_score", True),
    ("top3_lefty_count_home_edge", "home_top3_l_count", "away_top3_l_count", True),
    ("top3_righty_count_home_edge", "home_top3_r_count", "away_top3_r_count", True),
    ("top3_switch_count_home_edge", "home_top3_s_count", "away_top3_s_count", True),
)


@dataclass(frozen=True)
class DatasetBundle:
    dataframe: pd.DataFrame
    feature_columns: list[str]
    label_column: str = DEFAULT_LABEL_COLUMN
    date_column: str = DEFAULT_DATE_COLUMN
    as_of_column: str = DEFAULT_AS_OF_COLUMN
    id_column: str = DEFAULT_ID_COLUMN
    diagnostics: dict[str, Any] | None = None

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
        payload[DEFAULT_ID_COLUMN] = row[DEFAULT_ID_COLUMN]
        payload[DEFAULT_SEASON_COLUMN] = row[DEFAULT_SEASON_COLUMN]
        payload[DEFAULT_DATE_COLUMN] = row[DEFAULT_DATE_COLUMN]
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


def _validate_dataset(df: pd.DataFrame, *, feature_version: str) -> None:
    missing_columns = [
        column
        for column in (
            DEFAULT_ID_COLUMN,
            DEFAULT_SEASON_COLUMN,
            DEFAULT_DATE_COLUMN,
            DEFAULT_AS_OF_COLUMN,
            DEFAULT_LABEL_COLUMN,
            "source_contract_status",
        )
        if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"Loaded dataset is missing required columns: {missing_columns}")

    duplicate_game_ids = (
        df[df[DEFAULT_ID_COLUMN].duplicated(keep=False)][DEFAULT_ID_COLUMN].drop_duplicates().sort_values().tolist()
    )
    if duplicate_game_ids:
        raise ValueError(
            "Canonical pregame_1h training requires exactly one feature row per game. "
            f"Duplicate rows found for game_id values: {duplicate_game_ids[:10]}"
        )

    invalid_labels = sorted(set(df[DEFAULT_LABEL_COLUMN].dropna().astype(int).tolist()) - {0, 1})
    if invalid_labels:
        raise ValueError(f"Training labels must be binary 0/1 values; found: {invalid_labels}")

    if df[DEFAULT_AS_OF_COLUMN].isna().any():
        missing = int(df[DEFAULT_AS_OF_COLUMN].isna().sum())
        raise ValueError(f"Encountered {missing} rows with invalid as_of timestamps for `{feature_version}`")


def _copy_existing_columns(source: pd.DataFrame, target: pd.DataFrame, columns: Sequence[str]) -> None:
    for column in columns:
        if column in source.columns:
            target[column] = source[column]


def _derive_home_edge_feature(
    source: pd.DataFrame,
    target: pd.DataFrame,
    new_column: str,
    *,
    home_column: str,
    away_column: str,
    higher_is_better: bool,
) -> bool:
    if home_column not in source.columns or away_column not in source.columns:
        return False
    if higher_is_better:
        target[new_column] = source[home_column] - source[away_column]
    else:
        target[new_column] = source[away_column] - source[home_column]
    return True


def _drop_unusable_feature_columns(
    df: pd.DataFrame,
    *,
    explicit_feature_columns: Sequence[str] | None = None,
) -> tuple[pd.DataFrame, list[str], dict[str, list[str]]]:
    feature_columns = infer_feature_columns(df, explicit_feature_columns)
    all_null = [column for column in feature_columns if df[column].isna().all()]
    constant = [
        column
        for column in feature_columns
        if column not in all_null and df[column].dropna().nunique() <= 1
    ]
    drop_columns = sorted(all_null + constant)
    if drop_columns:
        df = df.drop(columns=drop_columns)
    resolved_feature_columns = infer_feature_columns(df, explicit_feature_columns=None)
    return df, resolved_feature_columns, {
        "all_null": sorted(all_null),
        "constant": sorted(constant),
    }


def _apply_comparative_feature_contract(
    df: pd.DataFrame,
    *,
    feature_contract: str,
    raw_quality_anchor_columns: Sequence[str] = (),
) -> tuple[pd.DataFrame, dict[str, Any]]:
    transformed = df.loc[:, [column for column in REQUIRED_DATASET_COLUMNS if column in df.columns]].copy()

    generated_columns: list[str] = []

    _copy_existing_columns(df, transformed, CHALLENGER_SHARED_CONTEXT_COLUMNS)
    _copy_existing_columns(df, transformed, CHALLENGER_RAW_ANCHOR_COLUMNS)
    _copy_existing_columns(df, transformed, raw_quality_anchor_columns)

    for new_column, home_column, away_column, higher_is_better in COMPARATIVE_FEATURE_SPECS:
        if _derive_home_edge_feature(
            df,
            transformed,
            new_column,
            home_column=home_column,
            away_column=away_column,
            higher_is_better=higher_is_better,
        ):
            generated_columns.append(new_column)

    transformed, feature_columns, dropped_feature_columns = _drop_unusable_feature_columns(transformed)
    return transformed, {
        "feature_contract": feature_contract,
        "included_raw_quality_anchor_columns": sorted(column for column in raw_quality_anchor_columns if column in transformed.columns),
        "generated_comparative_columns": sorted(generated_columns),
        "excluded_forbidden_identity_columns": sorted(column for column in FORBIDDEN_IDENTITY_FEATURE_COLUMNS if column in df.columns),
        "excluded_placeholder_grade_columns": sorted(column for column in PLACEHOLDER_GRADE_COLUMNS if column in df.columns),
        "dropped_feature_columns": dropped_feature_columns,
        "resolved_feature_columns": feature_columns,
    }


def _apply_cleaned_comparative_feature_contract(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    return _apply_comparative_feature_contract(
        df,
        feature_contract=CLEANED_COMPARATIVE_FEATURE_CONTRACT,
    )


def _apply_hybrid_comparative_feature_contract(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    return _apply_comparative_feature_contract(
        df,
        feature_contract=HYBRID_COMPARATIVE_FEATURE_CONTRACT,
        raw_quality_anchor_columns=HYBRID_RAW_QUALITY_ANCHOR_COLUMNS,
    )


def apply_feature_contract(
    df: pd.DataFrame,
    *,
    feature_contract: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if feature_contract == DEFAULT_FEATURE_CONTRACT:
        return df, {"feature_contract": DEFAULT_FEATURE_CONTRACT}
    if feature_contract == CLEANED_COMPARATIVE_FEATURE_CONTRACT:
        return _apply_cleaned_comparative_feature_contract(df)
    if feature_contract == HYBRID_COMPARATIVE_FEATURE_CONTRACT:
        return _apply_hybrid_comparative_feature_contract(df)
    raise ValueError(f"Unsupported feature contract: {feature_contract}")


def build_dataset_diagnostics(
    df: pd.DataFrame,
    *,
    feature_columns: Sequence[str],
    feature_version: str,
    feature_contract: str,
    contract_statuses: Sequence[str] | None,
    contract_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    seasons = sorted(int(value) for value in df[DEFAULT_SEASON_COLUMN].dropna().astype(int).unique().tolist())
    status_counts = {
        str(status): int(count)
        for status, count in df["source_contract_status"].fillna("missing").value_counts().sort_index().items()
    }
    feature_null_counts = {
        column: int(df[column].isna().sum())
        for column in feature_columns
        if int(df[column].isna().sum()) > 0
    }
    return {
        "n_rows": int(len(df)),
        "n_games": int(df[DEFAULT_ID_COLUMN].nunique()),
        "feature_version": feature_version,
        "feature_contract": feature_contract,
        "seasons": seasons,
        "allowed_contract_statuses": list(contract_statuses or []),
        "contract_status_counts": status_counts,
        "date_range": {
            "start": str(df[DEFAULT_DATE_COLUMN].min()),
            "end": str(df[DEFAULT_DATE_COLUMN].max()),
        },
        "as_of_range": {
            "start": str(df[DEFAULT_AS_OF_COLUMN].min()),
            "end": str(df[DEFAULT_AS_OF_COLUMN].max()),
        },
        "label_distribution": {
            "home_win_rate": float(df[DEFAULT_LABEL_COLUMN].astype(float).mean()),
            "positive_labels": int((df[DEFAULT_LABEL_COLUMN].astype(int) == 1).sum()),
            "negative_labels": int((df[DEFAULT_LABEL_COLUMN].astype(int) == 0).sum()),
        },
        "feature_columns": list(feature_columns),
        "feature_null_counts": feature_null_counts,
        "contract_diagnostics": dict(contract_diagnostics or {}),
    }


def resolve_feature_exclusions(
    df: pd.DataFrame,
    *,
    exclude_feature_groups: Sequence[str] | None = None,
    exclude_feature_columns: Sequence[str] | None = None,
) -> tuple[list[str], dict[str, Any]]:
    requested_groups = [str(group) for group in (exclude_feature_groups or ())]
    requested_columns = [str(column) for column in (exclude_feature_columns or ())]

    unknown_groups = sorted(set(requested_groups) - set(FEATURE_ABLATION_GROUPS))
    if unknown_groups:
        raise ValueError(
            f"Unsupported feature ablation groups: {unknown_groups}. "
            f"Supported groups: {sorted(FEATURE_ABLATION_GROUPS)}"
        )

    group_resolved = {
        group: sorted(column for column in FEATURE_ABLATION_GROUPS[group] if column in df.columns)
        for group in requested_groups
    }
    explicit_existing = sorted(column for column in requested_columns if column in df.columns)
    explicit_missing = sorted(column for column in requested_columns if column not in df.columns)
    excluded_columns = sorted(
        set(explicit_existing).union(*(set(columns) for columns in group_resolved.values()))
    )
    return excluded_columns, {
        "requested_feature_groups": requested_groups,
        "resolved_feature_groups": group_resolved,
        "requested_feature_columns": requested_columns,
        "resolved_feature_columns": explicit_existing,
        "missing_requested_feature_columns": explicit_missing,
    }


def apply_feature_exclusions(
    df: pd.DataFrame,
    *,
    exclude_feature_groups: Sequence[str] | None = None,
    exclude_feature_columns: Sequence[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    excluded_columns, diagnostics = resolve_feature_exclusions(
        df,
        exclude_feature_groups=exclude_feature_groups,
        exclude_feature_columns=exclude_feature_columns,
    )
    if not excluded_columns:
        diagnostics["excluded_feature_columns"] = []
        return df, diagnostics
    remaining = df.drop(columns=excluded_columns)
    diagnostics["excluded_feature_columns"] = excluded_columns
    return remaining, diagnostics


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
    forbidden_columns = [
        column for column in feature_columns if any(token in column.lower() for token in FORBIDDEN_FEATURE_TOKENS)
    ]
    if forbidden_columns:
        raise ValueError(f"Forbidden sportsbook-related features found in training data: {sorted(forbidden_columns)}")
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
    feature_contract: str = DEFAULT_FEATURE_CONTRACT,
    exclude_feature_groups: Sequence[str] | None = None,
    exclude_feature_columns: Sequence[str] | None = None,
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
          fr.game_id AS {DEFAULT_ID_COLUMN},
          g.season AS {DEFAULT_SEASON_COLUMN},
          g.game_date AS {DEFAULT_DATE_COLUMN},
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
    _validate_dataset(df, feature_version=feature_version)
    df, contract_diagnostics = apply_feature_contract(df, feature_contract=feature_contract)
    df, exclusion_diagnostics = apply_feature_exclusions(
        df,
        exclude_feature_groups=exclude_feature_groups,
        exclude_feature_columns=exclude_feature_columns,
    )
    feature_columns = infer_feature_columns(df, explicit_feature_columns=explicit_feature_columns)
    contract_diagnostics = dict(contract_diagnostics)
    contract_diagnostics["feature_exclusions"] = exclusion_diagnostics
    diagnostics = build_dataset_diagnostics(
        df,
        feature_columns=feature_columns,
        feature_version=feature_version,
        feature_contract=feature_contract,
        contract_statuses=contract_statuses,
        contract_diagnostics=contract_diagnostics,
    )
    return DatasetBundle(dataframe=df, feature_columns=feature_columns, diagnostics=diagnostics)
