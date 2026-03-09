from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence


@dataclass(frozen=True)
class SeasonReadiness:
    season: int
    scheduled_games: int
    labeled_games: int
    feature_ready_games: int
    trainable_games: int
    valid_feature_rows: int
    degraded_feature_rows: int


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def build_training_readiness_report(
    *,
    db_path: str | Path,
    required_seasons: Sequence[int],
    feature_version: str,
    allowed_contract_statuses: Sequence[str],
) -> dict[str, Any]:
    required_seasons = [int(season) for season in required_seasons]
    allowed_contract_statuses = list(allowed_contract_statuses)
    placeholders = ",".join("?" for _ in required_seasons)
    status_placeholders = ",".join("?" for _ in allowed_contract_statuses)
    params: list[Any] = [feature_version, *allowed_contract_statuses, *required_seasons]

    query = f"""
        SELECT
          g.season AS season,
          COUNT(DISTINCT g.game_id) AS scheduled_games,
          COUNT(DISTINCT CASE WHEN lb.game_id IS NOT NULL THEN g.game_id END) AS labeled_games,
          COUNT(DISTINCT CASE WHEN fr.game_id IS NOT NULL THEN g.game_id END) AS feature_ready_games,
          COUNT(DISTINCT CASE WHEN fr.game_id IS NOT NULL AND lb.game_id IS NOT NULL THEN g.game_id END) AS trainable_games,
          SUM(CASE WHEN fr.source_contract_status = 'valid' THEN 1 ELSE 0 END) AS valid_feature_rows,
          SUM(CASE WHEN fr.source_contract_status = 'degraded' THEN 1 ELSE 0 END) AS degraded_feature_rows
        FROM games g
        LEFT JOIN labels lb
          ON lb.game_id = g.game_id
        LEFT JOIN feature_rows fr
          ON fr.game_id = g.game_id
         AND fr.feature_version = ?
         AND fr.source_contract_status IN ({status_placeholders})
        WHERE g.season IN ({placeholders})
        GROUP BY g.season
        ORDER BY g.season ASC
    """

    with _connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    by_season = {
        int(row["season"]): SeasonReadiness(
            season=int(row["season"]),
            scheduled_games=int(row["scheduled_games"] or 0),
            labeled_games=int(row["labeled_games"] or 0),
            feature_ready_games=int(row["feature_ready_games"] or 0),
            trainable_games=int(row["trainable_games"] or 0),
            valid_feature_rows=int(row["valid_feature_rows"] or 0),
            degraded_feature_rows=int(row["degraded_feature_rows"] or 0),
        )
        for row in rows
    }

    season_reports = [
        by_season.get(
            season,
            SeasonReadiness(
                season=season,
                scheduled_games=0,
                labeled_games=0,
                feature_ready_games=0,
                trainable_games=0,
                valid_feature_rows=0,
                degraded_feature_rows=0,
            ),
        )
        for season in required_seasons
    ]

    missing_seasons = [item.season for item in season_reports if item.scheduled_games == 0]
    incomplete_seasons = [
        item.season
        for item in season_reports
        if item.scheduled_games > 0 and item.trainable_games < item.labeled_games
    ]
    no_label_seasons = [item.season for item in season_reports if item.scheduled_games > 0 and item.labeled_games == 0]

    ready = not missing_seasons and not incomplete_seasons and not no_label_seasons
    reasons: list[str] = []
    if missing_seasons:
        reasons.append(f"missing required seasons in games table: {missing_seasons}")
    if no_label_seasons:
        reasons.append(f"required seasons have no settled labels yet: {no_label_seasons}")
    if incomplete_seasons:
        reasons.append(f"required seasons still missing feature_rows for labeled games: {incomplete_seasons}")

    total_trainable_games = sum(item.trainable_games for item in season_reports)
    total_feature_rows = sum(item.valid_feature_rows + item.degraded_feature_rows for item in season_reports)
    degraded_feature_rows = sum(item.degraded_feature_rows for item in season_reports)
    degraded_share = float(degraded_feature_rows / total_feature_rows) if total_feature_rows else 0.0

    return {
        "ready": ready,
        "db_path": str(db_path),
        "feature_version": feature_version,
        "required_seasons": required_seasons,
        "allowed_contract_statuses": allowed_contract_statuses,
        "reasons": reasons,
        "totals": {
            "trainable_games": total_trainable_games,
            "feature_rows": total_feature_rows,
            "degraded_feature_rows": degraded_feature_rows,
            "degraded_feature_share": degraded_share,
        },
        "seasons": [asdict(item) for item in season_reports],
    }
