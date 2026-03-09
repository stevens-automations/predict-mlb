#!/usr/bin/env python3
"""Phase 2 QA validation for season-partitioned historical ingestion outputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "mlb_history.db"


def default_report_path(season: int) -> Path:
    return ROOT / "docs" / "reports" / f"phase2-validation-{season}.md"

TEAM_FIELDS = ["runs", "hits", "errors", "batting_avg", "obp", "slg", "ops", "strikeouts", "walks"]
PITCHER_FIELDS = [
    "pitcher_id",
    "probable_pitcher_id",
    "probable_pitcher_known",
    "season_era",
    "season_whip",
    "season_avg_allowed",
    "season_runs_per_9",
    "season_strike_pct",
    "season_win_pct",
    "career_era",
    "season_stats_scope",
    "season_stats_leakage_risk",
]

TEAM_RANGES: dict[str, tuple[float | None, float | None]] = {
    "runs": (0, 40),
    "hits": (0, 40),
    "errors": (0, 10),
    "batting_avg": (0.0, 1.0),
    "obp": (0.0, 1.0),
    "slg": (0.0, 4.0),
    "ops": (0.0, 5.0),
    "strikeouts": (0, 30),
    "walks": (0, 30),
}

PITCHER_RANGES: dict[str, tuple[float | None, float | None]] = {
    "pitcher_id": (1, None),
    "probable_pitcher_id": (1, None),
    "probable_pitcher_known": (0, 1),
    # Tiny season-to-date samples can legitimately spike after one disastrous outing,
    # but still stay well below the obviously broken decimal-innings-style values.
    "season_era": (0.0, 150.0),
    "season_whip": (0.0, 25.0),
    "season_avg_allowed": (0.0, 1.0),
    "season_runs_per_9": (0.0, 150.0),
    "season_strike_pct": (0.0, 1.0),
    "season_win_pct": (0.0, 1.0),
    "career_era": (0.0, 50.0),
    "season_stats_leakage_risk": (0, 1),
}


@dataclass
class CheckResult:
    name: str
    status: str
    summary: str
    details: dict[str, Any]


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def season_games_count(conn: sqlite3.Connection, season: int) -> int:
    return int(conn.execute("SELECT COUNT(*) AS c FROM games WHERE season = ?", (season,)).fetchone()["c"])


def completed_games_count(conn: sqlite3.Connection, season: int) -> int:
    placeholders = ", ".join("?" for _ in sorted(("Completed Early", "Final", "Game Over")))
    return int(
        conn.execute(
            f"SELECT COUNT(*) AS c FROM games WHERE season = ? AND status IN ({placeholders})",
            (season, "Completed Early", "Final", "Game Over"),
        ).fetchone()["c"]
    )


def table_digest_for_season(conn: sqlite3.Connection, table: str, season: int) -> str:
    cols = [row["name"] for row in conn.execute(f"PRAGMA table_info({table})")]
    stable_cols = [col for col in cols if col not in {"ingested_at", "source_updated_at"}]
    order_parts = ["t.game_id"]
    if "side" in cols:
        order_parts.append("COALESCE(t.side, '')")
    if "team_id" in cols:
        order_parts.append("COALESCE(t.team_id, -1)")
    if "feature_version" in cols:
        order_parts.append("COALESCE(t.feature_version, '')")
    if "as_of_ts" in cols:
        order_parts.append("COALESCE(t.as_of_ts, '')")
    order_sql = ", ".join(order_parts)
    select_sql = ", ".join(f"t.{col}" for col in stable_cols) if stable_cols else "t.game_id"
    cursor = conn.execute(
        f"""
        SELECT {select_sql}
        FROM {table} t
        INNER JOIN games g ON g.game_id = t.game_id
        WHERE g.season = ?
        ORDER BY {order_sql}
        """,
        (season,),
    )
    h = hashlib.sha256()
    for row in cursor.fetchall():
        h.update(repr(tuple(row)).encode("utf-8"))
    return h.hexdigest()


def coverage_check(conn: sqlite3.Connection, season: int) -> CheckResult:
    games = season_games_count(conn, season)
    completed_games = completed_games_count(conn, season)
    team_rows = int(
        conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM game_team_stats t
            INNER JOIN games g ON g.game_id = t.game_id
            WHERE g.season = ?
            """,
            (season,),
        ).fetchone()["c"]
    )
    pitcher_rows = int(
        conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM game_pitcher_context p
            INNER JOIN games g ON g.game_id = p.game_id
            WHERE g.season = ?
            """,
            (season,),
        ).fetchone()["c"]
    )
    feature_rows = int(
        conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM feature_rows f
            INNER JOIN games g ON g.game_id = f.game_id
            WHERE g.season = ? AND f.feature_version = 'v1'
            """,
            (season,),
        ).fetchone()["c"]
    )

    expected_team_rows = completed_games * 2
    expected_pitcher_rows = games * 2
    expected_feature_rows = games
    team_cov = (team_rows / expected_team_rows) if expected_team_rows else 0.0
    pitcher_cov = (pitcher_rows / expected_pitcher_rows) if expected_pitcher_rows else 0.0
    feature_cov = (feature_rows / expected_feature_rows) if expected_feature_rows else 0.0
    ok = (
        games > 0
        and team_rows == expected_team_rows
        and pitcher_rows == expected_pitcher_rows
        and feature_rows == expected_feature_rows
    )
    status = "PASS" if ok else "FAIL"
    return CheckResult(
        name=f"Row coverage vs {season} games",
        status=status,
        summary=(
            f"games={games}, completed_games={completed_games}, "
            f"game_team_stats={team_rows}/{expected_team_rows} ({team_cov:.1%}), "
            f"game_pitcher_context={pitcher_rows}/{expected_pitcher_rows} ({pitcher_cov:.1%}), "
            f"feature_rows(v1)={feature_rows}/{expected_feature_rows} ({feature_cov:.1%})"
        ),
        details={
            "season": season,
            "games": games,
            "completed_games": completed_games,
            "expected_game_team_stats_rows": expected_team_rows,
            "expected_game_pitcher_context_rows": expected_pitcher_rows,
            "expected_feature_rows": expected_feature_rows,
            "game_team_stats_rows": team_rows,
            "game_pitcher_context_rows": pitcher_rows,
            "feature_rows": feature_rows,
            "game_team_stats_coverage": team_cov,
            "game_pitcher_context_coverage": pitcher_cov,
            "feature_rows_coverage": feature_cov,
        },
    )


def missingness_for_table(
    conn: sqlite3.Connection, table: str, fields: list[str], season: int
) -> tuple[int, dict[str, dict[str, float | int | bool]], list[str]]:
    total = int(
        conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM {table} t
            INNER JOIN games g ON g.game_id = t.game_id
            WHERE g.season = ?
            """,
            (season,),
        ).fetchone()["c"]
    )
    available = {row['name'] for row in conn.execute(f"PRAGMA table_info({table})")}
    metrics: dict[str, dict[str, float | int | bool]] = {}
    missing_columns: list[str] = []
    for field in fields:
        if field not in available:
            metrics[field] = {"null_count": total, "null_pct": 1.0, "column_missing": True}
            missing_columns.append(field)
            continue
        null_count = int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS c
                FROM {table} t
                INNER JOIN games g ON g.game_id = t.game_id
                WHERE g.season = ? AND t.{field} IS NULL
                """,
                (season,),
            ).fetchone()["c"]
        )
        pct = (null_count / total) if total else 1.0
        metrics[field] = {"null_count": null_count, "null_pct": pct, "column_missing": False}
    return total, metrics, missing_columns


def missingness_check(conn: sqlite3.Connection, season: int) -> CheckResult:
    team_total, team, team_missing_cols = missingness_for_table(conn, "game_team_stats", TEAM_FIELDS, season)
    pitcher_total, pitcher, pitcher_missing_cols = missingness_for_table(
        conn, "game_pitcher_context", PITCHER_FIELDS, season
    )

    if team_total == 0 and pitcher_total == 0:
        status = "FAIL"
        summary = "No rows to evaluate missingness for either table"
    else:
        worst = 0.0
        for block in (team, pitcher):
            for payload in block.values():
                worst = max(worst, float(payload["null_pct"]))
        if team_missing_cols or pitcher_missing_cols:
            status = "FAIL"
        else:
            status = "PASS" if worst <= 0.20 else "WARN"
        summary = (
            f"row_counts(team={team_total}, pitcher={pitcher_total}), worst_null_pct={worst:.1%}, "
            f"missing_columns={team_missing_cols + pitcher_missing_cols}"
        )

    return CheckResult(
        name="Missingness per key feature field",
        status=status,
        summary=summary,
        details={
            "game_team_stats": team,
            "game_pitcher_context": pitcher,
            "missing_columns": {"game_team_stats": team_missing_cols, "game_pitcher_context": pitcher_missing_cols},
        },
    )


def sanity_for_table(
    conn: sqlite3.Connection, table: str, ranges: dict[str, tuple[float | None, float | None]], season: int
) -> tuple[dict[str, dict[str, int | bool]], list[str]]:
    out: dict[str, dict[str, int | bool]] = {}
    available = {row['name'] for row in conn.execute(f"PRAGMA table_info({table})")}
    missing_columns: list[str] = []
    for field, (min_v, max_v) in ranges.items():
        if field not in available:
            out[field] = {"non_null_count": 0, "out_of_range_count": 0, "column_missing": True}
            missing_columns.append(field)
            continue
        predicates = [f"t.{field} IS NOT NULL"]
        params: list[Any] = [season]
        if min_v is not None:
            predicates.append(f"t.{field} >= ?")
            params.append(min_v)
        if max_v is not None:
            predicates.append(f"t.{field} <= ?")
            params.append(max_v)
        in_range_sql = " AND ".join(predicates)

        total_non_null = int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS c
                FROM {table} t
                INNER JOIN games g ON g.game_id = t.game_id
                WHERE g.season = ? AND t.{field} IS NOT NULL
                """,
                (season,),
            ).fetchone()["c"]
        )
        in_range = int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS c
                FROM {table} t
                INNER JOIN games g ON g.game_id = t.game_id
                WHERE g.season = ? AND {in_range_sql}
                """,
                params,
            ).fetchone()["c"]
        )
        out[field] = {
            "non_null_count": total_non_null,
            "out_of_range_count": max(0, total_non_null - in_range),
            "column_missing": False,
        }
    return out, missing_columns


def sanity_ranges_check(conn: sqlite3.Connection, season: int) -> CheckResult:
    team, team_missing_cols = sanity_for_table(conn, "game_team_stats", TEAM_RANGES, season)
    pitcher, pitcher_missing_cols = sanity_for_table(conn, "game_pitcher_context", PITCHER_RANGES, season)

    out_of_range = sum(int(v["out_of_range_count"]) for v in team.values()) + sum(
        int(v["out_of_range_count"]) for v in pitcher.values()
    )
    has_any = sum(int(v["non_null_count"]) for v in team.values()) + sum(int(v["non_null_count"]) for v in pitcher.values())
    if has_any == 0:
        status = "FAIL"
        summary = "No non-null numeric values present for sanity evaluation"
    elif team_missing_cols or pitcher_missing_cols:
        status = "FAIL"
        summary = f"schema missing expected numeric fields: {team_missing_cols + pitcher_missing_cols}"
    else:
        status = "PASS" if out_of_range == 0 else "FAIL"
        summary = f"total_out_of_range={out_of_range}"

    return CheckResult(
        name="Sanity ranges for major numeric fields",
        status=status,
        summary=summary,
        details={
            "game_team_stats": team,
            "game_pitcher_context": pitcher,
            "missing_columns": {"game_team_stats": team_missing_cols, "game_pitcher_context": pitcher_missing_cols},
        },
    )


def observability_consistency_check(conn: sqlite3.Connection, season: int) -> CheckResult:
    partition = f"season={season}"
    run = conn.execute(
        """
        SELECT run_id, note
        FROM ingestion_runs
        WHERE mode = 'backfill' AND status = 'success' AND partition_key = ?
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (partition,),
    ).fetchone()
    checkpoint = conn.execute(
        """
        SELECT cursor_json, status, attempts
        FROM ingestion_checkpoints
        WHERE job_name = 'backfill' AND partition_key = ?
        """,
        (partition,),
    ).fetchone()

    if run is None or checkpoint is None:
        return CheckResult(
            name="Checkpoint/run observability consistency",
            status="FAIL",
            summary=f"Missing backfill run or checkpoint for season={season}",
            details={"run_found": run is not None, "checkpoint_found": checkpoint is not None},
        )

    try:
        note = json.loads(run["note"] or "{}")
    except json.JSONDecodeError:
        note = {}
    try:
        cursor = json.loads(checkpoint["cursor_json"] or "{}")
    except json.JSONDecodeError:
        cursor = {}

    keys = [
        "schedule_rows_fetched",
        "relevant_rows_processed",
        "distinct_games_touched",
        "games_inserted",
        "games_updated",
        "labels_inserted",
        "labels_updated",
    ]
    mismatches = {k: {"run": note.get(k), "checkpoint": cursor.get(k)} for k in keys if note.get(k) != cursor.get(k)}

    status = "PASS" if not mismatches and checkpoint["status"] == "success" else "FAIL"
    summary = (
        f"checkpoint_status={checkpoint['status']}, attempts={checkpoint['attempts']}, "
        f"mismatch_keys={list(mismatches.keys())}"
    )
    return CheckResult(
        name="Checkpoint/run observability consistency",
        status=status,
        summary=summary,
        details={"mismatches": mismatches, "run_note": note, "checkpoint_cursor": cursor},
    )


def pitcher_provenance_check(conn: sqlite3.Connection, season: int) -> CheckResult:
    totals = conn.execute(
        """
        SELECT
          COUNT(*) AS total_rows,
          SUM(CASE WHEN probable_pitcher_known = 1 AND season_stats_scope = 'season_to_date_prior_completed_games' THEN 1 ELSE 0 END) AS safe_scope_rows,
          SUM(CASE WHEN season_stats_leakage_risk = 0 THEN 1 ELSE 0 END) AS non_leaking_rows
        FROM game_pitcher_context p
        INNER JOIN games g ON g.game_id = p.game_id
        WHERE g.season = ?
        """,
        (season,),
    ).fetchone()
    total_rows = int(totals["total_rows"] or 0)
    safe_scope_rows = int(totals["safe_scope_rows"] or 0)
    non_leaking_rows = int(totals["non_leaking_rows"] or 0)
    ok = total_rows > 0 and total_rows == non_leaking_rows and safe_scope_rows <= total_rows
    return CheckResult(
        name="Pitcher provenance is parity-safe",
        status="PASS" if ok else "FAIL",
        summary=(
            f"rows={total_rows}, non_leaking_rows={non_leaking_rows}, "
            f"safe_scope_rows_for_known_pitchers={safe_scope_rows}"
        ),
        details={
            "total_rows": total_rows,
            "non_leaking_rows": non_leaking_rows,
            "safe_scope_rows_for_known_pitchers": safe_scope_rows,
        },
    )


def idempotency_check(conn: sqlite3.Connection, season: int, rerun_cmd: str | None) -> CheckResult:
    duplicates = {}
    for table, keys in [
        ("game_team_stats", "game_id, team_id"),
        ("game_pitcher_context", "game_id, side"),
        ("feature_rows", "game_id, feature_version, as_of_ts"),
    ]:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM (
              SELECT {keys}, COUNT(*) n
              FROM {table}
              GROUP BY {keys}
              HAVING n > 1
            )
            """
        ).fetchone()
        duplicates[table] = int(row["c"])

    before = {
        "game_team_stats": table_digest_for_season(conn, "game_team_stats", season),
        "game_pitcher_context": table_digest_for_season(conn, "game_pitcher_context", season),
        "feature_rows": table_digest_for_season(conn, "feature_rows", season),
    }

    rerun_executed = False
    rerun_exit_code: int | None = None
    rerun_stdout = ""
    rerun_stderr = ""

    if rerun_cmd:
        rerun_executed = True
        proc = subprocess.run(shlex.split(rerun_cmd), cwd=str(ROOT), capture_output=True, text=True)
        rerun_exit_code = proc.returncode
        rerun_stdout = (proc.stdout or "")[-4000:]
        rerun_stderr = (proc.stderr or "")[-4000:]

    after = {
        "game_team_stats": table_digest_for_season(conn, "game_team_stats", season),
        "game_pitcher_context": table_digest_for_season(conn, "game_pitcher_context", season),
        "feature_rows": table_digest_for_season(conn, "feature_rows", season),
    }
    changed = {table: before[table] != after[table] for table in before}

    ok = all(v == 0 for v in duplicates.values()) and all(not v for v in changed.values())
    if rerun_executed and rerun_exit_code not in (0, None):
        status = "FAIL"
        summary = f"rerun command failed (exit={rerun_exit_code})"
    else:
        status = "PASS" if ok else "FAIL"
        summary = f"duplicates={duplicates}, content_changed={changed}"

    return CheckResult(
        name="Idempotency checks after rerun",
        status=status,
        summary=summary,
        details={
            "duplicates": duplicates,
            "digests_before": before,
            "digests_after": after,
            "content_changed": changed,
            "rerun_executed": rerun_executed,
            "rerun_exit_code": rerun_exit_code,
            "rerun_stdout_tail": rerun_stdout,
            "rerun_stderr_tail": rerun_stderr,
        },
    )


def render_markdown(results: list[CheckResult], season: int, db_path: Path) -> str:
    ts = datetime.now().isoformat(timespec="seconds")
    overall = overall_status(results)
    blockers = [f"{r.name}: {r.summary}" for r in results if r.status == "FAIL"]

    lines = [
        f"# Phase 2 Validation Report ({season})",
        "",
        f"- Generated: `{ts}`",
        f"- DB: `{db_path}`",
        f"- Overall: **{overall}**",
        "",
        "## Check Results",
    ]
    for r in results:
        lines += [
            "",
            f"### {r.name}",
            f"- Status: **{r.status}**",
            f"- Summary: {r.summary}",
            "- Details:",
            "```json",
            json.dumps(r.details, indent=2, sort_keys=True),
            "```",
        ]

    lines += ["", "## Blockers", ""]
    if blockers:
        lines += [f"- {b}" for b in blockers]
    else:
        lines.append("- None")

    lines += ["", f"## Recommendation for season {season}", ""]
    lines.append("- **GO**" if overall == "PASS" else "- **NO-GO** until blockers above are resolved.")
    lines.append("")
    return "\n".join(lines)


def run_validation(db_path: Path, season: int, rerun_cmd: str | None) -> list[CheckResult]:
    with connect(db_path) as conn:
        return [
            coverage_check(conn, season),
            missingness_check(conn, season),
            pitcher_provenance_check(conn, season),
            idempotency_check(conn, season, rerun_cmd),
            sanity_ranges_check(conn, season),
            observability_consistency_check(conn, season),
        ]


def overall_status(results: list[CheckResult]) -> str:
    return "PASS" if all(r.status != "FAIL" for r in results) else "FAIL"


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Phase 2 historical outputs for a target season")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to mlb_history.db")
    parser.add_argument("--season", type=int, default=2020, help="Season partition (default: 2020)")
    parser.add_argument("--output", default=None, help="Markdown report output path")
    parser.add_argument(
        "--rerun-cmd",
        default=None,
        help="Optional command to execute between snapshots for idempotency verification",
    )
    parser.add_argument("--json", action="store_true", help="Also print machine-readable JSON summary")
    args = parser.parse_args()

    db_path = Path(args.db)
    output = Path(args.output) if args.output else default_report_path(args.season)
    output.parent.mkdir(parents=True, exist_ok=True)

    results = run_validation(db_path, args.season, args.rerun_cmd)
    markdown = render_markdown(results, args.season, db_path)
    output.write_text(markdown, encoding="utf-8")

    payload = {
        "report_path": str(output),
        "overall": overall_status(results),
        "checks": [{"name": r.name, "status": r.status, "summary": r.summary} for r in results],
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"Wrote report: {output}")
        print(f"Overall: {payload['overall']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
