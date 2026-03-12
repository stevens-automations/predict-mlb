#!/usr/bin/env python3
"""Lightweight SQLite healthcheck for prediction runtime state."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import get_predictions_db_path
from sqlite_phase1 import ensure_predictions_schema


def build_report(db_path: str) -> dict:
    ensure_predictions_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        total_predictions_rows = int(conn.execute("SELECT COUNT(*) FROM predictions;").fetchone()[0])
        pending_unsent_tweets_count = int(
            conn.execute("SELECT COUNT(*) FROM predictions WHERE COALESCE(tweeted, 0) = 0;").fetchone()[0]
        )
        rows_with_null_accuracy = int(
            conn.execute("SELECT COUNT(*) FROM predictions WHERE prediction_accuracy IS NULL;").fetchone()[0]
        )
        raw_dates = conn.execute(
            "SELECT date FROM predictions WHERE date IS NOT NULL ORDER BY date DESC LIMIT 200;"
        ).fetchall()

    date_values = [str(row[0])[:10] for row in raw_dates if row and row[0]]
    coverage = Counter(date_values)
    recent_date_coverage = [
        {"date": date, "rows": rows}
        for date, rows in sorted(coverage.items(), reverse=True)[:7]
    ]

    return {
        "db_path": db_path,
        "total_predictions_rows": total_predictions_rows,
        "pending_unsent_tweets_count": pending_unsent_tweets_count,
        "rows_with_null_accuracy": rows_with_null_accuracy,
        "recent_date_coverage": recent_date_coverage,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="SQLite healthcheck for predict-mlb runtime DB")
    parser.add_argument("--db", default=get_predictions_db_path(), help="SQLite DB path")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    args = parser.parse_args()

    report = build_report(args.db)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"DB: {report['db_path']}")
        print(f"total predictions rows: {report['total_predictions_rows']}")
        print(f"pending unsent tweets count: {report['pending_unsent_tweets_count']}")
        print(f"rows with null accuracy: {report['rows_with_null_accuracy']}")
        print("recent date coverage:")
        if not report["recent_date_coverage"]:
            print("  (no dated rows)")
        else:
            for item in report["recent_date_coverage"]:
                print(f"  - {item['date']}: {item['rows']} rows")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
