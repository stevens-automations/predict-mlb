#!/usr/bin/env python3
"""Import existing Excel predictions sheet into SQLite (phase 1 one-way import)."""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import get_env_path, get_predictions_db_path
from sqlite_phase1 import import_excel_to_sqlite


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--excel", default=get_env_path("DATA_SHEET_PATH", "data/predictions.xlsx"))
    parser.add_argument("--db", default=get_predictions_db_path())
    parser.add_argument("--append", action="store_true", help="Append rows instead of replacing table contents")
    args = parser.parse_args()

    result = import_excel_to_sqlite(args.excel, args.db, replace=not args.append)
    print(f"Imported {result.imported_rows} rows from {args.excel}")
    print(f"SQLite table total rows: {result.sqlite_total_rows}")


if __name__ == "__main__":
    main()
