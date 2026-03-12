#!/usr/bin/env python3
"""Read-only parity check between Excel and SQLite prediction stores."""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import get_env_path, get_predictions_db_path
from sqlite_phase1 import check_excel_sqlite_parity


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--excel", default=get_env_path("DATA_SHEET_PATH", "data/predictions.xlsx"))
    parser.add_argument("--db", default=get_predictions_db_path())
    parser.add_argument(
        "--key-fields",
        default="game_id,date,home,away,model",
        help="comma-separated key fields for set parity",
    )
    parser.add_argument("--strict", action="store_true", help="exit 1 on any mismatch")
    args = parser.parse_args()

    keys = [k.strip() for k in args.key_fields.split(",") if k.strip()]
    result = check_excel_sqlite_parity(args.excel, args.db, key_fields=keys)

    print(f"Excel rows:  {result.excel_row_count}")
    print(f"SQLite rows: {result.sqlite_row_count}")
    print(f"Key fields:  {', '.join(result.key_fields)}")
    print(f"Excel keys:  {result.excel_key_count}")
    print(f"SQLite keys: {result.sqlite_key_count}")
    print(f"Missing in SQLite: {result.missing_in_sqlite}")
    print(f"Missing in Excel:  {result.missing_in_excel}")
    print(f"PARITY_OK={result.matches}")

    if args.strict and not result.matches:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
