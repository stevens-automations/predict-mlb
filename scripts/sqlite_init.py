#!/usr/bin/env python3
"""Initialize local SQLite schema for predictions (phase 1)."""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paths import get_predictions_db_path
from sqlite_phase1 import ensure_predictions_schema


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=get_predictions_db_path())
    args = parser.parse_args()

    ensure_predictions_schema(args.db)
    print(f"Initialized SQLite schema at: {args.db}")


if __name__ == "__main__":
    main()
