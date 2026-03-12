# Legacy Runtime Scripts

These scripts support the older prediction/tweeting runtime or earlier SQLite migration work. They are retained for traceability and maintenance, but they are not part of the canonical rebuild/training workflow.

Retained legacy utilities:

- `sqlite_init.py`
- `sqlite_healthcheck.py`
- `import_excel_to_sqlite.py`
- `check_excel_sqlite_parity.py`
- `model_refresh.py`

Canonical active script surface remains:

- `scripts/history_ingest.py`
- `scripts/training/`
- `scripts/validate_phase2_2020.py`
