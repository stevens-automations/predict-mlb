# SQLite Phase 1 Progress

## Completed in this slice
- Added concrete SQLite schema initializer module: `sqlite_phase1.py`
  - table + indexes + unique constraint for `(game_id, date, model)`
  - migration-safe initializer: `ensure_predictions_schema(db_path)`
- Added one-way Excel -> SQLite importer:
  - helper API: `import_excel_to_sqlite(...)`
  - CLI script: `scripts/import_excel_to_sqlite.py`
- Added read-only parity checker:
  - helper API: `check_excel_sqlite_parity(...)`
  - CLI script: `scripts/check_excel_sqlite_parity.py`
- Added initializer CLI script: `scripts/sqlite_init.py`
- Added runbook with command sequence and rollback notes:
  - `docs/sqlite-phase1-runbook.md`
- Added tests for schema creation and import/parity helpers:
  - `tests/test_sqlite_phase1.py`

## Runtime impact
- No runtime switch introduced.
- Existing Excel read/write behavior is unchanged.

## Validation summary
- Unit tests cover:
  - schema + index creation
  - Excel import transformation (`tweeted?` -> integer `tweeted`)
  - parity match and mismatch detection
