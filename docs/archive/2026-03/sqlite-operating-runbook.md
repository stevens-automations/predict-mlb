# SQLite Operating Runbook (Final)

## Scope
This runbook covers day-to-day operation with SQLite as the only runtime storage backend.

## Runtime invariants
- Runtime paths (`predict.py`, `server/prep_tweet.py`) read/write prediction state from SQLite only.
- Runtime no longer auto-imports from Excel.
- `SQLITE_DB_PATH` (or `PREDICTIONS_DB_PATH`) must be set.

## Startup checks
1. Ensure env is loaded and DB path is configured.
2. Initialize/validate schema:
   ```bash
   .venv/bin/python scripts/sqlite_init.py
   ```
3. Run healthcheck:
   ```bash
   .venv/bin/python scripts/sqlite_healthcheck.py
   ```

## Healthcheck metrics
- total predictions rows
- pending unsent tweets count
- rows with null accuracy
- recent date coverage summary

## Optional migration/export tooling (manual only)
These are retained for maintenance use only, not runtime:
- `scripts/import_excel_to_sqlite.py`
- `scripts/check_excel_sqlite_parity.py`

## Rollback notes (emergency)
1. Stop schedulers/jobs.
2. Snapshot current SQLite DB file (`data/predictions.db`).
3. If rollback to legacy Excel runtime is required, revert to a pre-cutover commit.
4. Optionally export/migrate data from SQLite for that older revision.
5. Restart services only after validation in the reverted revision.

## Data consistency guarantees
- Upsert path is idempotent (`ON CONFLICT(game_id,date,model) DO UPDATE`).
- Replace path is all-or-nothing via explicit SQL transaction.
- On replace failure, DB is rolled back to pre-call state.
