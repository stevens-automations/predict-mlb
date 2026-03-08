# SQLite Cutover Runbook

## Status
SQLite is now the runtime source of truth for prediction state.

## Runtime behavior
- `predict.py` now reads/writes prediction state via `storage.SQLitePredictionStorage`.
- `server/prep_tweet.py` odds refresh path now updates SQLite (no Excel writes in active runtime).
- Tweet sent state (`tweeted?`) is persisted in SQLite.

## Auto-bootstrap migration rules
On startup (`check_and_predict`):
1. Ensure SQLite schema exists (`ensure_predictions_schema`).
2. If DB has rows: continue on SQLite.
3. If DB is empty/missing and Excel file exists at `DATA_SHEET_PATH`: import Excel once into SQLite.
4. If DB is empty and Excel is missing: continue with empty SQLite DB.
5. Bootstrap is idempotent; once rows exist in DB, Excel is ignored for runtime state.

## Required config
- `SQLITE_DB_PATH` (preferred) or `PREDICTIONS_DB_PATH` must be set.
- Runtime validation now fails fast if DB path is not configured or schema init fails.

## Rollback guidance
If emergency rollback to Excel is needed:
1. Stop schedulers/jobs.
2. Export from SQLite to Excel (manual script or ad-hoc tool; keep as operational utility only).
3. Revert to prior commit before this cutover.
4. Re-enable Excel path env/config in that prior revision.

## Safety and consistency notes
- Writes use SQLite upsert (`ON CONFLICT(game_id,date,model) DO UPDATE`) for idempotency.
- Full-table replacement writes are transactional (`DELETE` + upsert in same storage workflow).
- Failures are logged with warning lines; no silent row drops.
