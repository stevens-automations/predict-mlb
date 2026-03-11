# SQLite Cutover Runbook

## Status
SQLite is now the runtime source of truth for prediction state.

## Runtime behavior
- `predict.py` now reads/writes prediction state via `storage.SQLitePredictionStorage`.
- `server/prep_tweet.py` odds refresh path now updates SQLite (no Excel writes in active runtime).
- Tweet sent state (`tweeted?`) is persisted in SQLite.

## Runtime storage rules (SQLite-only)
On startup (`check_and_predict`):
1. Ensure SQLite schema exists (`ensure_predictions_schema`).
2. Read/write prediction state from SQLite only.
3. No runtime Excel bootstrap is executed.
4. Excel import/export remains available only via explicit maintenance scripts/docs.

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
- `replace_predictions` now runs in an explicit SQL transaction (`BEGIN`/`COMMIT` + rollback on error).
- If any row fails during replace, the transaction is fully rolled back (no partial state).
- Failures are logged with warning lines; no silent row drops.

## Healthcheck
Run:

```bash
.venv/bin/python scripts/sqlite_healthcheck.py
```

Reports:
- total predictions rows
- pending unsent tweets count
- rows with null accuracy
- recent date coverage summary
