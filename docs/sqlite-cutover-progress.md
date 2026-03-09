# SQLite Cutover Progress

## Completed
- [x] SQLite-first storage abstraction implemented (`storage.py`).
- [x] Runtime pipeline switched to SQLite reads/writes (`predict.py`).
- [x] Tweet prep odds update path switched to SQLite (`server/prep_tweet.py`).
- [x] One-time auto-bootstrap from Excel when DB is empty/missing.
- [x] Runtime validation updated to enforce DB path + schema readiness.
- [x] Idempotent upsert strategy in place for prediction writes.
- [x] Test coverage added for bootstrap and idempotent upsert flows.

## Validation executed
- Unit tests (targeted):
  - `tests/test_sqlite_phase1.py`
  - `tests/test_sqlite_phase2_shadow.py`
  - `tests/test_guardrails_and_scheduling.py`

## Remaining/known follow-ups
- Optional utility script for explicit SQLite->Excel export can be added if operations want a one-command rollback artifact.
- Notebook/docs that still mention Excel as runtime source should be cleaned up in a follow-on doc pass.
