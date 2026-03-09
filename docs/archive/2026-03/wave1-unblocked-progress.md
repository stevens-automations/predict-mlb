# Wave1 Unblocked Progress

Date: 2026-03-08
Branch: `feat/wave1-reliability-slice`

## What Changed

### A) Testing expansion
Added `tests/test_guardrails_and_scheduling.py` with coverage for:
- `server/prep_tweet.prepare` guardrails:
  - missing `game_id` column in dataframe returns fallback tweet line (no crash)
  - missing matching `game_id` row/index lookup returns fallback tweet line (no crash)
- `predict.py` scheduling dedupe/idempotency behavior:
  - bullet normalization + dedupe via `unique_tweet_lines`
  - repeated scheduling of same tweet body avoids duplicate job creation

Notes:
- Predict-module tests are loaded with stubbed module dependencies to avoid importing `lightgbm`.
- Tests run successfully in project venv.

### B) Observability improvements (behavior-preserving)
Updated `predict.py`:
- Added lightweight structured logger helper:
  - `log_event(stage, result, game_id=None, run_id=None)`
  - payload includes `run_id`, `stage`, `game_id`, `result`
- Added run correlation id:
  - `current_run_id` generated in `check_and_predict()` via UUID
- Integrated structured logs at key orchestration points:
  - pipeline start/completion (`check_and_predict`)
  - past-results load errors
  - prediction generation start/completion
  - per-game prediction errors and queued-to-tweet events
  - schedule events: scheduled + skipped duplicate job id

### C) SQLite migration planning artifact
Added `docs/sqlite-migration-plan.md` containing:
- target schema
- Excel -> SQLite data mapping
- phased migration strategy and rollback plan
- code adaptation points/modules
- validation plan
- cutover checklist

### D) Optional storage prep scaffold
Added `storage.py` with a minimal, non-integrated scaffold:
- `PredictionStorage` protocol
- `ExcelPredictionStorage` placeholder implementation
- TODOs for validation, atomic writes, and future SQLite adapter

No runtime behavior switch was introduced.

## Tests Run
Command:
- `.venv/bin/python -m unittest discover -s tests -p 'test*.py' -v`

Result:
- 7 tests passed
- Existing reliability tests still pass

## Next Steps
1. Add optional test for `mark_as_tweeted` guardrails (`tweet`/`tweeted?` missing columns).
2. Add dedicated storage adapter tests before wiring storage abstraction.
3. Implement SQLite importer prototype (offline/script-only) and golden-data parity check.
4. After validation, gate any runtime storage switch behind explicit feature flag.
