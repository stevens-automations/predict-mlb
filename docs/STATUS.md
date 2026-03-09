# Project Status (Current)

Last updated: 2026-03-09
Branch intent: `staging/preseason-consolidated` is the integration branch; `main` remains unchanged until explicit promotion.

## Implemented

### Reliability / Ops
- Retry + backoff + timeout + circuit-breaker behavior for posting and odds retrieval.
- Stale odds cache fallback for transient API failures.
- Runtime guardrail warnings and structured logging coverage.

### Storage
- SQLite-first runtime storage path for prediction outputs.
- Transactional replace/upsert safety and rollback behavior covered by tests.
- SQLite healthcheck utility and operating runbook available.

### Historical Ingestion Foundation (new)
- Added scaffold CLI: `scripts/history_ingest.py` with subcommands:
  - `init-db`
  - `backfill` (safe stub)
  - `incremental` (safe stub)
  - `dq`
- Added canonical historical schema SQL at `scripts/sql/history_schema.sql`.
- Added run/checkpoint ledger skeleton in DB (`ingestion_runs`, `ingestion_checkpoints`).
- Added idempotent upsert helpers scaffold (`games`, checkpoints).
- Added quick tests for schema initialization + checkpoint idempotency + game upsert behavior.

## Newly Aligned Direction (encoded)

- Canonical historical store is SQLite at `data/mlb_history.db`.
- Backfill scope target is seasons `2020–2025`.
- Historical odds backfill is out-of-scope (odds are forward-only capture during season).
- Data policy is strict contracts with degraded fallback predictions (no silent game skipping).
- Ingestion reliability posture: bounded retries/backoff, request budget, checkpoint resume.
- Incremental cadence starts with **pre-game + post-game** only.
- Primary model metric starts with **log loss**.

## Known Constraints / Open Gaps

1. `backfill` and `incremental` are intentionally safe stubs (no full historical pull yet).
2. Historical training dataset is not yet materialized in `data/mlb_history.db`.
3. Full statsapi fetch loop, contract evaluators, and DQ checks are scaffolded but not fully implemented.
4. Odds snapshot table exists but should remain forward-only until explicit policy change.

## Non-Goals (current phase)

- No long-running historical ingestion/backfill execution during scaffold phase.
- No historical odds backfill.
- No rushed promotion to `main` before staging validation and acceptance gates are complete.
