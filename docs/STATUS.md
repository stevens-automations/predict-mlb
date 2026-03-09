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
- Added historical ingestion CLI: `scripts/history_ingest.py` with subcommands:
  - `init-db`
  - `backfill` (bounded statsapi schedule ingest)
  - `incremental` (bounded one-day schedule ingest)
  - `dq`
- Added canonical historical schema SQL at `scripts/sql/history_schema.sql`.
- Added run/checkpoint ledger in DB (`ingestion_runs`, `ingestion_checkpoints`) with periodic/final checkpoint updates.
- Added idempotent upsert helpers for `games` + `labels` (`did_home_win`, `run_differential`, `total_runs` for final games).
- Added mocked tests for bounded backfill/incremental ingest behavior and idempotent upserts.

## Newly Aligned Direction (encoded)

- Canonical historical store is SQLite at `data/mlb_history.db`.
- Backfill scope target is seasons `2020–2025`.
- Historical odds backfill is out-of-scope (odds are forward-only capture during season).
- Data policy is strict contracts with degraded fallback predictions (no silent game skipping).
- Ingestion reliability posture: bounded retries/backoff, request budget, checkpoint resume.
- Incremental cadence starts with **pre-game + post-game** only.
- Primary model metric starts with **log loss**.

## Known Constraints / Open Gaps

1. Historical training dataset is not yet materialized in `data/mlb_history.db`.
2. Contract evaluators and DQ checks remain minimal placeholders.
3. Odds snapshot table exists but should remain forward-only until explicit policy change.

## Non-Goals (current phase)

- No historical odds backfill.
- No rushed promotion to `main` before staging validation and acceptance gates are complete.
