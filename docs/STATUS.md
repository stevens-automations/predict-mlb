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

### Simulation / Preseason Testing
- Deterministic simulation mode with fixture-backed game/prediction replay.
- Optional seed-controlled deterministic ordering (`PREDICT_SIM_SEED`).
- Dry-run / no-post behavior for safe rehearsal.

### Prediction Quality Guardrails
- Anomaly warning gating requires rate breach + minimum sample/count thresholds.
- Configurable warning thresholds via env vars.

### Explanation Layer Guardrails
- Structured explanation schema + validation contract.
- Unsupported evidence sources dropped/rejected per allowlist.

### Testing
- Unit/integration-style tests for reliability, simulation mode, guardrails, storage transactions, and healthcheck.
- Current suite passes in project venv (`45 passed`).

## Newly Aligned Direction

- We are **not** trying to replicate legacy Excel outputs as the main objective.
- We are building a robust historical stats foundation from `statsapi` for iterative new-model development.
- Historical source of truth will be local SQLite at `data/mlb_history.db`.
- Initial backfill scope is `2020–2025`, then extend if useful.
- Data policy is strict quality enforcement with **degraded fallback predictions**, incident logging, and root-cause fixes (not silent game skipping).

## Known Constraints / Open Gaps

1. Historical ingestion pipeline (`history_ingest`) is planned but not implemented yet.
2. Historical training dataset is not yet materialized in the new DB.
3. Model experimentation framework is partially scaffolded but still anchored to older assumptions.
4. Data reliability contract tables/rules are not yet codified in code.
5. Circuit-breaker state is process-memory scoped (not persisted across restart).

## Non-Goals (current phase)

- No rushed promotion to `main` before staging validation and acceptance gates are complete.
- No full architecture rewrite before foundational ingestion + experimentation loop is established.
