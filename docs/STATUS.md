# Project Status (Current)

Last updated: 2026-03-09
Branch intent: `staging/preseason-consolidated` is the integration branch; `main` remains unchanged until explicit promotion.

## Implemented

### Reliability / Ops
- Retry + backoff + timeout + circuit-breaker behavior for posting and odds retrieval.
- Stale odds cache fallback for transient API failures.
- Runtime guardrail warnings and structured logging coverage.

### Storage
- SQLite-first runtime storage path.
- Transactional replace/upsert safety and rollback behavior covered by tests.
- SQLite healthcheck utility and operating runbook available.

### Simulation / Preseason Testing
- Deterministic simulation mode with fixture-backed game/prediction replay.
- Optional seed-controlled deterministic ordering (`PREDICT_SIM_SEED`).
- Dry-run / no-post behavior for safe rehearsal.

### Prediction Quality Guardrails
- Anomaly warning gating now requires both rate breach and minimum sample/count thresholds.
- Configurable warning thresholds via env vars.

### Explanation Layer Guardrails
- Structured explanation schema + validation contract.
- Unsupported evidence sources dropped/rejected per allowlist.

### Testing
- Unit/integration-style tests for reliability, simulation mode, guardrails, storage transactions, and healthcheck.
- Current suite passes in project venv (`45 passed`).

## Known Constraints / Open Gaps

1. Model-refresh scaffold exists, but full retrain/eval requires reliable historical training dataset availability.
2. Circuit-breaker state is process-memory scoped (not persisted across restart).
3. Accuracy upgrades from research memo are not fully implemented yet (recommendations exist; staged execution pending).
4. Need formalized data reliability contract (must-have vs optional inputs, freshness/missingness SLAs).

## Non-Goals (current phase)

- No full modeling stack overhaul.
- No production promotion to `main` until staging validation and acceptance gates are complete.
