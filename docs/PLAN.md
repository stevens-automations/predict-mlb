# Remaining Work Plan (Reliability-First, New-Model Track)

Last updated: 2026-03-09

## Priority 1 — Activate Historical Ingestion Beyond Scaffold

Current scaffold exists at `scripts/history_ingest.py` and schema at `scripts/sql/history_schema.sql`.

Next implementation step:
- Replace safe stubs in `backfill` / `incremental` with bounded statsapi execution loops.
- Preserve checkpointed resume semantics and request budget controls to minimize statsapi calls.
- Keep odds ingestion forward-only.
- Keep incremental cadence at pre-game + post-game only for v1.

### Acceptance criteria
- Controlled sample partition pull succeeds (one month or one season slice).
- Re-run of same partition is idempotent (stable counts, no duplicate PK rows).
- Checkpoint resume works after forced interruption.

---

## Priority 2 — Backfill Historical Dataset (2020–2025)

Run explicit, approved season-by-season backfill.

### Acceptance criteria
- Seasons 2020–2025 complete or marked blocked with actionable reasons.
- Partition-level run records and checkpoints are complete.
- No silent game skips; degraded fallback reasons are logged.

---

## Priority 3 — Data Reliability Contract + Degraded Prediction Mode

Implement strict input contracts with explicit must-have vs optional fields.
- Must-have failures trigger degraded-mode prediction path (not silent skip).
- Degraded predictions emit reason codes + incident records.
- Recurring failures flow into fix queue.

### Acceptance criteria
- Contract checks run pre-prediction.
- No game silently skipped due to data errors.
- Incident logging/audit trail exists for degraded runs.

---

## Priority 4 — DQ Framework + Experiment Loop

- Expand `dq` from placeholder to full checks (completeness, null thresholds, duplicate guards, freshness).
- Materialize training datasets from `feature_rows` + `labels`.
- Start experiment loop with **log loss** as primary metric.

### Acceptance criteria
- DQ report artifacts generated per run.
- Reproducible baseline + challenger experiments completed.
- Model comparison includes reliability feasibility notes.

---

## Priority 5 — Safe Promotion Gates on Staging

Gate any production promotion on:
- ingestion/data-quality health,
- model quality improvements,
- operational reliability checks,
- rollback readiness.

### Acceptance criteria
- Explicit go/no-go decision documented.
- Staging checklist completed.
- Promotion to `main` only by explicit approval.
