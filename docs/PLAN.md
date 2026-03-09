# Remaining Work Plan (Reliability-First, New-Model Track)

Last updated: 2026-03-09

## Priority 1 — Complete Multi-Season Historical Training Readiness

Current implemented base exists at `scripts/history_ingest.py` and schema at `scripts/sql/history_schema.sql`.

Next implementation step:
- Extend parity-safe pitcher context and `feature_rows(v1)` materialization beyond 2020.
- Build reproducible training extracts from `feature_rows + labels` across multiple seasons.
- Preserve checkpointed resume semantics and request budget controls for request-heavy enrichment jobs.
- Keep odds ingestion forward-only.

### Acceptance criteria
- 2020 baseline remains stable, leakage-safe, and idempotent after reruns.
- Seasons beyond 2020 can be materialized without leakage regressions.
- Training extract generation is reproducible and version-tagged.

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

## Priority 4 — Baseline Experiment Loop + DQ Expansion

- Expand `dq` from placeholder to full checks (completeness, null thresholds, duplicate guards, freshness).
- Finish 2022-2025 `feature_rows(v1)` materialization and confirm readiness via `scripts/training/run_when_ready.py`.
- Run `configs/training/baseline_lgbm.json` and register the first incumbent metrics.
- Run `configs/training/tuned_candidate.json` only after the baseline artifacts are in place.
- Use `configs/training/promotion_gates.json` for every promotion review with **log loss** as primary metric.

### Acceptance criteria
- DQ report artifacts generated per run.
- Reproducible baseline + challenger experiments completed.
- Model comparison includes calibration and reliability feasibility notes.

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
