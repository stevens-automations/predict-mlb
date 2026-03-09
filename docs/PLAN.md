# Remaining Work Plan (Reliability-First, New-Model Track)

Last updated: 2026-03-09

## Priority 1 — Implement Historical Ingestion Foundation (statsapi -> SQLite)

Build `scripts/history_ingest.py` and schema for `data/mlb_history.db` with:
- Canonical tables (`games`, `team_game_stats`, `pitcher_game_context`, `odds_snapshots`, `feature_snapshots`)
- Run/checkpoint tracking (`ingestion_runs`, `ingestion_checkpoints`)
- Idempotent upserts and resumable partition processing

### Acceptance criteria
- One sample month ingests successfully.
- Re-run of same partition creates no duplicates and stable row counts.
- Checkpoint resume works after interruption.

---

## Priority 2 — Backfill Historical Dataset (2020–2025)

Run controlled season-by-season backfill with retry/backoff/circuit policies and DQ checks.

### Acceptance criteria
- Seasons 2020–2025 completed or explicitly marked with actionable blocked reasons.
- DQ summaries produced per partition.
- Feature snapshot coverage reaches agreed threshold for final games.

---

## Priority 3 — Data Reliability Contract + Degraded Prediction Mode

Implement strict input contracts with explicit must-have vs optional fields.
- Must-have failures trigger degraded-mode prediction path (not silent skip)
- Each degraded prediction emits reason codes + incident records
- Recurring failures become fix-queue items

### Acceptance criteria
- Contract checks run pre-prediction.
- No game silently skipped due to data errors.
- Incident logging/audit trail exists for degraded runs.

---

## Priority 4 — Exploratory Model Lab (Not Legacy Replication)

Create iterative training/evaluation workflow on historical DB:
- Start with recommended primary metric: **log loss**
- Track secondary: Brier, calibration, accuracy
- Compare multiple candidates and feature-set versions

### Acceptance criteria
- Experiment matrix and reports are reproducible.
- At least baseline + one stronger candidate trained/evaluated on walk-forward splits.
- Promotion candidates include reliability feasibility assessment.

---

## Priority 5 — Safe Promotion Gates on Staging

Gate any production promotion on:
- ingestion/data-quality health
- model quality improvements
- operational reliability checks
- rollback readiness

### Acceptance criteria
- Explicit go/no-go decision documented.
- Staging checklist completed.
- Promotion to `main` only by explicit approval.
