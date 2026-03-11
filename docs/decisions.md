# Project Decisions

Last updated: 2026-03-11

## Current State

The core data expansion direction is set. The remaining choices are about scope control and readiness for the first integrated materialization, not whether to pursue richer baseball context at all.

## What Is Done

1. Historical source of truth: `data/mlb_history.db` is the canonical local store.
2. Active historical scope: seasons `2020-2025`.
3. Historical odds policy: no historical odds backfill.
4. Contract policy: no silent game skipping; degraded behavior must stay explicit.
5. Train / inference parity: mandatory for every approved feature family.
6. Stable baseline contract: `feature_rows(feature_version='v1')` remains the approved current training baseline.
7. Data expansion direction: support-table-first, then materialize a versioned feature contract.
8. Current project priority: finish data expansion validation and canonical integrated feature materialization before retraining.
9. Current richer support families in scope: bullpen, lineup / platoon, and weather / venue.
10. Handedness is part of the approved lineup / platoon support path; completed-game lineup coverage is effectively complete and the remaining lineup gap is limited to postponed 2020 games without raw snapshots.
11. The weather contract is intentionally simplified; historical weather support is now considered sufficient for downstream integrated work.
12. `v2_phase1` materialization exists in code/tests; the remaining integrated blocker is canonical DB backfill plus validation, not materializer implementation.

## What Is In Progress

1. Whether the four completed Mexico City games missing weather snapshots should remain explicit degraded-path cases for the first integrated run or be backfilled before training.
2. The exact readiness evidence required before moving from "support layers exist" to "training-ready integrated contract."

## What Remains Before Training

1. Promotion path after the first integrated run
- Options: keep the integrated run as exploratory evidence only, or treat it as the candidate new baseline if it clears review gates.
- Lean: decide only after the first integrated review packet exists.

## Optional / High-Value Later Work

- Secondary run-margin modeling
- Richer player-level lineup quality
- More detailed park / weather interactions
