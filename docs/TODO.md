# Predict-MLB TODO

Last updated: 2026-03-11

## Current State

- The core expansion work is largely done.
- The project is waiting on final validation and integrated feature materialization, not more scattered feature ideation.

## What Is Done

- Pitcher appearances backfilled
- Bullpen support backfilled
- Lineup / platoon support implemented; completed-game coverage effectively complete
- Weather / venue support largely fixed; weather contract simplified
- `feature_rows(v1)` still available as the stable fallback
- `v2_phase1` materializer implemented but not yet backfilled into the canonical DB

## What Is In Progress

- Final validation across expanded support layers
- Canonical `v2_phase1` materialization and degraded-path review before retraining

## What Remains Before Training

- [ ] Keep the residual support gaps explicit in the audit/reporting output.
- [ ] Run one concise validation pass over bullpen, lineup / platoon, and weather support coverage.
- [ ] Materialize and validate the integrated feature rows.
- [ ] Tighten DQ / degraded-path checks enough for a serious run review.
- [ ] Run the first serious integrated model pass only after the above are complete.

## Optional / High-Value Later Work

- [ ] Add deeper lineup-quality features after the first integrated run.
- [ ] Add richer park / weather interaction features after the first integrated run.
- [ ] Revisit secondary run-margin modeling later.
