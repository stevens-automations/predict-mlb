# Project Status

Last updated: 2026-03-11

## Current State

The project is at a late-stage data-expansion checkpoint. Core support layers exist, retraining is still intentionally deferred, and the main remaining blocker is validating and materializing the first integrated feature version in the canonical DB.

- Pitcher appearances are backfilled.
- Bullpen support is backfilled.
- Lineup / platoon support is implemented; the rerun is effectively complete for completed games, with only two postponed 2020 games lacking raw lineup snapshot rows.
- Weather / venue support is largely fixed; the weather contract is simplified and historical support is effectively complete enough for downstream work.
- `feature_rows(v1)` is still the approved stable training contract.
- `feature_rows(feature_version='v2_phase1')` is implemented in code/tests but not yet materialized in the canonical DB.
- Retraining remains deferred pending final validation and integrated feature materialization.

## What Is Done

- SQLite is the canonical historical store.
- `scripts/history_ingest.py` and `scripts/sql/history_schema.sql` support the historical pipeline and support-table flow.
- Repo-local historical coverage and validation reporting exist for seasons `2020-2025`.
- Support coverage auditing now has a dedicated repo-local command for exact residual gaps.
- `feature_rows(feature_version='v1')` exists end to end.
- Baseline training / evaluation flow already exists under `train/`, `scripts/training/`, and `configs/training/`.
- Train / inference parity remains the governing rule for approved features.
- Pitcher-appearance support is implemented and backfilled.
- Bullpen support tables are implemented and backfilled.
- Lineup / platoon support is implemented, including the handedness fix and completed-game coverage.
- Weather / venue support is operationally usable, and the simplified weather contract is effectively complete enough for downstream work.

## What Is In Progress

- Running final coverage / sanity validation across bullpen, lineup / platoon, and weather support.
- Materializing `feature_rows(feature_version='v2_phase1')` in the canonical DB and reviewing degraded-path behavior.

## What Remains Before Training

The remaining gates before the first serious integrated model run are:

1. Keep the residual support gaps explicit in reports: two postponed 2020 games without lineup snapshots, plus four completed Mexico City games in 2023-2024 without weather snapshots.
2. Run the final coverage / sanity review across bullpen, lineup / platoon, and weather support tables.
3. Materialize and validate `feature_rows(feature_version='v2_phase1')` using the same parity-safe rules as `v1`.
4. Confirm readiness gates for training, including degraded-path handling and review outputs.
5. Only then run the first serious integrated training pass.

## Optional / High-Value Later Work

- Secondary run-margin modeling
- Deeper lineup-quality and player-level offensive context
- More detailed park / weather interaction features
- Additional baseball-specific interaction terms after the first integrated run is stable

## Working Summary

The repo is no longer blocked on raw data foundation or integrated-materializer implementation. The remaining work is to validate the expanded support layers as one coherent package, materialize `v2_phase1` into the canonical DB, and review the degraded-path distribution before retraining.
