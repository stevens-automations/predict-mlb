# Documentation Index

This docs set is centered on five canonical files. Read these first.

## Canonical Docs

1. `docs/STATUS.md` - current state, what is done, what is in progress, and what remains before training
2. `docs/PLAN.md` - the pre-training gate list for the first integrated materialization and run
3. `docs/TODO.md` - the short execution queue derived from the plan
4. `docs/decisions.md` - decisions that are locked vs still open
5. `docs/README.md` - doc map and cleanup rules

## Current State

- The project is past the ingestion-foundation phase.
- Pitcher appearances are backfilled.
- Bullpen support is backfilled.
- Lineup / platoon support is implemented; completed-game coverage is effectively complete and only two postponed 2020 games lack raw lineup snapshot rows.
- Weather / venue support is largely fixed; the weather contract is simplified and historical support is effectively complete enough for downstream work.
- `v2_phase1` integrated materialization already exists in code/tests, but it has not yet been materialized in the canonical DB.
- Retraining is still deferred pending final validation and integrated feature materialization.

## What Is Done

- Canonical historical storage and ingestion flow are in place.
- `feature_rows(v1)` remains the stable baseline contract.
- Richer support layers now exist for pitcher appearances, bullpen, lineup / platoon, and weather / venue.
- The weather pipeline is simplified enough that downstream integrated work no longer depends on more contract churn.
- Season validation reports live under `docs/reports/phase2-validation-*.md`.

## What Is In Progress

- Final validation across the expanded support layers.
- Canonical `v2_phase1` materialization plus degraded-path review for the first serious richer run.

## What Remains Before Training

- Keep the known residual support gaps explicit and close any remaining validation gaps.
- Materialize the integrated feature rows.
- Keep retraining deferred until `docs/PLAN.md` gates are complete.

## Optional / High-Value Later Work

- Secondary run-margin modeling
- Richer lineup-quality layers beyond first-pass platoon support
- Further park / weather interaction refinement

## Supporting Docs

- `docs/runbooks/historical-ingestion-runbook.md` - ingestion commands and sequencing
- `docs/runbooks/training-architecture.md` - training flow and entrypoints
- `docs/runbooks/model-optimization-plan.md` - baseline and challenger review gates
- `docs/research/feature-contract-v1.md` - stable `v1` contract reference
- `docs/research/integrated-feature-contract-v2-phase1-2026-03-10.md` - integrated materialization reference
- `docs/research/pre-training-validation-readiness-gate-2026-03-10.md` - detailed readiness criteria

## Reports And Archive

- `docs/reports/phase2-validation-2020.md` through `docs/reports/phase2-validation-2025.md`
- `docs/archive/2026-03/` - superseded checkpoint notes retained for traceability
- `docs/archive/runtime-logs/` - generated runtime artifacts retained for traceability

## Documentation Rules

- Keep project state in `STATUS`, execution gates in `PLAN`, short queue items in `TODO`, and scope calls in `decisions`.
- Keep run commands in runbooks and implementation detail in research docs, not in checkpoint notes.
- Do not create new mini-status docs when an update belongs in one of the canonical files.
- When a small note is still worth keeping, fold the takeaway into a canonical doc and leave the original in `docs/archive/` or `docs/research/`.
