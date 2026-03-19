# Documentation Index

This repo keeps one canonical file per concern. Read the root docs first, then the docs set.

## Root Docs

1. `README.md` - root repo orientation and current architecture
2. `AGENT.md` - operating contract for coding / ops agents

## Canonical Docs

1. `docs/STATUS.md` - current state, active exploratory training direction, and honest blockers
2. `docs/PLAN.md` - canonical roadmap to season start for model development, selection, and downstream freeze points
3. `docs/TODO.md` - the short execution queue derived from the plan
4. `docs/decisions.md` - decisions that are locked vs still open

## Recommended Read Order

1. `README.md`
2. `docs/README.md`
3. `docs/STATUS.md`
4. `docs/PLAN.md`
5. `docs/decisions.md`
6. `docs/runbooks/historical-ingestion-runbook.md`

## Current State

- The canonical historical DB has been recovered and promoted.
- The repo is now in exploratory model training and optimization, not MVP minimization.
- The immediate focus is training and comparing models on the full integrated `pregame_1h` feature set that the historical DB can already support.
- The canonical game-prediction training direction is fixed: integrated `pregame_1h`, `v2_phase1` features, no sportsbook inputs, and `2025` reserved as the untouched holdout.
- LightGBM is the first active model path. Logistic regression stays in the scaffold as a benchmark/comparison path.
- Daily inference architecture and any final schema lock should follow the training results rather than constrain them prematurely.

## What Is Done

- Canonical historical storage and ingestion flow are in place.
- `data/mlb_history.db` is the canonical local store.
- The canonical training path is now the integrated `pregame_1h` contract on `feature_rows(v2_phase1)`.
- Season validation and recovery records already exist under `docs/reports/` and `docs/handoffs/`.

## What Is In Progress

- Canonical training/doc consolidation around one exploratory-modeling narrative.
- LightGBM-first experiment scaffolding on the integrated `v2_phase1` contract.
- Honest readiness/blocker reporting so local dependency gaps do not obscure what can already be validated.

## What Remains Before Season Start

Follow the locked order:

1. Run and compare the first integrated LightGBM experiments on 2020-2024 development seasons plus 2025 holdout.
2. Use benchmark/challenger results to decide which feature families and schema requirements are truly worth carrying into live inference.
3. Backfill or harden any remaining feature families only when they improve the winning training path.
4. Finalize daily inference architecture and operational schema around the winning contract.
5. Complete cleanup and deployment-oriented hardening after the model direction is locked.

## Optional / High-Value Later Work

- Secondary run-margin modeling
- Richer lineup-quality layers beyond first-pass platoon support
- Further park / weather interaction refinement

## Supporting Docs

- `docs/schema-feature-map.md` - practical map of the canonical DB, major table groups, and what `v1` / `v2_phase1` materialize for training
- `docs/runbooks/historical-ingestion-runbook.md` - canonical ingestion/rebuild commands, including the preferred `rebuild-history` orchestration path
- `docs/runbooks/recovery-plan-2026-03-11.md` - recovery incident reference retained for traceability, not as the active execution plan
- `docs/runbooks/training-manifest.md` - canonical external-research-driven training strategy and planning surface
- `docs/runbooks/training-architecture.md` - current implemented training scaffold and entrypoints
- `docs/runbooks/model-optimization-plan.md` - supporting optimization roadmap aligned to the LightGBM-first integrated training phase
- `docs/research/feature-contract-v1.md` - stable `v1` contract reference
- `docs/research/integrated-feature-contract-v2-phase1-2026-03-10.md` - integrated materialization reference
- `docs/research/pre-training-validation-readiness-gate-2026-03-10.md` - detailed readiness criteria
- `legacy/README.md` - map of demoted notebook-era and scheduler artifacts
- `scripts/legacy_runtime/README.md` - map of retained runtime migration/util scripts

## Reports And Archive

- `docs/reports/phase2-validation-2020.md` through `docs/reports/phase2-validation-2025.md`
- `docs/archive/2026-03/` - superseded checkpoint notes retained for traceability
- `docs/archive/runtime-logs/` - generated runtime artifacts retained for traceability

## Documentation Rules

- Keep root orientation in `README.md` and agent operating guidance in `AGENT.md`.
- Keep this file as the docs map only.
- Keep project state in `STATUS`, execution gates in `PLAN`, short queue items in `TODO`, and scope calls in `decisions`.
- Keep one canonical file per concern; avoid creating overlapping mini-status or checkpoint docs.
- Keep run commands in runbooks and implementation detail in research docs, not in checkpoint notes.
- Do not create new mini-status docs when an update belongs in one of the canonical files.
- When a small note is still worth keeping, fold the takeaway into a canonical doc and leave the original in `docs/archive/` or `docs/research/`.
