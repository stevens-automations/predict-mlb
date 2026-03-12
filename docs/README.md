# Documentation Index

This repo keeps one canonical file per concern. Read the root docs first, then the docs set.

## Root Docs

1. `README.md` - root repo orientation and current architecture
2. `AGENT.md` - operating contract for coding / ops agents

## Canonical Docs

1. `docs/STATUS.md` - current state, what is done, what is in progress, and what remains before training
2. `docs/PLAN.md` - the ordered post-promotion stabilization gates before training resumes
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
- The repo is now in post-promotion stabilization, not training.
- The immediate focus is protecting the canonical DB workflow, finishing the durable rebuild path, and consolidating external-research-driven training planning into one canonical manifest.
- Broader cleanup and consolidation are intentionally deferred to the next passes so this phase stays reviewable.

## What Is Done

- Canonical historical storage and ingestion flow are in place.
- `data/mlb_history.db` is the canonical local store.
- `feature_rows(v1)` remains the stable baseline contract.
- Season validation and recovery records already exist under `docs/reports/` and `docs/handoffs/`.

## What Is In Progress

- Canonical DB workflow protection and boundary clarification.
- Durable rebuild-path / CLI tightening around `scripts/history_ingest.py rebuild-history` plus the minimum required stage subcommands.
- Isolation of obvious legacy notebook/runtime utilities away from the active root surface.

## What Remains Before Training

Follow the locked order:

1. Protect / lock the canonical DB workflow.
2. Finish the durable rebuild path / CLI.
3. Perform comprehensive repo cleanup.
4. Consolidate and update canonical docs.
5. Cut a clean checkpoint commit before training.

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
- `docs/runbooks/model-optimization-plan.md` - older optimization notes and gates; secondary to the manifest
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
