# Project Status

Last updated: 2026-03-11

## Current State

The canonical historical DB has been recovered and promoted. The repo is now in post-promotion stabilization. Training remains intentionally deferred, and the immediate goal is to protect the canonical DB workflow before finishing the durable rebuild path.

- `data/mlb_history.db` is the protected canonical local DB.
- `feature_rows(v1)` is still the approved stable training contract.
- The historical support-table foundation exists for seasons `2020-2025`.
- Recovery-specific notes remain on disk for traceability, but recovery execution is no longer the active project phase.

## What Is Done

- SQLite is the canonical historical store.
- `scripts/history_ingest.py` and `scripts/sql/history_schema.sql` support the historical pipeline and support-table flow.
- Repo-local historical coverage and validation reporting exist for seasons `2020-2025`.
- Support coverage auditing already exists for residual-gap reporting.
- `feature_rows(feature_version='v1')` exists end to end.
- Baseline training / evaluation flow already exists under `scripts/training/` and `configs/training/`.
- Train / inference parity remains the governing rule for approved features.
- Canonical-write hardening already protects richer `game_pitcher_context` rows from null-safe fallback overwrites.
- Mutating `scripts/history_ingest.py` commands against the canonical DB now require explicit opt-in via `--allow-canonical-writes`.

## What Is In Progress

- Protecting and simplifying the canonical DB workflow.
- Defining the smallest durable rebuild path / CLI that can recreate the canonical DB without proliferating one-off scripts.
- Isolating obvious legacy notebook/runtime artifacts away from the active root and script surfaces.

## What Remains Before Training

The locked order is:

1. Protect and lock the canonical DB workflow.
2. Finish the durable rebuild path / CLI.
3. Perform comprehensive repo cleanup.
4. Consolidate and update canonical docs.
5. Cut a clean checkpoint commit before training.

## Optional / High-Value Later Work

- Secondary run-margin modeling
- Deeper lineup-quality and player-level offensive context
- More detailed park / weather interaction features
- Additional baseball-specific interaction terms after the first integrated run is stable

## Working Summary

The repo is no longer in recovery execution. The next work is operational hardening and consolidation around the promoted canonical DB, followed by the durable rebuild path and a cleanup checkpoint before any renewed training work.
