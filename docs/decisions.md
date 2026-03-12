# Project Decisions

Last updated: 2026-03-11

## Current State

The recovery / promotion step is complete. The current decisions are about protecting the promoted canonical DB, minimizing durable script surface area, and keeping one canonical file per concern during stabilization.

## What Is Done

1. Historical source of truth: `data/mlb_history.db` is the canonical local store.
2. Active historical scope: seasons `2020-2025`.
3. Historical odds policy: no historical odds backfill.
4. Contract policy: no silent game skipping; degraded behavior must stay explicit.
5. Train / inference parity: mandatory for every approved feature family.
6. Stable baseline contract: `feature_rows(feature_version='v1')` remains the approved current training baseline.
7. Data expansion direction: support-table-first, then materialize a versioned feature contract.
8. Current project priority: protect the canonical DB workflow and finish the durable rebuild path before retraining.
9. Current richer support families in scope: bullpen, lineup / platoon, and weather / venue.
10. Handedness is part of the approved lineup / platoon support path; completed-game lineup coverage is effectively complete and the remaining lineup gap is limited to postponed 2020 games without raw snapshots.
11. The weather contract is intentionally simplified; historical weather support is now considered sufficient for downstream integrated work.
12. `v2_phase1` materialization exists in code/tests; the remaining integrated blocker is canonical DB backfill plus validation, not materializer implementation.
13. The repo should expose one durable rebuild command/CLI that can recreate the historical DB and feature layers from scratch for recovery and reproducible retraining.
14. Mutating the canonical local DB requires an explicit opt-in step in the primary ingestion CLI.
15. One canonical file per concern is the documentation and operational preference; one-off artifacts should be folded into canonical files or archived, not left as parallel sources of truth.
16. External research on real MLB pricing and forecasting practice should drive the eventual training rebuild more than stale internal training docs.
17. The primary planning surface for that rebuild is `docs/runbooks/training-manifest.md`.
18. The target operating window for the eventual prediction system is near first pitch (roughly one hour pregame).
19. The first serious training direction should use the strongest realistic integrated `pregame_1h` feature set available under train/inference parity, not an intentionally weakened `v1`-style spine.
20. Sportsbook odds are benchmark/comparison inputs only and are forbidden as model-training features unless Steven explicitly changes that policy later.

## What Is In Progress

1. The exact rebuild-path shape that best balances durability with minimal long-lived script surface area.
2. The scope and sequence for the broader cleanup pass once the rebuild path is locked.

## What Remains Before Training

1. Rebuild-path implementation details after the command/ownership boundary is finalized.
2. Training promotion choices after stabilization, cleanup, and a clean checkpoint commit exist.

## Optional / High-Value Later Work

- Secondary run-margin modeling
- Richer player-level lineup quality
- More detailed park / weather interactions
