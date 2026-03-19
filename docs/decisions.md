# Project Decisions

Last updated: 2026-03-16

## Current State

The recovery / promotion step is complete. The current decisions are about maximizing model-discovery value from the historical DB first, while keeping one canonical file per concern and avoiding premature inference-architecture lock-in.

## What Is Done

1. Historical source of truth: `data/mlb_history.db` is the canonical local store.
2. Active historical scope: seasons `2020-2025`.
3. Historical odds policy: no historical odds backfill.
4. Contract policy: no silent game skipping; degraded behavior must stay explicit.
5. Train / inference parity: mandatory for every approved feature family.
6. Canonical serious-training contract: `pregame_1h` on `feature_rows(feature_version='v2_phase1')` is the approved main path for the first honest integrated baseline stack.
7. Data expansion direction: support-table-first, then materialize a versioned feature contract.
8. Current project priority: exploratory model training and optimization on the recovered historical DB comes before final daily inference architecture work.
9. Current richer support families in scope: bullpen, lineup / platoon, and weather / venue.
10. Handedness is part of the approved lineup / platoon support path; completed-game lineup coverage is effectively complete and the remaining lineup gap is limited to postponed 2020 games without raw snapshots.
11. The weather contract is intentionally simplified; historical weather support is now considered sufficient for downstream integrated work.
12. `v2_phase1` materialization exists in code/tests; the remaining integrated blocker is canonical DB backfill plus validation, not materializer implementation.
13. The repo should expose one durable rebuild command/CLI that can recreate the historical DB and feature layers from scratch for recovery and reproducible retraining.
14. Mutating the canonical local DB requires an explicit opt-in step in the primary ingestion CLI.
15. One canonical file per concern is the documentation and operational preference; one-off artifacts should be folded into canonical files or archived, not left as parallel sources of truth.
16. External research on real MLB pricing and forecasting practice should drive model development more than stale internal training docs.
17. The primary planning surface for that rebuild is `docs/runbooks/training-manifest.md`.
18. The target operating window for the eventual prediction system is near first pitch (roughly one hour pregame).
19. The first serious training direction should use the strongest realistic integrated `pregame_1h` feature set available under train/inference parity, not an intentionally weakened `v1`-style spine.
20. Sportsbook odds are benchmark/comparison inputs only and are forbidden as model-training features unless Steven explicitly changes that policy later.
21. The first baseline evaluation regime is season-based: expanding development folds across 2020-2024, with 2025 kept as an untouched holdout season.
22. The active model-development stack should be LightGBM first, with logistic regression retained as a benchmark/comparison path producing calibration-ready probabilities for the home-win target.
23. The project is not optimizing for a deliberately weak MVP baseline; it should use the full breadth of historical fields intended to be available for live inference later.
24. The daily inference schema and system architecture should be finalized only after training/optimization identifies the winning model path and required feature contract.
25. Direct identity fields (for example team IDs, starter IDs, player IDs, venue IDs, team-name encodings, or similar identity proxies) are forbidden as canonical model-training inputs for the game-winner model. They may exist only as metadata / join keys used to derive baseball-state features.
26. Because the canonical label is `labels.did_home_win`, game-level feature engineering should preferentially express baseball-state comparisons in home-versus-away form rather than as unrelated identity buckets.
27. Comparative features must use a consistent home-edge sign convention. Default rule: positive values should mean an advantage for the home side relative to the away side.
28. For metrics where higher is better, comparative features should default to `home_minus_away`; for metrics where lower is better (for example ERA / WHIP / runs-allowed style metrics), comparative features should be transformed so positive values still mean a home-side advantage whenever practical.
29. Contextual features such as handedness / platoon must be represented in matchup-aware form (for example offense versus opposing starter hand, lineup hand mix versus opposing pitching context, or bullpen hand-mix matchup summaries), not as naive raw identity features or isolated split stats.
30. The project’s game-winner feature contract should treat comparative home-edge features as the primary representation, while retaining only a limited set of raw side-specific anchor features, reliability/sample-size fields, and shared game-context fields where they add clear modeling value.

## What Is In Progress

1. The exact LightGBM-first experiment sequence and challenger set that best exploits the integrated `v2_phase1` contract.
2. The scope and sequence for the broader cleanup pass once the winning training path is clearer.

## What Remains Before Season Start

1. Model-selection choices after LightGBM baseline/challenger runs and the logistic benchmark exist on the canonical frame.
2. Inference-architecture decisions after the winning feature/schema requirements are known.

## Optional / High-Value Later Work

- Secondary run-margin modeling
- Richer player-level lineup quality
- More detailed park / weather interactions
