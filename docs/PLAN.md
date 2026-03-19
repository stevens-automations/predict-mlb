# Exploratory Training Plan

Last updated: 2026-03-16

## Current State

- The canonical historical DB has been recovered and promoted.
- The active phase is exploratory model training and optimization.
- The canonical main path is the full integrated `pregame_1h` contract on `feature_rows(v2_phase1)`.
- LightGBM is the first active model path; logistic regression is a benchmark on the same evaluation frame.
- Daily inference architecture remains intentionally unfrozen until the winning training schema is known.

## What Is Done

- Historical ingestion foundation is in place.
- The canonical DB exists locally at `data/mlb_history.db`.
- The historical support-table and integrated feature-row path already exist for training scaffolding.
- Canonical configs already pin `2020-2024` as development seasons and `2025` as the untouched holdout.

## What Is In Progress

- Canonical doc consolidation around one exploratory-modeling strategy
- LightGBM-first experiment scaffolding and readiness gating
- Honest blocker reporting for optional local dependencies
- Supporting cleanup that reduces confusion without constraining model exploration

## Roadmap To Season Start

### Model Discovery And Selection

- [x] Run the canonical LightGBM baseline on integrated `v2_phase1` data across `2020-2024` development folds with `2025` holdout evaluation.
- [ ] Freeze the canonical feature-representation policy in docs: no direct identity inputs, home-label-aligned comparative features, positive=`home edge` sign conventions, and matchup-aware handedness/platoon representations.
- [ ] Audit the current materialized feature payload against that policy before the next challenger run.
- [ ] Run at least one LightGBM challenger/tuned candidate on the same frame and compare against the baseline.
- [ ] Run the logistic regression benchmark on the same frame once `scikit-learn` is installed.
- [ ] Iterate on feature engineering, missingness handling, and model parameters using the same honest evaluation frame.
- [ ] Decide which integrated feature families and degraded/fallback states are worth freezing into the eventual live schema.
- [ ] Retrain the chosen model family on the full historical span after the validation / holdout selection phase is complete.

### Post-Model Operationalization Backlog

- [ ] Define the exact daily data dependencies implied by the winning model/schema.
- [ ] Specify what must be pulled each day, from where, and with what freshness window before first pitch.
- [ ] Design the daily database update path so same-day inference data lands predictably and safely.
- [ ] Decide when the daily pipeline should run, what can run incrementally, and what should be re-materialized from scratch.
- [ ] Build the prediction execution path that loads the chosen artifact, scores the day slate, and writes canonical outputs.
- [ ] Design tweet-generation inputs/outputs around the final prediction artifact instead of hard-coding tweet logic prematurely.
- [ ] Explore non-deterministic tweet phrasing via a local model path (for example Qwen3.5-9B through Ollama) only after the prediction pipeline itself is stable.
- [ ] Define logging, monitoring, and operator visibility requirements, potentially including a lightweight internal dashboard.
- [ ] Finalize the daily inference architecture only after the winning training contract is identified.
- [ ] Complete remaining repo cleanup and operational hardening around the chosen model path.

## Optional / High-Value Later Work

- Add deeper lineup-quality and matchup interaction terms
- Expand weather realism beyond the first practical cutoff if later evidence says it matters
- Revisit secondary run-margin modeling after the main side model improves
