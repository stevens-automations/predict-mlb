# Predict-MLB TODO

Last updated: 2026-03-16

## Current State

- The canonical DB is promoted.
- This queue is now for exploratory training execution and model-selection support.

## What Is Done

- Canonical historical DB recovered and promoted
- Historical ingestion / support-table foundation present
- Canonical game-prediction training path frozen to integrated `pregame_1h` on `feature_rows(v2_phase1)`

## What Is In Progress

- Canonical doc consolidation around the exploratory training strategy
- LightGBM-first integrated training scaffolding
- Logistic benchmark support on the same evaluation frame
- Honest readiness and dependency-blocker reporting before local fitting starts

## What Remains Before Season Start

### Immediate model work

- [x] Run the integrated `v2_phase1` LightGBM baseline.
- [ ] Update canonical training docs to forbid direct team / starter / venue identity model inputs and lock one comparative home-edge sign convention.
- [ ] Audit current materialized features into: keep raw, convert to home-edge comparative form, backfill/fix, or drop.
- [ ] Run the first LightGBM challenger/tuned candidate on the cleaned season-based frame.
- [ ] Run the logistic regression benchmark once `scikit-learn` is installed.
- [ ] Review 2020-2024 development metrics plus 2025 holdout to choose the winning schema/features.
- [ ] Retrain the selected model on the full historical span after model-selection work is complete.

### Near-future product backlog after model selection

- [ ] Determine the exact daily data pulls required by the winning live-inference schema.
- [ ] Decide when each same-day data source should be pulled and how freshness/cutoff rules should work.
- [ ] Design the daily DB update/materialization path for inference-time vectors.
- [ ] Design scheduling/orchestration for the daily run loop.
- [ ] Build the canonical prediction-generation path for the full slate.
- [ ] Design how predictions should be transformed into tweet-ready artifacts.
- [ ] Explore non-deterministic tweet phrasing using a local LLM path such as Qwen3.5-9B via Ollama.
- [ ] Define logging/monitoring requirements and whether a lightweight internal dashboard is worth building.
- [ ] Lock daily inference architecture only after the winning training contract is selected.

## Optional / High-Value Later Work

- [ ] Add deeper lineup-quality features after the first integrated run.
- [ ] Add richer park / weather interaction features after the first integrated run.
- [ ] Revisit secondary run-margin modeling later.

## Future: LLM-Generated Tweet Format
- Replace deterministic tweet scaffold with local LLM generation
- Use Qwen 3.5 (9B or 4B) via Ollama (already configured in OpenClaw)
- Each tweet should feel unique, engaging, and not bot-like
- Include game context, key matchup angles, and confident but humble tone
- Trigger: after predictions are generated, pass game context to Ollama API
- Reference: server/tweet_scaffold.py for current format baseline

## Results Tweet Strategy
- Current: evaluate full-slate accuracy daily, log to pipeline_log (not tweeted yet)
- Decision needed: tweet daily results or weekly summary?
  - Daily: only tweet if tweeted-game accuracy is 50%+ AND sample ≥ 3 games
  - Weekly: tweet season-to-date record every Monday morning
- Long-term: consider selling full-slate picks access (subscription model)
  - Tweet 2-3 "free" picks daily, full slate behind paywall
  - Revenue model: $5-10/month for full daily slate + deeper insights
  - See TWEET_STRATEGY.md for tweet format baseline
