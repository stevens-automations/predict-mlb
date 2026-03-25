# Project Status
Last updated: 2026-03-24

## Current Phase
Production-ready. Season starts March 26, 2026.

## What's Complete
- Historical data pipeline: 2020-2025 ingested and validated
- Feature engineering: 6 Layer 2 tables (team stats, starter stats, bullpen, lineup, handedness matchup, career pitching)
- Training: LightGBM v4 tuned, 57.37% accuracy on 2025 holdout
- Inference pipeline: feature_builder → scorer → SHAP explainer → tweet generator
- Daily runner: APScheduler, 8 AM morning chain
- Dashboard: FastAPI + React, localhost:8765
- Tweet generation: Qwen 3.5 9B via Ollama with deterministic fallback
- Opening day readiness: cold-start fallback using 2025 season data

## Model Performance
- Best model: LightGBM v4 tuned
- Dev CV accuracy: 56.65%
- 2025 holdout accuracy: 57.37%
- Naive "pick better record" baseline: 56.0%
- Target: 58-60%+ as 2026 season data accumulates

## Active Season
- Season: 2026 (starts March 26)
- DB: data/mlb_history.db (2020-2025 historical + 2026 accumulating)
- Branch: staging/preseason-consolidated → merged to main

## Known Limitations
- Cold-start: first ~15 games use 2025 season stats as fallback
- No Twitter posting yet (credentials not configured)
- Weekly recap tweet: implemented but not yet live
