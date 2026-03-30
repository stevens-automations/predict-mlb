# Project Status
Last updated: 2026-03-30

## Current Phase
Active season — live since March 26, 2026.

## Early Season Record
- 3-day record: 23/35 (66%) — all low-confidence tier picks so far
- No medium/high confidence picks yet (first ~15 games use cold-start fallback stats)
- Next milestone: first medium/high confidence pick triggers tweet eligibility

## What's Working
- All pipeline jobs running daily at 8 AM ET (ingest → layer2 → evaluate → fetch → predict)
- Daily predictions written to `daily_predictions` table with SHAP explanations
- Evaluation scoring previous day's predictions automatically
- Dashboard live at http://localhost:8765
- Tweet generation (deterministic format) producing text for eligible games

## Tweeting Status
- **Pending:** X developer portal fix required — app must be placed in a Project for v2 API access
- Tweet eligibility: medium/high confidence tier only (win_prob >= 0.65 for high, >= 0.60 for medium)
- Additional filter: tweet_score >= 2 (interestingness scoring), max 3 tweets/day
- LLM tweet path available but disabled (USE_LLM=False) — enable mid-season once 2026 data is richer

## Model Performance
- Model: LightGBM v4 tuned (`matchup_lgbm_v4_tuned_final`)
- 2025 holdout accuracy: 57.37%
- Dev CV accuracy: 56.65%
- Target: 58-60%+ as 2026 season data accumulates

## Known Limitations
- Cold-start: first ~15 games per team use 2025 season stats as fallback
- Odds API: one call per day (cached, 23h TTL)
