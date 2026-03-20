# Project Status

Last updated: 2026-03-19

## Phase 4 (Active): Inference Core — COMPLETE

Inference core built and end-to-end tested. All 5 components working.

| Component | File | Status |
|-----------|------|--------|
| Feature builder | `scripts/inference/feature_builder.py` | ✅ Done |
| Scorer | `scripts/inference/scorer.py` | ✅ Done |
| Fetch today's games | `scripts/jobs/fetch_todays_games.py` | ✅ Done |
| Fetch odds | `scripts/jobs/fetch_odds.py` | ✅ Done |
| Predict today | `scripts/jobs/predict_today.py` | ✅ Done |

**End-to-end test (2024-08-02, 15 games):** 11/15 correct = **73.3% accuracy**

**Model in use:** `gridsearch_lgbm_v4__20260319T022323Z/model.pkl` (LGBMClassifier, 63 features)
Note: `matchup_lgbm_v4_tuned__*` dirs have no model.pkl — gridsearch pkl is the best saved classifier.

**Pipeline tables created:** `today_schedule`, `daily_predictions`, `pipeline_log`
**Dependencies added:** `scikit-learn` (installed into venv for model unpickling)

**Next:** Build `run_daily.py` (APScheduler wiring), `server/api.py` (FastAPI), `ingest_yesterday.py`, `update_layer2.py`, `evaluate_yesterday.py`

---

## Current Phase: Phase 3 Training — COMPLETE (final training pass done)

Phase 2 done. Phase 3 training complete. v4 + grid search + run-diff regressor + ensemble blend all evaluated.

---

## Phase 3: Final Training Results (grid search + ensemble — 2026-03-19)

| Model | CV avg | Holdout 2025 |
|-------|--------|--------------|
| LightGBM v4 tuned (previous best) | 56.65% | **57.37%** |
| Grid search best (combo 5: num_leaves=31, lr=0.01, ff=0.7, mdl=50, n=1500) | 57.44% | 57.20% |
| Run-diff regressor binary acc | 57.68% CV | 54.90% |
| Ensemble best (0.8 clf / 0.2 rundiff) | — | 56.34% |

**Best holdout overall: 57.37% (v4 tuned) — unchanged.**

Grid search: best CV params were combo 5 (more regularized, slower LR) but holdout 57.20% fell slightly below v4 tuned.
Run-diff regressor: strong CV (57.68%) but degraded to 54.90% on holdout — overfitting signal, adds noise to ensemble.
Ensemble: all blends hurt performance (56.34% best). Pure classifier dominates.

**Recommendation: Use v4 tuned LightGBM for inference. Do NOT use ensemble.**

### Best model for inference:
- Script: `scripts/training/train_matchup_lgbm.py` (v4 tuned)
- Params: `num_leaves=63, lr=0.03, ff=0.7, bagging=0.7, mdl=30, n_estimators=800`
- Artifact: latest `matchup_lgbm_v4_tuned__*` in model_registry
- Holdout: 57.37%

### Grid search artifact:
- `artifacts/model_registry/gridsearch_lgbm_v4__latest.json`
- Best params: `num_leaves=31, lr=0.01, ff=0.7, mdl=50, n_estimators=1500`

### New scripts added:
- `scripts/training/tune_lgbm.py` — grid search (5 combos, 3-fold CV)
- `scripts/training/train_run_diff.py` — run-diff regressor
- `scripts/training/ensemble_blend.py` — ensemble blend evaluation

**Gap to target:** 57.37% vs 60% target. ~2.6pp gap remains. Next phase: feature engineering.

---

## Phase 3: Training Results (v4 run — 2026-03-19)

---

## Phase 3: Training Results (v4 run — 2026-03-19)

| Model | Dev CV avg | Holdout 2025 |
|-------|-----------|--------------|
| Old v2_phase1 baseline | 54.85% | 52.44% |
| Previous matchup baseline | 55.95% | 55.76% |
| LightGBM v3 baseline | 55.65% | 56.26% |
| **LightGBM v4 baseline** | **56.59%** | **56.01%** |
| **LightGBM v4 tuned** | **56.65%** | **57.37%** ← best |
| Logistic Regression v4 | 56.80% | 56.58% |

**Best holdout: 57.37%** (+4.93pp vs old baseline, +1.11pp vs v3 best)

Key changes in v4:
- Added 5 lineup OPS features: `home/away_lineup_top5_ops`, `home/away_lineup_top5_batting_avg`, `lineup_top5_ops_delta`
- Career ERA features now fully active (home/away_starter_career_era — 100% fill)
- Total: 63 features (up from 56 in v3, 2 skipped in v3 are now populated)
- LR: ERA outlier clipping at 5th-95th percentile before imputation/scaling
- New script: `scripts/features/add_lineup_ops_features.py`

**Top 10 feature importances (tuned v4 model):**
1. away_starter_career_era (33)
2. starter_k_pct_delta (25)
3. home_starter_career_era (22)
4. humidity_pct (22)
5. starter_whip_delta (21)
6. home_starter_k_pct (20)
7. home_bullpen_season_bullpen_era (19)
8. away_bullpen_season_bullpen_era (19)
9. run_diff_per_game_delta (18)
10. bullpen_era_delta (18)

career_era is the top-ranked feature pair. lineup_top5_ops appears at #3 baseline (26 importance), #17 tuned (16 importance).

**Verification (game_id=661199):**
- away_lineup_top5_ops = 0.8242 ✅ (Dodgers: Betts/Turner/Freeman/W.Smith/Muncy)
- lineup_top5_ops_delta = -0.0702 (Dodgers lineup quality edge vs Brewers)

**Gap to target:** 57.37% vs 60% target. ~2.6pp gap remains.

---

## Phase 3: Training Results (v3 run — 2026-03-19)

| Model | Dev CV avg | Holdout 2025 |
|-------|-----------|--------------|
| **LightGBM v3 baseline** | **55.65%** | **56.26%** |
| LightGBM v3 tuned | 55.91% | 55.68% |
| Logistic Regression v3 | 55.66% | 54.77% |

**Best holdout: 56.26%**

---

## Phase 2 Complete: game_matchup_features

| Item | Result |
|------|--------|
| Script | `scripts/build_layer2_matchup_features.py` |
| Row count | 13,046 rows (regular season 2020-2025, games with labels) |
| Columns | ~130 features + identifiers + labels |
| game_id=661199 `did_home_win` | 0 ✅ (Dodgers won) |
| game_id=661199 `away_team_win_pct` | 0.6991 ✅ (~0.699) |
| game_id=661199 `away_starter_era` | 2.5041 ✅ (~2.504) |
| game_id=661199 `away_vs_starter_hand_ops` | 0.7520 ✅ (~0.752 LAD vs RHP) |
| game_id=661199 `temperature_f` | 68.5°F ✅ |
| Cold-start (opening day 2022) | win_pct = NULL ✅ |

---

## Phase 1 Complete: Layer 2 Tables

All tables live in `data/mlb_history.db`. Each has 26,618 rows (2 per game × ~13,309 games, 2020-2025).

| Table | Script | Verification (game 661199) |
|-------|--------|---------------------------|
| `player_career_pitching_stats` | `scripts/build_layer2_career_pitching.py` | Urías: ERA=3.11, IP=717 ✅ |
| `team_pregame_stats` | `scripts/build_layer2_team_pregame_stats.py` | LAD: 113G, 79W, win_pct=0.699, OPS=0.749 ✅ |
| `starter_pregame_stats` | `scripts/build_layer2_starter_pregame_stats.py` | Urías: 22 starts, ERA=2.504, strike_pct=0.693 ✅ |
| `bullpen_pregame_stats` | `scripts/build_layer2_bullpen_pregame_stats.py` | LAD bullpen: ERA=3.51, 390 apps, outs_last3d=31 ✅ |
| `lineup_pregame_context` | `scripts/build_layer2_lineup_pregame_context.py` | LAD away: 4L/5R, vs_advantage=0.444 vs RHP ✅ |
| `team_vs_hand_pregame_stats` | `scripts/build_layer2_team_vs_hand_pregame_stats.py` | LAD: 76 vs RHP, OPS=0.752 ✅ |

---

## What's Next: Phase 3 continued

Remaining ~2.6pp to reach 60% target. Candidates:
1. **Deeper hyperparameter tuning** — grid/random search on num_leaves, min_data_in_leaf, feature_fraction
2. **Feature engineering** — ERA tier bins, nonlinear transforms, OPS percentile bins
3. **Ensemble** — blend LightGBM tuned + LR + run-diff regressor
4. **2025 partial season coverage** — verify lineup OPS coverage for 2025 games
5. **Platoon OPS depth** — extend lineup from top 5 to top 7 or full lineup for more signal
