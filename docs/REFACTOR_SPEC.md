# predict-mlb: Data & Feature Refactor Spec
**Author:** Mako  
**Date:** 2026-03-18  
**Status:** Approved for implementation

---

## 1. Motivation

The current model achieves ~52-54% accuracy on holdout, near coin-flip. Root cause: the engineered feature layer (`feature_rows v2_phase1`) was not properly computing pre-game composite stats from raw history. Specifically:

- Pitcher season stats were pulled live from the MLB Stats API at *ingestion time* (2026), not computed from per-game logs as-of-game-date → potential leakage and staleness
- No season-to-date batting stats for teams going *into* each game
- ~100 zero-importance dead features (lineup quality, platoon splits) bloating the feature space
- Raw starter IDs used as features → model memorizes individual pitchers, no generalization
- The v1/v2 phase versioning created confusion and technical debt

The old model (`StevenDeFalco/predictMLB`) achieved 66% using a clean ~44-feature set computed from live API calls. Our goal is to surpass that by computing more and richer pre-game features *entirely from our own historical DB*, computed correctly as-of-each-game-date.

**Key finding from DB audit:**  
Pitcher stats can be computed accurately from `game_pitcher_appearances` (ERA computed from appearances matched statsapi to within 0.01). This means we do not need external API calls for any engineered feature — everything derives from our raw tables.

---

## 2. Architecture

### Two-Layer Principle
**Layer 1 — Raw Historical DB:** One row per entity per game. Append-only. Never derived or computed. Source of truth.  
**Layer 2 — Engineered Feature DB:** All composite/cumulative stats computed from Layer 1 using only prior completed games. Fully reproducible. What the model actually trains on.

These live in the same SQLite file as separate tables. The distinction is conceptual and enforced by naming convention: raw tables have no `_pregame_` or `_features` in their name; derived tables do.

### Daily Inference Loop
1. 9 AM: pull yesterday's completed game data → append to Layer 1 tables
2. Recompute Layer 2 for today's games using updated Layer 1
3. Generate `game_matchup_features` rows for today's slate
4. Score with trained model → output predictions

Train/inference parity is a hard requirement: every feature used in training must have an identical computation path at inference time.

---

## 3. Layer 1: Raw Tables (keep existing, fix gaps)

These tables already exist and have good coverage. **Do not restructure them.** Fix data quality issues only.

| Table | Description | Coverage |
|---|---|---|
| `games` | Schedule, result, venue, date, game type | ✅ Complete |
| `game_team_stats` | Per-game box score per team: runs, hits, avg, OBP, SLG, OPS, K, BB | ✅ 100% 2020-2025 |
| `game_pitcher_appearances` | Per-pitcher-per-game pitching line: IP, ER, K, BB, H, pitches, strikes, BF | ✅ Complete |
| `game_lineup_snapshots` | Batting order + player IDs per team per game | ✅ Complete; bat_side via join to `player_handedness_dim` |
| `player_handedness_dim` | bat_side + pitch_hand per player (2,887 players) | ✅ Good coverage |
| `game_weather_snapshots` | Temp, humidity, wind speed/dir/gust, pressure, cloud cover, precip | ✅ ~99.9% 2020-2025 |
| `venue_dim` | Roof type, lat/lon, timezone, weather exposure | ✅ Complete |
| `labels` | did_home_win, home_score, away_score, run_differential | ✅ Complete |

**Gaps to fix:**
- `game_pitcher_context.career_era` is 0% populated — drop this column from the derived feature; we will not use career ERA (not computable from our DB reliably)
- `team_platoon_splits` has 0 rows with >20 PA — platoon split features are unreliable; exclude from feature set
- `game_lineup_snapshots.bat_side` is only 6% populated inline — always join to `player_handedness_dim` for handedness

---

## 4. Layer 2: Engineered Feature Tables (new, replace v1/v2)

All tables below are computed from Layer 1. All use `prior_completed_games_only` scope (strictly `game_date < target_game_date`, same season). No versioning — these are the canonical features.

### 4.1 `team_pregame_stats`
One row per (game_id, side). Captures team strength going into the game.

**Columns to compute (from `game_team_stats` + `labels`):**
- `season_games` — games played so far this season
- `season_win_pct` — wins / games (from labels)
- `season_run_diff_per_game` — (runs scored - runs allowed) / games
- `season_runs_scored_per_game`
- `season_runs_allowed_per_game`
- `season_batting_avg` — cumulative season avg (mean of per-game batting_avg weighted by games)
- `season_obp`
- `season_slg`
- `season_ops`
- `season_strikeouts_per_game`
- `season_walks_per_game`
- `rolling_last10_win_pct`
- `rolling_last10_runs_scored_per_game`
- `rolling_last10_runs_allowed_per_game`
- `rolling_last10_ops`
- `rolling_last10_obp`
- `rolling_last10_batting_avg`
- `days_rest` — days since last completed game (0 = doubleheader)
- `doubleheader_flag`

### 4.2 `starter_pregame_stats`
One row per (game_id, side). Probable starter's stats going into the game.

**Computed from `game_pitcher_appearances` (is_starter=1) — NOT from statsapi:**
- `probable_pitcher_id`
- `probable_pitcher_known` (0/1)
- `pitcher_hand` — from `player_handedness_dim`
- `season_starts` — starts this season before this game
- `season_era` — (9 * total_earned_runs / total_ip) from prior starts this season
- `season_whip` — (walks + hits) / ip
- `season_k_pct` — strikeouts / batters_faced
- `season_bb_pct` — walks / batters_faced
- `season_hr_per_9` — home_runs * 9 / ip
- `season_avg_allowed` — hits / (batters_faced - walks - hit_by_pitch_approx)
- `season_strike_pct` — strikes / pitches
- `season_win_pct` — wins / decisions (requires join to game results)
- `stats_available_flag` — 1 if starter known and has ≥1 prior start this season

**Fallback:** if starter unknown or 0 prior starts, set stats_available_flag=0, all stats NULL. Model handles missingness via LightGBM native null handling.

### 4.3 `bullpen_pregame_stats`
One row per (game_id, side). Season-to-date and recent fatigue for the bullpen.

**Computed from `game_pitcher_appearances` (is_reliever=1):**
- `season_bullpen_era`
- `season_bullpen_whip`
- `season_bullpen_k_pct`
- `season_bullpen_bb_pct`
- `season_bullpen_hr_per_9`
- `bullpen_outs_last1d`, `last3d`, `last5d`
- `bullpen_pitches_last1d`, `last3d`
- `relievers_used_last3d_count`
- `high_usage_relievers_last3d` (>20 pitches in last 3 days)
- `back_to_back_relievers_count`
- `season_appearances` — total reliever appearances this season

### 4.4 `lineup_pregame_context`
One row per (game_id, side). Lineup composition and handedness going into the game.  
*(Note: lineup data is post-game actual lineups for training; at inference time this is the announced pregame lineup.)*

**Computed from `game_lineup_snapshots` + `player_handedness_dim`:**
- `lineup_known_flag` — 1 if full lineup available
- `lineup_lefty_count`, `righty_count`, `switch_count` — batters by hand
- `lineup_lefty_share`, `righty_share` — fractions
- `top3_lefty_count`, `top3_righty_count` — top 3 batting order
- `opposing_starter_hand` — from starter_pregame_stats for the opponent (L/R/NULL)
- `lineup_vs_starter_hand_advantage` — % of batters with handedness advantage vs opposing starter (e.g. LHB vs RHP)

**Note on lineup quality:** `lineup_quality_metric` was 0% populated — exclude. Handedness composition is reliable and useful; quality metrics are not.

---

## 5. Layer 3: Training/Inference Row — `game_matchup_features`

One flat row per game. Joins home + away from all Layer 2 tables. This is what the model sees.

### 5a. Feature Contract

**Team strength (home + away, ~18 features each = 36 total):**
- All `team_pregame_stats` fields for home and away

**Starter quality (home + away, ~10 each = 20 total):**
- All `starter_pregame_stats` stats fields (not pitcher_id — that was the #1 source of overfitting)
- `home_starter_hand`, `away_starter_hand` as L/R/NULL encoded flags

**Bullpen (home + away, ~10 each = 20 total):**
- All `bullpen_pregame_stats` fields

**Lineup composition (home + away, ~5 each = 10 total):**
- Handedness composition + vs-starter-hand advantage

**Delta features (~15 total):**  
Home minus away for the most predictive paired stats:
- `win_pct_delta`, `run_diff_delta`, `ops_delta`, `batting_avg_delta`
- `rolling_last10_ops_delta`, `rolling_last10_win_pct_delta`
- `starter_era_delta`, `starter_k_pct_delta`, `starter_whip_delta`
- `bullpen_era_delta`, `bullpen_fatigue_delta`
- `lineup_handedness_advantage_delta`

**Contextual (~8 total):**
- `temperature_f`, `wind_speed_mph`, `wind_direction_deg`, `wind_gust_mph`
- `humidity_pct`, `precipitation_mm`, `cloud_cover_pct`
- `roof_closed_or_fixed_flag`, `weather_exposed_flag`
- `is_day`, `hour_offset_from_first_pitch`
- `home_field_advantage` (always 1 — useful as an intercept anchor)
- `season` (for temporal context, not as an ID)
- `days_rest_delta` (home days rest - away days rest)

**Excluded intentionally:**
- Raw pitcher IDs, team IDs — no generalization value
- All zero-coverage features: lineup quality, platoon splits, career ERA
- v1/v2 versioning artifacts

**Estimated total: ~110 features.** All meaningful, all verifiable, all computable at inference time.

### 5b. Labels
The primary label is `did_home_win` (binary). Store alongside the row (not in model input):
- `did_home_win` — primary model target
- `home_score`, `away_score`, `run_differential` — for secondary model targets / ensembling candidates

---

## 6. Model Strategy

### Primary Model: LightGBM
- Handles NULL natively — no imputation needed for missing starter stats
- Train on `game_matchup_features` with `did_home_win` as target
- Cross-validation: 2020-2024 dev folds (walk-forward), 2025 holdout untouched
- Tune: `num_leaves`, `learning_rate`, `min_data_in_leaf`, `feature_fraction`, `bagging_fraction`

### Secondary Models (ensemble candidates):
- **Logistic Regression** (scikit-learn): good calibration baseline, interpretable
- **XGBoost or Random Forest**: if LightGBM plateau, try for diversity
- **Run differential regression**: train a separate regression model on `run_differential` as label; use its output as a probability calibration signal or ensemble component

### Ensemble approach (post-validation):
- Train LightGBM (binary) + logistic regression (binary) + run-diff regressor
- Blend predictions weighted by holdout calibration quality
- Only pursue if individual models plateau below target

### Target: Beat 66% on 2025 holdout. Stretch: 68%+.

---

## 7. Implementation Plan for Dev

### Phase 1: Schema & Computation (no model changes)
1. Create new Layer 2 tables: `team_pregame_stats`, `starter_pregame_stats`, `bullpen_pregame_stats`, `lineup_pregame_context`
2. Write materialization scripts computing each from Layer 1, strictly prior-games-only
3. Populate all 6 seasons (2020-2025)
4. **Verification gate:** for game 661199 (Aug 15, 2022), manually verify all computed stats match expected values (Urias ERA ~2.50, Dodgers win% correct, etc.)

### Phase 2: Feature Assembly
1. Build `game_matchup_features` by joining all Layer 2 tables
2. Verify feature row for game 661199 looks correct end-to-end
3. Verify feature row for opening day 2022 (cold start) has appropriate NULLs

### Phase 3: Train & Evaluate
1. Run LightGBM baseline on new feature set
2. Compare to old baseline (54.2% dev / 52.9% holdout)
3. Run feature importance — verify no IDs in top features
4. Tune hyperparameters
5. Run logistic regression benchmark
6. Report all metrics

### Phase 4: Cleanup
1. Drop `feature_rows` table and all v1/v2 artifacts
2. Drop `team_batting_game_state` (superseded)
3. Remove old feature materialization code (v1/v2 paths)
4. Update all docs

---

## 8. Additional Features (approved additions to spec)

### 8a. Career Pitching Stats (`player_career_pitching_stats` table)
Fetch once per pitcher via `statsapi.player_stat_data(pitcher_id, type="career", group="pitching")`. Store:
- `career_era`, `career_whip`, `career_k_pct`, `career_bb_pct`, `career_avg_allowed`, `career_ip`
- One row per pitcher, refreshed rarely (career stats are stable)
- Used in `starter_pregame_stats` as fallback when season stats are unavailable (< 2 starts)
- At inference time: fetch fresh from API if not already in table

### 8b. Team vs. Starter Handedness Splits (`team_vs_hand_pregame_stats` table)
For each game G, compute team T's season-to-date offensive performance specifically against LHP starters vs RHP starters, using prior games only.

**Source:** Join `game_team_stats` (box scores) + `game_pitcher_appearances` (opponent starter, is_starter=1) + `player_handedness_dim` (pitch_hand) for games before G in same season.

**Computed columns (per team per game, split by opposing starter hand):**
- `vs_rhp_games`, `vs_lhp_games` — sample size
- `vs_rhp_ops`, `vs_lhp_ops`
- `vs_rhp_batting_avg`, `vs_lhp_batting_avg`
- `vs_rhp_runs_per_game`, `vs_lhp_runs_per_game`

**Wired into `game_matchup_features` as:**
- `home_vs_away_starter_hand_ops` — home team's OPS against away starter's hand
- `away_vs_home_starter_hand_ops` — away team's OPS against home starter's hand
- `home_handedness_matchup_advantage` — delta (home_vs_starter_hand_ops - away_vs_starter_hand_ops)
- `home_vs_starter_hand_games` — sample size (important for nulling when < 10 games)

This is the right level of granularity: team-level vs pitcher hand, not player-level. Computable from our DB. Meaningful signal. Inference-compatible via daily update.

### 8c. Platoon Splits — Fix or Exclude
The existing `team_platoon_splits` table is empty (0 rows with > 20 PA). The new `team_vs_hand_pregame_stats` above replaces this concept cleanly and is fully computable from our existing data. **Drop `team_platoon_splits` as a feature source.** The new handedness matchup features cover this need better.

---

## 9. What We Are NOT Doing
- Using odds data (contract constraint)
- Lineup quality metrics (0% populated, no reliable source)
- Player-level handedness matchup stats (too granular, sparse, not worth it)
- Any feature not computable at inference time from daily-updated raw tables
- New external API data sources beyond the career stats one-time batch fetch

---

## 9. Open Questions (resolved)
- **Pitcher stats source:** Compute from `game_pitcher_appearances` — verified accurate to within 0.01 ERA vs statsapi
- **Platoon splits:** Exclude — 0 rows with meaningful data
- **Career ERA:** Exclude — 0% populated, not reconstructable
- **Lineup quality:** Exclude — not populated; handedness composition is reliable substitute
- **Versioning:** None — single canonical feature set, no v1/v2
