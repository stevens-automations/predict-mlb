# Training & Preprocessing Spec
Last updated: 2026-03-18

## Core Principle
This is a head-to-head binary prediction task: did the home team win?
Everything should be framed comparatively. We are NOT predicting team performance against an absolute benchmark — we are predicting which team is superior in this specific matchup.

## Feature Philosophy
- **Delta features are preferred** over raw home/away pairs for symmetric stats
  - Example: use `run_diff_per_game_delta` (home - away) instead of both `home_run_diff` and `away_run_diff`
  - Sign convention: positive delta = home team advantage for that metric
  - ERA, WHIP (lower=better): delta = home - away, so negative = home pitcher better
  - OPS, win%, run diff (higher=better): delta = home - away, so positive = home team better
- **Keep raw home/away only when asymmetric:** weather (affects both equally), bullpen fatigue counts, handedness counts
- **Context features:** weather, venue, home_field_advantage — keep as-is

## Training Sample Filtering
Exclude games from training where data is insufficient:
1. Either team has < 15 prior games this season (cold-start, stats not meaningful)
2. Either starter ERA is NULL AND `starter_stats_available_flag = 0` (unknown starter, no fallback)
   - Note: NULL ERA with known pitcher and ≥1 start is OK (LightGBM handles native NULL)

Do NOT filter holdout (2025) — we must evaluate on full holdout for honest comparison.

## Feature Set Design
### Drop entirely (zero or near-zero signal confirmed):
- `home_field_advantage` (constant 1, no variance)
- `home_lineup_known_flag`, `away_lineup_known_flag` (NaN correlation)
- `home_starter_probable_pitcher_known`, `away_starter_probable_pitcher_known` (NaN correlation)
- `is_day` (0.001 correlation)
- `home_team_season_strikeouts_per_game` (0.001 correlation)
- `home_bullpen_outs_last3d`, `away_bullpen_outs_last3d` (covered by delta)

### Feature structure — full redesigned set:

**Team record (delta form — home minus away):**
- `win_pct_delta`
- `run_diff_per_game_delta`
- `ops_delta`
- `batting_avg_delta`
- `obp_delta` (add if not already present)
- `rolling_last10_win_pct_delta`
- `rolling_last10_ops_delta`
- `rolling_last10_runs_scored_delta` (add)

**Team record (keep raw — for nonlinear absolute quality signal):**
- `home_team_season_win_pct`, `away_team_season_win_pct`
- `home_team_season_run_diff_per_game`, `away_team_season_run_diff_per_game`
- `home_team_season_ops`, `away_team_season_ops`

**Starter (delta form):**
- `starter_era_delta` (home_era - away_era; negative = home starter better)
- `starter_k_pct_delta`
- `starter_whip_delta`

**Starter (raw — important for absolute quality):**
- `home_starter_era`, `away_starter_era`
- `home_starter_k_pct`, `away_starter_k_pct`
- `home_starter_season_starts`, `away_starter_season_starts` (sample size)

**Bullpen:**
- `bullpen_era_delta`
- `home_bullpen_era`, `away_bullpen_era` (absolute quality)
- `bullpen_fatigue_outs_last3d_delta`
- `home_bullpen_pitches_last3d`, `away_bullpen_pitches_last3d` (raw fatigue signal)

**Handedness matchup:**
- `vs_starter_hand_ops_delta` (home team OPS vs opposing starter hand, minus away team OPS vs opposing starter hand)
- `home_vs_starter_hand_ops`, `away_vs_starter_hand_ops` (raw)
- `home_vs_starter_hand_games`, `away_vs_starter_hand_games` (sample size — quality gate)

**Lineup composition:**
- `home_lineup_lefty_share`, `away_lineup_lefty_share`
- `home_lineup_righty_share`, `away_lineup_righty_share`

**Days rest:**
- `days_rest_delta`
- `home_team_days_rest`, `away_team_days_rest`
- `home_team_doubleheader_flag`, `away_team_doubleheader_flag`

**Weather (raw — affects both teams):**
- `temperature_f`, `wind_speed_mph`, `wind_direction_deg`, `wind_gust_mph`
- `humidity_pct`, `precipitation_mm`, `cloud_cover_pct`
- `roof_closed_or_fixed_flag`, `weather_exposed_flag`

**Starter hand matchup flags:**
- `home_starter_hand_l_flag`, `home_starter_hand_r_flag`
- `away_starter_hand_l_flag`, `away_starter_hand_r_flag`

## Preprocessing
- **LightGBM:** No scaling needed. Handle NULLs natively (do NOT impute).
- **Logistic Regression:** StandardScaler on all features; fill NULLs with 0 before scaling.
- **No one-hot encoding needed** — LightGBM handles all numeric features directly.

## CV / Evaluation Protocol
- Dev folds (walk-forward): [2020-21→2022], [2020-22→2023], [2020-23→2024]
- Holdout: 2025 (never train on this)
- Filter training folds only (not holdout) for < 15 games threshold
- Report: accuracy + log_loss for each fold, aggregate, and holdout

## Labels
- Primary: `did_home_win` (binary, 1 = home team won)
- Secondary (store but don't train on yet): `run_differential` for future regression model

## Target Accuracy
- Old model (44 features, live API): 66%
- Current new model: 55.76%
- Gap to close: ~10pp
- Immediate target: 60%+
- Stretch: 66%+
