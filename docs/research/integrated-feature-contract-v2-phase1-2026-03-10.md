# Integrated feature contract recommendation: `v2_phase1`

_Date: 2026-03-10_

## Recommendation in one line

Ship the first serious integrated model feature version as **`v2_phase1`**: keep the proven `v1` team/starter spine, then add a **compact bullpen block**, a **practical lineup/platoon block**, and a **coarse venue/weather block** with explicit degraded handling and no train/inference asymmetry.

This memo is intentionally narrow: it is the contract to freeze before retraining, not a maximal wishlist.

---

## 1) Feature version name / scope

## Recommended version name

**`v2_phase1`**

## Scope

`v2_phase1` should equal:
- the existing parity-safe `v1` feature set
- plus bullpen support from:
  - `team_bullpen_game_state`
  - `team_bullpen_top_relievers`
- plus lineup/platoon support from:
  - `team_lineup_game_state`
  - `team_platoon_splits`
  - starter hand resolved via `player_handedness_dim`
- plus venue/weather support from:
  - `venue_dim`
  - `game_weather_snapshots`

## Contract goal

This version should be the **first training-ready integrated contract** that is:
- materially richer than `v1`
- still simple enough to validate end to end
- robust to missing lineup/weather coverage
- point-in-time safe for both backfill and live inference

## Design rule

For `v2_phase1`, prefer:
- compact summaries
- sample-size fields
- matchup deltas
- missingness flags

over:
- deep player-level feature blobs
- many redundant raw home/away fields
- clever but fragile interactions

---

## 2) Bullpen feature block

## Primary source tables

### `team_bullpen_game_state`
Use these fields as the core team-pen block:
- `season_games_in_sample`
- `bullpen_pitchers_in_sample`
- `bullpen_appearances_season`
- `bullpen_outs_season`
- `bullpen_era_season`
- `bullpen_whip_season`
- `bullpen_runs_per_9_season`
- `bullpen_k_rate_season`
- `bullpen_bb_rate_season`
- `bullpen_k_minus_bb_rate_season`
- `bullpen_hr_rate_season`
- `bullpen_outs_last1d`
- `bullpen_outs_last3d`
- `bullpen_outs_last5d`
- `bullpen_outs_last7d`
- `bullpen_pitches_last1d`
- `bullpen_pitches_last3d`
- `bullpen_pitches_last5d`
- `bullpen_appearances_last3d`
- `bullpen_appearances_last5d`
- `relievers_used_yesterday_count`
- `relievers_used_last3d_count`
- `relievers_back_to_back_count`
- `relievers_2_of_last3_count`
- `high_usage_relievers_last3d_count`
- `freshness_score`

### `team_bullpen_top_relievers`
Use **`top_n = 3` only** in `v2_phase1`.

Fields to use:
- `n_available`
- `topn_appearances_season`
- `topn_outs_season`
- `topn_era_season`
- `topn_whip_season`
- `topn_runs_per_9_season`
- `topn_k_rate_season`
- `topn_bb_rate_season`
- `topn_k_minus_bb_rate_season`
- `topn_outs_last3d`
- `topn_pitches_last3d`
- `topn_appearances_last3d`
- `topn_back_to_back_count`
- `topn_freshness_score`
- `quality_dropoff_vs_team`

## Recommendation

For the first integrated run, use:
- **aggregate bullpen quality + aggregate workload/freshness**
- **top-3 reliever quality/freshness**
- **skip top-5 as direct model inputs for now**

Why:
- aggregate-only is too lossy
- aggregate + top-3 captures both overall pen state and late-inning leverage talent
- adding top-5 immediately is probably redundant and raises collinearity/noise without much first-pass upside

---

## 3) Lineup / platoon feature block

## Primary source tables

### `team_lineup_game_state`
Use these fields:
- `lineup_known_flag`
- `announced_lineup_count`
- `lineup_l_count`
- `lineup_r_count`
- `lineup_s_count`
- `top3_l_count`
- `top3_r_count`
- `top3_s_count`
- `top5_l_count`
- `top5_r_count`
- `top5_s_count`
- `lineup_lefty_pa_share_proxy`
- `lineup_righty_pa_share_proxy`
- `lineup_switch_pa_share_proxy`
- `lineup_balance_score`
- `lineup_quality_metric`
- `lineup_quality_mean`
- `top3_lineup_quality_mean`
- `top5_lineup_quality_mean`
- `lineup_vs_rhp_quality`
- `lineup_vs_lhp_quality`
- `snapshot_type`
- `lineup_status`

### `team_platoon_splits`
For each side, use the row matching the **opposing probable starter hand**.

Use these fields:
- `games_in_sample`
- `plate_appearances`
- `batting_avg`
- `obp`
- `slg`
- `ops`
- `runs_per_game`
- `strikeout_rate`
- `walk_rate`

### `player_handedness_dim`
Use only to resolve the opposing probable starter hand when not already carried elsewhere.

Recommended resolved field for the contract:
- `opposing_starter_pitch_hand` in `{L, R, null}`

Do not expose large numbers of raw player-handedness fields directly in `v2_phase1`.

## Recommendation

For `v2_phase1`, the lineup/platoon block should be:
1. lineup availability / structure
2. lineup handedness balance and top-of-order handedness
3. lineup-vs-starter-hand quality summary when available
4. team-vs-opposing-hand platoon split fallback/support

This should **not** depend on full player-level offense support being perfect.

---

## 4) Venue / weather feature block

## Primary source tables

### `venue_dim`
Use these fields:
- `venue_id`
- `roof_type`
- `weather_exposure_default`
- `timezone` for audit / alignment only, not direct modeling

### `game_weather_snapshots`
Use the selected snapshot aligned to the row `as_of_ts`.

Fields to use:
- `snapshot_type`
- `source`
- `hour_offset_from_first_pitch`
- `temperature_f`
- `humidity_pct`
- `pressure_hpa`
- `precipitation_mm`
- `wind_speed_mph`
- `wind_gust_mph`
- `wind_direction_deg`
- `weather_code`
- `cloud_cover_pct`
- `is_day`
- `weather_exposure_flag`

Out of first-pass contract:
- `precipitation_probability`
  Forecast snapshots may retain the Open-Meteo `0-100` percent value for audit/operational use, but archive history does not supply a reliable parity-safe series, so `v2_phase1` should not model against it.

## Recommendation

For `v2_phase1`, keep weather/venue coarse:
- roof / exposure context
- temperature
- wind
- precipitation amount at the aligned hour
- humidity / pressure
- cloud cover / day-night context

Do **not** try to ship advanced weather physics or park-adjusted composites yet.

---

## 5) Direct inputs vs matchup deltas vs derived flags

## A. Direct model inputs

Use these as direct home/away-side features where raw magnitude matters.

### Bullpen direct inputs
- home/away `bullpen_era_season`
- home/away `bullpen_whip_season`
- home/away `bullpen_k_minus_bb_rate_season`
- home/away `bullpen_hr_rate_season`
- home/away `freshness_score`
- home/away `bullpen_outs_last3d`
- home/away `bullpen_pitches_last3d`
- home/away `relievers_back_to_back_count`
- home/away `high_usage_relievers_last3d_count`
- home/away `top3_freshness_score`
- home/away `top3_k_minus_bb_rate_season`
- home/away `quality_dropoff_vs_team`
- home/away bullpen sample-size fields (`season_games_in_sample`, `bullpen_appearances_season`, `n_available`)

### Lineup/platoon direct inputs
- home/away `lineup_known_flag`
- home/away `announced_lineup_count`
- home/away `lineup_lefty_pa_share_proxy`
- home/away `lineup_righty_pa_share_proxy`
- home/away `lineup_switch_pa_share_proxy`
- home/away `lineup_balance_score`
- home/away `top3_l_count`, `top3_r_count`, `top3_s_count`
- home/away resolved platoon row metrics vs opposing hand:
  - `ops`
  - `runs_per_game`
  - `strikeout_rate`
  - `walk_rate`
  - `games_in_sample`
  - `plate_appearances`
- home/away lineup quality fields only if present and trusted:
  - `lineup_quality_mean`
  - `top3_lineup_quality_mean`
  - `lineup_vs_opp_starter_hand_quality`

### Venue/weather direct inputs
These are game-level, not side-level:
- `temperature_f`
- `wind_speed_mph`
- `wind_gust_mph`
- `wind_direction_deg`
- `precipitation_mm`
- `humidity_pct`
- `pressure_hpa`
- `cloud_cover_pct`
- `is_day`
- `roof_type`
- `weather_exposure_flag`
- `hour_offset_from_first_pitch`

## B. Matchup deltas

These should be explicitly materialized because they are likely more learnable than separate home/away raw fields alone.

### Bullpen deltas
- `bullpen_era_delta = away - home`
- `bullpen_whip_delta = away - home`
- `bullpen_k_minus_bb_rate_delta = home - away`
- `bullpen_hr_rate_delta = away - home`
- `bullpen_freshness_delta = home - away`
- `bullpen_outs_last3d_delta = away - home` interpreted as rest edge for home
- `bullpen_pitches_last3d_delta = away - home` interpreted as rest edge for home
- `bullpen_back_to_back_delta = away - home`
- `bullpen_high_usage_delta = away - home`
- `top3_freshness_delta = home - away`
- `top3_quality_delta = home - away` using K-BB / WHIP / RP9 block

### Lineup/platoon deltas
- `lineup_balance_delta = home - away`
- `lineup_lefty_share_delta = home - away`
- `lineup_righty_share_delta = home - away`
- `top3_lefty_count_delta = home - away`
- `home_lineup_vs_opp_hand_ops_minus_away_lineup_vs_opp_hand_ops`
- `home_lineup_vs_opp_hand_runs_per_game_minus_away_...`
- `home_lineup_vs_opp_hand_walk_rate_minus_away_...`
- `home_lineup_vs_opp_hand_strikeout_rate_minus_away_...` (reverse-sign if desired, but keep one convention)
- `lineup_quality_delta` when quality exists for both sides
- `top3_lineup_quality_delta` when available

### Weather-derived matchup deltas
Do not create home/away weather deltas. Weather is shared game context.

## C. Derived flags

These should be explicit binary/categorical indicators.

### Bullpen flags
- `home_bullpen_low_sample_flag`
- `away_bullpen_low_sample_flag`
- `home_top3_availability_low_flag`
- `away_top3_availability_low_flag`
- `home_bullpen_fatigue_flag`
- `away_bullpen_fatigue_flag`

Suggested first-pass thresholds:
- low sample if `bullpen_appearances_season < 15`
- top3 low availability if `n_available < 3`
- fatigue if `freshness_score` below agreed threshold or `relievers_back_to_back_count >= 2`

### Lineup/platoon flags
- `home_lineup_known_flag`
- `away_lineup_known_flag`
- `home_lineup_partial_flag`
- `away_lineup_partial_flag`
- `home_lineup_quality_available_flag`
- `away_lineup_quality_available_flag`
- `home_platoon_low_sample_flag`
- `away_platoon_low_sample_flag`
- `opposing_starter_hand_known_flag_home_offense`
- `opposing_starter_hand_known_flag_away_offense`

### Venue/weather flags
- `weather_available_flag`
- `weather_forecast_flag`
- `weather_observed_archive_flag`
- `roof_closed_or_fixed_flag`
- `weather_exposed_flag`
- `windy_flag`
- `extreme_temp_flag`

Suggested first-pass thresholds:
- windy: `wind_speed_mph >= 12`
- extreme temp: `< 45F or > 85F`

---

## 6) Missingness / fallback / degraded policy

## Hard rule

`v2_phase1` should **never silently skip a game** because one support family is missing.

Rows should still materialize, but with:
- null-safe missing features
- explicit availability/sample flags
- `source_contract_status` and `source_contract_issues_json` updated consistently

## By block

### Bullpen policy
Bullpen support is treated as **expected core support**.

If missing:
- keep the row
- set bullpen metrics to `null`
- set bullpen availability flags to `0`
- mark the row degraded

If sample is thin:
- keep numeric fields when they exist
- set low-sample flags rather than zero-filling quality metrics

### Lineup/platoon policy
Lineups are **valuable but degradable**.

If lineup snapshot missing:
- `lineup_known_flag = 0`
- lineup-structure metrics become `null` except count/known fields
- use `team_platoon_splits` matched to opposing starter hand when available
- mark row valid if platoon fallback exists; degraded if both lineup and platoon support are weak/missing

If lineup exists but lineup quality fields are null:
- still use structure + handedness + platoon support
- do not mark degraded solely because `lineup_quality_mean` is unavailable
- carry `lineup_quality_available_flag = 0`

If opposing starter hand is unknown:
- resolved platoon-matchup fields become `null`
- keep lineup structure fields
- mark degraded only if this meaningfully removes most of the lineup/platoon block

### Venue/weather policy
Weather is **secondary but useful**.

If weather snapshot missing:
- keep venue static fields (`roof_type`, `weather_exposure_flag` if derivable from venue)
- set numeric weather fields to `null`
- set `weather_available_flag = 0`
- do **not** fail the row

If historical row uses `observed_archive` and live uses `forecast`:
- carry snapshot-type/source flags in the payload for auditability
- accept this in `v2_phase1`, but treat it as a documented model-risk note
- do not force probability-based precipitation parity when the historical path only supports `precipitation_mm`

## Row-status recommendation

Suggested row-status logic:
- `valid`: core v1 spine present; bullpen present or lightly missing; lineup/platoon has at least lineup structure or platoon fallback; weather optional
- `degraded`: core v1 spine present but bullpen missing, or lineup+platoon block mostly unavailable, or starter-hand resolution missing where platoon block depends on it
- `invalid`: only if required v1 spine is broken, same as baseline contract philosophy

---

## 7) Features to exclude for now

These should stay out of `v2_phase1` even if some raw data exists.

### Bullpen exclusions
- top-5 reliever block as direct inputs
- individual reliever IDs / names
- inferred leverage or manager-trust scores
- same-day intra-doubleheader bullpen stress logic beyond current coarse freshness fields

### Lineup/platoon exclusions
- raw `game_lineup_snapshots` slot-by-slot player IDs as model inputs
- large player-level offensive feature vectors
- injury/star-missing logic that depends on a separate reliable player-availability warehouse
- exact batting-order embeddings or sequence models
- using final actual lineup for historical rows when only fallback snapshots exist

### Venue/weather exclusions
- `precipitation_probability` as a first-pass model feature
- park factor composites not already locally validated
- wind-in/out-to-center transforms
- air density composite score
- retractable-roof open/closed inference beyond the current coarse venue exposure logic
- StatsAPI free-text weather/wind as direct numeric model inputs

### Global exclusions
- odds inputs
- end-of-season player/team aggregates
- any feature that cannot be reproduced from the same pregame cutoff path used at inference

---

## 8) Optional later upgrades

If `v2_phase1` validates cleanly, the best next upgrades are:

1. **Add top-5 reliever block selectively** if it improves over top-3 without redundancy.
2. **Promote lineup quality summaries** once player-offense support exists as a clean as-of layer.
3. **Add richer starter-vs-lineup interactions** beyond simple platoon matching.
4. **Upgrade weather from coarse numeric block to physics-aware derived features** after parity is proven.
5. **Add same-day doubleheader bullpen stress / earlier-game-completed logic** if operationally needed.
6. **Add park-factor fields** only once they are versioned and locally auditable.
7. **Train a secondary run-margin model** after the first integrated win-probability model is stable.

---

## 9) Recommended top 10 feature groups for `v2_phase1`

These are the 10 groups I would explicitly prioritize in the first integrated run:

1. `v1` stable team-strength deltas and rolling form backbone
2. starter-context backbone already in `v1`
3. aggregate bullpen quality block
4. aggregate bullpen freshness / workload block
5. top-3 reliever quality block
6. top-3 reliever freshness / availability block
7. lineup known / partial / handedness structure block
8. team offense vs opposing starter hand block
9. lineup-vs-opposing-hand quality block when available
10. coarse venue/weather environment block

---

## 10) Biggest open decisions

Only a few decisions still matter for freezing `v2_phase1`:

1. **Do we include lineup quality numeric summaries now, or only structure + platoon fallback?**
   - Lean: include them only if null rates and semantics are clean; otherwise keep the fields in schema but exclude from first training run.

2. **What exact low-sample thresholds should gate bullpen/platoon flags?**
   - Lean: make them simple and fixed for this version, then tune later.

3. **How much 2025 weather incompleteness is acceptable before training?**
   - Lean: small, explicit, auditable residual gaps are fine; confusing partial coverage is not.

4. **Do we train with historical `observed_archive` weather in `v2_phase1`, or hold weather out of the first integrated training pass if parity risk still looks material?**
   - Lean: keep weather in the contract, but be willing to ablate it from the first training experiment if it creates more risk than value.

5. **Do we materialize both raw home/away fields and deltas, or mostly deltas plus a few raws?**
   - Lean: keep both for the first pass, but bias the new block toward deltas and flags.

---

## Final recommendation

Freeze **`v2_phase1`** as the first serious integrated contract with:
- existing `v1` base unchanged
- bullpen = aggregate + top-3 only
- lineup/platoon = structure + opposing-hand split support, with quality summaries opportunistic not blocking
- weather/venue = coarse numeric/context block only
- explicit deltas, sample fields, and availability flags
- no player-level blobs, no advanced park/weather transforms, no odds

That is the best balance of realism, signal, and validation simplicity for the next model version.
