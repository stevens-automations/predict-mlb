# Pre-training validation report: first integrated model run (`v2_phase1`)

_Date: 2026-03-10_
_DB audited: `data/mlb_history.db`_

## Overall outcome

**Not ready**

Reason in one line:
- **Structural row coverage is mostly present, and bullpen support looks materially real, but lineup/handedness/platoon support is still placeholder-grade and weather still has a forecast-vs-archive parity caveat even after de-scoping `precipitation_probability` from the first-pass contract.**

---

## Gate summary

| Gate | Result | Notes |
|---|---|---|
| Gate 1 — Structural integrity | **Fail** | Join coverage is strong overall, but venue join loss exists for 4 games (venue_id `5340` absent from `venue_dim` in 2023-2024). |
| Gate 2 — Missingness / completeness | **Fail** | Bullpen mostly passes; lineup/handedness/platoon fails materially; weather no longer blocks on precipitation-probability completeness once that field is removed from the first-pass contract. |
| Gate 3 — Parity realism | **Fail** | Bullpen prior-only scope looks good; lineup is entirely `fallback` and does not prove pregame announced-state realism; weather is `observed_archive` only, not forecast-like parity. |
| Gate 4 — Domain sanity | **Fail** | Lineup arithmetic is internally inconsistent in all rows (`full` but handedness counts all zero). Platoon rows are fully zero/null. |
| Gate 5 — Edge-case resilience | **Warn / Fail** | Early-season bullpen degradation looks explainable, but retractable/unknown roof handling is still coarse and lineup/weather blockers remain unresolved. |

---

## 1) Structural coverage / join integrity

### Green lights
- Support-table row counts are present at the expected game grain for completed games (`Final` + `Completed Early`) from 2020-2025.
- No duplicate rows at the declared modeling grains:
  - `team_bullpen_game_state`: 0 duplicates
  - `team_lineup_game_state`: 0 duplicates
  - `team_platoon_splits`: 0 duplicates
  - `game_weather_snapshots`: 0 duplicates
  - `venue_dim`: 0 duplicates
- Full game-level support coverage exists for all completed games in:
  - bullpen state
  - lineup state
  - platoon rows
  - weather snapshots

### Structural gaps
- `venue_id` join loss exists for **4 completed games** due to missing `venue_dim` row for `venue_id = 5340`:
  - 2023-04-29 game `718384`
  - 2023-04-30 game `718368`
  - 2024-04-27 game `746560`
  - 2024-04-28 game `746561`
- This causes integrated full-family coverage to fall slightly below 100% in 2023 and 2024:
  - 2023: `2469 / 2471` fully covered
  - 2024: `2470 / 2472` fully covered

### Interpretation
- **The pipeline is populating rows, but row existence alone is overstating readiness.** Structural completeness is real for bullpen/weather, but lineup/platoon population currently includes placeholder rows that should not be mistaken for feature-ready support.

---

## 2) Missingness / completeness by family

## A. Bullpen

### Coverage / completeness
- Row coverage is effectively complete at the game-side grain in every season.
- Real-value coverage is high but not perfect:
  - `bullpen_era_season` present in ~98.3% of 2020 rows and ~99.39% in 2021-2025
  - `freshness_score` present at the same rates
- Missing bullpen quality rows are concentrated at season start:
  - 2021-04-01: 26 missing rows
  - 2022-04-07/08: 30 missing rows total
  - 2023-03-30: 30 missing rows
  - 2024-03-28/29 plus Seoul opener: 30 missing rows total
  - 2025-03-27/28 plus Seoul opener: 28 missing rows total

### Top reliever summaries
- `top_n = 3` coverage is complete structurally.
- `n_available >= 3` rates:
  - 2020: 98.0%
  - 2021-2025: ~99.3%
- `top_n = 5` is slightly weaker but still high (~98.9-99.0% in 2021-2025).

### Readiness view
- **Bullpen is the only new family that looks genuinely close to training-ready**, with expected early-season degradation rather than obvious pipeline collapse.

## B. Lineup / handedness / platoon

### Hard blockers
- `team_lineup_game_state` is **100% `snapshot_type='fallback'`**.
- `game_lineup_snapshots` is also **100% `snapshot_type='fallback'`**.
- `lineup_status='full'` for all rows, but:
  - `lineup_l_count = lineup_r_count = lineup_s_count = 0` in every sampled row
  - `top3_*` and `top5_*` handedness counts are all zero
  - all share proxies are `0.0`
  - `lineup_quality_mean`, `top3_lineup_quality_mean`, `top5_lineup_quality_mean`, `lineup_vs_rhp_quality`, `lineup_vs_lhp_quality` are **100% null**
- Internal consistency check fails everywhere:
  - for every row with `announced_lineup_count = 9`, `L + R + S != 9`
  - observed failure count by season equals total rows in every season

### Handedness completeness
- `player_handedness_dim` has **2,887 players**, but:
  - `bat_side` known: **0**
  - `pitch_hand` known: **0**
- `game_lineup_snapshots.bat_side` known rate: **0% across all seasons**
- probable starter pitch hand coverage via `player_handedness_dim`: **0% across all seasons**

### Platoon completeness
- `team_platoon_splits` is structurally present for every game-side-hand row, but values are placeholder-grade:
  - `games_in_sample > 0`: **0 rows**
  - `plate_appearances > 0`: **0 rows**
  - `ops` non-null: **0 rows**
  - same pattern across every season 2020-2025

### Readiness view
- **This family fails the gate outright.**
- It is not merely degraded; it is currently **non-functional as a predictive support family** while still advertising itself as present (`full`, `known_flag=1`). That is dangerous because it can create false confidence and silent train/inference distortion.

## C. Weather / venue

### Green lights
- Weather row coverage by game is high:
  - 2020: `951 / 951`
  - 2021: `2466 / 2466`
  - 2022: `2470 / 2470`
  - 2023: `2469 / 2471`
  - 2024: `2470 / 2472`
  - 2025: `2477 / 2477`
- Venue metadata table itself is fully populated for the rows it contains:
  - 42 venues
  - timezone/coordinates/roof/exposure present for all 42
- Timestamp alignment looks clean in current data:
  - `as_of_ts > target_game_ts`: 0 rows
  - `as_of_ts > games.scheduled_datetime`: 0 rows

### Removed from first-pass contract
- `precipitation_probability` is **100% null in every season**, including open-air venues.
- That means it should stay out of the first-pass modeling contract rather than forcing fake historical parity.

### Additional realism risk
- All weather rows are:
  - `snapshot_type='observed_archive'`
  - `source='open_meteo_archive'`
- That means the current historical weather family is **observed-only**, while live inference is expected to depend on a forecast path.
- This is explicitly the parity risk called out in the gate memo, and it is still unresolved in the support-table state.

### Roof/exposure caveat
- Roof typing is still coarse:
  - `open`: 31 venues
  - `retractable`: 7 venues
  - `unknown`: 3 venues
  - `fixed_dome`: 1 venue
- Coarse exposure handling is acceptable as degradation, but the `unknown` bucket is still a residual risk and should be documented if weather remains in scope.

---

## 3) Parity realism observations

## Bullpen parity
- Good signs:
  - `stats_scope != 'prior_completed_games_only'`: **0 rows** in bullpen support
  - scheduled-time leakage check found no obvious postgame timestamp violations
- Assessment:
  - **Bullpen parity looks plausibly safe** for first-pass training.

## Lineup parity
- Current state does **not** prove lineup realism.
- Everything is `fallback`, all raw snapshot rows are `fallback`, and support rows mark lineups as `full`/known without carrying actual handedness or quality content.
- Assessment:
  - **Lineup parity is not validated.** At best this family is a placeholder shell, not a real pregame snapshot layer.

## Starter-hand / platoon parity
- Since pitcher hand is 100% missing in `player_handedness_dim` and platoon rows are 100% zero/null, the project cannot currently demonstrate the intended “team vs opposing probable starter hand” runtime path.
- Assessment:
  - **Parity path not established.**

## Weather parity
- Timestamp discipline looks fine.
- Source realism does not:
  - historical training data is archive-observed only
  - live inference would be forecast-based
- Assessment:
  - **Weather remains parity-risky unless either (a) forecast-like historical snapshots are built/validated, or (b) weather is excluded from the first integrated training run.**

---

## 4) Major edge-case risks

1. **Opening-day / first-series bullpen thinness**
   - Real but bounded.
   - Current nulls cluster exactly where expected early in each season.
   - This is acceptable only as explicit degradation.

2. **Lineup family looks complete when it is not**
   - The most serious operational risk.
   - `lineup_known_flag=1`, `lineup_status='full'`, and `announced_lineup_count=9` create an illusion of readiness while all useful composition fields remain zero/null.

3. **Starter-hand dependency is broken**
   - If the contract depends on platoon matching to opposing starter hand, the current DB state cannot support it.

4. **Observed-weather vs live-forecast mismatch**
   - Still unresolved.
   - This does not automatically forbid weather forever, but it does block calling the current integrated family parity-safe.

5. **Venue/roof edge cases**
   - 4 games lose venue join due to missing venue `5340`.
   - `unknown` roof types and retractable-roof coarse handling remain nonzero residual risks.

---

## 5) Recommended outcome

## Recommended label

**Not ready**

## Why this is not just “Ready with explicit degradation”

Because the current failures are not bounded degradations of an otherwise working family. Two core parts of the intended contract are not actually feature-ready:
- lineup / handedness / platoon is structurally populated but substantively empty
- weather includes a contract-required field with 100% nulls and still has unresolved observed-vs-forecast parity risk

That goes beyond acceptable degradation.

---

## 6) Specific blockers / exclusions

## Blockers that must be resolved before training `v2_phase1` as currently defined

1. **Lineup/handedness/platoon support is not ready**
   - Populate real handedness values
   - Populate real platoon sample and split metrics
   - Stop marking placeholder rows as `full`/known in a way that mimics actual lineup availability
   - Validate internal lineup arithmetic and fallback semantics

2. **Weather parity caveat**
   - `precipitation_probability` should remain out of the first training contract unless a real historical source is proven
   - and the team still needs an explicit decision on whether archive-observed vs live-forecast weather is acceptable for this first pass

3. **Venue join cleanup**
   - Add/fix `venue_dim` entry for `venue_id = 5340`

## Acceptable temporary exclusions if the team wants to train sooner

If the goal is to get a first integrated run moving quickly, the most defensible narrower path is:
- **Proceed with `v1 + bullpen` only**, or
- **Proceed with `v1 + bullpen + coarse venue static fields`**, while explicitly excluding:
  - lineup/handedness/platoon block
  - weather numeric block unless the archive-vs-forecast parity caveat is accepted

---

## Short recommendation

- **Green light:** bullpen support is close enough to use with explicit early-season degradation.
- **Red lights:** lineup/handedness/platoon and weather parity realism beyond the narrowed first-pass weather contract.
- **Best next step:** freeze the narrower weather contract now, then either train without numeric weather or explicitly accept the remaining archive-vs-forecast caveat before integrated training.
