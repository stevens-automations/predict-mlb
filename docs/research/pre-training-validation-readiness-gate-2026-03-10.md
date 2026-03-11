# Predict-MLB pre-training validation and readiness gate

_Date: 2026-03-10_

## Purpose

This memo defines the **minimum validation gate** that should be passed before the first serious integrated model run using:
- bullpen support
- lineup / handedness / platoon support
- weather / venue support

This is not a generic data QA list. It is the specific go / no-go gate for the first integrated training pass.

Core principle:
- **Do not start iterative training on a richer contract until the richer contract is both complete enough and parity-safe enough to trust.**

---

## What this gate is trying to prevent

The main failure modes are:
1. training on support tables that look populated overall but are selectively sparse in important slices
2. leakage or hidden train/inference mismatch from lineup timing, probable-starter hand resolution, or observed-vs-forecast weather
3. unrealistic feature availability that will not exist at live prediction time
4. silent degradation concentrated in edge cases like early season, partial lineups, retractable roofs, or games with missing venue metadata

If any of those are materially unresolved, the right answer is **not ready**.

---

## Readiness decision categories

Use three outcomes only.

### Ready
All required checks pass, or only minor documented issues remain that:
- do not break train/inference parity
- do not affect core feature families materially
- do not concentrate in a way that biases model training

### Ready with explicit degradation
Use only if:
- the degradation path is already part of the intended contract
- the missingness is bounded and measurable
- the model will see the same degraded reality at inference time

Examples:
- some games fall back from full lineup state to team platoon support
- retractable-roof ambiguity is carried as coarse exposure logic rather than falsely precise roof-open state

### Not ready
Any core check fails in a way that would make the first integrated run misleading, over-optimistic, or operationally non-reproducible.

---

## Recommended order of validation work

Do validation in this order so failures surface early and cheaply.

### 1. Structural coverage and key integrity
Confirm the support tables are populated where expected and join cleanly to the game spine.

### 2. Null / missingness review
Measure missingness globally, by season, and by critical slices.

### 3. As-of / parity realism review
Confirm every feature family reflects information that would have been available at prediction time.

### 4. Domain realism and distribution sanity
Check whether values look baseball-realistic and internally consistent.

### 5. Edge-case review
Stress the exact scenarios most likely to break live use.

### 6. Integrated contract review
Only after the support layers pass should the first integrated feature contract be frozen for training.

---

## Required validation checks

## A. Cross-family structural checks

These are mandatory across bullpen, lineup/platoon, and weather/venue.

### A1. Join coverage to target training games
For every candidate training game / side:
- bullpen support row exists or documented degraded fallback exists
- lineup/platoon support row exists or documented degraded fallback exists
- venue identity resolves cleanly
- weather row exists or documented degraded fallback exists

Required result:
- no silent join loss
- no unexplained row drops during integrated feature materialization

### A2. Key uniqueness
Verify expected primary-grain uniqueness for support tables:
- one valid bullpen game-state row per `game_id / side / as_of_ts`
- one valid lineup-state row per `game_id / side / as_of_ts`
- one valid platoon row per `game_id / side / as_of_ts / vs_pitch_hand`
- one selected weather row per `game_id / selected_as_of_ts`
- one venue row per `venue_id`

Required result:
- no duplicate selected rows at the modeling grain

### A3. Season coverage consistency
Inspect coverage by season for 2020-2025 and compare adjacent seasons.

Required result:
- no season with abrupt unexplained support collapse
- no single season materially worse without a documented source reason

---

## B. Bullpen validation checks

### B1. Coverage completeness
Inspect, by season and team-side:
- percent of games with `team_bullpen_game_state`
- percent of games with top-3 reliever summary
- percent of games with top-5 reliever summary

Required result:
- aggregate bullpen state should be essentially complete for normal completed-game history
- top-N summaries may degrade in thin early-season samples, but not due to pipeline failure

### B2. Prior-only logic realism
Validate that bullpen features are built from **prior completed games only**.

Checks:
- no same-game contamination
- no future appearances included in recent workload windows
- no post-target-game reliever usage affecting target features

Required result:
- zero parity violations in sampled audits

### B3. Value sanity
Inspect distributions and outliers for:
- bullpen outs / pitches last 1/3/5/7 days
- relievers used yesterday / last 3 days
- back-to-back counts
- freshness score
- season ERA / WHIP / K-BB proxies

Required result:
- values are nonnegative where expected
- no impossible workload spikes unless traceable to real doubleheaders / unusual usage
- freshness moves in the right direction versus recent usage

### B4. Ranking stability for top relievers
Check that top-3/top-5 reliever summaries are not wildly unstable because of tiny samples or bad ranking logic.

Required result:
- ranking method produces plausible top-N groups
- early-season instability is either bounded or explicitly degraded

---

## C. Lineup / handedness / platoon validation checks

### C1. Handedness coverage
Inspect coverage for:
- hitter bat side in lineup snapshots or handedness dimension
- probable/starting pitcher throwing hand used for platoon and matchup context

Thresholds to inspect:
- bat side missing rate overall and by season
- starter pitch hand missing rate overall and by season

Required result:
- handedness lookup is near-complete for lineup and starter matchup usage
- any residual missingness is explicit and not concentrated in one season/team slice

### C2. Snapshot realism
Validate that the lineup state used for training is the latest eligible snapshot at or before the modeling cutoff.

Checks:
- no use of final/actual lineup when only pregame announced state should be allowed
- partial and missing lineups remain partial/missing rather than being filled by hindsight
- snapshot selection rule matches intended inference behavior

Required result:
- zero sampled cases of post-cutoff lineup leakage

### C3. Lineup-state completeness and degradation
Inspect:
- rate of `full` lineup status
- rate of `partial`
- rate of `missing`
- rate of fallback-to-platoon-only behavior

Required result:
- partial/missing states are acceptable only if they are deliberately modeled as degraded states
- not acceptable if the project is silently training mostly on unavailable live information

### C4. Platoon split realism
Inspect team platoon support for:
- games in sample
- plate appearances in sample
- split metrics versus LHP / RHP
- early-season sample thinness

Required result:
- platoon rows use prior completed games only
- split sample sizes are carried honestly
- early-season thin samples are degraded, capped, or documented

### C5. Internal consistency
Check that lineup structure summaries match raw composition.

Examples:
- `L + R + S == announced_lineup_count` when lineup is full
- top-3 and top-5 handedness counts do not exceed the corresponding window
- lineup side shares sum plausibly to ~1 when defined

Required result:
- no arithmetic inconsistencies in selected rows

---

## D. Weather / venue validation checks

### D1. Venue integrity
Inspect:
- `games.venue_id` coverage
- venue coordinates present
- venue timezone present
- roof type populated
- dome/exposure logic plausible

Required result:
- missing venue metadata should be effectively zero for scheduled historical MLB games
- dome parks should not be treated as weather-exposed

### D2. Weather coverage
Inspect, by season and by venue class:
- percent of games with selected weather snapshot
- percent missing temperature, wind, precip probability, humidity, pressure
- 2025 coverage specifically, since it is known in-progress

Required result:
- weather coverage is high enough that the first integrated run is not mostly learning on a weather-missing placeholder
- residual 2025 gaps must be documented and bounded if training includes 2025

### D3. Timestamp realism
Validate that selected weather rows are aligned to the intended prediction-time cutoff and not to a postgame or hindsight state.

Checks:
- selected `as_of_ts` is at or before feature-row cutoff
- target-game hour selection is consistent around first pitch
- no mixing of newer forecast rows into older as-of feature rows

Required result:
- zero sampled timestamp leakage cases

### D4. Exposure realism
Inspect weather-exposure behavior for:
- fixed domes
- retractable roofs
- open-air parks

Required result:
- fixed domes have exposure off
- open-air parks have exposure on
- retractable roofs are treated with documented coarse logic, not false precision

### D5. Numeric sanity
Inspect realistic distributions for:
- temperature
- wind speed
- wind direction
- precipitation probability
- humidity
- pressure

Required result:
- no impossible values from unit errors or parsing mistakes
- no suspicious concentration at one default value that suggests failed API handling

---

## Null / missingness thresholds to inspect

These thresholds are for the **first serious integrated run**, not final perfection.

## 1. Hard-stop thresholds
If any of these are true, call **not ready** unless the affected family is removed from the contract.

- Any core support family has **>10% unexplained missingness** at the selected modeling grain overall
- Any season in scope has **>15% missingness** for a core family due to pipeline/data issues rather than known intentional degradation
- Starter pitch-hand missingness is **>2%** overall
- Venue identity or required venue metadata missingness is **>1%** overall
- Selected weather rows missing for **>10%** of training games if weather is included in the contract
- Full + partial lineup availability is so low that the lineup feature family is mostly hindsight-only or mostly absent without a deliberate degraded design

## 2. Review-trigger thresholds
These do not automatically fail, but they require investigation.

- Any core metric missingness worsens by **>5 percentage points** in one season versus adjacent seasons
- Any team has materially worse support coverage than league average without a baseball/source explanation
- Partial lineup rate exceeds **15-20%** in a season slice
- Weather null rate for key numeric fields exceeds **5%** for open-air parks
- Top-N reliever summaries are absent in **>5%** of non-early-season rows

## 3. Acceptable-with-degradation zone
These are usually acceptable if explicitly represented and stable.

- early-season platoon sample thinness
- partial lineup states when fallback features are still available
- retractable-roof ambiguity handled by coarse exposure flag
- limited weather gaps in late 2025 if either:
  - training excludes affected rows, or
  - weather is degraded/null in a parity-safe and documented way

---

## Parity / inference realism checks

These are mandatory. The integrated run should not proceed without them.

### P1. Feature-family runtime path exists
For every approved feature family, there must be a documented live retrieval/computation path.

Required result:
- bullpen, lineup/platoon, and weather/venue all have a concrete inference-time path
- no training-only features slip in

### P2. As-of enforcement
Check that selected support rows respect the same as-of rule intended for live prediction.

Required result:
- no post-cutoff data in training rows
- no use of finalized game-state information where only pregame state should exist

### P3. Degraded-path parity
If live predictions may run with missing lineups or missing weather, training rows must represent the same degraded state honestly.

Required result:
- no silent historical filling from richer hindsight data
- degraded indicators are carried into the contract where needed

### P4. Forecast-vs-observed weather mismatch review
Because historical weather may come from observed archive while live inference uses forecast, inspect whether this mismatch is being treated as an explicit temporary risk.

Required result:
- snapshot/source labels are preserved
- the feature family stays modest and does not pretend observed weather equals live forecast certainty

### P5. Probable-starter hand realism
If lineup/platoon context depends on starter handedness, verify the hand comes from the probable-starter path used at inference, not from postgame actual substitutions.

Required result:
- zero sampled cases of actual-postgame starter correction leaking into pregame features

---

## Edge-case checks

These are the cases most likely to produce false confidence if skipped.

### E1. Early season
Inspect first 2-4 weeks separately.

Check:
- bullpen sample instability
- platoon sample thinness
- lineup quality summaries with low underlying sample

Required result:
- early-season rows are either sufficiently regularized/degraded or documented as a known weak zone

### E2. Partial and missing lineups
Inspect games with `partial` or `missing` lineup status.

Required result:
- they route cleanly to fallback behavior
- they do not create malformed lineup aggregates

### E3. Doubleheaders / unusual scheduling
Inspect a sample of doubleheaders, postponed games, and rescheduled games.

Required result:
- as-of and recent-workload logic still behaves correctly
- same-day multiple games do not corrupt bullpen recency

### E4. Rain-shortened / weird pitcher usage games
Inspect a sample of anomalous pitching games.

Required result:
- bullpen aggregation remains plausible
- extreme but real usage is distinguishable from parser bugs

### E5. Roof/weather ambiguity
Inspect retractable-roof parks and dome parks explicitly.

Required result:
- no obvious exposure miscoding
- roof ambiguity is documented as coarse logic rather than ignored

### E6. Team / venue relocations or naming changes
Inspect any rows where venue naming or metadata may have changed.

Required result:
- stable venue keying still works
- no venue-join loss due to name changes

---

## Pass / fail structure

Use this exact gate structure.

## Gate 1 — Structural integrity
Pass if:
- no silent join loss
- no duplicate selected rows at model grain
- season coverage is stable enough to proceed

Fail if:
- integrated materialization drops rows unexpectedly
- support-table joins are unreliable

## Gate 2 — Missingness and completeness
Pass if:
- missingness stays under hard-stop thresholds
- concentrated missingness is explained and acceptable

Fail if:
- any core family breaches hard-stop thresholds
- support gaps are concentrated in a way that biases training

## Gate 3 — Parity realism
Pass if:
- all sampled audits confirm prior-only / pregame-only logic
- degraded states are represented honestly
- no training-only information is used

Fail if:
- lineup leakage, bullpen future leakage, or starter/weather timestamp leakage is found

## Gate 4 — Domain sanity
Pass if:
- values are baseball-realistic
- exposure logic and sample-size logic behave plausibly

Fail if:
- parser/unit bugs or unrealistic distributions remain unresolved

## Gate 5 — Edge-case resilience
Pass if:
- early season, partial lineups, roof ambiguity, and scheduling oddities have documented behavior and no critical breakage

Fail if:
- edge cases materially change the contract or cause hidden corruption

Final decision:
- **Ready** only if all five gates pass
- **Ready with explicit degradation** only if the degradation is already part of the intended live contract and remains under threshold
- otherwise **Not ready**

---

## Minimum evidence package before saying Ready

Before training, there should be one compact validation summary that includes:
- coverage table by season for each feature family
- null/missingness table by season and key slice
- sampled parity audit examples
- sampled edge-case audit examples
- explicit known residual risks
- final go / no-go decision

If that evidence package does not exist, the project is not truly ready even if the data probably is.

---

## Recommended call for the current checkpoint

Based on current repo context, the biggest likely pre-training risk areas are:

1. **Weather completion and weather parity**
   - 2025 weather coverage is still described as in progress
   - historical observed weather versus live forecast weather remains an explicit realism risk

2. **Lineup snapshot realism**
   - the project must prove that historical lineup states reflect pregame available information rather than final realized lineups

3. **Early-season platoon and top-reliever instability**
   - these are likely valid but noisy; they need explicit degradation/regularization rather than blind trust

4. **Retractable-roof ambiguity**
   - likely acceptable for first pass, but only with documented coarse handling

5. **Concentrated missingness hidden by good overall totals**
   - season/team/venue slices matter more than one aggregate completion number

My recommendation:
- do **not** greenlight the first integrated model run until the project can show a compact validation report against these gates.
- If weather remains materially incomplete, either:
  - finish weather validation first, or
  - deliberately exclude weather from the first integrated contract rather than pretending the family is ready.

---

## Nice-to-have checks that can wait

These are valuable, but they should not block the first serious integrated run if the required gates pass.

- richer park-factor or park-physics interactions
- advanced wind-direction transformation into out/in/cross buckets beyond basic sanity
- player-level lineup quality warehouse expansion
- closer/setup-man role inference refinement beyond stable top-N summaries
- nuanced weather-confidence weighting by source quality
- second-order interaction audits like lineup-handedness x weather x venue
- model-uplift ablations by subfamily beyond the first basic comparison

The first objective is not maximum sophistication. It is a **trustworthy, parity-safe integrated baseline**.
