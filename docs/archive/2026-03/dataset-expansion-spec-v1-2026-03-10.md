# Predict-MLB Dataset Expansion Spec v1

_Date: 2026-03-10_

## Purpose

This doc captures the current planning agreement for the next dataset/model expansion phase.

Goal:
Build a significantly more intelligent MLB pregame prediction dataset by adding the highest-value baseball-specific information that is realistically available at inference time.

## Agreed operating assumptions

- Prediction timing target: **~1 hour before first pitch**
- **Announced lineups are in scope**
- No two-stage prediction system for now
- New data/features must be available both:
  - historically for backfill/training
  - at daily inference time for live predictions
- Train/inference parity is a hard requirement: every approved feature family must include a documented runtime computation/retrieval path so daily predictions can use the same information class and logic as training.
- The intended operating model is to keep the project DB updated in real time (or near-real time) each day so bullpen state, lineup/platoon context, and later weather/park context can be recomputed at inference time from fresh data.
- Priority is not blind retraining; priority is **dataset intelligence expansion**

## Expansion phase order

### Phase 1 (must-have, immediate)
1. Bullpen state / quality / fatigue
2. Lineup / handedness / platoon context

These should be combined into one practical ingestion/update flow if API/rate-limit economics support it.

### Phase 2 (next)
3. Weather / park / venue context

### Later / optional
4. Richer park-team interaction modeling
5. Secondary run-margin model
6. More advanced lineup-level player-quality modeling

## First-pass feature proposal

## A. Bullpen feature family

### Objective
Represent bullpen:
- quality
- depth
- recency workload
- likely freshness / fatigue
- availability proxies

### Raw/support data desired
- per-game pitcher appearances
- team ID
- pitcher ID
- game ID / game date
- starter vs reliever role for that game
- outs / innings pitched
- pitches thrown, if available
- batters faced, if available
- runs / hits / walks / strikeouts, if available
- leverage or save/hold-style role proxy later if available

### First-pass bullpen features

#### Quality
- bullpen season ERA proxy
- bullpen season WHIP proxy
- bullpen season runs allowed per inning/out
- bullpen recent ERA/WHIP proxy over last N games or days
- bullpen strikeout rate proxy
- bullpen walk rate proxy
- bullpen HR allowed proxy if available

#### Depth
- top 3 reliever quality aggregate
- top 5 reliever quality aggregate
- bullpen depth dispersion / dropoff metric
- bullpen usable arms count proxy

#### Fatigue / availability
- bullpen innings in last 1 / 3 / 5 / 7 days
- bullpen appearances in last 1 / 3 / 5 days
- top reliever innings in last 3 / 5 days
- top reliever appearance count in last 3 days
- bullpen freshness score
- closer/setup-man freshness proxy if role inference is feasible

#### Interaction / matchup terms
- home bullpen freshness minus away bullpen freshness
- home bullpen quality minus away bullpen quality
- starter expected length x bullpen freshness interaction
- weak starter x strong fresh bullpen interaction

### Notes
- First pass should prefer robust aggregate proxies over fragile bullpen-role inference.
- Use **prior completed games only**.
- If role identification is noisy, do not block first implementation on precise closer/setup classification.

## B. Lineup / handedness / platoon feature family

### Objective
Represent how the announced lineup matches up against the opposing starting pitcher and how the team’s handedness composition changes its offensive expectation.

### Raw/support data desired
- announced batting order
- hitter IDs in lineup
- hitter batting handedness
- pitcher throwing handedness
- team handedness mix in announced lineup
- if feasible: current-season hitter quality summaries or split-strength summaries

### First-pass lineup/platoon features

#### Basic lineup composition
- count of L / R / S hitters in lineup
- handedness mix in top 5 hitters
- handedness mix in top 3 hitters
- share of plate appearances expected from L / R / S hitters (order-weighted proxy)

#### Matchup context
- opposing starter throws left/right
- lineup left-heavy vs RHP/LHP flag
- lineup right-heavy vs RHP/LHP flag
- platoon-balance score
- top-of-order platoon advantage score

#### Team-level split offense
- team offense vs LHP
- team offense vs RHP
- recent offense vs LHP/RHP if feasible
- split-offense delta relative to team overall offense

#### Optional near-term lineup quality features
- lineup aggregate season OPS/wOBA-like proxy from currently available player data
- top 3 hitter quality aggregate
- top 5 hitter quality aggregate
- lineup missing-star proxy if feasible

#### Interaction / matchup terms
- lineup-vs-starter-handedness score
- top-of-order quality x platoon advantage
- lineup handedness mix x park factor later

### Notes
- Start with **announced-lineup structural features** first.
- If player-quality-in-lineup is easy and clean, include it in first implementation.
- If it becomes messy, split it into a later sub-phase.

## C. Weather / park feature family

### Objective
Represent game environment and run environment, especially where it affects offensive uplift, volatility, and side confidence.

### Raw/support data desired
- venue/stadium ID
- game coordinates if needed for weather joins
- game local time / UTC time
- historical hourly weather
- same-day forecast hourly weather
- roof/open flag if feasible
- park-factor support mapping later

### First-pass weather/park features

#### Weather
- temperature
- wind speed
- wind direction
- precipitation probability / rain flag
- humidity
- pressure
- dew point or air-density proxy if useful
- day/night flag

#### Venue / park
- venue ID
- dome/open-air flag if feasible
- later: park factor join
- later: handedness-sensitive park factor concepts

#### Interaction terms
- wind out/in/cross bucket
- weather-run-environment score
- lineup handedness mix x park later
- fly-ball risk proxy x wind/park later if inputs become available

### Notes
- Weather is likely more valuable for margin/totals and confidence than raw side alone, but it still belongs in the side model feature set.
- Venue ID should absolutely be stored even if the model only sees encoded representations later.

## What should happen next

### Immediate planning objective
Finalize exact data families and first-pass derived features for:
1. bullpen
2. lineup/platoon
3. weather/park

### After that
Spawn a schema-planning subagent to design the DB expansion against the existing schema with minimal disruption.

### Then
Delegate implementation to Codex/subagents.

## Open decisions still needing confirmation

1. Bullpen quality representation:
- aggregate-only first?
- or aggregate + top-N reliever summaries together?

2. Lineup quality depth in first pass:
- handedness composition only?
- or handedness composition + player-quality-in-lineup metrics immediately?

3. Weather feature breadth:
- minimal first pass (temp/wind/precip/day-night)
- or include broader atmospheric variables immediately?

4. Whether Phase 1 should be implemented as:
- bullpen first, then lineup/platoon
- or both in one coordinated build if API usage is efficient

## Current recommendation

Best current recommendation:
- Do **Phase 1 as one coordinated block** if API economics are reasonable.
- For bullpen, include **both aggregate quality and top-N reliever/fatigue proxies** in first pass.
- For lineup/platoon, include **announced lineup handedness structure immediately**, and include lineup quality summaries too if sourcing is straightforward.
- For weather, start with a **practical core set** before expanding to more atmospheric nuance.
