# Predict-MLB TODO

_Last updated: 2026-03-10_

## Top Now

### 1) Lock the modeling direction before more blind retraining
- [ ] Treat current LightGBM path as mechanically valid but not yet feature-rich enough for strong MLB pricing.
- [ ] Keep the **primary objective** as calibrated pregame **home win probability**.
- [ ] Keep **run margin** as a planned **secondary model**, not the primary target.
- [ ] Preserve core evaluation priorities: **accuracy**, **log loss**, **Brier score**, and **calibration/confidence quality**.
- [ ] Keep all training/inference features strictly **available at pregame inference time**.

### 2) Build a next-gen feature/data expansion plan
- [ ] Translate research findings into a concrete `v2` feature architecture.
- [ ] Separate feature work into:
  - [ ] **Use better now from current data**
  - [ ] **Add soon with current/near-term ingestion**
  - [ ] **Later / gated additions**
- [ ] Keep feature work MLB-specific, not just generic tabular ML.

### 3) Research and validate data availability for the biggest missing signal groups
- [ ] **Bullpen state / quality / fatigue**
- [ ] **Lineup / handedness / platoon context**
- [ ] **Park + weather context**
- [ ] Confirm what is available both:
  - [ ] historically
  - [ ] at daily inference time

## Next

### A) Existing-data-only `v2` feature engineering
These are the highest-priority upgrades even before new ingestion dependencies.

- [ ] Convert raw home/away fields into **matchup deltas**
  - [ ] season run diff/game delta
  - [ ] season win% delta
  - [ ] rolling offense delta
  - [ ] rolling prevention delta
  - [ ] starter quality delta
- [ ] Add **recency-vs-baseline gap** features
  - [ ] recent offense minus season offense
  - [ ] recent prevention minus season prevention
  - [ ] recent win% minus season win%
- [ ] Add **sample-size-aware shrinkage / early-season caution**
- [ ] Add stronger **missingness / uncertainty flags**
  - [ ] starter stats available
  - [ ] degraded feature row flag
  - [ ] low-sample starter/team flags
- [ ] Add baseball-specific **interaction features**
  - [ ] strong offense vs weak starter
  - [ ] weak offense vs elite starter
  - [ ] recent form interacting with stable team-strength baseline

### B) Bullpen feature expansion
User direction:
- likely obtainable historically from MLB Stats API or derivable from pitcher/game usage data
- should support quality + depth + fatigue proxies

Research / planning tasks:
- [ ] Confirm whether MLB Stats API gives enough historical data to reconstruct bullpen members and appearances by game.
- [ ] If no explicit bullpen entity exists, confirm whether we can derive bullpen from:
  - [ ] non-starting pitchers on roster
  - [ ] pitchers appearing after the starter in completed games
- [ ] Design bullpen **quality** features
  - [ ] aggregate bullpen quality metrics
  - [ ] top reliever quality metrics
  - [ ] bullpen depth / top-N reliever strength
- [ ] Design bullpen **fatigue / availability** features
  - [ ] innings pitched over last N games
  - [ ] appearances over last 1/2/3/5 days
  - [ ] top reliever recent workload
  - [ ] team bullpen freshness score
- [ ] Decide best representation for bullpen in feature rows
  - [ ] aggregate summary only
  - [ ] top reliever subset summaries
  - [ ] recency-weighted bullpen state features
- [ ] Plan any required new tables for bullpen support data.

### C) Handedness / lineup / platoon expansion
User direction:
- compare hitter handedness against starting pitcher handedness
- consider team/stadium interaction indirectly via ML features

Research / planning tasks:
- [ ] Confirm whether historical lineup / batting-order data is available pregame or only postgame.
- [ ] Confirm whether hitter handedness and pitcher handedness are available historically and at inference time.
- [ ] Add starter handedness as a core feature if not already present.
- [ ] Design platoon features:
  - [ ] team split offense vs LHP/RHP
  - [ ] projected/confirmed lineup handedness balance
  - [ ] best hitters’ handedness concentration
- [ ] Evaluate whether lineup-level features should be:
  - [ ] full projected lineup aggregates
  - [ ] fallback team-vs-handedness splits
  - [ ] star-batter availability proxies
- [ ] Consider stadium interaction features indirectly via:
  - [ ] park factor
  - [ ] handedness composition
  - [ ] weather/wind/run environment

### D) Weather and park-context expansion
User direction:
- likely free API available for historical weather
- first determine which weather features matter most

Research / planning tasks:
- [ ] Identify reliable free or low-friction historical weather source(s) for MLB game times.
- [ ] Confirm inference-time availability for same-day predictions.
- [ ] Determine most useful weather features for MLB prediction
  - [ ] temperature
  - [ ] wind speed
  - [ ] wind direction
  - [ ] precipitation / rain risk
  - [ ] humidity / air density proxy
  - [ ] roof / open-air context where relevant
  - [ ] time of day / day-night effect
- [ ] Separate weather features by likely value for:
  - [ ] side / win probability
  - [ ] run margin / totals environment
- [ ] Plan any required park-factor support tables.

## Later

### Secondary modeling path
- [ ] Add a **run-margin regression model** after stronger feature expansion is underway.
- [ ] Use it as a companion model for:
  - [ ] confidence ranking
  - [ ] discrepancy detection vs markets
  - [ ] later ensemble logic
- [ ] Do **not** let the margin model replace the main win-probability classifier.

### Model ladder / experimentation order
- [ ] Retrain improved LightGBM on `v2` feature set.
- [ ] Compare against current corrected baseline + tuned candidate.
- [ ] Consider CatBoost/XGBoost challengers only after richer features exist.
- [ ] Add calibration layer/governance review after stronger features are in place.

### Data-model contract / schema planning
- [ ] Determine new tables and materialization flow required for dataset expansion.
- [ ] Keep every added feature reconstructible as-of pregame inference time.
- [ ] Document new contracts clearly before implementation.

## Open Questions / Decisions
- [ ] How much historical lineup-level detail can we reliably source pregame, not postgame?
- [ ] What bullpen representation is best: aggregate-only, top-N relievers, or both?
- [ ] Which weather variables are worth including for side prediction vs margin-only?
- [ ] Should 2020 be downweighted in future model training because of season abnormality?
- [ ] At what point do we revisit historical odds / market features, if at all?

## Notes from research synthesis
- The main bottleneck appears to be **feature richness**, not model family.
- Highest-value missing baseball signals are likely:
  - bullpen state/quality/fatigue
  - platoon / handedness context
  - park + weather context
- Current data should still be used better before we assume we need a radically different model.
- We want the model to make **intelligent, informed predictions** using the best information realistically available at prediction time.
