# Predict-MLB next-generation modeling plan

_Date: 2026-03-10_

## 1) Executive summary

The repo is in a decent place mechanically, but the current feature set is still too thin to produce a strong MLB pregame model. Right now the model mostly knows: home field, basic team form, basic season strength, days rest, doubleheader flag, and coarse starting-pitcher stats. That is enough to build a functioning baseline, but not enough to price MLB games well.

My main recommendation is:

1. **Keep the primary objective as home win probability.** That aligns with deployment, log loss/Brier/calibration goals, and moneyline-style decisioning.
2. **Stay with tabular boosting as the mainline path for now** (LightGBM first, CatBoost/XGBoost challenger second), because the data size is modest (~13.3k games / ~13.3k feature rows) and the current architecture is already built for it.
3. **The biggest missing baseball signals are bullpen state, park/weather context, and lineup/platoon strength.** These are more likely to move model quality than exotic model architecture.
4. **Add a secondary run-margin model, but do not let it become the primary project right now.** It is useful as a companion model for ranking confidence, totals/side decomposition, and later ensembling, but the first goal should still be a clearly better calibrated win-probability model.
5. **Tighten evaluation around time ordering, calibration, and “as-of” realism.** MLB is noisy; if the model is only barely above coin-flip accuracy, calibration discipline matters more than chasing tiny in-sample gains.

Bottom line: the next real lift is much more likely to come from **better baseball features** than from swapping one generic classifier for another.

## 2) What likely matters most for predicting MLB winners

Opinionated ranking by practical value for same-day pregame prediction:

| Feature group | Likely signal | Feasibility soon | Why it matters |
| --- | --- | --- | --- |
| Team true-strength baseline | High | High | You need a stable prior before recency noise takes over. |
| Starting pitcher quality + handedness context | High | Medium | Starters matter a lot, but raw ERA/WHIP is not enough. |
| Bullpen quality + recent usage/fatigue | High | Medium | MLB games are not just starter vs starter; many are decided by the pen. |
| Offensive strength split by handedness | High | Medium | Team offense changes meaningfully by opposing starter hand/type. |
| Park + weather run environment | Medium-High | Medium | Park and weather change expected scoring and HR environment. |
| Recent team form / recency | Medium | High | Useful, but dangerous if it overwhelms stronger long-term signal. |
| Defense / run prevention support | Medium | Medium-Low | Matters, but usually as a second-order term unless measured well. |
| Travel, rest, getaway/doubleheader dynamics | Medium | Medium | Small edges that add up, especially in baseball’s schedule grind. |
| Public/market price comparison | Medium | Low for training, High for monitoring | Great for post-model decision support, but risky if mishandled in training. |
| Narrative factors / streak talk | Low | High | Usually noise unless it proxies for real lineup or pitcher changes. |

What baseball-specific framing matters here:

- **Single-game MLB is high variance.** Even the better team loses a lot. FanGraphs’ work on baseball variance is a good reminder not to overfit short-term outcomes.
- **Starters matter, but only for part of the game.** A great starter does not fully define the game if he is on a pitch limit, likely to go five innings, or followed by a weak/fatigued bullpen.
- **Run environment matters twice:** for the side and for confidence. Park/weather can make a matchup more or less volatile even if mean win probability barely moves.
- **Platoon context is real.** The offense facing a left-handed starter is not the same offense facing a right-handed starter.
- **Defense and sequencing distort pitcher results.** Fangraphs’ FIP framing is relevant: raw runs/ERA blend pitcher skill, defense, luck, and sequencing.
- **Stable team strength and recent form should be combined, not treated as substitutes.** A hot 10-game stretch should tilt the prior, not replace it.

Outside research that supports the above:

- Fangraphs park-factor material reinforces that run environment is not just fence dimensions; weather, air density, and park structure matter.
- Fangraphs wRC+/park-adjusted offensive framing supports using park-adjusted team offense rather than raw rate stats when possible.
- Fangraphs FIP discussion supports preferring defense/luck-stripped pitcher indicators over raw ERA alone.
- Scikit-learn calibration guidance is a useful reminder that log loss/Brier mix discrimination and calibration, so reliability diagrams and explicit calibration checks should remain first-class evaluation outputs.
- TimeSeriesSplit guidance reinforces the right instinct: no random shuffles, expanding-window or rolling chronological validation only.

## 3) Current-data opportunities inside this repo

### What the repo has now

From the local SQLite history and feature contract, the current `v1` feature payload is mostly:

- home/away team IDs
- home field flag
- team days rest
- doubleheader flag
- season games played
- season win% and season run-differential-per-game
- rolling last-10 offense / prevention summaries:
  - batting average
  - OBP
  - OPS
  - hits per game
  - runs for per game
  - runs against per game
  - win%
- starting pitcher context:
  - probable starter known flag / id
  - ERA
  - WHIP
  - runs per 9
  - strike%
  - win%
  - career ERA
  - stats available flag

Data coverage looks good enough to work with:

- `feature_rows`: 13,309
- `labels`: 13,307
- seasons: 2020-2025 present
- `source_contract_status`: ~92.1% valid, ~7.9% degraded
- starter stats available on roughly 87.4% to 87.6% of rows
- rolling team features available on ~99.3% of rows
- probable pitchers known on ~99.9% of pitcher-context rows
- no historical odds backfill currently in the DB (`odds_snapshot` count = 0)

### What the current model results say

The current tuned LightGBM candidate is basically a modestly positive but not clearly strong classifier:

- aggregate accuracy: **0.545**
- aggregate Brier: **0.2493**
- aggregate log loss: **0.6926**
- aggregate ECE: **0.0491**
- probabilities are compressed; the model rarely reaches truly strong conviction

My read: this is the profile of a model that has **some real signal**, but not enough high-quality game-state context to separate close MLB matchups well.

### Best immediate opportunities already inside the repo

1. **Re-express what already exists as matchup deltas rather than raw home/away fields only.**
   - Example: home season run diff/game minus away season run diff/game
   - home rolling OPS minus away rolling OPS
   - home starter WHIP minus away starter WHIP
2. **Blend long-term and short-term team form explicitly.**
   - Right now you have both season and last-10 style signals; use weighted blends and disagreement features.
3. **Add stronger missingness indicators and uncertainty flags.**
   - starter stats available
   - rolling available
   - early-season low-sample flags
   - degraded-contract flags
4. **Normalize for sample size / season phase.**
   - early April stats should not mean what August stats mean.
5. **Engineer interaction features that mirror baseball logic.**
   - good offense vs weak starter
   - weak offense vs elite starter
   - strong recent prevention + rested team vs opponent weak recent offense

## 4) Recommended feature architecture

The right design is a layered feature stack.

### Layer A: Stable priors (must-have)

These define the baseline team quality before same-day context:

- season win% and run diff/game
- season-to-date offense and run prevention rates
- park-adjusted team strength if you can derive it later
- prior-season carryover only if shrunk hard and used very early in season

Recommendation: do not let recency dominate this layer. MLB seasons are long enough that stable team strength matters.

### Layer B: Short-term form (must-have, but shrink it)

Use recent form, but make it behave.

Recommended representations:

- rolling 7/14/30 day windows, not just last 10
- exponential decay rather than one blunt last-10 bucket
- offense and prevention split separately
- deltas between recent and season baseline

Example useful features:

- recent offense minus season offense
- recent runs allowed minus season runs allowed
- recent win% minus season win%

### Layer C: Starting pitcher context (must improve)

Current starter features are directionally useful but too blunt.

Use better representations:

- opponent-adjusted or defense-stripped indicators when available (FIP/xFIP/SIERA style proxies are better than ERA)
- recent starter form and season form separately
- handedness
- expected workload / average innings per start
- strikeout, walk, HR tendencies if obtainable
- uncertainty flags for rookie/small-sample starters

Most important practical upgrade: **move from raw ERA/WHIP to a fuller starter skill profile plus expected innings share**.

### Layer D: Bullpen state (highest-value missing feature)

This is the most obvious baseball gap in the current architecture.

Recommended bullpen features:

- team bullpen season quality
- bullpen recent quality (last 7 / 14 days)
- reliever usage in last 1/2/3 days
- high-leverage reliever availability proxy
- projected bullpen freshness score

Why it matters: many games are priced as starter edge plus bullpen edge, not starter edge alone.

### Layer E: Lineup and platoon context (high-value add)

If you can capture projected/confirmed lineups pregame, this should become a major feature group.

Recommended features:

- projected lineup aggregate wRC+/OPS proxy
- projected lineup vs RHP / vs LHP split
- missing star hitter flags
- catcher and key defender availability later if feasible
- lineup continuity / bench-heavy lineup proxy

If full lineups are not yet feasible, start with simpler pregame approximations:

- team split offense vs RHP/LHP
- starter handedness interaction terms

### Layer F: Park and weather (worth adding soon)

Based on Fangraphs park-factor framing, these are not cosmetic features.

Recommended features:

- park factor / run factor
- HR factor
- temperature
- wind speed
- wind direction bucket (in/out/cross)
- precipitation risk / roof status where relevant
- air density or a coarse weather-run-environment score if available

These will likely help more for totals/margin than for side alone, but they should still help side confidence and calibration.

### Layer G: Travel / schedule friction (small but real)

Recommended features:

- days rest
- doubleheader flag
- previous-day game played flag
- cross-country travel proxy
- timezone shift proxy
- home stand / road trip length
- getaway-day travel spot proxy

These are edge features, not core signal. Add them after bullpen/park/platoon.

### Explicit grouping by action priority

#### Features we already have and should use better

- season win%
- season run diff/game
- rolling offense/prevention features
- days rest
- doubleheader flag
- current starter stats
- degraded/missingness flags

Main improvement: transform them into matchup deltas, recency-vs-baseline gaps, sample-size-aware shrinkage, and baseball interactions.

#### Features we can plausibly add soon with current architecture

- bullpen quality + recent usage/fatigue
- park factors
- weather at game time
- starter handedness
- team split offense vs RHP/LHP
- starter recent workload / innings-per-start
- broader rolling windows (7/14/30 or decayed)
- travel/timezone proxies from schedule + venue

These are the best near-term lift candidates.

#### Features that are attractive but should wait

- fully projected lineup quality if the source/process is messy
- injury-driven player-level availability if timestamps are unreliable
- statcast-heavy batter/pitcher micro features at player level
- public/consensus betting prices inside the core training label path
- complex stacking/NN architectures

These are attractive, but they increase leakage risk, maintenance cost, or both.

## 5) Recommended model architecture (primary + secondary if appropriate)

### Primary model: win-probability classifier

Keep this as the flagship model.

Recommended model ladder:

1. **LightGBM v2 feature set**
   - best first step because it is already integrated
2. **CatBoost challenger**
   - often very strong on medium tabular datasets with mixed missingness / nonlinear interactions
3. **XGBoost challenger**
   - useful sanity check against LightGBM-style overconfidence or leaf behavior
4. **Simple linear/logistic benchmark on engineered deltas**
   - not because it will win, but because it gives a calibration and feature-value anchor

Why this order:

- The current problem is mostly a feature problem, not a need-for-transformers problem.
- Gradient-boosted trees remain the most practical family here.
- CatBoost is especially worth trying once you have more categorical matchup context and missingness structure.

### Secondary model: run-margin regression

Yes, I think a secondary margin model is worth adding **soon**, but not as the mainline effort.

Recommended use:

- train a separate regression target on **home run differential**
- use it as a companion signal, not a replacement for the classifier
- compare its implied sign and confidence with the primary classifier
- later use it for modest ensembling, confidence ranking, and market-comparison workflows

Why it helps:

- Margin forces the system to learn scoring context, not just side outcome.
- It should benefit more directly from park/weather and bullpen context.
- It can help distinguish “coin-flip but likely low-scoring” from “coin-flip but volatile/high-scoring.”

What not to do:

- Do **not** derive your main published win probability from a weak margin model alone.
- Do **not** let the margin model delay the needed feature upgrades for the classifier.

Practical recommendation:

- Build it after the first better-featured classifier is running.
- Start with LightGBM regression on run differential.
- Evaluate MAE/RMSE plus whether margin sign / binned magnitude adds value when combined with the classifier.

### Calibration strategy

Treat calibration as part of the model, not a reporting afterthought.

Recommended path:

- keep raw model training optimized for log loss
- fit calibration only on strictly out-of-fold or forward-held-out predictions
- compare isotonic vs Platt/sigmoid; choose by out-of-sample log loss and reliability diagrams
- maintain season-segment calibration checks (early season vs midseason vs late season)

Given the current compressed probability range and middling ECE, a clean post-hoc calibrator may help, but feature improvements matter more than calibration alone.

## 6) Training/validation/promotion pipeline recommendations

### Validation structure

Keep chronological walk-forward. That is correct.

Recommended upgrades:

- continue expanding-window or rolling-forward evaluation
- add a **small temporal gap** between train and test when needed to reduce subtle leakage from rolling-state construction
- report by season segment, not just global aggregate
- preserve one final untouched recent holdout for promotion review

### Promotion philosophy

The current gates are directionally good, but one target looks unrealistically strict given the current problem shape:

- ECE threshold of 0.025 is probably too ambitious until feature quality improves materially.

Recommended promotion stack:

1. aggregate log loss improvement vs incumbent
2. no major Brier regression
3. calibration review by probability bin and by season slice
4. sanity check on confidence distribution (avoid compressed mush and avoid fake certainty)
5. stability check on recent folds only

### As-of realism rules

Hard rules worth enforcing:

- every feature row must remain reconstructible from data available before first pitch
- no postgame team stats sneaking into same-day features
- no backfilled closing odds in training unless the product truly has those odds available at inference time
- if lineup/injury timestamps are sloppy, keep them out until fixed

### Monitoring recommendations

Once live predictions resume, monitor:

- log loss / Brier by rolling 7-day and 30-day windows
- calibration by confidence bucket
- performance split by favorite vs underdog
- performance split by known vs unknown/degraded starter context
- performance split by early season vs rest of season

## 7) Immediate next experiments in priority order

1. **Feature-engineer v2 from existing data only**
   - matchup deltas
   - recency-vs-baseline gaps
   - sample-size shrinkage
   - explicit missingness flags
   - baseball interaction terms

2. **Train LightGBM v2 + logistic benchmark on the same walk-forward frame**
   - If v2 features do not move log loss, stop and re-check feature logic before adding complexity.

3. **Add starter-handedness and team split-offense features**
   - This is one of the cleanest baseball-specific upgrades available before full lineup work.

4. **Add bullpen quality and usage/fatigue features**
   - This is the highest-upside new feature family.

5. **Add park/weather context**
   - Especially valuable for margin modeling and confidence calibration.

6. **Run CatBoost as first model-family challenger**
   - Only after v2/v3 features exist; otherwise it is mostly a re-arrangement of the same limited signal.

7. **Add secondary run-margin regression**
   - Use as companion signal; do not let it hijack the roadmap.

8. **Only later: cautious ensemble experiments**
   - Blend only after one challenger clearly beats the incumbent.

## 8) Open questions / risks

1. **Bullpen data source risk**
   - The most valuable next feature family is also a data-engineering task. If the bullpen source is messy, progress may stall.

2. **Lineup/injury timing risk**
   - These are powerful features, but they are classic leakage traps if timestamps are not strict.

3. **Early-season instability**
   - MLB season stats are noisy in April. The model should use stronger priors and more shrinkage early.

4. **No historical odds in the current DB**
   - That is good from a leakage standpoint, but it also means you cannot yet do strong market-benchmark or residual-vs-market analysis offline.

5. **Current feature set may cap upside**
   - If v2 engineering on current data barely moves results, that is evidence that new baseball context is mandatory, not optional.

6. **2020 season weirdness**
   - The shortened season has different schedule and environment properties. It may deserve downweighting or at least explicit robustness review.

---

## Final recommendation

The best practical next-generation plan is:

- **Primary:** better-featured win-probability model using gradient-boosted trees
- **First upgrades:** smarter use of current team/starter signals, then bullpen, platoon, and park/weather
- **Secondary:** run-margin model as a companion, not the lead
- **Evaluation:** strict chronological validation, explicit calibration review, and zero tolerance for as-of leakage

If I had to bet on the highest-ROI sequence, it would be:

1. existing-data v2 feature engineering
2. bullpen + platoon features
3. park/weather
4. CatBoost challenger
5. secondary margin model

That is much more likely to improve real MLB prediction quality than chasing more exotic model families right now.
