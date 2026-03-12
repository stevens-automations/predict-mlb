# Training Manifest

Last updated: 2026-03-12

## Purpose

This is the canonical training-planning document for `predict-mlb`.

It centralizes:
- outside research on what drives MLB pregame pricing and prediction quality
- the working target operating window
- the highest-value signal families
- the model-design patterns worth emulating
- the open questions that must be resolved before a training rebuild is fully specified

This file is intentionally **not** the final implementation spec. It is the single planning surface that future coding/training agents should read first before changing training code or writing new training docs.

## Current Working Objective

Build a high-accuracy MLB pregame prediction system that:
- optimizes first for **probability quality** on game outcomes
- can later support **market disagreement detection** against sportsbook prices
- is designed around a **near-first-pitch (~1 hour pregame)** decision window, not a generic morning-only snapshot

Primary prediction target for the first serious rebuild:
- **pregame home win probability**

Secondary targets for later phases:
- run differential / run line support
- totals / run-environment support
- market-disagreement ranking / filtering layer

## External-Research-First Summary

The strongest repeated consensus across public forecasting systems, public baseball analytics, and betting-market research is:

1. **Starting pitcher quality / expected length** is the single most important game-specific pricing input.
2. **Baseline team true talent** matters more than streaks, generic momentum, or raw recent W-L.
3. **Confirmed lineup strength** matters materially close to first pitch.
4. **Bullpen quality plus short-term availability/fatigue** matters a lot for full-game moneylines.
5. **Platoon / handedness matchups** matter, especially when attached to the actual lineup and opposing starter hand.
6. **Park + weather + roof state** matter, especially through run environment and context-sensitive matchup effects.
7. Strong public systems and likely sharper markets behave more like **priors + late updates** than “one flat model on recent team stats.”

## What Outside Sources Suggest Sportsbooks / Strong Forecasters Care About

### Tier 1 — highest-signal inputs near first pitch
- starting pitcher quality
- expected starter leash / innings expectation
- team true-talent baseline
- confirmed lineup offensive strength

### Tier 2 — very important matchup/context inputs
- bullpen quality
- bullpen availability / fatigue in the last 1-3 days
- lineup handedness / platoon fit versus opposing starter
- late scratches / injury / rest news

### Tier 3 — meaningful contextual modifiers
- park
- weather
- roof status
- travel / rest
- possibly umpire, where reliable

### What appears overrated
- hot-team narratives
- recent record without skill context
- pitcher win-loss record
- tiny batter-vs-pitcher samples
- generic momentum features without shrinkage

## Public-System Patterns Worth Emulating

The most credible public design patterns came from the structure of systems such as:
- FanGraphs-style projection + depth-chart aggregation
- ATC-style smart ensembling / consensus weighting
- FiveThirtyEight-style dynamic ratings with starter/home/rest/travel adjustments

What those systems have in common:
- strong priors
- gradual updating, not overreaction
- player/team talent separated from game-specific context
- explicit situational adjustments
- simulation or ensemble thinking later, not as the only first step

## Recommended Modeling Shape

The external research supports a **two-stage system**:

1. **Base / prior forecast**
   - built from stable talent and context inputs
   - useful early in the day and as a prior
2. **Near-first-pitch updater (~60-75 minutes pregame)**
   - decision-grade forecast
   - absorbs confirmed lineup, late weather/roof, bullpen state, and scratch/starter certainty changes

If only one production-grade forecast is prioritized first, it should be the **near-first-pitch snapshot**.

## Signal Families We Should Treat As Serious Candidates

### 1) Starting pitcher block
Should likely include some combination of:
- projected talent / true-skill proxies
- recent form, but shrunk
- expected pitch count / leash / likely innings
- handedness
- uncertainty / confirmation status

### 2) Team true-talent block
Should likely emphasize:
- longer-run team strength
- roster-adjusted offensive and run-prevention quality
- recency only with shrinkage
- not just raw streaks or last-10 records

### 3) Confirmed lineup block
Near first pitch, we should likely care about:
- actual nine hitters
- batting-order-weighted offensive quality
- top-of-order strength
- star absences / replacement penalties
- lineup handedness mix
- projected offense versus opposing starter hand

### 4) Bullpen block
Should likely include:
- baseline bullpen quality
- recent usage and fatigue
- top-leverage arm availability
- handedness mix of likely available relievers
- interaction with starter leash / opener risk

### 5) Platoon / handedness block
Should likely be modeled through:
- regressed lineup-vs-hand quality
- batter-hand mix versus opposing starter hand
- eventual interaction with likely bullpen hand mix
- not naive raw split stats alone

### 6) Park / weather / roof block
Should likely include:
- park identity
- roof type / roof status
- temperature
- wind speed and direction
- precipitation / delay risk
- park-weather interactions

Note: outside research says weather matters more for totals/run environment than for sides directly, but it is still a meaningful candidate signal for moneyline through matchup and variance effects.

## Modeling Tactics The Outside Research Supports

High-value tactics:
- strong priors + late updates
- calibrated probabilistic modeling
- shrinkage / empirical-Bayes handling for noisy baseball features
- explicit missingness / uncertainty handling
- disciplined walk-forward validation
- comparing against market prices honestly, but not blindly training to copy them

Likely later, but not first:
- small conservative blends / ensembles
- run-differential companion model
- simulation layer for derivative markets

Lower priority / likely overkill early:
- deep-learning-for-the-sake-of-deep-learning
- heavy regime explosion
- generic feature sprawl without hierarchy

## External-Research-Informed Architecture Direction

### Morning/base layer
Use this as the prior / early estimate:
- team talent baseline
- expected starter
- projected lineups if available
- baseline bullpen quality
- park
- coarse weather

### Near-first-pitch layer
Use this as the main decision-grade forecast:
- confirmed lineup
- confirmed / much firmer starter state
- bullpen freshness and leverage-arm availability
- final weather / roof state
- late scratches / rest changes
- stronger matchup interactions

## How To Think About The Market

Outside research suggests:
- MLB moneyline markets are generally pretty efficient, especially near close
- the close is a hard benchmark
- the most realistic opportunity is usually in:
  - stale/open numbers
  - reaction windows around lineup/weather/scratch news
  - fragmented books / inconsistent repricing
- therefore the honest first goal is:
  1. match market competence
  2. build strong probability quality and calibration
  3. then test disagreement edges

## Mapping To Current Project Data Families

This section is intentionally high-level and noncommittal for now.

Current known project families that map well to the external research:
- starter context
- team strength / rolling form context
- bullpen quality / freshness
- lineup / handedness / platoon support
- venue / weather support

The main question is **not** whether these families are conceptually important.
The outside research says they are.

The main remaining questions are:
- which exact derived features best represent them
- which should enter the first rebuilt training contract
- how snapshot timing should be encoded
- how much of the model should be prior-driven vs snapshot-driven

## Open Questions

1. What is the best first rebuilt model architecture?
   - one near-first-pitch tabular model
   - two-stage prior + updater
   - additive/rating prior plus discriminative updater
   - small ensemble of these views

2. How much of the system should be player-level versus aggregated lineup/team features?

3. How should lineup quality be represented?
   - full lineup aggregate only
   - top-order weighting
   - explicit platoon matchup summaries
   - separate early vs late-inning / bullpen-facing summaries

4. How should weather enter the moneyline model?
   - directly as game-level context
   - mostly through run-environment transformations
   - with park interactions only

5. What is the right role of the market in the eventual system?
   - benchmark only
   - calibration check
   - separate disagreement engine
   - later meta-model input

6. Should the first serious rebuild keep one model per snapshot or one model with snapshot-time features?

## What Future Agents Should Do Next

Before rebuilding training code, future agents should:
1. keep this file as the single canonical strategy/planning surface
2. continue external research only when it materially changes the architecture or feature hierarchy
3. map the external signal hierarchy onto the actual project schema/feature inventory
4. then produce the implementation spec/config changes from that mapping

Do **not** create new mini-strategy docs if the update belongs here.

## Related Canonical Files

- `docs/runbooks/training-architecture.md` — current implemented training scaffold and entrypoints
- `docs/runbooks/model-optimization-plan.md` — older optimization/runbook notes; now secondary to this manifest
- `docs/schema-feature-map.md` — current data-family map
- `docs/decisions.md` — locked vs open project decisions
- `docs/STATUS.md` / `docs/PLAN.md` / `docs/TODO.md` — current project state and execution queue
