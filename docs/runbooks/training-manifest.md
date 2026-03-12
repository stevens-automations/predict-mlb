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

The current project direction is a **single canonical `pregame_1h` system**.

That means:
- one training/inference snapshot contract
- one primary decision-grade forecast built for roughly 1 hour before first pitch
- no separate morning-model track in the first rebuild unless a later result clearly justifies it

The model should therefore be built directly around the best information available near first pitch:
- confirmed/probable starter context
- lineup/projection fallback context
- bullpen freshness and leverage-arm availability
- weather / roof / park context
- team true-talent and rolling form priors

The goal is not to intentionally narrow the first serious model. The goal is to train on the **strongest realistic pregame_1h feature set** we can support while preserving train/inference parity.

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

### Canonical `pregame_1h` model
Use one primary snapshot contract centered on roughly 1 hour before first pitch.

That model should combine:
- team talent baseline / rolling form priors
- starter quality and expected role
- lineup quality and handedness context
- bullpen quality and recent availability/fatigue
- park / venue / roof context
- weather context
- explicit uncertainty / fallback indicators for lineup, starter, and weather inputs

The first serious build should target the **full integrated pregame feature set**, not an intentionally weakened subset. Simpler feature subsets can still be used later as ablations or benchmarks, but not as the planned main model direction.

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

Current known project families that map well to the external research:
- starter context
- team strength / rolling form context
- bullpen quality / freshness / top-reliever availability
- lineup / handedness / platoon support
- venue / weather support
- confidence / fallback / availability indicators

The main question is **not** whether these families should be used in the first serious model.
The current direction is that they **should** be used wherever they can be represented under the `pregame_1h` contract without leakage.

The main remaining questions are:
- which exact derived features best represent each family
- how those features should be grouped into the first integrated contract
- how fallback/degraded states should be encoded
- what ablations we should run after the first full integrated baseline is trained

## Open Questions

1. How much of the first serious model should be player-level versus aggregated lineup/team features?

2. How should lineup quality be represented?
   - full lineup aggregate only
   - top-order weighting
   - explicit platoon matchup summaries
   - separate early vs late-inning / bullpen-facing summaries

3. How should weather enter the moneyline model?
   - directly as game-level context
   - mostly through run-environment transformations
   - with park interactions only

4. What is the right role of the market in the eventual system?
   - benchmark only
   - calibration check
   - separate disagreement engine
   - later meta-model input

5. What is the best confidence-layer design when lineup/starter/weather data fall back to weaker sources?

6. What exact integrated feature set should define the first full `pregame_1h` model versus later challengers?

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
