# MLB Accuracy Improvement Memo (No Major Model Overhaul)

Date: 2026-03-09  
Scope: Improve predictive accuracy for current LightGBM/tabular pipeline while preserving reliability and operational simplicity.

---

## 1) Current-state inspection summary (local codebase)

### What is implemented now
- **Model/inference**
  - LightGBM binary model artifact in production: `models/mlb4year.txt`.
  - Scaler artifact: `models/scalers/mlb4year_scaler.pkl`.
  - Inference path: `predict.py -> LeagueStats.predict_game()` in `data.py`.
  - Uses **44-feature order2** tabular input, then averages **10 perturbed predictions** (`perturbation_scale=0.001`) before final probability.
- **Pipeline/runtime**
  - Runtime validation gates in `runtime.py` (required env vars, model/scaler existence, sqlite schema readiness).
  - Prediction state is now **SQLite-first** via `storage.py` (`ON CONFLICT` upserts, transactional replace with rollback).
- **Reliability guardrails**
  - Guardrail warnings for missing required fields, invalid/too-long tweet lines, enrichment threshold anomalies.
  - Circuit breaker + retry/backoff for odds API and tweet posting.
  - Dry-run and no-post paths (`PREDICT_DRY_RUN`, `PREDICT_DISABLE_POST`).
- **Simulation/replay**
  - Deterministic sim mode with fixture-backed games/predictions (`simulation.py`, `tests/fixtures/sim_games.json`).
  - Optional seed-based replay order (`PREDICT_SIM_SEED`) and forced no-post behavior in sim.
- **Recent workstream trajectory (git log)**
  - SQLite migration and cutover completed.
  - Reliability slices + structured logging + guardrails landed.
  - Phase-B enrichment and rollout-mode controls added.
  - Deterministic simulation mode recently added.

### Strengths
- Mature reliability controls already in place (circuit breakers, retries, fail-open behaviors, schema validation).
- Good test surface around guardrails/scheduling/sqlite/simulation.
- Existing walk-forward retrain scaffold added (`scripts/model_refresh.py`) with log loss/Brier/accuracy outputs.
- Operationally simple architecture (single model artifact + local storage + scheduled runtime).

### Constraints
- Current production decision rule is fixed threshold (`p >= 0.5`) with no probability calibration layer.
- Feature set is still mostly aggregate team/pitcher stats; no deeper context (bullpen fatigue, travel/rest, lineup quality deltas).
- No explicit ensemble/blend layer (model vs market vs priors).
- Backtest automation exists but depends on historical dataset availability (currently noted missing in repo report).

---

## 2) Ranked recommendations

## Quick wins (low effort / high ROI)

### QW1. Add **post-model probability calibration** (Platt + isotonic candidates)
- **Why now**: Minimal code change; preserves LightGBM core; improves decision quality when downstream uses probabilities.
- **Expected impact**: 
  - Accuracy: **small** (+0.2 to +1.0 pp typical, sometimes neutral).
  - Decision quality/log loss/Brier: **moderate** improvement likely.
- **Reliability risk**: Low (offline fit + deterministic transform at inference).
- **Complexity**: Low.
- **New data needed**: None (use existing historical labels/preds).
- **Safe validation**:
  1. Walk-forward compare uncalibrated vs calibrated by fold (log loss, Brier, ECE).
  2. Run 1-2 weeks shadow mode: log both raw and calibrated probabilities; keep production picks unchanged initially.
  3. Promote only if no degradation in hit rate and clear gain in Brier/log loss.

### QW2. Replace fixed 0.5 threshold with **rolling, objective threshold policy**
- **Why now**: Home/away base rate and market environment drift; fixed 0.5 often suboptimal.
- **Expected impact**: Accuracy **small to moderate** (+0.3 to +1.5 pp) depending on season regime.
- **Reliability risk**: Low if threshold updates are slow and bounded.
- **Complexity**: Low.
- **New data needed**: None.
- **Safe validation**:
  - Use nested walk-forward to tune threshold on train window only.
  - Add guardrail: threshold can only move within bounded band (e.g., 0.48–0.54) unless strong evidence.

### QW3. Add **recency weighting / sample weighting** in LightGBM training
- **Why now**: MLB environments drift (run environment, bullpen usage, baseball composition, etc.).
- **Expected impact**: Accuracy **small to moderate** (+0.5 to +1.5 pp).
- **Reliability risk**: Low-medium (too aggressive decay can overfit recent noise).
- **Complexity**: Low-medium.
- **New data needed**: None.
- **Safe validation**:
  - Try 2–3 decay half-lives (e.g., 30/60/120 days) in walk-forward.
  - Choose by primary log loss, secondary Brier, no meaningful hit-rate drop.

### QW4. Add **missingness indicators** (not just coercion/imputation)
- **Why now**: Current pipeline coerces numeric and scales, but missingness itself can be predictive (late lineup info, pitcher uncertainty).
- **Expected impact**: Accuracy **small** (+0.2 to +0.8 pp), robustness gain under partial-data conditions.
- **Reliability risk**: Low.
- **Complexity**: Low-medium.
- **New data needed**: None.
- **Safe validation**:
  - Backtest under synthetic missingness stress (drop selected features by random schedule) and compare degradation slope.

---

## Medium-effort upgrades

### M1. Add **market-implied probability features** and/or conservative blend
- **Approach**: Convert moneylines to implied probs (remove vig approximation), add as features and optionally blend final prob: `p_final = w_model*p_model + (1-w)*p_market` with small `w_market` initially.
- **Expected impact**: Accuracy **moderate** (+0.8 to +2.5 pp) in many practical setups; can reduce bad outlier picks.
- **Reliability risk**: Medium (risk of overfitting to stale/opening lines; dependence on odds feed quality).
- **Complexity**: Medium.
- **New data needed**: Existing odds feed is sufficient; better if close-to-game snapshots captured.
- **Safe validation**:
  - Backtest by line timestamp strata (open/mid/close where available).
  - Shadow in production with `blend_off` (log candidate only) for 2 weeks.
  - Promote with hard cap on blend weight and fallback to model-only when odds missing.

### M2. Add **starting pitcher quality delta + bullpen fatigue proxies**
- **Approach**: Use rolling pitcher skill deltas and team bullpen usage/rest proxies (last 3 days IP, back-to-back high leverage usage).
- **Expected impact**: Accuracy **moderate** (+0.5 to +2.0 pp), especially in daily slate edges.
- **Reliability risk**: Medium (feature freshness and data quality).
- **Complexity**: Medium.
- **New data needed**: Additional game log slices (already accessible from baseball data sources).
- **Safe validation**:
  - Feature ablation (with/without bullpen-fatigue block) in walk-forward.
  - Shadow monitor missing-rate + stale-rate for new features.

### M3. Add **model uncertainty output** and abstention/tier logic for low-confidence games
- **Approach**: Keep full game predictions but explicitly track confidence bins and optionally suppress “strong pick” labeling when uncertainty high.
- **Expected impact**: Hit-rate among “high-confidence” subset improves; overall slate hit-rate may be flat/slightly up.
- **Reliability risk**: Low.
- **Complexity**: Medium.
- **New data needed**: None.
- **Safe validation**:
  - Evaluate calibration-by-tier, not just global accuracy.
  - Require monotonic tier quality (H > M > L win rate over rolling windows).

### M4. Expand walk-forward refresh into a **scheduled model governance loop**
- **Approach**: Monthly/biweekly retrain candidates via `scripts/model_refresh.py`, promote only through objective gate.
- **Expected impact**: Avoids stale-model decay; periodic modest gains.
- **Reliability risk**: Low if strict promotion gates are enforced.
- **Complexity**: Medium.
- **New data needed**: Historical training table availability and stable snapshot process.
- **Safe validation**:
  - Keep incumbent model as canary baseline every run.
  - No auto-promote unless gate passes across multiple recent folds.

---

## Avoid-for-now (high risk/complexity vs current constraints)

1. **Full architecture replacement** (deep seq models, transformers, online RL betting policy)
   - Potential upside, but large reliability/ops burden and hard-to-debug failure modes.
2. **Heavy feature explosion from pitch-level Statcast at game-time inference**
   - Data freshness latency and pipeline fragility likely to hurt reliability right now.
3. **Frequent threshold/model auto-updates without guardrails**
   - Creates non-stationary behavior and rollback pain.

---

## 3) Phased implementation plan

### Phase 0 (1–3 days): Measurement and safety harness
- Restore/point historical dataset for reproducible backtests.
- Add eval outputs: ECE/reliability bins, per-month accuracy/log loss/Brier.
- Add shadow logging fields for candidate probability + candidate pick.

**Acceptance criteria**
- Reproducible walk-forward report generated on demand.
- Baseline metrics frozen and versioned.
- Shadow logs written with zero runtime regressions.

### Phase 1 (3–7 days): Quick wins in shadow
- Implement QW1 calibration, QW2 threshold tuning, QW3 recency weighting (offline only at first).
- Keep production picks unchanged; log candidate outputs for side-by-side comparison.

**Acceptance criteria**
- Candidate beats incumbent on log loss and Brier in walk-forward.
- No >0.5 pp degradation in accuracy across recent folds.
- Zero increase in runtime failure metrics (sqlite/tweet/odds guardrails unchanged).

### Phase 2 (1–2 weeks): Controlled promotion + medium upgrade
- Promote best Phase-1 candidate.
- Add M1 (market features/blend) with capped influence and strict fallback.
- Optionally add M2 pitcher/bullpen fatigue block if data quality checks pass.

**Acceptance criteria**
- 2-week shadow vs incumbent shows stable or better hit-rate and better calibration.
- No reliability regressions (API retries/circuit events, missing-field guardrails, sqlite write failures).
- Rollback switch documented and tested.

---

## 4) Source notes (external)

1. https://scikit-learn.org/stable/modules/calibration.html  
   - Official calibration guidance (reliability curves, calibrated probabilities, Brier/log-loss context).

2. https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html  
   - Official time-ordered CV method to avoid future-data leakage.

3. https://otexts.com/fpp3/tscv.html  
   - Rolling-origin time-series cross-validation explanation; supports walk-forward validation approach.

4. https://arxiv.org/abs/2303.06021  
   - Sports betting ML study emphasizing calibration over raw accuracy for probabilistic decision quality.

5. https://library.fangraphs.com/principles/regression/  
   - Practitioner sabermetrics reference on regression-to-mean, supporting conservative recency handling.

6. https://library.fangraphs.com/principles/projections/  
   - Practitioner projection-system perspective (blend of history, recency, aging/context) aligned with feature/weighting upgrades.

7. https://github.com/jldbc/pybaseball  
   - Widely used open-source baseball data toolkit; practical path for adding pitcher/bullpen/context features without re-architecting.

8. https://en.wikipedia.org/wiki/Pythagorean_expectation  
   - Canonical baseball baseline concept for run-differential expectation; useful as an additional low-complexity feature signal.

---

## Bottom line
- Best immediate ROI with minimal risk: **Calibration + threshold policy + recency-weighted retrain**, validated by walk-forward and shadow logging before any cutover.
- Next lever after that: **conservative model-market blending** with strict fallback/weight caps.
- Keep the current reliability-first posture; avoid architectural leaps until governance and data quality loops are fully mature.
