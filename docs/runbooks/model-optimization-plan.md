# Model Optimization Plan

Last updated: 2026-03-09

## Goal

Train immediately once historical ingestion completes, with a reliability-first loop that improves log loss first, preserves train/inference parity, and keeps later uplift tracks isolated behind explicit gates.

## Canonical Files

- `configs/training/baseline_lgbm.json` — immediate baseline run
- `configs/training/tuned_candidate.json` — first challenger tuned for stability
- `configs/training/ensemble_candidate_placeholder.json` — disabled placeholder for later blend work
- `configs/training/experiment_suite.json` — canonical multi-experiment entrypoint
- `configs/training/promotion_gates.json` — exact promotion thresholds template
- `scripts/training/run_when_ready.py` — readiness gate + launch script
- `docs/runbooks/training-architecture.md` — implementation details for the scaffold

## Phase 1: Baseline Training (Immediate)

Objective: establish the first reproducible 2020-2025 walk-forward baseline off `feature_rows + labels` with no new feature dependencies.

Scope:
- Keep `feature_version='v1'`
- Train on contract statuses `valid` and `degraded` only
- Use walk-forward evaluation only
- Register artifacts and metrics under `artifacts/model_registry/`

Baseline configuration:
- Trainer: LightGBM binary classifier
- Primary metric: log loss
- Walk-forward window: `min_train_samples=1500`, `test_size=300`, `step_size=300` across the full eligible history
- Output metrics: log loss, Brier, accuracy, expected calibration error, max calibration gap, calibration bins

Why this is first:
- It matches the current inference contract most closely.
- It avoids new ingestion dependencies while the multi-season backfill finishes.
- It gives the team a stable incumbent reference before any uplift work.

## Phase 2: Robustness and Stability Upgrades

Objective: improve consistency before chasing larger lifts.

Priority sequence:
1. Freeze the baseline feature set and split scheme so every challenger uses the same leakage-safe evaluation frame.
2. Add promotion-gate review to every candidate run using `configs/training/promotion_gates.json`.
3. Keep degraded rows allowed, but monitor degraded share and block promotion if it rises above threshold.
4. Add calibration review to the standard evaluation checklist before any threshold or ensemble policy changes.

Expected work in this phase:
- Compare baseline vs tuned candidate using the same data slice
- Add light post-training governance helpers later if the team wants automatic gate pass/fail evaluation
- Keep all candidate changes offline until they beat the incumbent on log loss without reliability regressions

## Phase 3: Performance Lift Experiments

Objective: try higher-upside experiments only after the baseline loop is stable.

Ordered experiments:
1. Parameter tuning inside the existing LightGBM/tabular path
2. Recency weighting and missingness-indicator variants, but only if train/inference parity can be maintained
3. Conservative calibrated blend or simple ensemble, only after one single-model challenger clears gates

Rules:
- No feature additions that the live inference path cannot reproduce reliably
- No random train/test shuffle validation
- No experiments that require future information not available as-of each game
- Keep the first ensemble track disabled until a single tuned model proves stable

## Phase 4: Optional Uplift Feature Tracks

These are intentionally separate from the immediate training launch.

### Weather track

Candidate features:
- game-time temperature
- wind direction and speed
- precipitation risk

Gate before starting:
- historical weather coverage >= 98% for the training horizon
- timestamp alignment proven to be pre-game only
- missingness fallback path defined for daily inference

### Roster / lineup track

Candidate features:
- confirmed lineup strength deltas
- rested starters / bench concentration
- catcher and platoon context

Gate before starting:
- source can be captured before prediction cutoff for at least 95% of games
- late-swap behavior is documented
- feature freshness can be monitored in runtime

### Injury / availability track

Candidate features:
- starter absence flags
- bullpen availability stress
- high-impact batter/pitcher inactive counts

Gate before starting:
- structured injury source is available with consistent timestamps
- injury status can be normalized into stable categories
- daily missing/inconsistent source behavior has an explicit degraded-mode policy

If any uplift track cannot meet those gates, it stays out of the training set.

## Promotion Gates

Canonical thresholds live in `configs/training/promotion_gates.json`.

Candidate promotion requires all of the following:
- At least 4 completed walk-forward folds
- Aggregate log loss <= `0.6930`
- Aggregate log loss improves on incumbent by at least `0.0020`
- No recent fold regresses against incumbent by more than `0.0050` log loss
- Brier score <= `0.2500` and not worse than incumbent by more than `0.0010`
- Accuracy >= `0.5000` and not worse than incumbent by more than `0.0050`
- Expected calibration error <= `0.0250`
- Max calibration gap <= `0.0750`
- Probability output range remains sensible: min <= `0.0500`, max >= `0.9500`
- Required seasons are fully trainable
- Degraded feature-row share <= `0.1000`

Promotion review template:

| Field | Candidate | Incumbent | Gate |
| --- | --- | --- | --- |
| Run ID |  |  | record |
| Data window |  |  | must match |
| Feature version |  |  | must match |
| Folds completed |  |  | `>= 4` |
| Aggregate log loss |  |  | improve by `>= 0.0020` |
| Fold-average log loss |  |  | review only |
| Aggregate Brier |  |  | `<= incumbent + 0.0010` |
| Aggregate accuracy |  |  | `>= incumbent - 0.0050` |
| Expected calibration error |  |  | `<= 0.0250` |
| Max calibration gap |  |  | `<= 0.0750` |
| Probability min / max |  |  | `<= 0.0500` / `>= 0.9500` |
| Degraded feature share |  |  | `<= 0.1000` |
| Leakage review | pass/fail | pass/fail | must pass |
| Train/inference parity review | pass/fail | pass/fail | must pass |
| Promotion decision |  |  | go / no-go |

## First Run After Ingestion Completes

Readiness check only:

```bash
.venv/bin/python scripts/training/run_when_ready.py --action check
```

Launch the baseline as soon as all 2020-2025 seasons are trainable:

```bash
.venv/bin/python scripts/training/run_when_ready.py --action baseline --max-wait-seconds 3600 --poll-seconds 300
```

Run the baseline plus tuned candidate suite after the baseline finishes:

```bash
.venv/bin/python scripts/training/run_when_ready.py --action suite
```

## Immediate Operating Notes

- Do not start weather, roster, or injury features in the baseline window.
- Do not promote any challenger solely on accuracy.
- Keep the first ensemble candidate as a placeholder until a single-model tuned candidate proves stable.
- Treat the model registry output as the source of truth for promotion review.
