# Workstream E — Preseason Model Refresh Assessment (2026-03-09)

## Scope completed
- Inspected current prediction pipeline in `data.py` + `predict.py`.
- Assessed current model artifact (`models/mlb4year.txt`) and inference behavior.
- Added minimal retrain/eval scaffold: `scripts/model_refresh.py`.
- Attempted to run backtest with configured historical dataset path.

## Current pipeline (what exists today)
- **Model type:** LightGBM binary classifier loaded from static file `models/mlb4year.txt`.
- **Feature shape:** 44 features (home/away engineered stats), `FEATURE_ORDER=order2`.
- **Inference strategy:** 10 perturbed forward passes (`perturbation_scale=0.001`) averaged to produce final probability.
- **Decision rule:** `p >= 0.5` => home team; else away team.
- **Operational note:** no in-repo retraining script or repeatable offline validation harness existed before this update.

## Observed metrics / diagnostics
- Loaded model metadata:
  - `num_trees`: 500
  - `num_feature`: 44
- Inference micro-benchmark (local, model-only core):
  - single pass `model.predict(...)`: **~0.041 ms** average
  - current 10-perturbation averaging loop: **~0.397 ms** average
- Backtest status:
  - Could not run full historical backtest because configured training data path `data/predictions.xlsx` is not present in this workspace.

## Implemented scaffold update
### New file
- `scripts/model_refresh.py`

### What it does
- Loads historical game-level data (`.xlsx`, `.csv`, or `.parquet`).
- Uses existing feature convention (prefers known `order2` feature list; falls back to numeric columns).
- Runs **walk-forward** validation (expanding window, default 3 splits).
- Evaluates two concrete options:
  1. `lgbm_baseline` (smaller/regularized)
  2. `lgbm_stronger` (deeper/slower, stronger capacity)
- Reports per-fold + averaged metrics:
  - accuracy
  - log loss
  - Brier score
  - train wall-time
- Writes JSON report to `docs/reports/model-refresh-latest.json` (configurable).

### How to run
```bash
.venv/bin/python scripts/model_refresh.py \
  --input <historical_data.xlsx|csv|parquet> \
  --output docs/reports/model-refresh-latest.json
```

## Recommendation (go-forward)
1. **Do refresh pre-season (recommended): YES**
   - Current model appears to be a static artifact with no reproducible in-repo retrain loop.
   - A pre-season data shift check + retrain/eval pass is low-risk and high expected value.

2. **Adopt two-tier model policy**
   - **Baseline (production safety):** current-style LightGBM with conservative params (`lgbm_baseline` in new scaffold).
   - **Stronger candidate (promotion target):** higher-capacity LightGBM (`lgbm_stronger`).

3. **Promotion gate (objective)**
   - Promote stronger candidate only if on walk-forward validation it beats baseline on:
     - log loss (primary),
     - Brier score (secondary),
     - with no meaningful degradation in accuracy.

4. **Complexity / latency tradeoff expectation**
   - Baseline: lower complexity, faster training, lower overfit risk.
   - Stronger: potentially better calibration/discrimination, higher tuning sensitivity and training cost.
   - Runtime inference cost should remain negligible relative to API/data retrieval overhead.

## Next unblock needed
- Provide/restore historical training file at `DATA_PATH` (or pass explicit `--input`) so scaffold can produce real backtest metrics and final promotion decision.
