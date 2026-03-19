# Training Architecture Runbook

## Goal

This runbook documents the **currently implemented** offline training scaffold for `predict-mlb` using the SQLite historical store as the canonical source of model rows.

For the broader externally informed training strategy, target operating window, and next-rebuild planning direction, see `docs/runbooks/training-manifest.md`.

Primary design choices:

- Canonical training input is `feature_rows(feature_version='v2_phase1') + labels`
- The canonical contract is `pregame_1h`, pregame-only, with `did_home_win` as the home-win label
- Sportsbook odds are forbidden as model features and the loader rejects sportsbook-like feature columns
- Training and evaluation stay script/module based, not notebook dependent
- Evaluation is time-aware via expanding season-level development folds over 2020-2024 plus an untouched 2025 holdout
- The first active training path is LightGBM on the full integrated pregame contract
- Logistic regression stays available as a benchmark/comparison trainer on the same contract and evaluation frame
- Trained models are saved into a local model-registry-style directory with metadata and metrics
- Config normalization now enforces one canonical path: integrated `v2_phase1`, allowed contract statuses `["valid", "degraded"]`, and the fixed 2020-2025 season window

## Files

- `train/data_loader.py`
  - Loads `feature_rows` JSON payloads, joins canonical game metadata and `labels`, filters by season/date/feature version, validates one row per game, and infers numeric feature columns while rejecting sportsbook-like columns
- `train/splits.py`
  - Generates either generic walk-forward splits or the canonical season-based development/holdout evaluation plan
- `train/config.py`
  - Normalizes configs onto the canonical `pregame_1h` contract and rejects drift from the integrated holdout scheme
- `train/metrics.py`
  - Computes log loss, Brier score, accuracy, and calibration bins
- `train/train_lgbm.py`
  - Baseline LightGBM trainer using the SQLite loader and the canonical evaluation plan
- `train/train_logreg.py`
  - Logistic regression baseline with median imputation and scaling for the same contract/evaluation path
- `train/experiment_runner.py`
  - Runs one or more JSON-defined experiments across supported trainers
- `train/model_registry.py`
  - Persists model artifacts plus `metadata.json` and `metrics.json`
- `scripts/training/train_lgbm.py`
  - CLI entrypoint for a single config
- `scripts/training/experiment_runner.py`
  - CLI entrypoint for multi-experiment runs

## Configs

Config templates live under `configs/training/`:

- `baseline_lgbm.json`
- `tuned_candidate.json`
- `ensemble_candidate_placeholder.json`
- `experiment_suite.json`
- `promotion_gates.json`

The default config intentionally points at `data/mlb_history.db` but does not run automatically.

The canonical training configs now encode:
- `contract.name = "pregame_1h"`
- `contract.target = "home_team_win_probability"`
- `data.feature_version = "v2_phase1"`
- `data.contract_statuses = ["valid", "degraded"]`
- `data.seasons = [2020, 2021, 2022, 2023, 2024, 2025]`
- `evaluation.seasonal_holdout.development_seasons = [2020, 2021, 2022, 2023, 2024]`
- `evaluation.seasonal_holdout.holdout_season = 2025`

## Model Registry Layout

Artifacts are written under `artifacts/model_registry/` by default:

```text
artifacts/model_registry/
  baseline_pregame_1h_v2_phase1_lgbm__lgbm_baseline__20260312T...
    lgbm_baseline.txt
    metadata.json
    metrics.json
  baseline_pregame_1h_v2_phase1_lgbm__latest.json
```

`metadata.json` captures the config snapshot, feature list, row counts, and trainer params. `metrics.json` captures development-fold metrics plus holdout metrics when a holdout regime is configured.
For season-based runs it also captures the 2025 holdout evaluation and the exact contract snapshot used to create it.

## How To Inspect Data Readiness

Print the normalized canonical config without training:

```bash
.venv/bin/python scripts/training/train_lgbm.py --config configs/training/baseline_lgbm.json --print-only
```

If ingestion is incomplete, the loader raises a clear error when no eligible `feature_rows` are found for the configured filters.
If dataset assembly is malformed, the loader now fails before fitting on:
- duplicate feature rows for the same game
- missing required contract columns
- non-binary labels
- sportsbook-like feature leakage

To poll the historical DB until the requested seasons are fully trainable:

```bash
.venv/bin/python scripts/training/run_when_ready.py --action check
```

## First Honest Baseline Run

Once historical ingestion and `feature_rows(feature_version='v2_phase1')` are ready for 2020-2025:

```bash
.venv/bin/python scripts/training/experiment_runner.py --config configs/training/experiment_suite.json
```

That suite is the canonical first honest exploratory flow:
- LightGBM baseline development folds on 2020-2024, then 2025 holdout evaluation
- LightGBM tuned challenger on the same frame
- logistic regression benchmark on the same frame when `scikit-learn` is available
- a disabled ensemble placeholder kept out of the main path

If `scikit-learn` is not installed yet, the suite reports the logistic benchmark as `blocked` and continues to the LightGBM experiments instead of failing the whole manifest.

To run only the LightGBM baseline:

```bash
.venv/bin/python scripts/training/train_lgbm.py --config configs/training/baseline_lgbm.json
```

To block until ingestion completes and then launch the baseline automatically:

```bash
.venv/bin/python scripts/training/run_when_ready.py --action baseline --max-wait-seconds 3600 --poll-seconds 300
```

## Notebook-Friendly Usage

The package is notebook-friendly without making notebooks the execution path. Example:

```python
from train.data_loader import load_feature_rows
from train.splits import build_seasonal_evaluation_plan

dataset = load_feature_rows(
    db_path="data/mlb_history.db",
    feature_version="v2_phase1",
    seasons=[2020, 2021, 2022, 2023, 2024, 2025],
)
plan = build_seasonal_evaluation_plan(
    seasons=dataset.dataframe["season"],
    dates=dataset.dataframe["game_date"],
    development_seasons=[2020, 2021, 2022, 2023, 2024],
    holdout_season=2025,
)
dataset.dataframe.head()
```

## Operational Notes

- The base training scaffold expects `numpy` and `pandas`; without them even loader/split/trainer imports are blocked before model fitting starts.
- The trainer imports `lightgbm` lazily so test modules can run even if the local machine is missing `libomp`.
- The logistic benchmark imports scikit-learn lazily so environments without those packages fail only when the logistic trainer is invoked.
- The experiment runner treats missing optional trainer dependencies as per-experiment `blocked` results, so a missing sklearn install does not poison the LightGBM path or readiness checks.
- Season-based evaluation requires rows sorted by `game_date`; the loader enforces date ordering.
- Feature inference only trains on numeric payload columns and excludes identifiers, labels, and date fields.
- Feature inference fails fast if sportsbook-odds-style columns appear in the usable feature set.
- Dataset metadata now captures contract diagnostics such as season coverage, label balance, contract-status counts, and feature-null counts inside each model-registry record.
- Experiment suites can carry disabled placeholders; the runner skips them instead of failing.
- Readiness polling verifies each requested season has labeled games with matching eligible `feature_rows`.
- If `v2_phase1` materialization or backfill is incomplete, finish that in the ingestion path rather than weakening the trainer back to `v1`.
- This scaffold is for exploratory model development first; the final daily inference schema should follow the winning training contract rather than lead it.
- This scaffold does not automatically trigger any expensive multi-season run.
