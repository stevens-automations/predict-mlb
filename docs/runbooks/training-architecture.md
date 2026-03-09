# Training Architecture Runbook

## Goal

This scaffold adds a repeatable offline training path for `predict-mlb` using the SQLite historical store as the canonical source of model rows.

Primary design choices:

- Canonical training input is `feature_rows + labels`
- Training and evaluation stay script/module based, not notebook dependent
- Evaluation is time-aware via walk-forward splits
- Trained models are saved into a local model-registry-style directory with metadata and metrics

## Files

- `train/data_loader.py`
  - Loads `feature_rows` JSON payloads, joins `labels`, filters by season/date/feature version, and infers numeric feature columns
- `train/splits.py`
  - Generates expanding-window walk-forward splits over ordered game dates
- `train/metrics.py`
  - Computes log loss, Brier score, accuracy, and calibration bins
- `train/train_lgbm.py`
  - Baseline LightGBM trainer using the SQLite loader and walk-forward evaluation
- `train/experiment_runner.py`
  - Runs one or more JSON-defined experiments
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

## Model Registry Layout

Artifacts are written under `artifacts/model_registry/` by default:

```text
artifacts/model_registry/
  baseline_walk_forward_v1__lgbm_baseline__20260309T...
    lgbm_baseline.txt
    metadata.json
    metrics.json
  baseline_walk_forward_v1__latest.json
```

`metadata.json` captures the config snapshot, feature list, row counts, and trainer params. `metrics.json` captures fold-level and aggregate walk-forward metrics.

## How To Inspect Data Readiness

Print the resolved config without training:

```bash
.venv/bin/python scripts/training/train_lgbm.py --config configs/training/baseline_lgbm.json --print-only
```

If ingestion is incomplete, the loader raises a clear error when no eligible `feature_rows` are found for the configured filters.

To poll the historical DB until the requested seasons are fully trainable:

```bash
.venv/bin/python scripts/training/run_when_ready.py --action check
```

## First Baseline Run

Once historical ingestion and `feature_rows(feature_version='v1')` are ready for the desired seasons:

```bash
.venv/bin/python scripts/training/train_lgbm.py --config configs/training/baseline_lgbm.json
```

To run a small experiment suite:

```bash
.venv/bin/python scripts/training/experiment_runner.py --config configs/training/experiment_suite.json
```

To block until ingestion completes and then launch the baseline automatically:

```bash
.venv/bin/python scripts/training/run_when_ready.py --action baseline --max-wait-seconds 3600 --poll-seconds 300
```

## Notebook-Friendly Usage

The package is notebook-friendly without making notebooks the execution path. Example:

```python
from train.data_loader import load_feature_rows
from train.splits import generate_walk_forward_splits

dataset = load_feature_rows(db_path="data/mlb_history.db", feature_version="v1", seasons=[2020, 2021, 2022])
splits = generate_walk_forward_splits(dataset.dataframe["game_date"], min_train_samples=750, test_size=250)
dataset.dataframe.head()
```

## Operational Notes

- The trainer imports `lightgbm` lazily so test modules can run even if the local machine is missing `libomp`.
- Walk-forward splitting requires rows sorted by `game_date`; the loader enforces date ordering.
- Feature inference only trains on numeric payload columns and excludes identifiers, labels, and date fields.
- Experiment suites can carry disabled placeholders; the runner skips them instead of failing.
- Readiness polling verifies each requested season has labeled games with matching eligible `feature_rows`.
- This scaffold does not automatically trigger any expensive multi-season run.
