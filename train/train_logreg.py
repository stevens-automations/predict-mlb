from __future__ import annotations

import argparse
import json
import pickle
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np

from train.config import MissingOptionalDependency, normalize_training_config, validate_training_config
from train.data_loader import DatasetBundle, load_feature_rows
from train.metrics import classification_metrics
from train.model_registry import register_model_run
from train.splits import resolve_evaluation_plan
from train.train_lgbm import FoldResult, _average_fold_metrics, _fold_result


def _load_sklearn():
    try:
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
    except Exception as exc:  # pragma: no cover - environment dependent
        raise MissingOptionalDependency(
            "scikit-learn",
            "scikit-learn import failed. Install the local training dependencies before running the logistic baseline."
        ) from exc
    return Pipeline, SimpleImputer, StandardScaler, LogisticRegression


def _build_pipeline(params: dict[str, Any]):
    Pipeline, SimpleImputer, StandardScaler, LogisticRegression = _load_sklearn()
    estimator_params = dict(params)
    estimator_params.setdefault("max_iter", 2000)
    estimator_params.setdefault("solver", "lbfgs")
    estimator_params.setdefault("random_state", 20260312)
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(**estimator_params)),
        ]
    )


def fit_logistic_baseline(
    dataset: DatasetBundle,
    *,
    experiment_name: str,
    model_name: str,
    params: dict[str, Any],
    evaluation_config: dict[str, Any],
    registry_root: str | Path,
    config_snapshot: dict[str, Any],
) -> dict[str, Any]:
    evaluation_plan = resolve_evaluation_plan(
        seasons=dataset.dataframe["season"],
        dates=dataset.dataframe[dataset.date_column],
        evaluation_config=evaluation_config,
    )

    X = dataset.X
    y = dataset.y.astype(float).to_numpy()

    fold_results: list[FoldResult] = []
    all_predictions = np.full(shape=len(X), fill_value=np.nan, dtype=float)
    train_started = time.perf_counter()

    for split in evaluation_plan.development_splits:
        model = _build_pipeline(params)
        model.fit(X.iloc[split.train_idx], y[split.train_idx])
        probabilities = model.predict_proba(X.iloc[split.test_idx])[:, 1]
        all_predictions[split.test_idx] = probabilities
        fold_results.append(_fold_result(split, y, probabilities))

    if not fold_results:
        raise RuntimeError("No model was trained because no development folds were produced")

    final_train_idx = (
        evaluation_plan.holdout_split.train_idx
        if evaluation_plan.holdout_split is not None
        else np.arange(len(X), dtype=int)
    )
    final_model = _build_pipeline(params)
    final_model.fit(X.iloc[final_train_idx], y[final_train_idx])

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_model_path = Path(tmpdir) / f"{model_name}.pkl"
        with temp_model_path.open("wb") as handle:
            pickle.dump(final_model, handle)

        development_predictions = all_predictions[~np.isnan(all_predictions)]
        development_truth = y[~np.isnan(all_predictions)]
        holdout_metrics = None
        if evaluation_plan.holdout_split is not None:
            holdout = evaluation_plan.holdout_split
            holdout_probabilities = final_model.predict_proba(X.iloc[holdout.test_idx])[:, 1]
            holdout_metrics = {
                "holdout_season": holdout.holdout_season,
                "n_train": int(len(holdout.train_idx)),
                "n_test": int(len(holdout.test_idx)),
                "train_start": holdout.train_start,
                "train_end": holdout.train_end,
                "test_start": holdout.test_start,
                "test_end": holdout.test_end,
                "metrics": classification_metrics(y[holdout.test_idx], holdout_probabilities),
            }

        summary_metrics = {
            "development": {
                "aggregate": classification_metrics(development_truth, development_predictions),
                "folds": [
                    {
                        "fold": result.fold,
                        "n_train": result.n_train,
                        "n_test": result.n_test,
                        "train_start": result.train_start,
                        "train_end": result.train_end,
                        "test_start": result.test_start,
                        "test_end": result.test_end,
                        "metrics": result.metrics,
                    }
                    for result in fold_results
                ],
                "fold_average": _average_fold_metrics(fold_results),
            },
            "holdout": holdout_metrics,
            "train_seconds": time.perf_counter() - train_started,
        }
        metadata = {
            "experiment_name": experiment_name,
            "model_name": model_name,
            "feature_columns": dataset.feature_columns,
            "n_rows": int(len(dataset.dataframe)),
            "n_features": int(len(dataset.feature_columns)),
            "dataset_diagnostics": dataset.diagnostics,
            "feature_version": config_snapshot["data"]["feature_version"],
            "db_path": config_snapshot["data"]["db_path"],
            "training_contract": config_snapshot["contract"],
            "evaluation_strategy": evaluation_plan.strategy,
            "evaluation_config": evaluation_config,
            "logreg_params": params,
            "final_train_rows": int(len(final_train_idx)),
            "config_snapshot": config_snapshot,
        }
        record = register_model_run(
            registry_root=registry_root,
            experiment_name=experiment_name,
            model_name=model_name,
            metadata=metadata,
            metrics=summary_metrics,
            source_model_path=temp_model_path,
        )

    return {
        "registry": {
            "run_id": record.run_id,
            "run_dir": str(record.run_dir),
            "model_path": str(record.model_path),
            "metadata_path": str(record.metadata_path),
            "metrics_path": str(record.metrics_path),
        },
        "metrics": summary_metrics,
    }


def run_from_config(config: dict[str, Any]) -> dict[str, Any]:
    config = validate_training_config(config)
    data_config = dict(config["data"])
    dataset = load_feature_rows(
        db_path=data_config["db_path"],
        seasons=data_config.get("seasons"),
        start_date=data_config.get("start_date"),
        end_date=data_config.get("end_date"),
        feature_version=data_config.get("feature_version", "v1"),
        contract_statuses=data_config.get("contract_statuses", ("valid", "degraded")),
        explicit_feature_columns=data_config.get("feature_columns"),
        feature_contract=data_config.get("feature_contract", "raw"),
    )
    return fit_logistic_baseline(
        dataset,
        experiment_name=config["experiment"]["name"],
        model_name=config["model"]["name"],
        params=config["model"].get("params", {}),
        evaluation_config=config["evaluation"],
        registry_root=config["registry"]["root_dir"],
        config_snapshot=config,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train a baseline logistic regression model from SQLite feature_rows + labels"
    )
    parser.add_argument("--config", required=True, help="Path to JSON config file")
    parser.add_argument("--print-only", action="store_true", help="Load config and print resolved settings without training")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config_path = Path(args.config)
    config = normalize_training_config(json.loads(config_path.read_text()))
    if args.print_only:
        print(json.dumps(config, indent=2, sort_keys=True))
        return
    result = run_from_config(config)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
