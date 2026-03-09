from __future__ import annotations

import argparse
import json
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from train.data_loader import DatasetBundle, load_feature_rows
from train.metrics import classification_metrics
from train.model_registry import register_model_run
from train.splits import WalkForwardSplit, generate_walk_forward_splits


@dataclass(frozen=True)
class FoldResult:
    fold: int
    n_train: int
    n_test: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    metrics: dict[str, Any]


def _load_lightgbm():
    try:
        import lightgbm as lgb  # type: ignore
    except Exception as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "LightGBM import failed. Ensure the local environment can import `lightgbm` "
            "(for macOS this may require `libomp`)."
        ) from exc
    return lgb


def fit_walk_forward_lgbm(
    dataset: DatasetBundle,
    *,
    experiment_name: str,
    model_name: str,
    params: dict[str, Any],
    split_config: dict[str, Any],
    registry_root: str | Path,
    config_snapshot: dict[str, Any],
) -> dict[str, Any]:
    lgb = _load_lightgbm()
    dates = dataset.dataframe[dataset.date_column]
    splits = generate_walk_forward_splits(dates, **split_config)

    X = dataset.X
    y = dataset.y.astype(float).to_numpy()

    params = dict(params)
    num_boost_round = int(params.pop("num_boost_round", 300))

    fold_results: list[FoldResult] = []
    all_predictions = np.full(shape=len(X), fill_value=np.nan, dtype=float)
    train_started = time.perf_counter()
    final_model = None

    for split in splits:
        train_set = lgb.Dataset(X.iloc[split.train_idx], label=y[split.train_idx], free_raw_data=True)
        final_model = lgb.train(params=params, train_set=train_set, num_boost_round=num_boost_round)
        probabilities = final_model.predict(X.iloc[split.test_idx])
        all_predictions[split.test_idx] = probabilities
        fold_results.append(_fold_result(split, y, probabilities))

    if final_model is None:
        raise RuntimeError("No model was trained because no walk-forward splits were produced")

    final_train_idx = np.arange(len(X), dtype=int)
    final_train_set = lgb.Dataset(X.iloc[final_train_idx], label=y[final_train_idx], free_raw_data=True)
    final_model = lgb.train(params=params, train_set=final_train_set, num_boost_round=num_boost_round)

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_model_path = Path(tmpdir) / f"{model_name}.txt"
        final_model.save_model(str(temp_model_path))

        aggregate_predictions = all_predictions[~np.isnan(all_predictions)]
        aggregate_truth = y[~np.isnan(all_predictions)]
        aggregate_metrics = classification_metrics(aggregate_truth, aggregate_predictions)
        summary_metrics = {
            "aggregate": aggregate_metrics,
            "folds": [asdict(result) for result in fold_results],
            "fold_average": _average_fold_metrics(fold_results),
            "train_seconds": time.perf_counter() - train_started,
        }
        metadata = {
            "experiment_name": experiment_name,
            "model_name": model_name,
            "feature_columns": dataset.feature_columns,
            "n_rows": int(len(dataset.dataframe)),
            "n_features": int(len(dataset.feature_columns)),
            "feature_version": config_snapshot["data"]["feature_version"],
            "db_path": config_snapshot["data"]["db_path"],
            "split_config": split_config,
            "lgbm_params": params,
            "num_boost_round": num_boost_round,
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


def _fold_result(split: WalkForwardSplit, y: np.ndarray, probabilities: np.ndarray) -> FoldResult:
    return FoldResult(
        fold=split.fold,
        n_train=int(len(split.train_idx)),
        n_test=int(len(split.test_idx)),
        train_start=split.train_start,
        train_end=split.train_end,
        test_start=split.test_start,
        test_end=split.test_end,
        metrics=classification_metrics(y[split.test_idx], probabilities),
    )


def _average_fold_metrics(fold_results: list[FoldResult]) -> dict[str, float]:
    if not fold_results:
        return {}
    metric_names = ("log_loss", "brier_score", "accuracy")
    averages = {}
    for metric_name in metric_names:
        averages[metric_name] = float(np.mean([result.metrics[metric_name] for result in fold_results]))
    return averages


def run_from_config(config: dict[str, Any]) -> dict[str, Any]:
    data_config = dict(config["data"])
    dataset = load_feature_rows(
        db_path=data_config["db_path"],
        seasons=data_config.get("seasons"),
        start_date=data_config.get("start_date"),
        end_date=data_config.get("end_date"),
        feature_version=data_config.get("feature_version", "v1"),
        contract_statuses=data_config.get("contract_statuses", ("valid", "degraded")),
        explicit_feature_columns=data_config.get("feature_columns"),
    )
    return fit_walk_forward_lgbm(
        dataset,
        experiment_name=config["experiment"]["name"],
        model_name=config["model"]["name"],
        params=config["model"]["params"],
        split_config=config["evaluation"]["walk_forward"],
        registry_root=config["registry"]["root_dir"],
        config_snapshot=config,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train a baseline LightGBM model from SQLite feature_rows + labels")
    parser.add_argument("--config", required=True, help="Path to JSON config file")
    parser.add_argument("--print-only", action="store_true", help="Load config and print resolved settings without training")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config_path = Path(args.config)
    config = json.loads(config_path.read_text())
    if args.print_only:
        print(json.dumps(config, indent=2, sort_keys=True))
        return
    result = run_from_config(config)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

