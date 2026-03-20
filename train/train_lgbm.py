from __future__ import annotations

import argparse
import csv
import json
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from train.config import MissingOptionalDependency, normalize_training_config, validate_training_config
from train.data_loader import DatasetBundle, load_feature_rows
from train.metrics import classification_metrics
from train.model_registry import register_model_run
from train.splits import EvaluationPlan, WalkForwardSplit, resolve_evaluation_plan


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
        raise MissingOptionalDependency(
            "lightgbm",
            "LightGBM import failed. Ensure the local environment can import `lightgbm` "
            "(for macOS this may require `libomp`).",
        ) from exc
    return lgb


def summarize_feature_importance(
    booster: Any,
    *,
    feature_names: list[str] | None = None,
) -> list[dict[str, Any]]:
    resolved_feature_names = list(feature_names or booster.feature_name())
    gain_values = booster.feature_importance(importance_type="gain")
    split_values = booster.feature_importance(importance_type="split")
    total_gain = float(np.sum(gain_values))
    total_splits = int(np.sum(split_values))

    rows: list[dict[str, Any]] = []
    for index, feature_name in enumerate(resolved_feature_names):
        gain = float(gain_values[index])
        split = int(split_values[index])
        rows.append(
            {
                "feature": feature_name,
                "gain": gain,
                "gain_share": float(gain / total_gain) if total_gain > 0 else 0.0,
                "split": split,
                "split_share": float(split / total_splits) if total_splits > 0 else 0.0,
            }
        )

    by_gain = sorted(rows, key=lambda row: (-row["gain"], -row["split"], row["feature"]))
    gain_ranks = {row["feature"]: rank for rank, row in enumerate(by_gain, start=1)}
    by_split = sorted(rows, key=lambda row: (-row["split"], -row["gain"], row["feature"]))
    split_ranks = {row["feature"]: rank for rank, row in enumerate(by_split, start=1)}

    summary = []
    for row in by_gain:
        enriched = dict(row)
        enriched["gain_rank"] = gain_ranks[row["feature"]]
        enriched["split_rank"] = split_ranks[row["feature"]]
        summary.append(enriched)
    return summary


def write_feature_importance_artifacts(
    output_dir: str | Path,
    importance_rows: list[dict[str, Any]],
) -> tuple[Path, Path]:
    output_dir = Path(output_dir)
    json_path = output_dir / "feature_importance.json"
    csv_path = output_dir / "feature_importance.csv"
    json_path.write_text(json.dumps({"feature_importance": importance_rows}, indent=2, sort_keys=True) + "\n")
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "feature",
                "gain",
                "gain_share",
                "gain_rank",
                "split",
                "split_share",
                "split_rank",
            ],
        )
        writer.writeheader()
        writer.writerows(importance_rows)
    return json_path, csv_path


def fit_walk_forward_lgbm(
    dataset: DatasetBundle,
    *,
    experiment_name: str,
    model_name: str,
    params: dict[str, Any],
    evaluation_config: dict[str, Any],
    registry_root: str | Path,
    config_snapshot: dict[str, Any],
) -> dict[str, Any]:
    lgb = _load_lightgbm()
    evaluation_plan = resolve_evaluation_plan(
        seasons=dataset.dataframe["season"],
        dates=dataset.dataframe[dataset.date_column],
        evaluation_config=evaluation_config,
    )

    X = dataset.X
    y = dataset.y.astype(float).to_numpy()

    params = dict(params)
    num_boost_round = int(params.pop("num_boost_round", 300))

    fold_results: list[FoldResult] = []
    all_predictions = np.full(shape=len(X), fill_value=np.nan, dtype=float)
    train_started = time.perf_counter()
    final_model = None

    for split in evaluation_plan.development_splits:
        train_set = lgb.Dataset(X.iloc[split.train_idx], label=y[split.train_idx], free_raw_data=True)
        final_model = lgb.train(params=params, train_set=train_set, num_boost_round=num_boost_round)
        probabilities = final_model.predict(X.iloc[split.test_idx])
        all_predictions[split.test_idx] = probabilities
        fold_results.append(_fold_result(split, y, probabilities))

    if final_model is None:
        raise RuntimeError("No model was trained because no walk-forward splits were produced")

    final_train_idx = (
        evaluation_plan.holdout_split.train_idx
        if evaluation_plan.holdout_split is not None
        else np.arange(len(X), dtype=int)
    )
    final_train_set = lgb.Dataset(X.iloc[final_train_idx], label=y[final_train_idx], free_raw_data=True)
    final_model = lgb.train(params=params, train_set=final_train_set, num_boost_round=num_boost_round)

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_model_path = Path(tmpdir) / f"{model_name}.txt"
        final_model.save_model(str(temp_model_path))
        importance_rows = summarize_feature_importance(final_model, feature_names=dataset.feature_columns)
        temp_importance_json, temp_importance_csv = write_feature_importance_artifacts(tmpdir, importance_rows)

        development_metrics = _summarize_development_metrics(all_predictions, y, fold_results)
        holdout_metrics = _summarize_holdout_metrics(evaluation_plan, final_model.predict(X), y)
        summary_metrics = {
            "development": development_metrics,
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
            "feature_exclusions": dataset.diagnostics.get("contract_diagnostics", {}).get("feature_exclusions", {})
            if dataset.diagnostics
            else {},
            "lgbm_params": params,
            "num_boost_round": num_boost_round,
            "final_train_rows": int(len(final_train_idx)),
            "feature_importance_top_gain": importance_rows[:15],
            "config_snapshot": config_snapshot,
        }
        record = register_model_run(
            registry_root=registry_root,
            experiment_name=experiment_name,
            model_name=model_name,
            metadata=metadata,
            metrics=summary_metrics,
            source_model_path=temp_model_path,
            extra_artifact_paths=[temp_importance_json, temp_importance_csv],
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


def _summarize_development_metrics(
    all_predictions: np.ndarray,
    y_true: np.ndarray,
    fold_results: list[FoldResult],
) -> dict[str, Any]:
    aggregate_predictions = all_predictions[~np.isnan(all_predictions)]
    aggregate_truth = y_true[~np.isnan(all_predictions)]
    return {
        "aggregate": classification_metrics(aggregate_truth, aggregate_predictions),
        "folds": [asdict(result) for result in fold_results],
        "fold_average": _average_fold_metrics(fold_results),
    }


def _summarize_holdout_metrics(
    evaluation_plan: EvaluationPlan,
    final_predictions: np.ndarray,
    y_true: np.ndarray,
) -> dict[str, Any] | None:
    if evaluation_plan.holdout_split is None:
        return None
    holdout = evaluation_plan.holdout_split
    holdout_probabilities = final_predictions[holdout.test_idx]
    return {
        "holdout_season": holdout.holdout_season,
        "n_train": int(len(holdout.train_idx)),
        "n_test": int(len(holdout.test_idx)),
        "train_start": holdout.train_start,
        "train_end": holdout.train_end,
        "test_start": holdout.test_start,
        "test_end": holdout.test_end,
        "metrics": classification_metrics(y_true[holdout.test_idx], holdout_probabilities),
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
        exclude_feature_groups=data_config.get("exclude_feature_groups"),
        exclude_feature_columns=data_config.get("exclude_feature_columns"),
    )
    return fit_walk_forward_lgbm(
        dataset,
        experiment_name=config["experiment"]["name"],
        model_name=config["model"]["name"],
        params=config["model"]["params"],
        evaluation_config=config["evaluation"],
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
    config = normalize_training_config(json.loads(config_path.read_text()))
    if args.print_only:
        print(json.dumps(config, indent=2, sort_keys=True))
        return
    result = run_from_config(config)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
