from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from train.config import MissingOptionalDependency, validate_training_config
from train.train_lgbm import run_from_config as run_lgbm_from_config
from train.train_logreg import run_from_config as run_logreg_from_config


def _deep_merge(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_experiment_config(path: str | Path) -> dict[str, Any]:
    config = json.loads(Path(path).read_text())
    if "experiments" not in config:
        return {"experiments": [config]}

    base = config.get("base", {})
    resolved = []
    for experiment in config["experiments"]:
        resolved.append(_deep_merge(base, experiment))
    return {"experiments": resolved}


def _trainer_runner(trainer: str):
    if trainer == "lgbm":
        return run_lgbm_from_config
    if trainer == "logreg":
        return run_logreg_from_config
    raise ValueError(f"Unsupported trainer: {trainer}")


def run_experiments(path: str | Path) -> dict[str, Any]:
    loaded = load_experiment_config(path)
    results = []
    seen_names: set[str] = set()
    for experiment in loaded["experiments"]:
        experiment = validate_training_config(experiment)
        experiment_name = experiment["experiment"]["name"]
        if experiment_name in seen_names:
            raise ValueError(f"Experiment names must be unique within a suite: {experiment_name}")
        seen_names.add(experiment_name)
        execution = experiment.get("execution", {})
        if not execution.get("enabled", True):
            results.append(
                {
                    "status": "skipped",
                    "experiment_name": experiment_name,
                    "model_name": experiment["model"]["name"],
                    "reason": execution.get("reason", "disabled"),
                }
            )
            continue
        trainer = experiment["model"].get("trainer", "lgbm")
        try:
            results.append(_trainer_runner(trainer)(experiment))
        except MissingOptionalDependency as exc:
            results.append(
                {
                    "status": "blocked",
                    "experiment_name": experiment_name,
                    "model_name": experiment["model"]["name"],
                    "trainer": trainer,
                    "dependency": exc.dependency,
                    "reason": str(exc),
                }
            )
    return {"results": results}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one or more configured training experiments")
    parser.add_argument("--config", required=True, help="Path to JSON experiment config")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    results = run_experiments(args.config)
    print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
