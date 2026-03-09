from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from train.train_lgbm import run_from_config


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


def run_experiments(path: str | Path) -> dict[str, Any]:
    loaded = load_experiment_config(path)
    results = []
    for experiment in loaded["experiments"]:
        trainer = experiment["model"].get("trainer", "lgbm")
        if trainer != "lgbm":
            raise ValueError(f"Unsupported trainer: {trainer}")
        results.append(run_from_config(experiment))
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

