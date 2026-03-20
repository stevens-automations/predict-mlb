#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from train.config import load_training_config, training_requirements_from_config
from train.experiment_runner import load_experiment_config
from train.readiness import build_training_readiness_report

DEFAULT_BASELINE_CONFIG = "configs/training/baseline_lgbm.json"
DEFAULT_SUITE_CONFIG = "configs/training/experiment_suite.json"


def _requirements_from_action(args: argparse.Namespace) -> dict[str, object]:
    if args.action == "baseline":
        return training_requirements_from_config(load_training_config(args.baseline_config))
    if args.action == "suite":
        experiments = load_experiment_config(args.suite_config)["experiments"]
        if not experiments:
            raise ValueError("Experiment suite must contain at least one experiment")
        requirements = training_requirements_from_config(experiments[0])
        for index, experiment in enumerate(experiments[1:], start=2):
            candidate = training_requirements_from_config(experiment)
            if candidate != requirements:
                raise ValueError(
                    "All suite experiments must share the same readiness requirements. "
                    f"Mismatch found at experiment #{index}."
                )
        return requirements
    return {
        "db_path": args.db_path,
        "feature_version": args.feature_version,
        "required_seasons": args.required_seasons,
        "allowed_contract_statuses": args.allowed_contract_statuses,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Wait for historical training data readiness, then launch baseline or suite training."
    )
    parser.add_argument("--db-path", default="data/mlb_history.db", help="SQLite historical DB path")
    parser.add_argument("--feature-version", default="v2_phase1", help="Feature version required for training")
    parser.add_argument(
        "--required-seasons",
        nargs="+",
        type=int,
        default=[2020, 2021, 2022, 2023, 2024, 2025],
        help="Seasons that must be present and trainable before launch",
    )
    parser.add_argument(
        "--allowed-contract-statuses",
        nargs="+",
        default=["valid", "degraded"],
        help="Feature-row contract statuses allowed into training",
    )
    parser.add_argument(
        "--action",
        choices=("check", "baseline", "suite"),
        default="check",
        help="Readiness only, baseline launch, or experiment suite launch",
    )
    parser.add_argument("--baseline-config", default=DEFAULT_BASELINE_CONFIG, help="Baseline training config")
    parser.add_argument("--suite-config", default=DEFAULT_SUITE_CONFIG, help="Experiment suite config")
    parser.add_argument("--poll-seconds", type=int, default=300, help="Polling interval while waiting")
    parser.add_argument(
        "--max-wait-seconds",
        type=int,
        default=0,
        help="Maximum wait before exiting. `0` means check once and exit immediately.",
    )
    return parser


def _print_report(report: dict[str, object]) -> None:
    print(json.dumps(report, indent=2, sort_keys=True))


def _run_command(command: list[str]) -> int:
    print(json.dumps({"launch_command": command}, indent=2))
    completed = subprocess.run(command, cwd=ROOT, check=False)
    return int(completed.returncode)


def main() -> None:
    args = build_parser().parse_args()
    deadline = time.time() + args.max_wait_seconds if args.max_wait_seconds > 0 else None
    requirements = _requirements_from_action(args)

    while True:
        report = build_training_readiness_report(
            db_path=requirements["db_path"],
            required_seasons=requirements["required_seasons"],
            feature_version=requirements["feature_version"],
            allowed_contract_statuses=requirements["allowed_contract_statuses"],
        )
        _print_report(report)

        if report["ready"]:
            if args.action == "check":
                raise SystemExit(0)

            config_path = args.baseline_config if args.action == "baseline" else args.suite_config
            target_script = (
                ROOT / "scripts" / "training" / "train_lgbm.py"
                if args.action == "baseline"
                else ROOT / "scripts" / "training" / "experiment_runner.py"
            )
            command = [sys.executable, str(target_script), "--config", config_path]
            raise SystemExit(_run_command(command))

        if args.max_wait_seconds == 0:
            raise SystemExit(1)
        if deadline is not None and time.time() >= deadline:
            raise SystemExit(1)
        time.sleep(max(1, args.poll_seconds))


if __name__ == "__main__":
    main()
