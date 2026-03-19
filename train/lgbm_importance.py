from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from train.train_lgbm import _load_lightgbm, summarize_feature_importance, write_feature_importance_artifacts


def load_registry_run(run_dir: str | Path) -> tuple[dict[str, Any], dict[str, Any], Path]:
    run_dir = Path(run_dir)
    metadata = json.loads((run_dir / "metadata.json").read_text())
    metrics = json.loads((run_dir / "metrics.json").read_text())
    model_candidates = sorted(run_dir.glob("*.txt"))
    if not model_candidates:
        raise FileNotFoundError(f"No LightGBM model text artifact found in {run_dir}")
    return metadata, metrics, model_candidates[0]


def build_importance_summary(run_dir: str | Path, *, top_n: int = 15, write_artifacts: bool = True) -> dict[str, Any]:
    metadata, metrics, model_path = load_registry_run(run_dir)
    lgb = _load_lightgbm()
    booster = lgb.Booster(model_file=str(model_path))
    feature_names = metadata.get("feature_columns") or metadata.get("dataset_diagnostics", {}).get("feature_columns")
    importance_rows = summarize_feature_importance(booster, feature_names=feature_names)

    artifact_paths: dict[str, str] = {}
    if write_artifacts:
        json_path, csv_path = write_feature_importance_artifacts(run_dir, importance_rows)
        artifact_paths = {
            "feature_importance_json": str(json_path),
            "feature_importance_csv": str(csv_path),
        }

    return {
        "run_id": metadata.get("run_id"),
        "run_dir": str(run_dir),
        "model_path": str(model_path),
        "holdout_metrics": metrics.get("holdout"),
        "top_by_gain": importance_rows[:top_n],
        "top_by_split": sorted(
            importance_rows,
            key=lambda row: (-row["split"], -row["gain"], row["feature"]),
        )[:top_n],
        "artifact_paths": artifact_paths,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract LightGBM feature importance from a registered run")
    parser.add_argument("--run-dir", required=True, help="Path to an artifacts/model_registry run directory")
    parser.add_argument("--top", type=int, default=15, help="Number of top features to print for gain/split")
    parser.add_argument(
        "--no-write-artifacts",
        action="store_true",
        help="Do not write feature_importance.json/csv back into the run directory",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    summary = build_importance_summary(
        args.run_dir,
        top_n=args.top,
        write_artifacts=not args.no_write_artifacts,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
