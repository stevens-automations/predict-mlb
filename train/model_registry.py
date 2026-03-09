from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True)
class RegistryRecord:
    run_id: str
    run_dir: Path
    model_path: Path
    metadata_path: Path
    metrics_path: Path


def _json_default(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Unsupported JSON serialization type: {type(value)!r}")


def register_model_run(
    *,
    registry_root: str | Path,
    experiment_name: str,
    model_name: str,
    metadata: dict[str, Any],
    metrics: dict[str, Any],
    source_model_path: str | Path,
) -> RegistryRecord:
    registry_root = Path(registry_root)
    run_id = f"{experiment_name}__{model_name}__{utc_timestamp()}"
    run_dir = registry_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    source_model_path = Path(source_model_path)
    model_path = run_dir / source_model_path.name
    shutil.copy2(source_model_path, model_path)

    metadata_path = run_dir / "metadata.json"
    metrics_path = run_dir / "metrics.json"

    metadata_payload = dict(metadata)
    metadata_payload["run_id"] = run_id
    metadata_payload["registered_at"] = utc_timestamp()
    metadata_payload["artifact_files"] = [model_path.name, metadata_path.name, metrics_path.name]

    metadata_path.write_text(json.dumps(metadata_payload, indent=2, sort_keys=True, default=_json_default) + "\n")
    metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True, default=_json_default) + "\n")

    latest_payload = {
        "run_id": run_id,
        "experiment_name": experiment_name,
        "model_name": model_name,
        "run_dir": str(run_dir),
    }
    (registry_root / f"{experiment_name}__latest.json").write_text(
        json.dumps(latest_payload, indent=2, sort_keys=True) + "\n"
    )

    return RegistryRecord(
        run_id=run_id,
        run_dir=run_dir,
        model_path=model_path,
        metadata_path=metadata_path,
        metrics_path=metrics_path,
    )

