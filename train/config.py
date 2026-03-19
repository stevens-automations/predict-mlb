from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any


CANONICAL_CONTRACT_NAME = "pregame_1h"
CANONICAL_TARGET_NAME = "home_team_win_probability"
CANONICAL_LABEL_COLUMN = "did_home_win"
CANONICAL_FEATURE_VERSION = "v2_phase1"
CANONICAL_TRAINING_SEASONS = [2020, 2021, 2022, 2023, 2024, 2025]
CANONICAL_DEVELOPMENT_SEASONS = [2020, 2021, 2022, 2023, 2024]
CANONICAL_HOLDOUT_SEASON = 2025
CANONICAL_ALLOWED_CONTRACT_STATUSES = ["valid", "degraded"]
DEFAULT_REGISTRY_ROOT = "artifacts/model_registry"
SUPPORTED_FEATURE_CONTRACTS = ["raw", "cleaned_comparative_v1", "hybrid_comparative_v1"]
SUPPORTED_FEATURE_ABLATION_GROUPS = [
    "starter_ids",
    "placeholder_lineup_quality",
    "weather",
    "platoon",
]


class MissingOptionalDependency(RuntimeError):
    def __init__(self, dependency: str, message: str) -> None:
        super().__init__(message)
        self.dependency = dependency


def _normalize_int_list(values: list[Any]) -> list[int]:
    return [int(value) for value in values]


def _canonical_seasonal_holdout() -> dict[str, Any]:
    return {
        "development_seasons": list(CANONICAL_DEVELOPMENT_SEASONS),
        "holdout_season": CANONICAL_HOLDOUT_SEASON,
        "min_train_seasons": 2,
    }


def normalize_training_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(config)

    contract = normalized.setdefault("contract", {})
    contract.setdefault("name", CANONICAL_CONTRACT_NAME)
    contract.setdefault("target", CANONICAL_TARGET_NAME)
    contract.setdefault("label_column", CANONICAL_LABEL_COLUMN)
    contract.setdefault("feature_version", CANONICAL_FEATURE_VERSION)
    contract["sportsbook_odds_as_features"] = bool(contract.get("sportsbook_odds_as_features", False))

    experiment = normalized.setdefault("experiment", {})
    model = normalized.setdefault("model", {})
    if "name" in model and "name" not in experiment:
        experiment["name"] = model["name"]

    model.setdefault("trainer", "lgbm")
    model.setdefault("params", {})

    data_config = normalized.setdefault("data", {})
    data_config.setdefault("db_path", "data/mlb_history.db")
    data_config.setdefault("feature_version", contract["feature_version"])
    data_config.setdefault("feature_contract", "raw")
    data_config.setdefault("exclude_feature_groups", [])
    data_config.setdefault("exclude_feature_columns", [])
    data_config.setdefault("seasons", list(CANONICAL_TRAINING_SEASONS))
    data_config.setdefault("contract_statuses", list(CANONICAL_ALLOWED_CONTRACT_STATUSES))
    data_config["seasons"] = _normalize_int_list(list(data_config["seasons"]))
    data_config["contract_statuses"] = [str(status) for status in data_config["contract_statuses"]]
    data_config["feature_contract"] = str(data_config["feature_contract"])
    data_config["exclude_feature_groups"] = [str(value) for value in data_config["exclude_feature_groups"]]
    data_config["exclude_feature_columns"] = [str(value) for value in data_config["exclude_feature_columns"]]

    evaluation = normalized.setdefault("evaluation", {})
    if "seasonal_holdout" not in evaluation and "walk_forward" not in evaluation:
        evaluation["seasonal_holdout"] = _canonical_seasonal_holdout()
    if "seasonal_holdout" in evaluation:
        season_config = dict(evaluation["seasonal_holdout"])
        season_config.setdefault("development_seasons", list(CANONICAL_DEVELOPMENT_SEASONS))
        season_config.setdefault("holdout_season", CANONICAL_HOLDOUT_SEASON)
        season_config.setdefault("min_train_seasons", 2)
        season_config["development_seasons"] = _normalize_int_list(list(season_config["development_seasons"]))
        season_config["holdout_season"] = int(season_config["holdout_season"])
        season_config["min_train_seasons"] = int(season_config["min_train_seasons"])
        evaluation["seasonal_holdout"] = season_config

    registry = normalized.setdefault("registry", {})
    registry.setdefault("root_dir", DEFAULT_REGISTRY_ROOT)

    return normalized


def load_training_config(path: str | Path) -> dict[str, Any]:
    return normalize_training_config(json.loads(Path(path).read_text()))


def training_requirements_from_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_training_config(config)
    return {
        "feature_version": normalized["data"]["feature_version"],
        "feature_contract": normalized["data"]["feature_contract"],
        "required_seasons": list(normalized["data"]["seasons"]),
        "allowed_contract_statuses": list(normalized["data"]["contract_statuses"]),
        "db_path": normalized["data"]["db_path"],
    }


def validate_training_config(config: dict[str, Any]) -> dict[str, Any]:
    config = normalize_training_config(config)
    contract = dict(config.get("contract", {}))
    if contract.get("name") != CANONICAL_CONTRACT_NAME:
        raise ValueError(f"Training contract must declare `{CANONICAL_CONTRACT_NAME}`")
    if contract.get("target") != CANONICAL_TARGET_NAME:
        raise ValueError(f"Training target must declare `{CANONICAL_TARGET_NAME}`")
    if contract.get("label_column", CANONICAL_LABEL_COLUMN) != CANONICAL_LABEL_COLUMN:
        raise ValueError(f"Training label column must remain `{CANONICAL_LABEL_COLUMN}`")
    if bool(contract.get("sportsbook_odds_as_features", False)):
        raise ValueError("Sportsbook odds are forbidden as training features")

    data_config = dict(config.get("data", {}))
    feature_version = data_config.get("feature_version")
    if not feature_version:
        raise ValueError("Training config must set data.feature_version")
    if feature_version != CANONICAL_FEATURE_VERSION:
        raise ValueError(f"Canonical training feature_version must remain `{CANONICAL_FEATURE_VERSION}`")
    if contract.get("feature_version") and contract["feature_version"] != feature_version:
        raise ValueError("contract.feature_version must match data.feature_version")
    if data_config.get("seasons") != CANONICAL_TRAINING_SEASONS:
        raise ValueError(f"Canonical training seasons must remain {CANONICAL_TRAINING_SEASONS}")
    if data_config.get("contract_statuses") != CANONICAL_ALLOWED_CONTRACT_STATUSES:
        raise ValueError(
            "Canonical training contract statuses must remain "
            f"{CANONICAL_ALLOWED_CONTRACT_STATUSES}"
        )
    if data_config.get("feature_contract") not in SUPPORTED_FEATURE_CONTRACTS:
        raise ValueError(
            f"Training config data.feature_contract must be one of: {SUPPORTED_FEATURE_CONTRACTS}"
        )
    unknown_feature_groups = sorted(
        set(data_config.get("exclude_feature_groups", ())) - set(SUPPORTED_FEATURE_ABLATION_GROUPS)
    )
    if unknown_feature_groups:
        raise ValueError(
            "Training config data.exclude_feature_groups contains unsupported groups: "
            f"{unknown_feature_groups}. Supported groups: {SUPPORTED_FEATURE_ABLATION_GROUPS}"
        )

    experiment = dict(config.get("experiment", {}))
    if not experiment.get("name"):
        raise ValueError("Training config must define experiment.name")

    model = dict(config.get("model", {}))
    if not model.get("name"):
        raise ValueError("Training config must define model.name")
    if model.get("trainer") not in {"lgbm", "logreg"}:
        raise ValueError("Training config model.trainer must be one of: lgbm, logreg")

    evaluation = dict(config.get("evaluation", {}))
    if "seasonal_holdout" in evaluation and "walk_forward" in evaluation:
        raise ValueError("Choose one evaluation regime, not both `seasonal_holdout` and `walk_forward`")
    if "seasonal_holdout" not in evaluation and "walk_forward" not in evaluation:
        raise ValueError("Training config must define `evaluation.seasonal_holdout` or `evaluation.walk_forward`")
    if "seasonal_holdout" in evaluation:
        season_config = dict(evaluation["seasonal_holdout"])
        if season_config.get("development_seasons") != CANONICAL_DEVELOPMENT_SEASONS:
            raise ValueError(
                "Canonical development seasons must remain "
                f"{CANONICAL_DEVELOPMENT_SEASONS}"
            )
        if int(season_config.get("holdout_season", -1)) != CANONICAL_HOLDOUT_SEASON:
            raise ValueError(f"Canonical holdout season must remain {CANONICAL_HOLDOUT_SEASON}")

    return config
