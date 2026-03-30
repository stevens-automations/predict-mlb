import json
import unittest
from pathlib import Path

from train.config import (
    CANONICAL_ALLOWED_CONTRACT_STATUSES,
    CANONICAL_DEVELOPMENT_SEASONS,
    CANONICAL_FEATURE_VERSION,
    CANONICAL_HOLDOUT_SEASON,
    CANONICAL_TRAINING_SEASONS,
    SUPPORTED_FEATURE_ABLATION_GROUPS,
    normalize_training_config,
    validate_training_config,
)


ROOT = Path(__file__).resolve().parents[1]


def _load_json(relative_path: str) -> dict:
    return json.loads((ROOT / relative_path).read_text())


class TestTrainingConfigs(unittest.TestCase):
    def test_canonical_configs_use_pregame_1h_v2_phase1_contract(self) -> None:
        baseline = _load_json("configs/training/baseline_lgbm.json")
        suite = _load_json("configs/training/experiment_suite.json")

        self.assertEqual(baseline["contract"]["name"], "pregame_1h")
        self.assertEqual(baseline["contract"]["target"], "home_team_win_probability")
        self.assertEqual(baseline["data"]["feature_version"], CANONICAL_FEATURE_VERSION)
        self.assertFalse(baseline["contract"]["sportsbook_odds_as_features"])
        self.assertEqual(
            baseline["evaluation"]["seasonal_holdout"]["development_seasons"],
            CANONICAL_DEVELOPMENT_SEASONS,
        )
        self.assertEqual(baseline["evaluation"]["seasonal_holdout"]["holdout_season"], CANONICAL_HOLDOUT_SEASON)

        self.assertEqual(suite["base"]["contract"]["name"], "pregame_1h")
        self.assertEqual(suite["base"]["evaluation"]["seasonal_holdout"]["holdout_season"], CANONICAL_HOLDOUT_SEASON)

    def test_experiment_suite_includes_logreg_and_lgbm_baselines(self) -> None:
        suite = _load_json("configs/training/experiment_suite.json")
        trainers = [experiment["model"]["trainer"] for experiment in suite["experiments"]]
        self.assertIn("logreg", trainers)
        self.assertIn("lgbm", trainers)

    def test_normalize_training_config_fills_canonical_defaults(self) -> None:
        normalized = normalize_training_config(
            {
                "experiment": {"name": "test"},
                "model": {"name": "candidate"},
            }
        )

        self.assertEqual(normalized["contract"]["feature_version"], CANONICAL_FEATURE_VERSION)
        self.assertEqual(normalized["data"]["seasons"], CANONICAL_TRAINING_SEASONS)
        self.assertEqual(normalized["data"]["contract_statuses"], CANONICAL_ALLOWED_CONTRACT_STATUSES)
        self.assertEqual(
            normalized["evaluation"]["seasonal_holdout"]["development_seasons"],
            CANONICAL_DEVELOPMENT_SEASONS,
        )
        self.assertEqual(
            normalized["evaluation"]["seasonal_holdout"]["holdout_season"],
            CANONICAL_HOLDOUT_SEASON,
        )
        self.assertEqual(normalized["data"]["exclude_feature_groups"], [])
        self.assertEqual(normalized["data"]["exclude_feature_columns"], [])

    def test_validate_training_config_rejects_noncanonical_feature_version(self) -> None:
        with self.assertRaisesRegex(ValueError, "feature_version"):
            validate_training_config(
                {
                    "contract": {
                        "name": "pregame_1h",
                        "target": "home_team_win_probability",
                        "label_column": "did_home_win",
                        "feature_version": "v1",
                    },
                    "experiment": {"name": "bad_version"},
                    "data": {"feature_version": "v1", "seasons": CANONICAL_TRAINING_SEASONS},
                    "model": {"name": "candidate", "trainer": "lgbm"},
                    "evaluation": {
                        "seasonal_holdout": {
                            "development_seasons": CANONICAL_DEVELOPMENT_SEASONS,
                            "holdout_season": CANONICAL_HOLDOUT_SEASON,
                        }
                    },
                }
            )

    def test_validate_training_config_rejects_unknown_ablation_group(self) -> None:
        with self.assertRaisesRegex(ValueError, "exclude_feature_groups"):
            validate_training_config(
                {
                    "contract": {
                        "name": "pregame_1h",
                        "target": "home_team_win_probability",
                        "label_column": "did_home_win",
                        "feature_version": "v2_phase1",
                    },
                    "experiment": {"name": "bad_ablation"},
                    "data": {
                        "feature_version": "v2_phase1",
                        "feature_contract": "raw",
                        "seasons": CANONICAL_TRAINING_SEASONS,
                        "contract_statuses": CANONICAL_ALLOWED_CONTRACT_STATUSES,
                        "exclude_feature_groups": ["unknown_group"],
                    },
                    "model": {"name": "candidate", "trainer": "lgbm"},
                    "evaluation": {
                        "seasonal_holdout": {
                            "development_seasons": CANONICAL_DEVELOPMENT_SEASONS,
                            "holdout_season": CANONICAL_HOLDOUT_SEASON,
                        }
                    },
                }
            )


if __name__ == "__main__":
    unittest.main()
