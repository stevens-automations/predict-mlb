import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from train.config import MissingOptionalDependency
from train.experiment_runner import load_experiment_config, run_experiments


class TestTrainingExperimentRunner(unittest.TestCase):
    def _write_suite(self, payload: dict) -> Path:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "suite.json"
        path.write_text(json.dumps(payload))
        return path

    def test_load_experiment_config_merges_base_into_experiments(self) -> None:
        path = self._write_suite(
            {
                "base": {
                    "contract": {
                        "name": "pregame_1h",
                        "target": "home_team_win_probability",
                        "label_column": "did_home_win",
                        "feature_version": "v2_phase1",
                        "sportsbook_odds_as_features": False,
                    },
                    "data": {
                        "feature_version": "v2_phase1",
                        "seasons": [2020, 2021, 2022, 2023, 2024, 2025],
                        "contract_statuses": ["valid", "degraded"],
                    },
                    "evaluation": {
                        "seasonal_holdout": {
                            "development_seasons": [2020, 2021, 2022, 2023, 2024],
                            "holdout_season": 2025,
                        }
                    },
                    "registry": {"root_dir": "artifacts/model_registry"},
                },
                "experiments": [
                    {
                        "experiment": {"name": "logreg_case"},
                        "model": {"name": "logreg_baseline", "trainer": "logreg"},
                    }
                ],
            }
        )

        loaded = load_experiment_config(path)
        self.assertEqual(loaded["experiments"][0]["contract"]["feature_version"], "v2_phase1")
        self.assertEqual(loaded["experiments"][0]["registry"]["root_dir"], "artifacts/model_registry")

    def test_missing_sklearn_blocks_only_logreg_experiment(self) -> None:
        path = self._write_suite(
            {
                "base": {
                    "contract": {
                        "name": "pregame_1h",
                        "target": "home_team_win_probability",
                        "label_column": "did_home_win",
                        "feature_version": "v2_phase1",
                        "sportsbook_odds_as_features": False,
                    },
                    "data": {
                        "feature_version": "v2_phase1",
                        "seasons": [2020, 2021, 2022, 2023, 2024, 2025],
                        "contract_statuses": ["valid", "degraded"],
                    },
                    "evaluation": {
                        "seasonal_holdout": {
                            "development_seasons": [2020, 2021, 2022, 2023, 2024],
                            "holdout_season": 2025,
                        }
                    },
                    "registry": {"root_dir": "artifacts/model_registry"},
                },
                "experiments": [
                    {
                        "experiment": {"name": "logreg_case"},
                        "model": {"name": "logreg_baseline", "trainer": "logreg"},
                    },
                    {
                        "experiment": {"name": "lgbm_case"},
                        "model": {"name": "lgbm_baseline", "trainer": "lgbm"},
                    },
                ],
            }
        )

        with patch(
            "train.experiment_runner.run_logreg_from_config",
            side_effect=MissingOptionalDependency("scikit-learn", "scikit-learn import failed"),
        ), patch(
            "train.experiment_runner.run_lgbm_from_config",
            return_value={"status": "ok", "experiment_name": "lgbm_case"},
        ):
            result = run_experiments(path)

        self.assertEqual(result["results"][0]["status"], "blocked")
        self.assertEqual(result["results"][0]["dependency"], "scikit-learn")
        self.assertEqual(result["results"][1]["status"], "ok")


if __name__ == "__main__":
    unittest.main()
