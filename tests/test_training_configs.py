import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_json(relative_path: str) -> dict:
    return json.loads((ROOT / relative_path).read_text())


class TestTrainingConfigs(unittest.TestCase):
    def test_canonical_full_span_configs_do_not_cap_walk_forward_splits(self) -> None:
        baseline = _load_json("configs/training/baseline_lgbm.json")
        tuned = _load_json("configs/training/tuned_candidate.json")
        suite = _load_json("configs/training/experiment_suite.json")

        self.assertNotIn("max_splits", baseline["evaluation"]["walk_forward"])
        self.assertNotIn("max_splits", tuned["evaluation"]["walk_forward"])
        self.assertNotIn("max_splits", suite["base"]["evaluation"]["walk_forward"])


if __name__ == "__main__":
    unittest.main()
