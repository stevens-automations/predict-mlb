import importlib.util
import os
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from storage import NullShadowWriter, ShadowWriteStats, shadow_writer_from_env


class BadShadowWriter:
    def mirror_predictions(self, _df):
        raise RuntimeError("sqlite down")


class TestShadowWriterToggle(unittest.TestCase):
    def test_shadow_writer_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            writer = shadow_writer_from_env()
            self.assertIsInstance(writer, NullShadowWriter)

    def test_shadow_writer_enabled_flag(self):
        with patch.dict(os.environ, {"SQLITE_SHADOW_WRITE": "true"}, clear=True):
            writer = shadow_writer_from_env()
            self.assertNotIsInstance(writer, NullShadowWriter)


class TestPredictFailOpenShadow(unittest.TestCase):
    def _load_predict_module(self):
        stub_data = types.ModuleType("data")

        class StubLeagueStats:
            def get_days_games(self, *_args, **_kwargs):
                return []

            def predict_game(self, *_args, **_kwargs):
                return None

        stub_data.LeagueStats = StubLeagueStats

        stub_tweet_gen = types.ModuleType("server.tweet_generator")
        stub_tweet_gen.gen_result_tweet = lambda *args, **kwargs: ""
        stub_tweet_gen.gen_game_line = lambda *args, **kwargs: ""
        stub_tweet_gen.create_tweets = lambda lines: ["\n".join(lines)]

        stub_get_odds = types.ModuleType("server.get_odds")
        stub_get_odds.get_todays_odds = lambda: ([], "")

        stub_prep = types.ModuleType("server.prep_tweet")
        stub_prep.prepare = lambda row: ""

        stub_runtime = types.ModuleType("runtime")
        stub_runtime.validate_runtime = lambda: None

        stub_paths = types.ModuleType("paths")
        stub_paths.get_env_path = lambda *_args, **_kwargs: "data/predictions.xlsx"
        stub_paths.load_env = lambda: None

        stub_reliability = types.ModuleType("reliability_utils")
        stub_reliability.get_predicted_winner_location = lambda *_args, **_kwargs: "home"

        stub_statsapi = types.ModuleType("statsapi")
        stub_statsapi.schedule = lambda *_args, **_kwargs: []

        module_path = "/Users/openclaw/.openclaw/workspace/projects/predict-mlb/predict.py"
        spec = importlib.util.spec_from_file_location("predict_phase2_tests", module_path)
        module = importlib.util.module_from_spec(spec)

        with patch.dict(
            os.sys.modules,
            {
                "data": stub_data,
                "server.tweet_generator": stub_tweet_gen,
                "server.get_odds": stub_get_odds,
                "server.prep_tweet": stub_prep,
                "runtime": stub_runtime,
                "paths": stub_paths,
                "reliability_utils": stub_reliability,
                "statsapi": stub_statsapi,
            },
        ):
            spec.loader.exec_module(module)  # type: ignore[union-attr]

        return module

    def test_load_unchecked_predictions_fails_open_when_shadow_errors(self):
        predict = self._load_predict_module()
        stats = ShadowWriteStats()

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = Path(tmpdir) / "predictions.xlsx"
            pd.DataFrame(
                [
                    {
                        "game_id": 123,
                        "predicted_winner": "A",
                        "home": "A",
                        "away": "B",
                        "home_odds": 110,
                        "away_odds": -120,
                        "prediction_accuracy": None,
                    }
                ]
            ).to_excel(excel_path, index=False)

            out = predict.load_unchecked_predictions_from_excel(
                str(excel_path),
                shadow_writer=BadShadowWriter(),
                shadow_stats=stats,
            )

            self.assertIsNotNone(out)
            self.assertEqual(stats.success, 0)
            self.assertEqual(stats.failure, 1)


if __name__ == "__main__":
    unittest.main()
