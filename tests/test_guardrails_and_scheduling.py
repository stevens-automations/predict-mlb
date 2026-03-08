import importlib.util
import sys
import types
import unittest
from unittest.mock import patch

import pandas as pd

from server import prep_tweet


class StubStorage:
    def __init__(self, df):
        self.df = df

    def read_predictions(self):
        return self.df

    def replace_predictions(self, df):
        self.df = df
        return (len(df), 0)


class TestPrepTweetGuardrails(unittest.TestCase):
    @patch("server.prep_tweet.gen_game_line", return_value="fallback tweet")
    @patch("server.prep_tweet.get_todays_odds", return_value=([], "2026-03-08T10:00:00Z"))
    def test_prepare_missing_game_id_column_returns_generated_line(self, _mock_odds, mock_gen_game_line):
        game_info = pd.Series({"home": "A", "away": "B", "time": "1:00 PM"})
        with patch("server.prep_tweet.get_primary_storage", return_value=StubStorage(pd.DataFrame({"home": ["A"], "away": ["B"]}))):
            out = prep_tweet.prepare(game_info)
        self.assertEqual(out, "fallback tweet")
        mock_gen_game_line.assert_called_once()

    @patch("server.prep_tweet.gen_game_line", return_value="fallback tweet")
    @patch("server.prep_tweet.get_todays_odds", return_value=([], "2026-03-08T10:00:00Z"))
    def test_prepare_missing_matching_row_returns_generated_line(self, _mock_odds, mock_gen_game_line):
        game_info = pd.Series({"home": "A", "away": "B", "time": "1:00 PM", "game_id": 999})
        with patch("server.prep_tweet.get_primary_storage", return_value=StubStorage(pd.DataFrame({"game_id": [123], "tweet": [""]}))):
            out = prep_tweet.prepare(game_info)
        self.assertEqual(out, "fallback tweet")
        mock_gen_game_line.assert_called_once()


class FakeScheduler:
    def __init__(self):
        self.jobs = {}
        self.added = []

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def add_job(self, fn, args, trigger, run_date, id, replace_existing):
        self.jobs[id] = {"fn": fn, "args": args, "trigger": trigger, "run_date": run_date, "replace_existing": replace_existing}
        self.added.append(id)


class TestScheduleDeduping(unittest.TestCase):
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
        stub_tweet_gen.summarize_enrichment_observability = lambda lines: {
            "total_game_lines": len(lines),
            "confidence_tier_distribution": {"H": 0, "M": 0, "L": 0},
            "mismatch_count": 0,
            "mismatch_rate": 0.0,
        }

        stub_get_odds = types.ModuleType("server.get_odds")
        stub_get_odds.get_todays_odds = lambda: ([], "")

        stub_prep = types.ModuleType("server.prep_tweet")
        stub_prep.prepare = lambda row: ""

        stub_runtime = types.ModuleType("runtime")
        stub_runtime.validate_runtime = lambda: None

        stub_paths = types.ModuleType("paths")
        stub_paths.load_env = lambda: None

        stub_reliability = types.ModuleType("reliability_utils")
        stub_reliability.get_predicted_winner_location = lambda *_args, **_kwargs: "home"

        stub_statsapi = types.ModuleType("statsapi")
        stub_statsapi.schedule = lambda *_args, **_kwargs: []

        stub_storage = types.ModuleType("storage")
        stub_storage.WriteStats = object
        stub_storage.get_primary_storage = lambda: None

        module_path = "/Users/openclaw/.openclaw/workspace/projects/predict-mlb/predict.py"
        spec = importlib.util.spec_from_file_location("predict_for_tests", module_path)
        module = importlib.util.module_from_spec(spec)

        with patch.dict(sys.modules, {
            "data": stub_data,
            "server.tweet_generator": stub_tweet_gen,
            "server.get_odds": stub_get_odds,
            "server.prep_tweet": stub_prep,
            "runtime": stub_runtime,
            "paths": stub_paths,
            "reliability_utils": stub_reliability,
            "statsapi": stub_statsapi,
            "storage": stub_storage,
        }):
            spec.loader.exec_module(module)  # type: ignore[union-attr]

        return module

    def test_unique_tweet_lines_strips_bullet_and_dedupes(self):
        predict = self._load_predict_module()
        out = predict.unique_tweet_lines(["• Yankees ML", "Yankees ML", "• Mets ML", "Mets ML"])
        self.assertEqual(out, ["Yankees ML", "Mets ML"])

    def test_schedule_tweets_idempotent_with_duplicate_lines(self):
        predict = self._load_predict_module()
        fake_scheduler = FakeScheduler()
        predict.daily_scheduler = fake_scheduler

        with patch.object(predict, "create_tweets", return_value=["tweet-body"]):
            predict.schedule_tweets(["• Yankees ML", "Yankees ML"])
            predict.schedule_tweets(["Yankees ML"])

        self.assertEqual(len(fake_scheduler.added), 1)


if __name__ == "__main__":
    unittest.main()
