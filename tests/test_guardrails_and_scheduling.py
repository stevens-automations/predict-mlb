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
        stub_tweet_gen.gen_game_line_with_observability = lambda row, mode=None: ("", "")
        stub_tweet_gen.get_enrichment_mode = lambda: "on"
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

        module_path = "/Users/openclaw/.openclaw/projects/predict-mlb/predict.py"
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


class TestPredictGuardrails(unittest.TestCase):
    def _load_predict_module(self):
        return TestScheduleDeduping()._load_predict_module()

    def test_validate_tweet_line_rejects_empty_and_long(self):
        predict = self._load_predict_module()
        is_valid, reason, _ = predict._validate_tweet_line("   ")
        self.assertFalse(is_valid)
        self.assertEqual(reason, "empty_line")

        too_long = "x" * (predict.MAX_TWEET_LINE_LENGTH + 1)
        is_valid, reason, _ = predict._validate_tweet_line(too_long)
        self.assertFalse(is_valid)
        self.assertEqual(reason, "line_too_long")

    def test_threshold_warning_emission(self):
        predict = self._load_predict_module()
        summary = {
            "total_game_lines": 10,
            "confidence_tier_distribution": {"H": 1, "M": 1, "L": 8},
            "mismatch_count": 7,
            "mismatch_rate": 0.7,
        }
        with patch.dict("os.environ", {
            "ENRICHMENT_MISMATCH_RATE_WARN": "0.60",
            "ENRICHMENT_LOW_CONFIDENCE_RATE_WARN": "0.70",
        }, clear=False):
            warnings = predict._emit_enrichment_threshold_warnings(summary, run_id="r1", stage="run_summary")
        self.assertIn("enrichment_mismatch_rate_high", warnings)
        self.assertIn("enrichment_low_confidence_rate_high", warnings)

    def test_threshold_warning_suppressed_for_small_sample(self):
        predict = self._load_predict_module()
        summary = {
            "total_game_lines": 2,
            "confidence_tier_distribution": {"H": 0, "M": 0, "L": 2},
            "mismatch_count": 2,
            "mismatch_rate": 1.0,
        }
        with patch.dict("os.environ", {
            "ENRICHMENT_MISMATCH_RATE_WARN": "0.60",
            "ENRICHMENT_LOW_CONFIDENCE_RATE_WARN": "0.70",
            "ENRICHMENT_MIN_SAMPLE_WARN": "5",
            "ENRICHMENT_MIN_MISMATCH_COUNT_WARN": "3",
            "ENRICHMENT_MIN_LOW_CONFIDENCE_COUNT_WARN": "3",
        }, clear=False):
            warnings = predict._emit_enrichment_threshold_warnings(summary, run_id="r2", stage="run_summary")
        self.assertEqual(warnings, [])


class TestTweetReliability(unittest.TestCase):
    def _load_predict_module(self):
        return TestScheduleDeduping()._load_predict_module()

    def test_send_tweet_retries_then_succeeds(self):
        predict = self._load_predict_module()
        with patch.object(predict, "posting_disabled", return_value=False), \
             patch.object(predict, "mark_as_tweeted") as mark_mock, \
             patch.dict("os.environ", {"TWEET_RETRY_ATTEMPTS": "2", "TWEET_RETRY_BACKOFF_SEC": "0"}, clear=False):

            class _PopenFailThenOk:
                calls = 0

                def __init__(self, *args, **kwargs):
                    type(self).calls += 1
                    self.returncode = 1 if type(self).calls == 1 else 0

                def communicate(self, timeout=None):
                    if self.returncode == 0:
                        return ("ok", "")
                    return ("", "rate limit")

            with patch.object(predict.subprocess, "Popen", _PopenFailThenOk), \
                 patch.object(predict.time, "sleep", return_value=None):
                ok = predict.send_tweet("hello")

        self.assertTrue(ok)
        mark_mock.assert_called_once_with("hello")

    def test_send_tweet_opens_circuit_after_failures(self):
        predict = self._load_predict_module()
        predict._tweet_consecutive_failures = 0
        predict._tweet_circuit_open_until = 0.0

        with patch.object(predict, "posting_disabled", return_value=False), \
             patch.dict("os.environ", {
                 "TWEET_RETRY_ATTEMPTS": "1",
                 "TWEET_CIRCUIT_FAILURE_THRESHOLD": "1",
                 "TWEET_RETRY_BACKOFF_SEC": "0",
             }, clear=False):

            class _PopenAlwaysFail:
                def __init__(self, *args, **kwargs):
                    self.returncode = 1

                def communicate(self, timeout=None):
                    return ("", "boom")

            with patch.object(predict.subprocess, "Popen", _PopenAlwaysFail):
                ok = predict.send_tweet("hello")

        self.assertFalse(ok)
        self.assertTrue(predict._tweet_circuit_open_until > 0)


if __name__ == "__main__":
    unittest.main()
