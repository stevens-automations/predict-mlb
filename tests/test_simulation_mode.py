import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from server import get_odds
import predict
from storage import get_primary_storage
import simulation


FIXTURE_PATH = "tests/fixtures/sim_games.json"


class FakeScheduler:
    def __init__(self):
        self.jobs = {}

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def add_job(self, fn, args, trigger, run_date, id, replace_existing):
        self.jobs[id] = {
            "fn": fn,
            "args": args,
            "trigger": trigger,
            "run_date": run_date,
            "replace_existing": replace_existing,
        }


class TestSimulationMode(unittest.TestCase):
    def test_get_todays_odds_sim_mode_short_circuits_requests(self):
        with patch.dict(os.environ, {
            "PREDICT_SIM_MODE": "true",
            "PREDICT_SIM_FIXTURE_PATH": FIXTURE_PATH,
        }, clear=False):
            with patch("server.get_odds.make_request", side_effect=AssertionError("live odds request should not happen")):
                games, _ = get_odds.get_todays_odds()

        self.assertGreater(len(games), 0)
        self.assertEqual(games[0]["date"], "Today")
        self.assertIn("sim_game_id", games[0])

    def test_sim_seed_reorders_games_deterministically(self):
        with patch.dict(os.environ, {
            "PREDICT_SIM_MODE": "true",
            "PREDICT_SIM_FIXTURE_PATH": FIXTURE_PATH,
            "PREDICT_SIM_SEED": "123",
        }, clear=False):
            games_seed_123_a, _ = get_odds.get_todays_odds()

        with patch.dict(os.environ, {
            "PREDICT_SIM_MODE": "true",
            "PREDICT_SIM_FIXTURE_PATH": FIXTURE_PATH,
            "PREDICT_SIM_SEED": "123",
        }, clear=False):
            games_seed_123_b, _ = get_odds.get_todays_odds()

        with patch.dict(os.environ, {
            "PREDICT_SIM_MODE": "true",
            "PREDICT_SIM_FIXTURE_PATH": FIXTURE_PATH,
            "PREDICT_SIM_SEED": "999",
        }, clear=False):
            games_seed_999, _ = get_odds.get_todays_odds()

        ids_123_a = [g["sim_game_id"] for g in games_seed_123_a]
        ids_123_b = [g["sim_game_id"] for g in games_seed_123_b]
        ids_999 = [g["sim_game_id"] for g in games_seed_999]

        self.assertEqual(ids_123_a, ids_123_b)
        self.assertCountEqual(ids_123_a, ids_999)

        sample_game = games_seed_123_a[0]
        self.assertNotEqual(
            simulation._seeded_game_order_key(sample_game, 123),
            simulation._seeded_game_order_key(sample_game, 999),
        )

    def test_generate_and_schedule_from_fixture_without_live_calls(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sim.db")
            with patch.dict(os.environ, {
                "PREDICT_SIM_MODE": "true",
                "PREDICT_SIM_DATE": "2026-07-04",
                "PREDICT_SIM_FIXTURE_PATH": FIXTURE_PATH,
                "PREDICT_SIM_SEED": "123",
                "PREDICT_DRY_RUN": "true",
                "SQLITE_DB_PATH": db_path,
            }, clear=False):
                storage = get_primary_storage()
                storage.ensure_ready()

                with patch.object(predict.mlb, "get_days_games", side_effect=AssertionError("statsapi schedule should not be used in sim mode")):
                    with patch.object(predict.mlb, "predict_game", side_effect=AssertionError("model prediction path should not be used in sim mode")):
                        tweet_lines, observability_lines, predicted_games = predict.generate_daily_predictions(
                            storage=storage,
                            date=datetime(2026, 7, 4, 9, 30),
                            run_id="sim-test",
                        )

                self.assertEqual(predicted_games, 2)
                self.assertGreater(len(tweet_lines), 0)
                self.assertEqual(len(observability_lines), len(tweet_lines))

                predict.daily_scheduler = FakeScheduler()
                scheduled_jobs, summary = predict.schedule_tweets(
                    tweet_lines,
                    run_id="sim-test",
                    observability_lines=observability_lines,
                )
                self.assertGreaterEqual(scheduled_jobs, 1)
                self.assertEqual(summary["total_game_lines"], len(tweet_lines))


if __name__ == "__main__":
    unittest.main()
