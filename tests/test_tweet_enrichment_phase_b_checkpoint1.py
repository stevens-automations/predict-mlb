import os
import unittest
from unittest.mock import patch

import pandas as pd

from server import tweet_generator


TEAM_IDS = {
    "team_to_id": {"New York Yankees": 1, "Boston Red Sox": 2},
    "id_to_team": {
        "1": {"abbreviation": "NYY"},
        "2": {"abbreviation": "BOS"},
    },
}


class TestConfidenceTiering(unittest.TestCase):
    def test_default_thresholds(self):
        with patch.dict(os.environ, {}, clear=False):
            self.assertEqual(tweet_generator.derive_confidence_tier(0.70), "high")
            self.assertEqual(tweet_generator.derive_confidence_tier(0.58), "medium")
            self.assertEqual(tweet_generator.derive_confidence_tier(0.51), "low")

    def test_configurable_thresholds(self):
        with patch.dict(os.environ, {"PREDICTION_CONFIDENCE_THRESHOLDS": "0.75,0.60"}, clear=False):
            self.assertEqual(tweet_generator.derive_confidence_tier(0.74), "medium")
            self.assertEqual(tweet_generator.derive_confidence_tier(0.76), "high")


class TestMismatchAndPhrasing(unittest.TestCase):
    def _row(self, **overrides):
        row = {
            "home": "New York Yankees",
            "away": "Boston Red Sox",
            "predicted_winner": "New York Yankees",
            "home_odds": -120,
            "away_odds": 110,
            "favorite": "Boston Red Sox",
            "prediction_value": 0.66,
            "game_id": 123,
            "date": "2026-03-08",
        }
        row.update(overrides)
        return pd.Series(row)

    @patch("server.tweet_generator._load_team_ids", return_value=TEAM_IDS)
    def test_market_mismatch_signal_included_when_prediction_disagrees(self, _mock_ids):
        line = tweet_generator.gen_game_line(self._row())
        self.assertIn("| value", line)

    @patch("server.tweet_generator._load_team_ids", return_value=TEAM_IDS)
    def test_market_mismatch_omitted_when_prediction_matches_favorite(self, _mock_ids):
        line = tweet_generator.gen_game_line(self._row(favorite="New York Yankees"))
        self.assertNotIn("| value", line)

    @patch("server.tweet_generator._load_team_ids", return_value=TEAM_IDS)
    def test_phrase_selection_is_deterministic(self, _mock_ids):
        row = self._row(game_id=456)
        line1 = tweet_generator.gen_game_line(row)
        line2 = tweet_generator.gen_game_line(row)
        self.assertEqual(line1, line2)


class TestTweetLengthSafety(unittest.TestCase):
    def test_create_tweets_never_exceeds_char_limit(self):
        long_line = "NYY (-120) to beat BOS (+110) [H] | value " + ("x" * 200)
        lines = [long_line for _ in range(6)]
        tweets = tweet_generator.create_tweets(lines)
        self.assertTrue(tweets)
        for tweet in tweets:
            self.assertLessEqual(len(tweet), tweet_generator.TWITTER_MAX_CHAR_COUNT)


if __name__ == "__main__":
    unittest.main()
