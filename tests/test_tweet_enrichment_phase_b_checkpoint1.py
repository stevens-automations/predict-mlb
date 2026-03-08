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


class TestEnvConfigurability(unittest.TestCase):
    def test_phrase_bank_parsing_fallbacks(self):
        with patch.dict(os.environ, {"TWEET_PHRASE_BANK": ""}, clear=False):
            self.assertEqual(tweet_generator.get_phrase_bank(), tweet_generator.DEFAULT_WIN_PHRASE_BANK)

        with patch.dict(os.environ, {"TWEET_PHRASE_BANK": " , ,  "}, clear=False):
            self.assertEqual(tweet_generator.get_phrase_bank(), tweet_generator.DEFAULT_WIN_PHRASE_BANK)

        with patch.dict(os.environ, {"TWEET_PHRASE_BANK": "smash, edge past , outlast"}, clear=False):
            self.assertEqual(tweet_generator.get_phrase_bank(), ["smash", "edge past", "outlast"])

    def test_mismatch_label_fallback(self):
        with patch.dict(os.environ, {"TWEET_MISMATCH_LABEL": ""}, clear=False):
            self.assertEqual(tweet_generator.get_mismatch_label(), "value")


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
    def test_custom_mismatch_label_used(self, _mock_ids):
        with patch.dict(os.environ, {"TWEET_MISMATCH_LABEL": "market"}, clear=False):
            line = tweet_generator.gen_game_line(self._row())
            self.assertIn("| market", line)

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

    def test_phrase_selection_stable_for_same_seed_and_bank(self):
        row = self._row(game_id=999)
        seed = tweet_generator.build_phrase_seed(row, "New York Yankees", "Boston Red Sox")
        bank = ["over", "to beat", "vs", "outlast"]
        self.assertEqual(tweet_generator._pick_phrase(seed, bank), tweet_generator._pick_phrase(seed, bank))


class TestObservabilityCounters(unittest.TestCase):
    def test_summary_counts_and_rate(self):
        lines = [
            "NYY (-120) over BOS (+110) [H] | value",
            "NYY (-120) over BOS (+110) [M]",
            "NYY (-120) over BOS (+110) [L] | value",
            "NYY (-120) over BOS (+110) [H]",
        ]
        summary = tweet_generator.summarize_enrichment_observability(lines)
        self.assertEqual(summary["total_game_lines"], 4)
        self.assertEqual(summary["confidence_tier_distribution"], {"H": 2, "M": 1, "L": 1})
        self.assertEqual(summary["mismatch_count"], 2)
        self.assertEqual(summary["mismatch_rate"], 0.5)


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
