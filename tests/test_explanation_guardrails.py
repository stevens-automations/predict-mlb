import unittest

from server.explanation_guardrails import (
    build_replay_explanation_prompt,
    validate_replay_explanation_output,
)


class TestExplanationGuardrails(unittest.TestCase):
    def test_prompt_includes_schema_and_context(self):
        prompt = build_replay_explanation_prompt(
            {
                "allowed_sources": ["odds_snapshot", "model_features"],
                "game_id": 123,
            }
        )
        self.assertIn("Output JSON only", prompt)
        self.assertIn('"cause"', prompt)
        self.assertIn('"confidence"', prompt)
        self.assertIn('"evidence"', prompt)
        self.assertIn('"caveats"', prompt)
        self.assertIn("odds_snapshot", prompt)

    def test_valid_output_passes_and_preserves_fields(self):
        raw = {
            "cause": "Model edge on home bullpen and favorable price.",
            "confidence": {"label": "medium", "score": 0.64},
            "evidence": [
                {"source": "odds_snapshot", "detail": "Home at -118 while model implied is -130."},
                {"source": "model_features", "detail": "Recent K-BB split favors home bullpen."},
            ],
            "caveats": ["Lineup confirmation pending."]
        }
        res = validate_replay_explanation_output(
            raw,
            allowed_sources=["odds_snapshot", "model_features", "weather_feed"],
        )
        self.assertTrue(res.valid)
        self.assertEqual(res.errors, [])
        self.assertIsNotNone(res.explanation)
        self.assertEqual(res.explanation["confidence"]["label"], "medium")
        self.assertEqual(len(res.explanation["evidence"]), 2)
        self.assertEqual(res.dropped_evidence_items, 0)

    def test_unsupported_evidence_is_dropped_and_can_invalidate(self):
        raw = {
            "cause": "Edge on pitching matchup.",
            "confidence": {"label": "high", "score": 0.81},
            "evidence": [
                {"source": "twitter_rumor", "detail": "Starter is secretly injured."}
            ],
            "caveats": ["Injury report not official."]
        }
        res = validate_replay_explanation_output(raw, allowed_sources=["odds_snapshot"])
        self.assertFalse(res.valid)
        self.assertIn("evidence_empty_after_validation", res.errors)
        self.assertEqual(res.dropped_evidence_items, 1)

    def test_non_json_output_fails_fast(self):
        res = validate_replay_explanation_output("not json", allowed_sources=["odds_snapshot"])
        self.assertFalse(res.valid)
        self.assertIn("output_not_json_object", res.errors)


if __name__ == "__main__":
    unittest.main()
