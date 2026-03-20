import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from train.data_loader import (
    CLEANED_COMPARATIVE_FEATURE_CONTRACT,
    HYBRID_COMPARATIVE_FEATURE_CONTRACT,
    load_feature_rows,
    resolve_feature_exclusions,
)


SCHEMA = """
CREATE TABLE games (
  game_id INTEGER PRIMARY KEY,
  season INTEGER NOT NULL,
  game_date TEXT NOT NULL
);
CREATE TABLE labels (
  game_id INTEGER PRIMARY KEY,
  did_home_win INTEGER
);
CREATE TABLE feature_rows (
  game_id INTEGER NOT NULL,
  feature_version TEXT NOT NULL,
  as_of_ts TEXT NOT NULL,
  feature_payload_json TEXT NOT NULL,
  source_contract_status TEXT NOT NULL,
  source_contract_issues_json TEXT,
  PRIMARY KEY (game_id, feature_version, as_of_ts)
);
"""


class TestTrainingDataLoader(unittest.TestCase):
    def _make_db(self) -> Path:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        db_path = Path(tempdir.name) / "history.db"
        with sqlite3.connect(db_path) as conn:
            conn.executescript(SCHEMA)
        return db_path

    def test_load_feature_rows_builds_diagnostics_for_canonical_dataset(self) -> None:
        db_path = self._make_db()
        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                "INSERT INTO games (game_id, season, game_date) VALUES (?, ?, ?)",
                [(1, 2020, "2020-07-24"), (2, 2025, "2025-04-01")],
            )
            conn.executemany(
                "INSERT INTO labels (game_id, did_home_win) VALUES (?, ?)",
                [(1, 1), (2, 0)],
            )
            conn.executemany(
                """
                INSERT INTO feature_rows (
                  game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status, source_contract_issues_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        1,
                        "v2_phase1",
                        "2020-07-24T17:00:00Z",
                        json.dumps({"starter_rating_diff": 0.8, "team_form_diff": 0.3}),
                        "valid",
                        json.dumps([]),
                    ),
                    (
                        2,
                        "v2_phase1",
                        "2025-04-01T16:00:00Z",
                        json.dumps({"starter_rating_diff": -0.2, "team_form_diff": 0.1}),
                        "degraded",
                        json.dumps(["weather_fallback"]),
                    ),
                ],
            )

        dataset = load_feature_rows(
            db_path=db_path,
            feature_version="v2_phase1",
            seasons=[2020, 2025],
            contract_statuses=["valid", "degraded"],
        )

        self.assertEqual(dataset.feature_columns, ["starter_rating_diff", "team_form_diff"])
        self.assertIsNotNone(dataset.diagnostics)
        assert dataset.diagnostics is not None
        self.assertEqual(dataset.diagnostics["n_games"], 2)
        self.assertEqual(dataset.diagnostics["seasons"], [2020, 2025])
        self.assertEqual(dataset.diagnostics["contract_status_counts"]["degraded"], 1)

    def test_load_feature_rows_rejects_duplicate_rows_for_same_game(self) -> None:
        db_path = self._make_db()
        with sqlite3.connect(db_path) as conn:
            conn.execute("INSERT INTO games (game_id, season, game_date) VALUES (?, ?, ?)", (1, 2025, "2025-04-01"))
            conn.execute("INSERT INTO labels (game_id, did_home_win) VALUES (?, ?)", (1, 1))
            conn.executemany(
                """
                INSERT INTO feature_rows (
                  game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status, source_contract_issues_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (1, "v2_phase1", "2025-04-01T16:00:00Z", json.dumps({"feature_a": 1.0}), "valid", json.dumps([])),
                    (1, "v2_phase1", "2025-04-01T17:00:00Z", json.dumps({"feature_a": 1.1}), "valid", json.dumps([])),
                ],
            )

        with self.assertRaisesRegex(ValueError, "exactly one feature row per game"):
            load_feature_rows(
                db_path=db_path,
                feature_version="v2_phase1",
                seasons=[2025],
                contract_statuses=["valid"],
            )

    def test_load_feature_rows_rejects_sportsbook_feature_leakage(self) -> None:
        db_path = self._make_db()
        with sqlite3.connect(db_path) as conn:
            conn.execute("INSERT INTO games (game_id, season, game_date) VALUES (?, ?, ?)", (1, 2025, "2025-04-01"))
            conn.execute("INSERT INTO labels (game_id, did_home_win) VALUES (?, ?)", (1, 1))
            conn.execute(
                """
                INSERT INTO feature_rows (
                  game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status, source_contract_issues_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    1,
                    "v2_phase1",
                    "2025-04-01T16:00:00Z",
                    json.dumps({"team_strength_diff": 0.4, "bookmaker_price": -120}),
                    "valid",
                    json.dumps([]),
                ),
            )

        with self.assertRaisesRegex(ValueError, "Forbidden sportsbook-related features"):
            load_feature_rows(
                db_path=db_path,
                feature_version="v2_phase1",
                seasons=[2025],
                contract_statuses=["valid"],
            )

    def test_load_feature_rows_applies_named_feature_ablation_groups(self) -> None:
        db_path = self._make_db()
        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                "INSERT INTO games (game_id, season, game_date) VALUES (?, ?, ?)",
                [(1, 2025, "2025-04-01"), (2, 2025, "2025-04-02")],
            )
            conn.executemany(
                "INSERT INTO labels (game_id, did_home_win) VALUES (?, ?)",
                [(1, 1), (2, 0)],
            )
            conn.executemany(
                """
                INSERT INTO feature_rows (
                  game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status, source_contract_issues_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        1,
                        "v2_phase1",
                        "2025-04-01T16:00:00Z",
                        json.dumps(
                            {
                                "home_starter_id": 101,
                                "away_starter_id": 202,
                                "home_platoon_ops": 0.765,
                                "away_platoon_ops": 0.701,
                                "home_lineup_quality_mean": 0.42,
                                "away_lineup_quality_mean": 0.39,
                                "temperature_f": 61.0,
                                "starter_rating_diff": 0.5,
                            }
                        ),
                        "valid",
                        json.dumps([]),
                    ),
                    (
                        2,
                        "v2_phase1",
                        "2025-04-02T16:00:00Z",
                        json.dumps(
                            {
                                "home_starter_id": 103,
                                "away_starter_id": 204,
                                "home_platoon_ops": 0.688,
                                "away_platoon_ops": 0.744,
                                "home_lineup_quality_mean": 0.33,
                                "away_lineup_quality_mean": 0.36,
                                "temperature_f": 67.0,
                                "starter_rating_diff": -0.2,
                            }
                        ),
                        "valid",
                        json.dumps([]),
                    ),
                ],
            )

        dataset = load_feature_rows(
            db_path=db_path,
            feature_version="v2_phase1",
            seasons=[2025],
            contract_statuses=["valid"],
            exclude_feature_groups=["starter_ids", "placeholder_lineup_quality", "weather", "platoon"],
        )

        self.assertEqual(dataset.feature_columns, ["starter_rating_diff"])
        assert dataset.diagnostics is not None
        feature_exclusions = dataset.diagnostics["contract_diagnostics"]["feature_exclusions"]
        self.assertEqual(
            feature_exclusions["requested_feature_groups"],
            ["starter_ids", "placeholder_lineup_quality", "weather", "platoon"],
        )
        self.assertIn("home_starter_id", feature_exclusions["excluded_feature_columns"])
        self.assertIn("home_platoon_ops", feature_exclusions["excluded_feature_columns"])
        self.assertIn("temperature_f", feature_exclusions["excluded_feature_columns"])

    def test_resolve_feature_exclusions_rejects_unknown_group(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported feature ablation groups"):
            resolve_feature_exclusions(
                pd.DataFrame(),
                exclude_feature_groups=["unknown_group"],
            )

    def test_cleaned_comparative_contract_keeps_home_edge_features_and_safe_anchors(self) -> None:
        db_path = self._make_db()
        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                "INSERT INTO games (game_id, season, game_date) VALUES (?, ?, ?)",
                [(1, 2025, "2025-04-01"), (2, 2025, "2025-04-02")],
            )
            conn.executemany(
                "INSERT INTO labels (game_id, did_home_win) VALUES (?, ?)",
                [(1, 1), (2, 0)],
            )
            conn.executemany(
                """
                INSERT INTO feature_rows (
                  game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status, source_contract_issues_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        1,
                        "v2_phase1",
                        "2025-04-01T16:00:00Z",
                        json.dumps(
                            {
                                "home_starter_id": 101,
                                "away_starter_id": 202,
                                "home_starter_era": 3.2,
                                "away_starter_era": 4.4,
                                "home_starter_whip": 1.08,
                                "away_starter_whip": 1.26,
                                "home_starter_strike_pct": 0.66,
                                "away_starter_strike_pct": 0.62,
                                "home_bullpen_era_season": 3.5,
                                "away_bullpen_era_season": 4.1,
                                "home_bullpen_pitches_last3d": 48,
                                "away_bullpen_pitches_last3d": 64,
                                "home_top3_freshness_score": 0.74,
                                "away_top3_freshness_score": 0.41,
                                "home_team_season_win_pct": 0.610,
                                "away_team_season_win_pct": 0.470,
                                "home_team_rolling_last10_runs_against_per_game": 3.6,
                                "away_team_rolling_last10_runs_against_per_game": 5.1,
                                "home_platoon_ops": 0.765,
                                "away_platoon_ops": 0.701,
                                "home_lineup_quality_mean": 0.42,
                                "away_lineup_quality_mean": 0.39,
                                "lineup_quality_delta": 0.03,
                                "home_lineup_known_flag": 1,
                                "away_lineup_known_flag": 0,
                                "home_platoon_games_in_sample": 41,
                                "away_platoon_games_in_sample": 39,
                                "home_field_advantage": 1,
                                "wind_speed_mph": 9.0,
                            }
                        ),
                        "valid",
                        json.dumps([]),
                    ),
                    (
                        2,
                        "v2_phase1",
                        "2025-04-02T16:00:00Z",
                        json.dumps(
                            {
                                "home_starter_id": 103,
                                "away_starter_id": 204,
                                "home_starter_era": 4.0,
                                "away_starter_era": 3.6,
                                "home_starter_whip": 1.25,
                                "away_starter_whip": 1.17,
                                "home_starter_strike_pct": 0.61,
                                "away_starter_strike_pct": 0.64,
                                "home_bullpen_era_season": 4.2,
                                "away_bullpen_era_season": 3.7,
                                "home_bullpen_pitches_last3d": 71,
                                "away_bullpen_pitches_last3d": 52,
                                "home_top3_freshness_score": 0.38,
                                "away_top3_freshness_score": 0.63,
                                "home_team_season_win_pct": 0.455,
                                "away_team_season_win_pct": 0.590,
                                "home_team_rolling_last10_runs_against_per_game": 4.9,
                                "away_team_rolling_last10_runs_against_per_game": 4.1,
                                "home_platoon_ops": 0.688,
                                "away_platoon_ops": 0.744,
                                "home_lineup_quality_mean": 0.33,
                                "away_lineup_quality_mean": 0.36,
                                "lineup_quality_delta": -0.03,
                                "home_lineup_known_flag": 0,
                                "away_lineup_known_flag": 1,
                                "home_platoon_games_in_sample": 35,
                                "away_platoon_games_in_sample": 42,
                                "home_field_advantage": 1,
                                "wind_speed_mph": 12.0,
                            }
                        ),
                        "valid",
                        json.dumps([]),
                    ),
                ],
            )

        dataset = load_feature_rows(
            db_path=db_path,
            feature_version="v2_phase1",
            seasons=[2025],
            contract_statuses=["valid"],
            feature_contract=CLEANED_COMPARATIVE_FEATURE_CONTRACT,
        )

        self.assertIn("starter_era_home_edge", dataset.feature_columns)
        self.assertIn("bullpen_era_home_edge", dataset.feature_columns)
        self.assertIn("top3_freshness_home_edge", dataset.feature_columns)
        self.assertIn("team_season_win_pct_home_edge", dataset.feature_columns)
        self.assertIn("team_rolling_last10_runs_against_per_game_home_edge", dataset.feature_columns)
        self.assertIn("platoon_ops_home_edge", dataset.feature_columns)
        self.assertIn("home_lineup_known_flag", dataset.feature_columns)
        self.assertIn("away_platoon_games_in_sample", dataset.feature_columns)
        self.assertIn("wind_speed_mph", dataset.feature_columns)
        self.assertNotIn("home_starter_id", dataset.feature_columns)
        self.assertNotIn("away_starter_id", dataset.feature_columns)
        self.assertNotIn("home_lineup_quality_mean", dataset.feature_columns)
        self.assertNotIn("lineup_quality_delta", dataset.feature_columns)
        self.assertNotIn("home_field_advantage", dataset.feature_columns)

        first_row = dataset.dataframe.iloc[0]
        self.assertAlmostEqual(first_row["starter_era_home_edge"], 1.2, places=6)
        self.assertAlmostEqual(first_row["starter_whip_home_edge"], 0.18, places=6)
        self.assertAlmostEqual(first_row["starter_strike_pct_home_edge"], 0.04, places=6)
        self.assertAlmostEqual(first_row["bullpen_era_home_edge"], 0.6, places=6)
        self.assertAlmostEqual(first_row["bullpen_pitches_last3d_home_edge"], 16.0, places=6)
        self.assertAlmostEqual(first_row["top3_freshness_home_edge"], 0.33, places=6)
        self.assertAlmostEqual(first_row["team_season_win_pct_home_edge"], 0.14, places=6)
        self.assertAlmostEqual(first_row["team_rolling_last10_runs_against_per_game_home_edge"], 1.5, places=6)
        self.assertAlmostEqual(first_row["platoon_ops_home_edge"], 0.064, places=6)

        assert dataset.diagnostics is not None
        contract_diagnostics = dataset.diagnostics["contract_diagnostics"]
        self.assertIn("home_starter_id", contract_diagnostics["excluded_forbidden_identity_columns"])
        self.assertIn("home_lineup_quality_mean", contract_diagnostics["excluded_placeholder_grade_columns"])
        self.assertIn("home_field_advantage", contract_diagnostics["dropped_feature_columns"]["constant"])
        self.assertEqual(dataset.diagnostics["feature_contract"], CLEANED_COMPARATIVE_FEATURE_CONTRACT)

    def test_hybrid_comparative_contract_restores_raw_quality_anchors_without_identity_leakage(self) -> None:
        db_path = self._make_db()
        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                "INSERT INTO games (game_id, season, game_date) VALUES (?, ?, ?)",
                [(1, 2025, "2025-04-01"), (2, 2025, "2025-04-02")],
            )
            conn.executemany(
                "INSERT INTO labels (game_id, did_home_win) VALUES (?, ?)",
                [(1, 1), (2, 0)],
            )
            conn.executemany(
                """
                INSERT INTO feature_rows (
                  game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status, source_contract_issues_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        1,
                        "v2_phase1",
                        "2025-04-01T16:00:00Z",
                        json.dumps(
                            {
                                "home_team_id": 10,
                                "away_team_id": 20,
                                "home_starter_id": 101,
                                "away_starter_id": 202,
                                "home_starter_era": 3.2,
                                "away_starter_era": 4.4,
                                "home_starter_whip": 1.08,
                                "away_starter_whip": 1.26,
                                "home_starter_runs_per_9": 3.4,
                                "away_starter_runs_per_9": 4.5,
                                "home_starter_career_era": 3.45,
                                "away_starter_career_era": 4.05,
                                "home_bullpen_era_season": 3.5,
                                "away_bullpen_era_season": 4.1,
                                "home_bullpen_whip_season": 1.17,
                                "away_bullpen_whip_season": 1.29,
                                "home_bullpen_pitches_last3d": 48,
                                "away_bullpen_pitches_last3d": 64,
                                "home_top3_freshness_score": 0.74,
                                "away_top3_freshness_score": 0.41,
                                "home_team_season_win_pct": 0.610,
                                "away_team_season_win_pct": 0.470,
                                "home_team_season_run_diff_per_game": 1.2,
                                "away_team_season_run_diff_per_game": -0.4,
                                "home_team_rolling_last10_win_pct": 0.700,
                                "away_team_rolling_last10_win_pct": 0.400,
                                "home_team_rolling_last10_ops": 0.792,
                                "away_team_rolling_last10_ops": 0.711,
                                "home_team_rolling_last10_runs_against_per_game": 3.6,
                                "away_team_rolling_last10_runs_against_per_game": 5.1,
                                "home_lineup_quality_mean": 0.42,
                                "away_lineup_quality_mean": 0.39,
                                "home_lineup_vs_opp_starter_hand_quality": 0.51,
                                "away_lineup_vs_opp_starter_hand_quality": 0.48,
                                "home_lineup_known_flag": 1,
                                "away_lineup_known_flag": 0,
                                "home_bullpen_available_flag": 1,
                                "away_bullpen_available_flag": 1,
                                "home_bullpen_appearances_season": 22,
                                "away_bullpen_appearances_season": 25,
                                "home_team_season_games": 12,
                                "away_team_season_games": 12,
                                "home_team_rolling_games": 10,
                                "away_team_rolling_games": 10,
                                "home_field_advantage": 1,
                                "wind_speed_mph": 9.0,
                            }
                        ),
                        "valid",
                        json.dumps([]),
                    ),
                    (
                        2,
                        "v2_phase1",
                        "2025-04-02T16:00:00Z",
                        json.dumps(
                            {
                                "home_team_id": 11,
                                "away_team_id": 21,
                                "home_starter_id": 103,
                                "away_starter_id": 204,
                                "home_starter_era": 4.0,
                                "away_starter_era": 3.6,
                                "home_starter_whip": 1.25,
                                "away_starter_whip": 1.17,
                                "home_starter_runs_per_9": 4.8,
                                "away_starter_runs_per_9": 3.9,
                                "home_starter_career_era": 4.22,
                                "away_starter_career_era": 3.74,
                                "home_bullpen_era_season": 4.2,
                                "away_bullpen_era_season": 3.7,
                                "home_bullpen_whip_season": 1.31,
                                "away_bullpen_whip_season": 1.19,
                                "home_bullpen_pitches_last3d": 71,
                                "away_bullpen_pitches_last3d": 52,
                                "home_top3_freshness_score": 0.38,
                                "away_top3_freshness_score": 0.63,
                                "home_team_season_win_pct": 0.455,
                                "away_team_season_win_pct": 0.590,
                                "home_team_season_run_diff_per_game": -0.8,
                                "away_team_season_run_diff_per_game": 0.7,
                                "home_team_rolling_last10_win_pct": 0.300,
                                "away_team_rolling_last10_win_pct": 0.600,
                                "home_team_rolling_last10_ops": 0.684,
                                "away_team_rolling_last10_ops": 0.749,
                                "home_team_rolling_last10_runs_against_per_game": 4.9,
                                "away_team_rolling_last10_runs_against_per_game": 4.1,
                                "home_lineup_quality_mean": 0.33,
                                "away_lineup_quality_mean": 0.36,
                                "home_lineup_vs_opp_starter_hand_quality": 0.47,
                                "away_lineup_vs_opp_starter_hand_quality": 0.52,
                                "home_lineup_known_flag": 0,
                                "away_lineup_known_flag": 1,
                                "home_bullpen_available_flag": 1,
                                "away_bullpen_available_flag": 1,
                                "home_bullpen_appearances_season": 24,
                                "away_bullpen_appearances_season": 26,
                                "home_team_season_games": 13,
                                "away_team_season_games": 13,
                                "home_team_rolling_games": 10,
                                "away_team_rolling_games": 10,
                                "home_field_advantage": 1,
                                "wind_speed_mph": 12.0,
                            }
                        ),
                        "valid",
                        json.dumps([]),
                    ),
                ],
            )

        dataset = load_feature_rows(
            db_path=db_path,
            feature_version="v2_phase1",
            seasons=[2025],
            contract_statuses=["valid"],
            feature_contract=HYBRID_COMPARATIVE_FEATURE_CONTRACT,
        )

        self.assertIn("starter_era_home_edge", dataset.feature_columns)
        self.assertIn("home_starter_era", dataset.feature_columns)
        self.assertIn("away_starter_era", dataset.feature_columns)
        self.assertIn("home_bullpen_era_season", dataset.feature_columns)
        self.assertIn("away_bullpen_whip_season", dataset.feature_columns)
        self.assertIn("home_top3_freshness_score", dataset.feature_columns)
        self.assertIn("away_team_season_win_pct", dataset.feature_columns)
        self.assertIn("home_team_rolling_last10_ops", dataset.feature_columns)
        self.assertIn("home_lineup_known_flag", dataset.feature_columns)
        self.assertNotIn("home_starter_id", dataset.feature_columns)
        self.assertNotIn("away_starter_id", dataset.feature_columns)
        self.assertNotIn("home_team_id", dataset.feature_columns)
        self.assertNotIn("away_team_id", dataset.feature_columns)
        self.assertNotIn("home_lineup_quality_mean", dataset.feature_columns)
        self.assertNotIn("home_lineup_vs_opp_starter_hand_quality", dataset.feature_columns)

        first_row = dataset.dataframe.iloc[0]
        self.assertAlmostEqual(first_row["home_starter_era"], 3.2, places=6)
        self.assertAlmostEqual(first_row["away_bullpen_whip_season"], 1.29, places=6)
        self.assertAlmostEqual(first_row["home_team_rolling_last10_ops"], 0.792, places=6)
        self.assertAlmostEqual(first_row["starter_era_home_edge"], 1.2, places=6)

        assert dataset.diagnostics is not None
        contract_diagnostics = dataset.diagnostics["contract_diagnostics"]
        self.assertIn("home_starter_era", contract_diagnostics["included_raw_quality_anchor_columns"])
        self.assertIn("away_team_rolling_last10_ops", contract_diagnostics["included_raw_quality_anchor_columns"])
        self.assertIn("home_starter_id", contract_diagnostics["excluded_forbidden_identity_columns"])
        self.assertIn("home_lineup_quality_mean", contract_diagnostics["excluded_placeholder_grade_columns"])
        self.assertEqual(dataset.diagnostics["feature_contract"], HYBRID_COMPARATIVE_FEATURE_CONTRACT)


if __name__ == "__main__":
    unittest.main()
