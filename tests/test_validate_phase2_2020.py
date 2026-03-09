import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.history_ingest import ensure_schema
from scripts.validate_phase2_2020 import CheckResult, render_markdown, run_validation, sanity_ranges_check, table_digest_for_season


class TestValidatePhase22020(unittest.TestCase):
    def _seed_minimal_partition(self, conn: sqlite3.Connection, season: int) -> None:
        conn.execute(
            """
            INSERT INTO games (game_id, season, game_date, game_type, status, home_team_id, away_team_id, home_score, away_score)
            VALUES (?, ?, ?, 'R', 'Final', 1, 2, 5, 3)
            """,
            (1001, season, f"{season}-07-24"),
        )
        conn.execute(
            """
            INSERT INTO game_team_stats (game_id, team_id, side, runs, hits, errors, batting_avg, obp, slg, ops, strikeouts, walks)
            VALUES
              (1001, 1, 'home', 5, 9, 0, 0.280, 0.340, 0.410, 0.750, 8, 3),
              (1001, 2, 'away', 3, 7, 1, 0.250, 0.310, 0.370, 0.680, 10, 2)
            """
        )
        conn.execute(
            """
            INSERT INTO game_pitcher_context (
              game_id, side, pitcher_id, pitcher_name,
              probable_pitcher_id, probable_pitcher_name, probable_pitcher_known,
              season_era, season_whip, season_avg_allowed, season_runs_per_9,
              season_strike_pct, season_win_pct, career_era,
              stats_source, stats_as_of_date, season_stats_scope, season_stats_leakage_risk
            )
            VALUES
              (1001, 'home', 11, 'Home P', 11, 'Home P', 1, 3.2, 1.1, 0.240, 4.2, 0.230, 0.600, NULL, 'statsapi.boxscore_data', ?, 'season_to_date_prior_completed_games', 0),
              (1001, 'away', 22, 'Away P', 22, 'Away P', 1, 4.1, 1.3, 0.260, 5.0, 0.210, 0.450, NULL, 'statsapi.boxscore_data', ?, 'season_to_date_prior_completed_games', 0)
            """,
            (f"{season}-07-23", f"{season}-07-23"),
        )
        conn.execute(
            """
            INSERT INTO feature_rows (
              game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status
            )
            VALUES (1001, 'v1', ?, '{"home_team_strength_available":1}', 'valid')
            """,
            (f"{season}-07-24T00:00:00Z",),
        )
        conn.execute(
            """
            INSERT INTO ingestion_runs (run_id, mode, status, partition_key, started_at, ended_at, note)
            VALUES (
              'run-1', 'backfill', 'success', ?, '2026-03-09T00:00:00Z', '2026-03-09T00:10:00Z',
              '{"schedule_rows_fetched":1,"relevant_rows_processed":1,"distinct_games_touched":1,"games_inserted":1,"games_updated":0,"labels_inserted":1,"labels_updated":0}'
            )
            """,
            (f"season={season}",),
        )
        conn.execute(
            """
            INSERT INTO ingestion_checkpoints (job_name, partition_key, cursor_json, last_game_id, attempts, status, updated_at)
            VALUES (
              'backfill', ?,
              '{"schedule_rows_fetched":1,"relevant_rows_processed":1,"distinct_games_touched":1,"games_inserted":1,"games_updated":0,"labels_inserted":1,"labels_updated":0}',
              1001, 2, 'success', '2026-03-09T00:10:00Z'
            )
            """,
            (f"season={season}",),
        )
        conn.commit()

    def test_validation_fails_when_phase2_tables_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "mlb_history.db"
            with sqlite3.connect(db) as conn:
                conn.row_factory = sqlite3.Row
                ensure_schema(conn)
                conn.execute(
                    "INSERT INTO games (game_id, season, game_date, game_type, status, home_team_id, away_team_id) VALUES (1, 2020, '2020-01-01', 'R', 'Final', 1, 2)"
                )
                conn.commit()

            results = run_validation(db, season=2020, rerun_cmd=None)
            by_name = {r.name: r for r in results}
            self.assertEqual(by_name["Row coverage vs 2020 games"].status, "FAIL")
            self.assertEqual(by_name["Missingness per key feature field"].status, "FAIL")

    def test_validation_passes_on_minimal_complete_partition(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "mlb_history.db"
            with sqlite3.connect(db) as conn:
                conn.row_factory = sqlite3.Row
                ensure_schema(conn)
                self._seed_minimal_partition(conn, 2021)

            results = run_validation(db, season=2021, rerun_cmd=None)
            failing = [r for r in results if r.status == "FAIL"]
            self.assertEqual(failing, [])

    def test_team_stats_coverage_ignores_postponed_games(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "mlb_history.db"
            with sqlite3.connect(db) as conn:
                conn.row_factory = sqlite3.Row
                ensure_schema(conn)
                self._seed_minimal_partition(conn, 2021)
                conn.execute(
                    """
                    INSERT INTO games (game_id, season, game_date, game_type, status, home_team_id, away_team_id)
                    VALUES (1002, 2021, '2021-07-25', 'R', 'Postponed', 3, 4)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO game_pitcher_context (
                      game_id, side, pitcher_id, probable_pitcher_id, probable_pitcher_known, season_stats_leakage_risk
                    )
                    VALUES
                      (1002, 'home', NULL, NULL, 0, 0),
                      (1002, 'away', NULL, NULL, 0, 0)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO feature_rows (
                      game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status
                    )
                    VALUES (1002, 'v1', '2021-07-25T00:00:00Z', '{}', 'degraded')
                    """
                )
                conn.commit()

            results = run_validation(db, season=2021, rerun_cmd=None)
            by_name = {r.name: r for r in results}
            self.assertEqual(by_name["Row coverage vs 2021 games"].status, "PASS")

    def test_sanity_ranges_allow_high_small_sample_pitching_rates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "mlb_history.db"
            with sqlite3.connect(db) as conn:
                conn.row_factory = sqlite3.Row
                ensure_schema(conn)
                conn.execute(
                    """
                    INSERT INTO games (game_id, season, game_date, game_type, status, home_team_id, away_team_id)
                    VALUES (1003, 2020, '2020-07-26', 'R', 'Final', 5, 6)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO game_team_stats (
                      game_id, team_id, side, runs, hits, batting_avg, obp, slg, ops, strikeouts, walks
                    )
                    VALUES
                      (1003, 5, 'home', 3, 6, 0.250, 0.310, 0.380, 0.690, 8, 2),
                      (1003, 6, 'away', 8, 11, 0.333, 0.400, 0.520, 0.920, 6, 4)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO game_pitcher_context (
                      game_id, side, pitcher_id, probable_pitcher_id, probable_pitcher_known,
                      season_era, season_whip, season_avg_allowed, season_runs_per_9,
                      season_strike_pct, season_win_pct, career_era, season_stats_leakage_risk
                    )
                    VALUES
                      (1003, 'home', 31, 31, 1, 135.0, 21.0, 0.800, 135.0, 0.531, NULL, NULL, 0),
                      (1003, 'away', 32, 32, 1, 54.0, 10.5, 0.571, 94.5, 0.462, NULL, NULL, 0)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO feature_rows (
                      game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status
                    )
                    VALUES (1003, 'v1', '2020-07-26T00:00:00Z', '{}', 'valid')
                    """
                )
                conn.commit()

                result = sanity_ranges_check(conn, season=2020)

            self.assertEqual(result.status, "PASS")

    def test_sanity_ranges_still_fail_obviously_broken_pitching_rates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "mlb_history.db"
            with sqlite3.connect(db) as conn:
                conn.row_factory = sqlite3.Row
                ensure_schema(conn)
                conn.execute(
                    """
                    INSERT INTO games (game_id, season, game_date, game_type, status, home_team_id, away_team_id)
                    VALUES (1004, 2020, '2020-07-27', 'R', 'Final', 7, 8)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO game_team_stats (
                      game_id, team_id, side, runs, hits, batting_avg, obp, slg, ops, strikeouts, walks
                    )
                    VALUES
                      (1004, 7, 'home', 4, 8, 0.267, 0.333, 0.411, 0.744, 7, 3),
                      (1004, 8, 'away', 5, 9, 0.290, 0.355, 0.420, 0.775, 9, 4)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO game_pitcher_context (
                      game_id, side, pitcher_id, probable_pitcher_id, probable_pitcher_known,
                      season_era, season_whip, season_avg_allowed, season_runs_per_9,
                      season_strike_pct, season_win_pct, career_era, season_stats_leakage_risk
                    )
                    VALUES
                      (1004, 'home', 41, 41, 1, 450.0, 70.0, 0.800, 450.0, 0.531, NULL, NULL, 0),
                      (1004, 'away', 42, 42, 1, 3.5, 1.1, 0.240, 3.8, 0.650, NULL, NULL, 0)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO feature_rows (
                      game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status
                    )
                    VALUES (1004, 'v1', '2020-07-27T00:00:00Z', '{}', 'valid')
                    """
                )
                conn.commit()

                result = sanity_ranges_check(conn, season=2020)

            self.assertEqual(result.status, "FAIL")
            self.assertEqual(result.details["game_pitcher_context"]["season_era"]["out_of_range_count"], 1)
            self.assertEqual(result.details["game_pitcher_context"]["season_whip"]["out_of_range_count"], 1)
            self.assertEqual(result.details["game_pitcher_context"]["season_runs_per_9"]["out_of_range_count"], 1)

    def test_render_markdown_treats_warn_without_fail_as_overall_pass(self):
        report = render_markdown(
            [
                CheckResult(name="coverage", status="PASS", summary="ok", details={}),
                CheckResult(name="missingness", status="WARN", summary="warn only", details={}),
            ],
            season=2021,
            db_path=Path("data/mlb_history.db"),
        )

        self.assertIn("- Overall: **PASS**", report)
        self.assertIn("- None", report)

    def test_table_digest_ignores_ingestion_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "mlb_history.db"
            with sqlite3.connect(db) as conn:
                conn.row_factory = sqlite3.Row
                ensure_schema(conn)
                self._seed_minimal_partition(conn, 2021)
                before = table_digest_for_season(conn, "feature_rows", 2021)
                conn.execute(
                    """
                    UPDATE feature_rows
                    SET ingested_at = '2026-03-09 20:00:00'
                    WHERE game_id = 1001 AND feature_version = 'v1'
                    """
                )
                conn.commit()
                after = table_digest_for_season(conn, "feature_rows", 2021)

            self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
