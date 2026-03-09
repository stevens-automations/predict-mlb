import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.history_ingest import ensure_schema
from scripts.validate_phase2_2020 import run_validation


class TestValidatePhase22020(unittest.TestCase):
    def _seed_minimal_2020(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            INSERT INTO games (game_id, season, game_date, game_type, status, home_team_id, away_team_id, home_score, away_score)
            VALUES (1001, 2020, '2020-07-24', 'R', 'Final', 1, 2, 5, 3)
            """
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
              (1001, 'home', 11, 'Home P', 11, 'Home P', 1, 3.2, 1.1, 0.240, 4.2, 0.230, 0.600, 3.8, 'statsapi', '2020-07-23', 'through_prior_game', 1),
              (1001, 'away', 22, 'Away P', 22, 'Away P', 1, 4.1, 1.3, 0.260, 5.0, 0.210, 0.450, 4.3, 'statsapi', '2020-07-23', 'through_prior_game', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO ingestion_runs (run_id, mode, status, partition_key, started_at, ended_at, note)
            VALUES (
              'run-1', 'backfill', 'success', 'season=2020', '2026-03-09T00:00:00Z', '2026-03-09T00:10:00Z',
              '{"schedule_rows_fetched":1,"relevant_rows_processed":1,"distinct_games_touched":1,"games_inserted":1,"games_updated":0,"labels_inserted":1,"labels_updated":0}'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO ingestion_checkpoints (job_name, partition_key, cursor_json, last_game_id, attempts, status, updated_at)
            VALUES (
              'backfill', 'season=2020',
              '{"schedule_rows_fetched":1,"relevant_rows_processed":1,"distinct_games_touched":1,"games_inserted":1,"games_updated":0,"labels_inserted":1,"labels_updated":0}',
              1001, 2, 'success', '2026-03-09T00:10:00Z'
            )
            """
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
                self._seed_minimal_2020(conn)

            results = run_validation(db, season=2020, rerun_cmd=None)
            failing = [r for r in results if r.status == "FAIL"]
            self.assertEqual(failing, [])


if __name__ == "__main__":
    unittest.main()
