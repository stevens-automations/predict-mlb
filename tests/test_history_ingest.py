from __future__ import annotations

import json
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.history_ingest import (
    _team_stats_row_from_boxscore,
    build_parser,
    connect_db,
    ensure_schema,
    upsert_checkpoint,
    upsert_game,
    upsert_game_pitcher_context,
    upsert_game_team_stats,
)


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


class TestHistoryIngestSchemaAndUpserts(unittest.TestCase):
    def test_init_schema_creates_required_tables(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                required = {
                    "games",
                    "game_team_stats",
                    "game_pitcher_context",
                    "feature_rows",
                    "labels",
                    "ingestion_runs",
                    "ingestion_checkpoints",
                    "dq_results",
                    "odds_snapshot",
                }
                missing = {name for name in required if not table_exists(conn, name)}
                self.assertFalse(missing)

                label_cols = {row["name"] for row in conn.execute("PRAGMA table_info(labels)").fetchall()}
                self.assertIn("run_differential", label_cols)
                self.assertIn("total_runs", label_cols)

    def test_checkpoint_upsert_is_idempotent_and_increments_attempts(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_checkpoint(
                    conn,
                    job_name="backfill",
                    partition_key="season=2024",
                    cursor={"last_game_id": 1},
                    status="running",
                    last_game_id=1,
                )
                upsert_checkpoint(
                    conn,
                    job_name="backfill",
                    partition_key="season=2024",
                    cursor={"last_game_id": 2},
                    status="running",
                    last_game_id=2,
                )
                row = conn.execute(
                    "SELECT attempts, last_game_id FROM ingestion_checkpoints WHERE job_name=? AND partition_key=?",
                    ("backfill", "season=2024"),
                ).fetchone()
                self.assertEqual(row["attempts"], 2)
                self.assertEqual(row["last_game_id"], 2)

    def test_game_upsert_updates_existing_row_without_duplicates(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 123,
                        "season": 2024,
                        "game_date": "2024-04-01",
                        "status": "Scheduled",
                        "home_team_id": 1,
                        "away_team_id": 2,
                    },
                )
                upsert_game(
                    conn,
                    {
                        "game_id": 123,
                        "season": 2024,
                        "game_date": "2024-04-01",
                        "status": "Final",
                        "home_team_id": 1,
                        "away_team_id": 2,
                        "home_score": 5,
                        "away_score": 4,
                        "winning_team_id": 1,
                    },
                )
                count_row = conn.execute("SELECT COUNT(*) AS c FROM games WHERE game_id=123").fetchone()
                game_row = conn.execute("SELECT status, home_score, away_score FROM games WHERE game_id=123").fetchone()
                self.assertEqual(count_row["c"], 1)
                self.assertEqual(game_row["status"], "Final")
                self.assertEqual(game_row["home_score"], 5)
                self.assertEqual(game_row["away_score"], 4)

    def test_game_team_stats_upsert_updates_existing_row_without_duplicates(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(conn, {"game_id": 123, "season": 2020, "game_date": "2020-07-24", "status": "Final"})
                upsert_game_team_stats(
                    conn,
                    {
                        "game_id": 123,
                        "team_id": 147,
                        "side": "home",
                        "runs": 3,
                        "hits": 8,
                        "batting_avg": 0.250,
                        "obp": 0.320,
                        "slg": 0.410,
                        "ops": 0.730,
                        "strikeouts": 9,
                        "walks": 2,
                    },
                )
                upsert_game_team_stats(
                    conn,
                    {
                        "game_id": 123,
                        "team_id": 147,
                        "side": "home",
                        "runs": 4,
                        "hits": 9,
                        "batting_avg": 0.265,
                        "obp": 0.333,
                        "slg": 0.455,
                        "ops": 0.788,
                        "strikeouts": 8,
                        "walks": 3,
                    },
                )
                count_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM game_team_stats WHERE game_id=123 AND team_id=147"
                ).fetchone()
                stats_row = conn.execute(
                    "SELECT runs, hits, batting_avg, obp, slg, ops, strikeouts, walks FROM game_team_stats WHERE game_id=123 AND team_id=147"
                ).fetchone()
                self.assertEqual(count_row["c"], 1)
                self.assertEqual(stats_row["runs"], 4)
                self.assertEqual(stats_row["hits"], 9)
                self.assertEqual(stats_row["strikeouts"], 8)
                self.assertEqual(stats_row["walks"], 3)


class TestHistoryIngestCommands(unittest.TestCase):
    def test_backfill_pitcher_context_for_season_derives_parity_safe_stats_and_is_idempotent(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game_id, game_date in ((3001, "2021-08-01"), (3002, "2021-08-05")):
                    upsert_game(
                        conn,
                        {
                            "game_id": game_id,
                            "season": season,
                            "game_date": game_date,
                            "status": "Final",
                            "home_team_id": 147,
                            "away_team_id": 121,
                        },
                    )

            parser = build_parser()
            args = parser.parse_args(
                ["--db", str(db_path), "--checkpoint-every", "1", "backfill-pitcher-context", "--season", str(season)]
            )

            schedule_rows = [
                {
                    "game_id": 3001,
                    "season": season,
                    "game_date": "2021-08-01",
                    "home_probable_pitcher": "Home Starter",
                    "away_probable_pitcher": "Away Starter",
                },
                {
                    "game_id": 3002,
                    "season": season,
                    "game_date": "2021-08-05",
                    "home_probable_pitcher": "Home Starter",
                    "away_probable_pitcher": "Away Starter",
                }
            ]

            def fake_lookup_player(name, season=None):
                if name == "Home Starter":
                    return [{"id": 501}]
                if name == "Away Starter":
                    return [{"id": 502}]
                return []

            def fake_boxscore_data(game_id):
                if game_id == 3001:
                    return {
                        "decisions": {"winner": {"id": 501}, "loser": {"id": 502}},
                        "home": {
                            "players": {
                                "ID501": {
                                    "person": {"id": 501, "fullName": "Home Starter"},
                                    "stats": {
                                        "pitching": {
                                            "inningsPitched": "6.0",
                                            "hits": 4,
                                            "baseOnBalls": 2,
                                            "earnedRuns": 2,
                                            "runs": 2,
                                            "atBats": 24,
                                            "strikes": 60,
                                            "numberOfPitches": 90,
                                        }
                                    },
                                }
                            }
                        },
                        "away": {
                            "players": {
                                "ID502": {
                                    "person": {"id": 502, "fullName": "Away Starter"},
                                    "stats": {
                                        "pitching": {
                                            "inningsPitched": "5.0",
                                            "hits": 7,
                                            "baseOnBalls": 1,
                                            "earnedRuns": 4,
                                            "runs": 4,
                                            "atBats": 22,
                                            "strikes": 50,
                                            "numberOfPitches": 80,
                                        }
                                    },
                                }
                            }
                        },
                    }
                return {"home": {"players": {}}, "away": {"players": {}}}

            stub_statsapi = types.SimpleNamespace(
                schedule=lambda **_kwargs: schedule_rows,
                lookup_player=fake_lookup_player,
                boxscore_data=fake_boxscore_data,
            )

            with patch("scripts.history_ingest.statsapi", stub_statsapi):
                args.func(args)
                args.func(args)

            with connect_db(str(db_path)) as conn:
                rows = conn.execute(
                    """
                    SELECT game_id, side, probable_pitcher_id, probable_pitcher_name,
                           season_era, season_whip, season_avg_allowed, season_runs_per_9,
                           season_strike_pct, season_win_pct, career_era,
                           stats_source, stats_as_of_date, season_stats_scope, season_stats_leakage_risk
                    FROM game_pitcher_context
                    ORDER BY game_id, side
                    """
                ).fetchall()
                row_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM game_pitcher_context"
                ).fetchone()["c"]
                checkpoint = conn.execute(
                    """
                    SELECT status, attempts, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='pitcher-context-2021' AND partition_key='season=2021'
                    """
                ).fetchone()

            self.assertEqual(row_count, 4)
            self.assertEqual(len(rows), 4)
            first_game_home = next(row for row in rows if row["game_id"] == 3001 and row["side"] == "home")
            second_game_home = next(row for row in rows if row["game_id"] == 3002 and row["side"] == "home")
            second_game_away = next(row for row in rows if row["game_id"] == 3002 and row["side"] == "away")

            self.assertIsNone(first_game_home["season_era"])
            self.assertEqual(second_game_home["season_era"], 3.0)
            self.assertEqual(second_game_home["season_whip"], 1.0)
            self.assertEqual(second_game_home["season_avg_allowed"], 0.167)
            self.assertEqual(second_game_home["season_runs_per_9"], 3.0)
            self.assertEqual(second_game_home["season_strike_pct"], 0.667)
            self.assertEqual(second_game_home["season_win_pct"], 1.0)
            self.assertIsNone(second_game_home["career_era"])
            self.assertEqual(second_game_home["stats_as_of_date"], "2021-08-05")
            self.assertEqual(second_game_home["season_stats_scope"], "season_to_date_prior_completed_games")
            self.assertEqual(second_game_home["season_stats_leakage_risk"], 0)
            self.assertIn("prior_completed_games_only", second_game_home["stats_source"])
            self.assertEqual(second_game_away["season_era"], 7.2)
            self.assertEqual(second_game_away["season_whip"], 1.6)
            self.assertEqual(second_game_away["season_avg_allowed"], 0.318)
            self.assertEqual(second_game_away["season_runs_per_9"], 7.2)
            self.assertEqual(second_game_away["season_strike_pct"], 0.625)
            self.assertEqual(second_game_away["season_win_pct"], 0.0)
            self.assertEqual(checkpoint["status"], "success")
            self.assertGreaterEqual(checkpoint["attempts"], 2)
            checkpoint_cursor = json.loads(checkpoint["cursor_json"])
            self.assertEqual(checkpoint_cursor["season"], season)
            self.assertEqual(checkpoint_cursor["games_seen"], 2)
            self.assertEqual(checkpoint_cursor["rows_upserted"], 4)

    def test_backfill_pitcher_context_for_season_falls_back_to_existing_identity_without_leakage(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 3010,
                        "season": season,
                        "game_date": "2021-08-06",
                        "status": "Final",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                )
                for side, pitcher_id, pitcher_name in (("home", 610, "Fallback Home"), ("away", 611, "Fallback Away")):
                    upsert_game_pitcher_context(
                        conn,
                        {
                            "game_id": 3010,
                            "side": side,
                            "pitcher_id": pitcher_id,
                            "pitcher_name": pitcher_name,
                            "probable_pitcher_id": pitcher_id,
                            "probable_pitcher_name": pitcher_name,
                            "probable_pitcher_known": 1,
                            "season_era": 4.5,
                            "season_whip": 1.3,
                            "season_stats_scope": "full_season_year_aggregate",
                            "season_stats_leakage_risk": 1,
                            "stats_source": "statsapi.player_stat_data(type=yearByYear,career)+lookup_player",
                        },
                    )

            parser = build_parser()
            args = parser.parse_args(["--db", str(db_path), "backfill-pitcher-context", "--season", str(season)])

            with patch("scripts.history_ingest.statsapi", None):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                rows = conn.execute(
                    """
                    SELECT side, probable_pitcher_id, probable_pitcher_name, probable_pitcher_known,
                           season_era, season_whip, season_stats_scope, season_stats_leakage_risk, stats_source
                    FROM game_pitcher_context
                    WHERE game_id = 3010
                    ORDER BY side
                    """
                ).fetchall()

            self.assertEqual(len(rows), 2)
            for row in rows:
                self.assertEqual(row["probable_pitcher_known"], 1)
                self.assertIn(row["probable_pitcher_name"], {"Fallback Home", "Fallback Away"})
                self.assertIsNone(row["season_era"])
                self.assertIsNone(row["season_whip"])
                self.assertEqual(row["season_stats_scope"], "season_to_date_prior_completed_games")
                self.assertEqual(row["season_stats_leakage_risk"], 0)
                self.assertIn("leakage_safe_null_fallback", row["stats_source"])

    def test_backfill_pitcher_context_legacy_alias_defaults_to_2020(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["backfill-pitcher-context-2020"])
        self.assertEqual(args.season, 2020)

    def test_backfill_ingests_bounded_schedule_and_labels_idempotently(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "--checkpoint-every",
                    "1",
                    "backfill",
                    "--season",
                    "2024",
                ]
            )
            schedule_rows = [
                {
                    "game_id": 1001,
                    "season": 2024,
                    "game_date": "2024-04-01",
                    "game_type": "R",
                    "status": "Final",
                    "game_datetime": "2024-04-01T23:05:00Z",
                    "home_id": 147,
                    "away_id": 121,
                    "home_score": 6,
                    "away_score": 3,
                },
                {
                    "game_id": 1002,
                    "season": 2024,
                    "game_date": "2024-04-02",
                    "game_type": "R",
                    "status": "Scheduled",
                    "game_datetime": "2024-04-02T23:05:00Z",
                    "home_id": 147,
                    "away_id": 121,
                },
                {
                    "game_id": 1003,
                    "season": 2024,
                    "game_date": "2024-03-10",
                    "game_type": "S",
                    "status": "Final",
                    "game_datetime": "2024-03-10T18:05:00Z",
                    "home_id": 147,
                    "away_id": 121,
                    "home_score": 2,
                    "away_score": 1,
                },
            ]
            stub_statsapi = types.SimpleNamespace(schedule=lambda **_kwargs: schedule_rows)
            with patch("scripts.history_ingest.statsapi", stub_statsapi):
                args.func(args)
                args.func(args)

            with connect_db(str(db_path)) as conn:
                game_count = conn.execute("SELECT COUNT(*) AS c FROM games").fetchone()["c"]
                label_count = conn.execute("SELECT COUNT(*) AS c FROM labels").fetchone()["c"]
                run_count = conn.execute("SELECT COUNT(*) AS c FROM ingestion_runs WHERE mode='backfill'").fetchone()["c"]
                run_rows = conn.execute(
                    "SELECT note, request_count FROM ingestion_runs WHERE mode='backfill' ORDER BY started_at"
                ).fetchall()
                label_row = conn.execute(
                    "SELECT did_home_win, run_differential, total_runs FROM labels WHERE game_id=1001"
                ).fetchone()
                checkpoint_row = conn.execute(
                    """
                    SELECT status, last_game_id, attempts, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='backfill' AND partition_key='season=2024'
                    """
                ).fetchone()
                request_counts = [row["request_count"] for row in run_rows]
                run_notes = [json.loads(row["note"]) for row in run_rows]
                checkpoint_cursor = json.loads(checkpoint_row["cursor_json"])

            self.assertEqual(game_count, 2)
            self.assertEqual(label_count, 1)
            self.assertEqual(run_count, 2)
            self.assertEqual(label_row["did_home_win"], 1)
            self.assertEqual(label_row["run_differential"], 3)
            self.assertEqual(label_row["total_runs"], 9)
            self.assertEqual(checkpoint_row["status"], "success")
            self.assertEqual(checkpoint_row["last_game_id"], 1002)
            self.assertGreaterEqual(checkpoint_row["attempts"], 2)
            self.assertEqual(request_counts, [1, 1])
            for run_note in run_notes:
                self.assertEqual(run_note["schedule_rows_fetched"], 3)
                self.assertEqual(run_note["relevant_rows_processed"], 2)
                self.assertEqual(run_note["distinct_games_touched"], 2)
                self.assertEqual(run_note["games_inserted"] + run_note["games_updated"], 2)
                self.assertEqual(run_note["labels_inserted"] + run_note["labels_updated"], 1)
                self.assertEqual(run_note["final_distinct_counts_snapshot"], {"games": 2, "labels": 1})
            self.assertEqual(run_notes[0]["games_inserted"], 2)
            self.assertEqual(run_notes[0]["games_updated"], 0)
            self.assertEqual(run_notes[0]["labels_inserted"], 1)
            self.assertEqual(run_notes[0]["labels_updated"], 0)
            self.assertEqual(run_notes[1]["games_inserted"], 0)
            self.assertEqual(run_notes[1]["games_updated"], 2)
            self.assertEqual(run_notes[1]["labels_inserted"], 0)
            self.assertEqual(run_notes[1]["labels_updated"], 1)
            self.assertEqual(checkpoint_cursor["schedule_rows_fetched"], 3)
            self.assertEqual(checkpoint_cursor["relevant_rows_processed"], 2)
            self.assertEqual(checkpoint_cursor["distinct_games_touched"], 2)
            self.assertEqual(checkpoint_cursor["games_inserted"], 0)
            self.assertEqual(checkpoint_cursor["games_updated"], 2)
            self.assertEqual(checkpoint_cursor["labels_inserted"], 0)
            self.assertEqual(checkpoint_cursor["labels_updated"], 1)
            self.assertEqual(checkpoint_cursor["final_distinct_counts_snapshot"], {"games": 2, "labels": 1})

    def test_incremental_one_day_schedule_ingest(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "--checkpoint-every",
                    "1",
                    "incremental",
                    "--date",
                    "2026-03-09",
                ]
            )
            schedule_rows = [
                {
                    "game_id": 2001,
                    "season": 2026,
                    "game_date": "2026-03-09",
                    "game_type": "R",
                    "status": "Final",
                    "home_id": 147,
                    "away_id": 121,
                    "home_score": 4,
                    "away_score": 5,
                }
            ]
            captured_kwargs = {}

            def fake_schedule(**kwargs):
                captured_kwargs.update(kwargs)
                return schedule_rows

            stub_statsapi = types.SimpleNamespace(schedule=fake_schedule)
            with patch("scripts.history_ingest.statsapi", stub_statsapi):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                run = conn.execute(
                    "SELECT status, request_count, note FROM ingestion_runs WHERE mode='incremental' ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                label = conn.execute(
                    "SELECT did_home_win, run_differential, total_runs FROM labels WHERE game_id=2001"
                ).fetchone()
                checkpoint = conn.execute(
                    """
                    SELECT status, partition_key, last_game_id, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='incremental' AND partition_key='date=2026-03-09'
                    """
                ).fetchone()
                run_note = json.loads(run["note"])
                checkpoint_cursor = json.loads(checkpoint["cursor_json"])

            self.assertEqual(captured_kwargs["start_date"], "2026-03-09")
            self.assertEqual(captured_kwargs["end_date"], "2026-03-09")
            self.assertEqual(run["status"], "success")
            self.assertEqual(run["request_count"], 1)
            self.assertEqual(label["did_home_win"], 0)
            self.assertEqual(label["run_differential"], -1)
            self.assertEqual(label["total_runs"], 9)
            self.assertEqual(checkpoint["status"], "success")
            self.assertEqual(checkpoint["partition_key"], "date=2026-03-09")
            self.assertEqual(checkpoint["last_game_id"], 2001)
            self.assertEqual(run_note["schedule_rows_fetched"], 1)
            self.assertEqual(run_note["relevant_rows_processed"], 1)
            self.assertEqual(run_note["distinct_games_touched"], 1)
            self.assertEqual(run_note["games_inserted"], 1)
            self.assertEqual(run_note["games_updated"], 0)
            self.assertEqual(run_note["labels_inserted"], 1)
            self.assertEqual(run_note["labels_updated"], 0)
            self.assertEqual(run_note["final_distinct_counts_snapshot"], {"games": 1, "labels": 1})
            self.assertEqual(checkpoint_cursor["schedule_rows_fetched"], 1)
            self.assertEqual(checkpoint_cursor["distinct_games_touched"], 1)
            self.assertEqual(checkpoint_cursor["games_inserted"], 1)
            self.assertEqual(checkpoint_cursor["labels_inserted"], 1)
            self.assertEqual(checkpoint_cursor["final_distinct_counts_snapshot"], {"games": 1, "labels": 1})

    def test_backfill_checkpoint_failed_on_schedule_error(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "--max-attempts",
                    "2",
                    "--initial-backoff-seconds",
                    "0",
                    "--max-backoff-seconds",
                    "0",
                    "--jitter-seconds",
                    "0",
                    "backfill",
                    "--season",
                    "2024",
                ]
            )
            stub_statsapi = types.SimpleNamespace(schedule=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
            with patch("scripts.history_ingest.statsapi", stub_statsapi):
                with patch("scripts.history_ingest.time.sleep"):
                    with self.assertRaises(RuntimeError):
                        args.func(args)

            with connect_db(str(db_path)) as conn:
                run = conn.execute(
                    "SELECT status, request_count, note FROM ingestion_runs WHERE mode='backfill' ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                checkpoint = conn.execute(
                    """
                    SELECT status, last_error
                    FROM ingestion_checkpoints
                    WHERE job_name='backfill' AND partition_key='season=2024'
                    """
                ).fetchone()

            self.assertEqual(run["status"], "failed")
            self.assertEqual(run["request_count"], 2)
            self.assertIn("boom", run["note"])
            self.assertEqual(checkpoint["status"], "failed")
            self.assertIn("boom", checkpoint["last_error"])

    def test_team_stats_mapping_extracts_required_fields(self) -> None:
        row = _team_stats_row_from_boxscore(
            999,
            "home",
            {
                "home": {
                    "team": {"id": 147},
                    "teamStats": {
                        "batting": {
                            "runs": 5,
                            "hits": 10,
                            "avg": ".278",
                            "obp": ".345",
                            "slg": ".456",
                            "ops": ".801",
                            "strikeOuts": 7,
                            "baseOnBalls": 4,
                        },
                        "fielding": {"errors": 1},
                    },
                }
            },
        )
        assert row is not None
        self.assertEqual(row["game_id"], 999)
        self.assertEqual(row["team_id"], 147)
        self.assertEqual(row["runs"], 5)
        self.assertEqual(row["hits"], 10)
        self.assertEqual(row["errors"], 1)
        self.assertAlmostEqual(row["batting_avg"], 0.278)
        self.assertAlmostEqual(row["obp"], 0.345)
        self.assertAlmostEqual(row["slg"], 0.456)
        self.assertAlmostEqual(row["ops"], 0.801)
        self.assertEqual(row["strikeouts"], 7)
        self.assertEqual(row["walks"], 4)

    def test_backfill_team_stats_for_season_idempotent(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            parser = build_parser()
            args = parser.parse_args(
                ["--db", str(db_path), "--checkpoint-every", "1", "backfill-team-stats", "--season", str(season)]
            )

            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 4001,
                        "season": season,
                        "game_date": "2021-07-24",
                        "status": "Final",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                )

            def fake_boxscore(_game_id: int):
                return {
                    "home": {
                        "team": {"id": 147},
                        "teamStats": {
                            "batting": {
                                "runs": 6,
                                "hits": 11,
                                "avg": ".280",
                                "obp": ".350",
                                "slg": ".470",
                                "ops": ".820",
                                "strikeOuts": 8,
                                "baseOnBalls": 5,
                            },
                            "fielding": {"errors": 0},
                        },
                    },
                    "away": {
                        "team": {"id": 121},
                        "teamStats": {
                            "batting": {
                                "runs": 3,
                                "hits": 7,
                                "avg": ".233",
                                "obp": ".300",
                                "slg": ".390",
                                "ops": ".690",
                                "strikeOuts": 10,
                                "baseOnBalls": 2,
                            },
                            "fielding": {"errors": 1},
                        },
                    },
                }

            stub_statsapi = types.SimpleNamespace(boxscore_data=fake_boxscore)
            with patch("scripts.history_ingest.statsapi", stub_statsapi):
                args.func(args)
                args.func(args)

            with connect_db(str(db_path)) as conn:
                row_count = conn.execute("SELECT COUNT(*) AS c FROM game_team_stats WHERE game_id=4001").fetchone()["c"]
                home_row = conn.execute(
                    "SELECT runs, hits, batting_avg, obp, slg, ops, strikeouts, walks FROM game_team_stats WHERE game_id=4001 AND side='home'"
                ).fetchone()
                away_row = conn.execute(
                    "SELECT runs, hits, batting_avg, obp, slg, ops, strikeouts, walks FROM game_team_stats WHERE game_id=4001 AND side='away'"
                ).fetchone()
                runs = conn.execute(
                    "SELECT note, request_count FROM ingestion_runs WHERE mode='backfill' AND partition_key='team-stats-season=2021' ORDER BY started_at"
                ).fetchall()
                checkpoint = conn.execute(
                    "SELECT status, attempts, cursor_json FROM ingestion_checkpoints WHERE job_name='team-stats-backfill' AND partition_key='team-stats-season=2021'"
                ).fetchone()

            self.assertEqual(row_count, 2)
            self.assertEqual(home_row["runs"], 6)
            self.assertEqual(away_row["runs"], 3)
            self.assertEqual(len(runs), 2)
            self.assertEqual(runs[0]["request_count"], 1)
            self.assertEqual(runs[1]["request_count"], 1)
            first_note = json.loads(runs[0]["note"])
            second_note = json.loads(runs[1]["note"])
            self.assertEqual(first_note["rows_inserted"], 2)
            self.assertEqual(first_note["rows_updated"], 0)
            self.assertEqual(second_note["rows_inserted"], 0)
            self.assertEqual(second_note["rows_updated"], 2)
            self.assertEqual(checkpoint["status"], "success")
            self.assertGreaterEqual(checkpoint["attempts"], 2)

    def test_materialize_feature_rows_v1_for_season_is_idempotent(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            parser = build_parser()
            args = parser.parse_args(
                ["--db", str(db_path), "--checkpoint-every", "1", "materialize-feature-rows", "--season", str(season)]
            )

            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game_id, game_date, home_score, away_score in (
                    (5001, "2021-07-24", 5, 3),
                    (5002, "2021-07-25", 4, 2),
                ):
                    upsert_game(
                        conn,
                        {
                            "game_id": game_id,
                            "season": season,
                            "game_date": game_date,
                            "scheduled_datetime": f"{game_date}T23:05:00Z",
                            "status": "Final",
                            "home_team_id": 147,
                            "away_team_id": 121,
                            "home_score": home_score,
                            "away_score": away_score,
                            "winning_team_id": 147,
                        },
                    )
                conn.execute(
                    """
                    INSERT INTO feature_rows (
                      game_id, feature_version, as_of_ts, feature_payload_json, source_contract_status
                    )
                    VALUES (5002, 'v1', '2021-07-25T00:00:00Z', '{}', 'degraded')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO labels (game_id, did_home_win, home_score, away_score, run_differential, total_runs)
                    VALUES
                      (5001, 1, 5, 3, 2, 8),
                      (5002, 1, 4, 2, 2, 6)
                    """
                )
                for payload in (
                    {"game_id": 5001, "team_id": 147, "side": "home", "hits": 9, "batting_avg": 0.281, "obp": 0.340, "ops": 0.790},
                    {"game_id": 5001, "team_id": 121, "side": "away", "hits": 7, "batting_avg": 0.245, "obp": 0.300, "ops": 0.680},
                    {"game_id": 5002, "team_id": 147, "side": "home", "hits": 8, "batting_avg": 0.260, "obp": 0.330, "ops": 0.760},
                    {"game_id": 5002, "team_id": 121, "side": "away", "hits": 6, "batting_avg": 0.230, "obp": 0.295, "ops": 0.650},
                ):
                    upsert_game_team_stats(conn, payload)
                for side, pitcher_id, era, whip in (
                    ("home", 501, None, None),
                    ("away", 502, None, None),
                ):
                    upsert_game_pitcher_context(
                        conn,
                        {
                            "game_id": 5001,
                            "side": side,
                            "probable_pitcher_id": pitcher_id,
                            "probable_pitcher_name": f"{side} starter 1",
                            "probable_pitcher_known": 1,
                            "season_era": era,
                            "season_whip": whip,
                            "season_stats_scope": "season_to_date_prior_completed_games",
                            "season_stats_leakage_risk": 0,
                        },
                    )
                for side, pitcher_id, era, whip in (
                    ("home", 501, 3.0, 1.0),
                    ("away", 502, 7.2, 1.6),
                ):
                    upsert_game_pitcher_context(
                        conn,
                        {
                            "game_id": 5002,
                            "side": side,
                            "probable_pitcher_id": pitcher_id,
                            "probable_pitcher_name": f"{side} starter 2",
                            "probable_pitcher_known": 1,
                            "season_era": era,
                            "season_whip": whip,
                            "season_avg_allowed": 0.25,
                            "season_runs_per_9": era,
                            "season_strike_pct": 0.66,
                            "season_win_pct": 1.0 if side == "home" else 0.0,
                            "season_stats_scope": "season_to_date_prior_completed_games",
                            "season_stats_leakage_risk": 0,
                        },
                    )

            args.func(args)
            args.func(args)

            with connect_db(str(db_path)) as conn:
                row_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM feature_rows WHERE feature_version='v1'"
                ).fetchone()["c"]
                second_game = conn.execute(
                    """
                    SELECT as_of_ts, feature_payload_json, source_contract_status
                    FROM feature_rows
                    WHERE game_id=5002 AND feature_version='v1'
                    """
                ).fetchone()
                first_game = conn.execute(
                    """
                    SELECT source_contract_status, source_contract_issues_json
                    FROM feature_rows
                    WHERE game_id=5001 AND feature_version='v1'
                    """
                ).fetchone()
                checkpoint = conn.execute(
                    """
                    SELECT status, attempts, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='feature-rows-v1-2021' AND partition_key='feature-rows-season=2021:version=v1'
                    """
                ).fetchone()
                run_notes = conn.execute(
                    """
                    SELECT note
                    FROM ingestion_runs
                    WHERE partition_key='feature-rows-season=2021:version=v1'
                    ORDER BY started_at
                    """
                ).fetchall()

            payload = json.loads(second_game["feature_payload_json"])
            self.assertEqual(row_count, 2)
            self.assertEqual(second_game["as_of_ts"], "2021-07-25T23:05:00Z")
            self.assertEqual(second_game["source_contract_status"], "valid")
            self.assertEqual(first_game["source_contract_status"], "degraded")
            self.assertEqual(json.loads(first_game["source_contract_issues_json"]), ["away_starter_stats_unavailable", "home_starter_stats_unavailable"])
            self.assertEqual(payload["home_team_strength_available"], 1)
            self.assertEqual(payload["home_team_season_games"], 1)
            self.assertEqual(payload["home_team_season_win_pct"], 1.0)
            self.assertEqual(payload["home_team_rolling_last10_hits_per_game"], 9.0)
            self.assertEqual(payload["away_team_season_run_diff_per_game"], -2.0)
            self.assertEqual(payload["home_starter_stats_available"], 1)
            self.assertEqual(payload["home_starter_era"], 3.0)
            self.assertEqual(payload["away_starter_whip"], 1.6)
            self.assertEqual(checkpoint["status"], "success")
            self.assertGreaterEqual(checkpoint["attempts"], 2)
            self.assertEqual(json.loads(checkpoint["cursor_json"])["rows_upserted"], 2)
            self.assertEqual(len(run_notes), 2)


if __name__ == "__main__":
    unittest.main()
