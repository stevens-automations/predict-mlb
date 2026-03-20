from __future__ import annotations

import json
import types
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.history_ingest import (
    DEFAULT_MAX_NULL_SAFE_FALLBACK_SHARE,
    DEFAULT_DB_PATH,
    _derive_day_night,
    _parse_statsapi_schedule_payload,
    _parse_statsapi_venue_payload,
    _select_open_meteo_hour,
    RequestBudget,
    RequestPolicy,
    build_support_coverage_report,
    build_pitcher_context_quality_report,
    build_game_pitcher_appearance_rows,
    build_game_lineup_snapshot_rows,
    _team_stats_row_from_boxscore,
    build_parser,
    connect_db,
    enforce_canonical_write_guard,
    ensure_schema,
    fetch_statsapi_venue_details_bounded,
    game_row_from_schedule,
    upsert_checkpoint,
    upsert_game,
    upsert_game_pitcher_appearance,
    upsert_game_pitcher_context,
    upsert_game_weather_snapshot,
    upsert_game_team_stats,
    upsert_team_bullpen_game_state,
    upsert_team_bullpen_top_relievers,
    upsert_team_lineup_game_state,
    upsert_team_platoon_split,
    upsert_venue_dim,
    upsert_player_handedness,
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
                    "venue_dim",
                    "game_weather_snapshots",
                    "game_team_stats",
                    "game_pitcher_context",
                    "game_pitcher_appearances",
                    "player_handedness_dim",
                    "game_lineup_snapshots",
                    "team_bullpen_game_state",
                    "team_bullpen_top_relievers",
                    "team_lineup_game_state",
                    "team_platoon_splits",
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

                game_cols = {row["name"] for row in conn.execute("PRAGMA table_info(games)").fetchall()}
                self.assertIn("venue_id", game_cols)
                self.assertIn("day_night", game_cols)

                venue_cols = {row["name"] for row in conn.execute("PRAGMA table_info(venue_dim)").fetchall()}
                self.assertIn("timezone", venue_cols)
                self.assertIn("roof_type", venue_cols)
                self.assertIn("weather_exposure_default", venue_cols)

                weather_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(game_weather_snapshots)").fetchall()
                }
                self.assertIn("target_game_ts", weather_cols)
                self.assertIn("snapshot_type", weather_cols)
                self.assertIn("weather_exposure_flag", weather_cols)

                appearance_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(game_pitcher_appearances)").fetchall()
                }
                self.assertIn("appearance_order", appearance_cols)
                self.assertIn("is_starter", appearance_cols)
                self.assertIn("batters_faced", appearance_cols)

                bullpen_state_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_bullpen_game_state)").fetchall()
                }
                self.assertIn("freshness_method", bullpen_state_cols)
                self.assertIn("bullpen_k_minus_bb_rate_season", bullpen_state_cols)
                self.assertIn("high_usage_relievers_last3d_count", bullpen_state_cols)

                bullpen_top_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_bullpen_top_relievers)").fetchall()
                }
                self.assertIn("ranking_method", bullpen_top_cols)
                self.assertIn("selected_pitcher_ids_json", bullpen_top_cols)
                self.assertIn("quality_dropoff_vs_team", bullpen_top_cols)
                lineup_snapshot_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(game_lineup_snapshots)").fetchall()
                }
                self.assertIn("snapshot_type", lineup_snapshot_cols)
                self.assertIn("bat_side", lineup_snapshot_cols)

                lineup_state_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_lineup_game_state)").fetchall()
                }
                self.assertIn("lineup_quality_metric", lineup_state_cols)
                self.assertIn("lineup_vs_rhp_quality", lineup_state_cols)

                platoon_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_platoon_splits)").fetchall()
                }
                self.assertIn("stats_scope", platoon_cols)
                self.assertIn("plate_appearances", platoon_cols)

                handedness_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(player_handedness_dim)").fetchall()
                }
                self.assertIn("bat_side", handedness_cols)
                self.assertIn("pitch_hand", handedness_cols)

                lineup_snapshot_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(game_lineup_snapshots)").fetchall()
                }
                self.assertIn("snapshot_type", lineup_snapshot_cols)
                self.assertIn("bat_side", lineup_snapshot_cols)

                lineup_state_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_lineup_game_state)").fetchall()
                }
                self.assertIn("lineup_quality_metric", lineup_state_cols)
                self.assertIn("lineup_vs_rhp_quality", lineup_state_cols)

                platoon_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_platoon_splits)").fetchall()
                }
                self.assertIn("stats_scope", platoon_cols)
                self.assertIn("plate_appearances", platoon_cols)

    def test_upsert_game_pitcher_context_preserves_richer_existing_stats_against_null_safe_fallback(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 99,
                        "season": 2024,
                        "game_date": "2024-06-11",
                        "status": "Final",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                )
                upsert_game_pitcher_context(
                    conn,
                    {
                        "game_id": 99,
                        "side": "home",
                        "pitcher_id": 501,
                        "pitcher_name": "Rich Starter",
                        "probable_pitcher_id": 501,
                        "probable_pitcher_name": "Rich Starter",
                        "probable_pitcher_known": 1,
                        "season_era": 3.1,
                        "season_whip": 1.07,
                        "season_avg_allowed": 0.231,
                        "season_runs_per_9": 3.1,
                        "season_strike_pct": 0.664,
                        "season_win_pct": 0.625,
                        "stats_source": "statsapi.schedule+statsapi.lookup_player+statsapi.boxscore_data(prior_completed_games_only)",
                        "stats_as_of_date": "2024-06-10",
                        "season_stats_scope": "season_to_date_prior_completed_games",
                        "season_stats_leakage_risk": 0,
                    },
                )
                upsert_game_pitcher_context(
                    conn,
                    {
                        "game_id": 99,
                        "side": "home",
                        "pitcher_id": 501,
                        "pitcher_name": "Rich Starter",
                        "probable_pitcher_id": 501,
                        "probable_pitcher_name": "Rich Starter",
                        "probable_pitcher_known": 1,
                        "season_era": None,
                        "season_whip": None,
                        "season_avg_allowed": None,
                        "season_runs_per_9": None,
                        "season_strike_pct": None,
                        "season_win_pct": None,
                        "stats_source": "leakage_safe_null_fallback(probable_pitcher_identity_without_prior_completed_pitching)",
                        "stats_as_of_date": "2024-06-11",
                        "season_stats_scope": "season_to_date_prior_completed_games",
                        "season_stats_leakage_risk": 0,
                    },
                )
                row = conn.execute(
                    """
                    SELECT season_era, season_whip, season_avg_allowed, season_runs_per_9,
                           season_strike_pct, season_win_pct, stats_source, stats_as_of_date
                    FROM game_pitcher_context
                    WHERE game_id = 99 AND side = 'home'
                    """
                ).fetchone()

            self.assertEqual(row["season_era"], 3.1)
            self.assertEqual(row["season_whip"], 1.07)
            self.assertEqual(row["season_avg_allowed"], 0.231)
            self.assertEqual(row["season_runs_per_9"], 3.1)
            self.assertEqual(row["season_strike_pct"], 0.664)
            self.assertEqual(row["season_win_pct"], 0.625)
            self.assertIn("prior_completed_games_only", row["stats_source"])
            self.assertEqual(row["stats_as_of_date"], "2024-06-10")

    def test_game_row_from_schedule_keeps_venue_id_and_day_night(self) -> None:
        row = game_row_from_schedule(
            {
                "game_id": 101,
                "season": 2024,
                "game_date": "2024-06-01",
                "game_datetime": "2024-06-01T23:10:00Z",
                "status": "Scheduled",
                "venue_id": 5001,
                "dayNight": "night",
                "home_id": 147,
                "away_id": 121,
            }
        )
        assert row is not None
        self.assertEqual(row["venue_id"], 5001)
        self.assertEqual(row["day_night"], "night")

    def test_parse_statsapi_schedule_payload_keeps_day_night_and_venue_context(self) -> None:
        rows = _parse_statsapi_schedule_payload(
            {
                "dates": [
                    {
                        "date": "2024-06-01",
                        "games": [
                            {
                                "gamePk": 101,
                                "gameDate": "2024-06-01T23:10:00Z",
                                "season": "2024",
                                "gameType": "R",
                                "dayNight": "night",
                                "status": {"detailedState": "Scheduled"},
                                "venue": {
                                    "id": 5001,
                                    "name": "Example Park",
                                    "location": {
                                        "city": "Example City",
                                        "stateAbbrev": "EX",
                                        "country": "USA",
                                        "defaultCoordinates": {"latitude": 40.0, "longitude": -73.0},
                                        "timeZone": {"id": "America/New_York"},
                                    },
                                },
                                "teams": {
                                    "away": {
                                        "team": {"id": 121, "name": "Away"},
                                        "probablePitcher": {"id": 301, "fullName": "Away Pitcher"},
                                    },
                                    "home": {
                                        "team": {"id": 147, "name": "Home"},
                                        "probablePitcher": {"id": 302, "fullName": "Home Pitcher", "note": "RHP"},
                                    },
                                },
                            }
                        ],
                    }
                ]
            }
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["venue_id"], 5001)
        self.assertEqual(row["dayNight"], "night")
        self.assertEqual(row["home_probable_pitcher_id"], 302)
        self.assertEqual(row["away_probable_pitcher_id"], 301)
        self.assertEqual(row["venue"]["location"]["timeZone"]["id"], "America/New_York")

    def test_upsert_game_preserves_existing_non_null_venue_and_day_night(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 9001,
                        "season": 2024,
                        "game_date": "2024-06-01",
                        "scheduled_datetime": "2024-06-01T23:10:00Z",
                        "status": "Scheduled",
                        "venue_id": 5001,
                        "day_night": "night",
                    },
                )
                upsert_game(
                    conn,
                    {
                        "game_id": 9001,
                        "season": 2024,
                        "game_date": "2024-06-01",
                        "scheduled_datetime": "2024-06-01T23:10:00Z",
                        "status": "Final",
                        "venue_id": None,
                        "day_night": None,
                    },
                )
                row = conn.execute("SELECT venue_id, day_night, status FROM games WHERE game_id = 9001").fetchone()

            self.assertEqual(row["venue_id"], 5001)
            self.assertEqual(row["day_night"], "night")
            self.assertEqual(row["status"], "Final")

    def test_select_open_meteo_hour_picks_nearest_local_hour(self) -> None:
        selected = _select_open_meteo_hour(
            {
                "hourly": {
                    "time": ["2024-06-01T18:00", "2024-06-01T19:00", "2024-06-01T20:00"],
                    "temperature_2m": [71.0, 74.0, 77.0],
                    "relative_humidity_2m": [40, 45, 50],
                    "surface_pressure": [1015, 1014, 1013],
                    "precipitation": [0.0, 0.1, 0.3],
                    "precipitation_probability": [5, 10, 20],
                    "wind_speed_10m": [7.0, 9.0, 12.0],
                    "wind_gusts_10m": [9.0, 13.0, 17.0],
                    "wind_direction_10m": [180, 190, 200],
                    "weather_code": [0, 1, 2],
                    "cloud_cover": [5, 10, 15],
                    "is_day": [1, 1, 0],
                }
            },
            datetime.fromisoformat("2024-06-01T19:10:00+00:00"),
        )
        assert selected is not None
        self.assertEqual(selected["temperature_2m"], 74.0)
        self.assertAlmostEqual(selected["hour_offset_from_first_pitch"], -0.167, places=3)

    def test_parse_statsapi_venue_payload_extracts_required_fields(self) -> None:
        parsed = _parse_statsapi_venue_payload(
            {
                "venues": [
                    {
                        "id": 5001,
                        "name": "Example Park",
                        "location": {
                            "city": "Example City",
                            "stateAbbrev": "EX",
                            "country": "USA",
                            "defaultCoordinates": {"latitude": 40.0, "longitude": -73.0},
                            "timeZone": {"id": "America/New_York"},
                        },
                    }
                ]
            }
        )
        assert parsed is not None
        self.assertEqual(parsed["venue_id"], 5001)
        self.assertEqual(parsed["timezone"], "America/New_York")
        self.assertEqual(parsed["roof_type"], "unknown")

    def test_parse_statsapi_venue_payload_accepts_top_level_timezone_and_field_info(self) -> None:
        parsed = _parse_statsapi_venue_payload(
            {
                "venues": [
                    {
                        "id": 15,
                        "name": "Chase Field",
                        "location": {
                            "city": "Phoenix",
                            "stateAbbrev": "AZ",
                            "country": "USA",
                            "defaultCoordinates": {"latitude": 33.4453, "longitude": -112.0667},
                        },
                        "timeZone": {"id": "America/Phoenix"},
                        "fieldInfo": {"roofType": "Retractable"},
                    }
                ]
            }
        )
        assert parsed is not None
        self.assertEqual(parsed["venue_id"], 15)
        self.assertEqual(parsed["timezone"], "America/Phoenix")
        self.assertEqual(parsed["roof_type"], "retractable")

    def test_fetch_statsapi_venue_details_uses_hydrated_endpoint(self) -> None:
        captured: dict[str, str] = {}

        def fake_fetch(url: str, policy: RequestPolicy, budget: RequestBudget) -> dict[str, object]:
            captured["url"] = url
            return {
                "venues": [
                    {
                        "id": 15,
                        "name": "Chase Field",
                        "location": {
                            "city": "Phoenix",
                            "stateAbbrev": "AZ",
                            "country": "USA",
                            "defaultCoordinates": {"latitude": 33.4453, "longitude": -112.0667},
                        },
                        "timeZone": {"id": "America/Phoenix"},
                        "fieldInfo": {"roofType": "Retractable"},
                    }
                ]
            }

        with patch("scripts.history_ingest.fetch_json_url_bounded", side_effect=fake_fetch):
            parsed = fetch_statsapi_venue_details_bounded(15, RequestPolicy(), RequestBudget(limit=10))

        assert parsed is not None
        self.assertEqual(parsed["timezone"], "America/Phoenix")
        self.assertEqual(
            captured["url"],
            "https://statsapi.mlb.com/api/v1/venues/15?hydrate=location%2CfieldInfo%2Ctimezone",
        )

    def test_derive_day_night_prefers_game_value_before_weather_or_clock(self) -> None:
        value, source = _derive_day_night("day", 0, types.SimpleNamespace(hour=22))
        self.assertEqual(value, "day")
        self.assertEqual(source, "games.day_night")

    def test_schema_migrations_add_pitcher_appearance_columns_idempotently(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                conn.executescript(
                    """
                    CREATE TABLE game_pitcher_appearances (
                      game_id INTEGER NOT NULL,
                      team_id INTEGER NOT NULL,
                      side TEXT NOT NULL,
                      pitcher_id INTEGER NOT NULL,
                      ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
                      PRIMARY KEY (game_id, pitcher_id)
                    );
                    """
                )
                conn.commit()

                ensure_schema(conn)
                ensure_schema(conn)

                appearance_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(game_pitcher_appearances)").fetchall()
                }

            self.assertIn("pitcher_name", appearance_cols)
            self.assertIn("appearance_order", appearance_cols)
            self.assertIn("is_starter", appearance_cols)
            self.assertIn("is_reliever", appearance_cols)
            self.assertIn("earned_runs", appearance_cols)
            self.assertIn("source_updated_at", appearance_cols)

    def test_schema_migrations_add_games_venue_columns_idempotently(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                conn.executescript(
                    """
                    CREATE TABLE games (
                      game_id INTEGER PRIMARY KEY,
                      season INTEGER NOT NULL,
                      game_date TEXT NOT NULL,
                      status TEXT,
                      ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
                    );
                    """
                )
                conn.commit()

                ensure_schema(conn)
                ensure_schema(conn)

                game_cols = {row["name"] for row in conn.execute("PRAGMA table_info(games)").fetchall()}

            self.assertIn("venue_id", game_cols)
            self.assertIn("day_night", game_cols)

    def test_schema_migrations_add_bullpen_support_columns_idempotently(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                conn.executescript(
                    """
                    CREATE TABLE team_bullpen_game_state (
                      game_id INTEGER NOT NULL,
                      team_id INTEGER NOT NULL,
                      side TEXT NOT NULL,
                      as_of_ts TEXT NOT NULL,
                      ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
                      PRIMARY KEY (game_id, side, as_of_ts)
                    );
                    CREATE TABLE team_bullpen_top_relievers (
                      game_id INTEGER NOT NULL,
                      team_id INTEGER NOT NULL,
                      side TEXT NOT NULL,
                      as_of_ts TEXT NOT NULL,
                      top_n INTEGER NOT NULL,
                      ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
                      PRIMARY KEY (game_id, side, as_of_ts, top_n)
                    );
                    """
                )
                conn.commit()

                ensure_schema(conn)
                ensure_schema(conn)

                bullpen_state_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_bullpen_game_state)").fetchall()
                }
                bullpen_top_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_bullpen_top_relievers)").fetchall()
                }

            self.assertIn("stats_scope", bullpen_state_cols)
            self.assertIn("freshness_method", bullpen_state_cols)
            self.assertIn("bullpen_outs_last7d", bullpen_state_cols)
            self.assertIn("ranking_method", bullpen_top_cols)
            self.assertIn("selected_pitcher_ids_json", bullpen_top_cols)
            self.assertIn("topn_freshness_score", bullpen_top_cols)

    def test_schema_migrations_add_lineup_support_columns_idempotently(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                conn.executescript(
                    """
                    CREATE TABLE player_handedness_dim (
                      player_id INTEGER PRIMARY KEY,
                      ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
                    );
                    CREATE TABLE game_lineup_snapshots (
                      game_id INTEGER NOT NULL,
                      team_id INTEGER NOT NULL,
                      side TEXT NOT NULL,
                      as_of_ts TEXT NOT NULL,
                      player_id INTEGER NOT NULL,
                      batting_order INTEGER,
                      ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
                      PRIMARY KEY (game_id, side, as_of_ts, batting_order)
                    );
                    CREATE TABLE team_lineup_game_state (
                      game_id INTEGER NOT NULL,
                      team_id INTEGER NOT NULL,
                      side TEXT NOT NULL,
                      as_of_ts TEXT NOT NULL,
                      ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
                      PRIMARY KEY (game_id, side, as_of_ts)
                    );
                    CREATE TABLE team_platoon_splits (
                      game_id INTEGER NOT NULL,
                      team_id INTEGER NOT NULL,
                      side TEXT NOT NULL,
                      as_of_ts TEXT NOT NULL,
                      vs_pitch_hand TEXT NOT NULL,
                      ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
                      PRIMARY KEY (game_id, side, as_of_ts, vs_pitch_hand)
                    );
                    """
                )
                conn.commit()

                ensure_schema(conn)
                ensure_schema(conn)

                handedness_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(player_handedness_dim)").fetchall()
                }
                lineup_snapshot_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(game_lineup_snapshots)").fetchall()
                }
                lineup_state_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_lineup_game_state)").fetchall()
                }
                platoon_cols = {
                    row["name"] for row in conn.execute("PRAGMA table_info(team_platoon_splits)").fetchall()
                }

            self.assertIn("bat_side", handedness_cols)
            self.assertIn("source_updated_at", handedness_cols)
            self.assertIn("snapshot_type", lineup_snapshot_cols)
            self.assertIn("pitch_hand", lineup_snapshot_cols)
            self.assertIn("lineup_quality_metric", lineup_state_cols)
            self.assertIn("lineup_vs_lhp_quality", lineup_state_cols)
            self.assertIn("stats_scope", platoon_cols)
            self.assertIn("walk_rate", platoon_cols)

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

    def test_game_pitcher_appearance_upsert_updates_existing_row_without_duplicates(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(conn, {"game_id": 123, "season": 2020, "game_date": "2020-07-24", "status": "Final"})
                upsert_game_pitcher_appearance(
                    conn,
                    {
                        "game_id": 123,
                        "team_id": 147,
                        "side": "home",
                        "pitcher_id": 501,
                        "pitcher_name": "Pitcher One",
                        "appearance_order": 1,
                        "is_starter": 1,
                        "is_reliever": 0,
                        "outs_recorded": 15,
                        "innings_pitched": 5.0,
                        "batters_faced": 20,
                        "pitches": 81,
                        "strikes": 55,
                        "hits": 4,
                        "walks": 2,
                        "strikeouts": 6,
                        "runs": 2,
                        "earned_runs": 2,
                        "home_runs": 1,
                        "holds": 0,
                        "save_flag": 0,
                        "blown_save_flag": 0,
                        "inherited_runners": 0,
                        "inherited_runners_scored": 0,
                    },
                )
                upsert_game_pitcher_appearance(
                    conn,
                    {
                        "game_id": 123,
                        "team_id": 147,
                        "side": "home",
                        "pitcher_id": 501,
                        "pitcher_name": "Pitcher One",
                        "appearance_order": 1,
                        "is_starter": 1,
                        "is_reliever": 0,
                        "outs_recorded": 18,
                        "innings_pitched": 6.0,
                        "batters_faced": 23,
                        "pitches": 93,
                        "strikes": 63,
                        "hits": 5,
                        "walks": 2,
                        "strikeouts": 7,
                        "runs": 2,
                        "earned_runs": 2,
                        "home_runs": 1,
                        "holds": 0,
                        "save_flag": 0,
                        "blown_save_flag": 0,
                        "inherited_runners": 0,
                        "inherited_runners_scored": 0,
                    },
                )
                count_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM game_pitcher_appearances WHERE game_id=123 AND pitcher_id=501"
                ).fetchone()
                appearance_row = conn.execute(
                    """
                    SELECT outs_recorded, innings_pitched, batters_faced, pitches, strikes, strikeouts
                    FROM game_pitcher_appearances
                    WHERE game_id=123 AND pitcher_id=501
                    """
                ).fetchone()
                self.assertEqual(count_row["c"], 1)
                self.assertEqual(appearance_row["outs_recorded"], 18)
                self.assertEqual(appearance_row["innings_pitched"], 6.0)
                self.assertEqual(appearance_row["batters_faced"], 23)
                self.assertEqual(appearance_row["pitches"], 93)
                self.assertEqual(appearance_row["strikes"], 63)
                self.assertEqual(appearance_row["strikeouts"], 7)

    def test_build_game_pitcher_appearance_rows_parses_expected_fields(self) -> None:
        rows = build_game_pitcher_appearance_rows(
            3001,
            {
                "gamePk": 3001,
                "home": {
                    "team": {"id": 147},
                    "players": {
                        "ID501": {
                            "person": {"id": 501, "fullName": "Home Starter"},
                            "stats": {
                                "pitching": {
                                    "inningsPitched": "6.0",
                                    "battersFaced": 24,
                                    "hits": 4,
                                    "baseOnBalls": 2,
                                    "strikeOuts": 7,
                                    "earnedRuns": 2,
                                    "runs": 2,
                                    "homeRuns": 1,
                                    "strikes": 60,
                                    "numberOfPitches": 90,
                                    "holds": 0,
                                    "inheritedRunners": 0,
                                    "inheritedRunnersScored": 0,
                                }
                            },
                        },
                        "ID502": {
                            "person": {"id": 502, "fullName": "Home Reliever"},
                            "stats": {
                                "pitching": {
                                    "inningsPitched": "1.0",
                                    "battersFaced": 5,
                                    "hits": 0,
                                    "baseOnBalls": 1,
                                    "strikeOuts": 2,
                                    "earnedRuns": 0,
                                    "runs": 0,
                                    "homeRuns": 0,
                                    "strikes": 12,
                                    "numberOfPitches": 18,
                                    "holds": 1,
                                    "saves": 1,
                                    "blownSaves": 0,
                                    "inheritedRunners": 2,
                                    "inheritedRunnersScored": 1,
                                }
                            },
                        },
                    },
                },
                "away": {"team": {"id": 121}, "players": {}},
            },
        )

        self.assertEqual(len(rows), 2)
        starter_row = rows[0]
        reliever_row = rows[1]
        self.assertEqual(starter_row["game_id"], 3001)
        self.assertEqual(starter_row["team_id"], 147)
        self.assertEqual(starter_row["appearance_order"], 1)
        self.assertEqual(starter_row["is_starter"], 1)
        self.assertEqual(starter_row["is_reliever"], 0)
        self.assertEqual(starter_row["outs_recorded"], 18)
        self.assertEqual(starter_row["innings_pitched"], 6.0)
        self.assertEqual(starter_row["batters_faced"], 24)
        self.assertEqual(starter_row["strikeouts"], 7)
        self.assertEqual(reliever_row["appearance_order"], 2)
        self.assertEqual(reliever_row["is_starter"], 0)
        self.assertEqual(reliever_row["is_reliever"], 1)
        self.assertEqual(reliever_row["holds"], 1)
        self.assertEqual(reliever_row["save_flag"], 1)
        self.assertEqual(reliever_row["blown_save_flag"], 0)
        self.assertEqual(reliever_row["inherited_runners"], 2)
        self.assertEqual(reliever_row["inherited_runners_scored"], 1)

    def test_build_game_pitcher_appearance_rows_uses_statsapi_pitcher_order_for_real_multi_reliever_game(self) -> None:
        rows = build_game_pitcher_appearance_rows(
            401227061,
            {
                "gamePk": 401227061,
                "home": {
                    "team": {"id": 112},
                    "pitchers": [543294, 642180, 592858, 676664, 664126, 595928, 571670],
                    "players": {
                        "ID543294": {
                            "person": {"id": 543294, "fullName": "Kyle Hendricks"},
                            "stats": {"pitching": {"inningsPitched": "6.0", "hits": 3, "runs": 3, "earnedRuns": 3, "baseOnBalls": 2, "strikeOuts": 6, "homeRuns": 1, "strikes": 63, "numberOfPitches": 93}},
                        },
                        "ID642180": {
                            "person": {"id": 642180, "fullName": "Dan Winkler"},
                            "stats": {"pitching": {"inningsPitched": "0.1", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 0, "homeRuns": 0, "strikes": 2, "numberOfPitches": 5}},
                        },
                        "ID592858": {
                            "person": {"id": 592858, "fullName": "Andrew Chafin"},
                            "stats": {"pitching": {"inningsPitched": "0.2", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 1, "homeRuns": 0, "strikes": 6, "numberOfPitches": 10, "holds": 1}},
                        },
                        "ID676664": {
                            "person": {"id": 676664, "fullName": "Ryan Tepera"},
                            "stats": {"pitching": {"inningsPitched": "1.0", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 1, "homeRuns": 0, "strikes": 12, "numberOfPitches": 18, "holds": 1}},
                        },
                        "ID664126": {
                            "person": {"id": 664126, "fullName": "Alec Mills"},
                            "stats": {"pitching": {"inningsPitched": "0.1", "hits": 1, "runs": 1, "earnedRuns": 1, "baseOnBalls": 0, "strikeOuts": 0, "homeRuns": 0, "strikes": 3, "numberOfPitches": 6, "holds": 1}},
                        },
                        "ID595928": {
                            "person": {"id": 595928, "fullName": "Brandon Workman"},
                            "stats": {"pitching": {"inningsPitched": "0.1", "hits": 1, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 0, "homeRuns": 0, "strikes": 5, "numberOfPitches": 8}},
                        },
                        "ID571670": {
                            "person": {"id": 571670, "fullName": "Rex Brothers"},
                            "stats": {"pitching": {"inningsPitched": "0.1", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 1, "strikeOuts": 0, "homeRuns": 0, "strikes": 3, "numberOfPitches": 8, "save": 1}},
                        },
                    },
                },
                "away": {
                    "team": {"id": 134},
                    "pitchers": [543243, 641907, 664286, 607192, 605177, 669923, 592767, 670456],
                    "players": {
                        "ID543243": {
                            "person": {"id": 543243, "fullName": "Chad Kuhl"},
                            "stats": {"pitching": {"inningsPitched": "5.1", "hits": 5, "runs": 3, "earnedRuns": 3, "baseOnBalls": 2, "strikeOuts": 8, "homeRuns": 1, "strikes": 54, "numberOfPitches": 84}},
                        },
                        "ID641907": {
                            "person": {"id": 641907, "fullName": "Sam Howard"},
                            "stats": {"pitching": {"inningsPitched": "0.2", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 1, "homeRuns": 0, "strikes": 6, "numberOfPitches": 10}},
                        },
                        "ID664286": {
                            "person": {"id": 664286, "fullName": "Duane Underwood Jr."},
                            "stats": {"pitching": {"inningsPitched": "1.0", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 1, "homeRuns": 0, "strikes": 10, "numberOfPitches": 15}},
                        },
                        "ID607192": {
                            "person": {"id": 607192, "fullName": "Richard Rodriguez"},
                            "stats": {"pitching": {"inningsPitched": "1.0", "hits": 1, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 2, "homeRuns": 0, "strikes": 10, "numberOfPitches": 16, "save": 1}},
                        },
                        "ID605177": {
                            "person": {"id": 605177, "fullName": "Kyle Crick"},
                            "stats": {"pitching": {"inningsPitched": "0.2", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 1, "strikeOuts": 1, "homeRuns": 0, "strikes": 6, "numberOfPitches": 14}},
                        },
                        "ID669923": {
                            "person": {"id": 669923, "fullName": "David Bednar"},
                            "stats": {"pitching": {"inningsPitched": "0.1", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 0, "homeRuns": 0, "strikes": 2, "numberOfPitches": 4}},
                        },
                        "ID592767": {
                            "person": {"id": 592767, "fullName": "Michael Feliz"},
                            "stats": {"pitching": {"inningsPitched": "0.1", "hits": 1, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 1, "homeRuns": 0, "strikes": 5, "numberOfPitches": 6}},
                        },
                        "ID670456": {
                            "person": {"id": 670456, "fullName": "Chris Stratton"},
                            "stats": {"pitching": {"inningsPitched": "0.1", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 1, "homeRuns": 0, "strikes": 4, "numberOfPitches": 5}},
                        },
                    },
                },
            },
        )

        self.assertEqual(len(rows), 15)
        home_rows = [row for row in rows if row["side"] == "home"]
        away_rows = [row for row in rows if row["side"] == "away"]
        self.assertEqual(
            [row["pitcher_name"] for row in home_rows],
            ["Kyle Hendricks", "Dan Winkler", "Andrew Chafin", "Ryan Tepera", "Alec Mills", "Brandon Workman", "Rex Brothers"],
        )
        self.assertEqual(
            [row["pitcher_name"] for row in away_rows],
            ["Chad Kuhl", "Sam Howard", "Duane Underwood Jr.", "Richard Rodriguez", "Kyle Crick", "David Bednar", "Michael Feliz", "Chris Stratton"],
        )
        self.assertEqual(home_rows[0]["is_starter"], 1)
        self.assertEqual(home_rows[1]["is_reliever"], 1)
        self.assertEqual(home_rows[2]["holds"], 1)
        self.assertEqual(home_rows[-1]["save_flag"], 1)
        self.assertEqual(away_rows[0]["is_starter"], 1)
        self.assertEqual(away_rows[1]["appearance_order"], 2)
        self.assertEqual(away_rows[3]["save_flag"], 1)

    def test_build_game_pitcher_appearance_rows_reads_raw_game_boxscore_team_pitchers(self) -> None:
        rows = build_game_pitcher_appearance_rows(
            630900,
            {
                "teams": {
                    "home": {
                        "team": {"id": 147},
                        "pitchers": [501, 502, 503],
                        "players": {
                            "ID501": {
                                "person": {"id": 501, "fullName": "Home Starter"},
                                "stats": {"pitching": {"inningsPitched": "5.0", "strikeOuts": 6, "numberOfPitches": 82}},
                            },
                            "ID502": {
                                "person": {"id": 502, "fullName": "Home Setup"},
                                "stats": {"pitching": {"inningsPitched": "1.0", "holds": 1, "strikeOuts": 2, "numberOfPitches": 14}},
                            },
                            "ID503": {
                                "person": {"id": 503, "fullName": "Home Closer"},
                                "stats": {"pitching": {"inningsPitched": "1.0", "save": 1, "strikeOuts": 1, "numberOfPitches": 13}},
                            },
                        },
                    },
                    "away": {
                        "team": {"id": 121},
                        "pitchers": [601],
                        "players": {
                            "ID601": {
                                "person": {"id": 601, "fullName": "Away Starter"},
                                "stats": {"pitching": {"inningsPitched": "7.0", "strikeOuts": 8, "numberOfPitches": 95}},
                            }
                        },
                    },
                }
            },
        )

        home_rows = [row for row in rows if row["side"] == "home"]
        self.assertEqual([row["pitcher_id"] for row in home_rows], [501, 502, 503])
        self.assertEqual(home_rows[1]["is_reliever"], 1)
        self.assertEqual(home_rows[1]["holds"], 1)
        self.assertEqual(home_rows[2]["save_flag"], 1)
        self.assertEqual(home_rows[2]["appearance_order"], 3)

    def test_build_game_pitcher_appearance_rows_validates_reliever_rich_game_746046(self) -> None:
        rows = build_game_pitcher_appearance_rows(
            746046,
            {
                "gamePk": 746046,
                "teams": {
                    "away": {
                        "team": {"id": 145},
                        "pitchers": [1004, 1002, 1003, 1001],
                        "players": {
                            "ID1001": {
                                "person": {"id": 1001, "fullName": "Michael Kopech"},
                                "stats": {"pitching": {"inningsPitched": "1.1", "hits": 2, "runs": 1, "earnedRuns": 1, "baseOnBalls": 1, "strikeOuts": 1, "strikes": 14, "numberOfPitches": 25, "save": 1}},
                            },
                            "ID1002": {
                                "person": {"id": 1002, "fullName": "John Brebbia"},
                                "stats": {"pitching": {"inningsPitched": "0.2", "hits": 2, "runs": 0, "earnedRuns": 0, "baseOnBalls": 1, "strikeOuts": 1, "strikes": 14, "numberOfPitches": 23, "holds": 1}},
                            },
                            "ID1003": {
                                "person": {"id": 1003, "fullName": "Tanner Banks"},
                                "stats": {"pitching": {"inningsPitched": "0.2", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 1, "strikes": 9, "numberOfPitches": 11, "holds": 1}},
                            },
                            "ID1004": {
                                "person": {"id": 1004, "fullName": "Drew Thorpe"},
                                "stats": {"pitching": {"inningsPitched": "6.1", "hits": 3, "runs": 1, "earnedRuns": 1, "baseOnBalls": 2, "strikeOuts": 5, "strikes": 51, "numberOfPitches": 90}},
                            },
                        },
                    },
                    "home": {
                        "team": {"id": 146},
                        "pitchers": [2001, 2002, 2003, 2004],
                        "players": {
                            "ID2001": {
                                "person": {"id": 2001, "fullName": "Bryan Hoeing"},
                                "stats": {"pitching": {"inningsPitched": "3.0", "hits": 5, "runs": 1, "earnedRuns": 1, "baseOnBalls": 1, "strikeOuts": 4, "strikes": 32, "numberOfPitches": 46}},
                            },
                            "ID2002": {
                                "person": {"id": 2002, "fullName": "Roddery Munoz"},
                                "stats": {"pitching": {"inningsPitched": "4.0", "hits": 4, "runs": 2, "earnedRuns": 2, "baseOnBalls": 4, "strikeOuts": 2, "strikes": 55, "numberOfPitches": 87}},
                            },
                            "ID2003": {
                                "person": {"id": 2003, "fullName": "J.T. Chargois"},
                                "stats": {"pitching": {"inningsPitched": "1.0", "hits": 1, "runs": 0, "earnedRuns": 0, "baseOnBalls": 0, "strikeOuts": 1, "strikes": 10, "numberOfPitches": 13}},
                            },
                            "ID2004": {
                                "person": {"id": 2004, "fullName": "Declan Cronin"},
                                "stats": {"pitching": {"inningsPitched": "1.0", "hits": 0, "runs": 0, "earnedRuns": 0, "baseOnBalls": 1, "strikeOuts": 1, "strikes": 6, "numberOfPitches": 13}},
                            },
                        },
                    },
                },
            },
        )

        away_rows = [row for row in rows if row["side"] == "away"]
        home_rows = [row for row in rows if row["side"] == "home"]

        self.assertEqual(
            [row["pitcher_name"] for row in away_rows],
            ["Drew Thorpe", "John Brebbia", "Tanner Banks", "Michael Kopech"],
        )
        self.assertEqual(
            [row["pitcher_name"] for row in home_rows],
            ["Bryan Hoeing", "Roddery Munoz", "J.T. Chargois", "Declan Cronin"],
        )
        self.assertEqual(away_rows[0]["appearance_order"], 1)
        self.assertEqual(away_rows[0]["is_starter"], 1)
        self.assertEqual(away_rows[1]["holds"], 1)
        self.assertEqual(away_rows[2]["holds"], 1)
        self.assertEqual(away_rows[3]["save_flag"], 1)
        self.assertEqual(home_rows[0]["appearance_order"], 1)
        self.assertEqual(home_rows[0]["is_starter"], 1)
        self.assertEqual(home_rows[1]["is_reliever"], 1)
        self.assertEqual(len(rows), 8)

    def test_build_game_pitcher_appearance_rows_handles_starter_only_game_630851(self) -> None:
        rows = build_game_pitcher_appearance_rows(
            630851,
            {
                "gamePk": 630851,
                "teams": {
                    "away": {
                        "team": {"id": 147},
                        "pitchers": [543037],
                        "players": {
                            "ID543037": {
                                "person": {"id": 543037, "fullName": "Gerrit Cole"},
                                "stats": {"pitching": {"inningsPitched": "5.0", "hits": 3, "runs": 1, "earnedRuns": 1, "baseOnBalls": 1, "strikeOuts": 5, "numberOfPitches": 68}},
                            }
                        },
                    },
                    "home": {
                        "team": {"id": 120},
                        "pitchers": [453286],
                        "players": {
                            "ID453286": {
                                "person": {"id": 453286, "fullName": "Max Scherzer"},
                                "stats": {"pitching": {"inningsPitched": "5.1", "hits": 6, "runs": 4, "earnedRuns": 4, "baseOnBalls": 3, "strikeOuts": 11, "numberOfPitches": 103}},
                            }
                        },
                    },
                },
            },
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            [(row["side"], row["pitcher_name"], row["is_starter"], row["is_reliever"]) for row in rows],
            [("home", "Max Scherzer", 1, 0), ("away", "Gerrit Cole", 1, 0)],
        )


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

            def fake_lookup_player(name, season=None, *args, **kwargs):
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
                lookup_player=fake_lookup_player,
                boxscore_data=fake_boxscore_data,
            )

            with patch("scripts.history_ingest.fetch_schedule_bounded", return_value=schedule_rows):
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
                           season_era, season_whip, season_stats_scope, season_stats_leakage_risk,
                           stats_source, stats_as_of_date
                    FROM game_pitcher_context
                    WHERE game_id = 3010
                    ORDER BY side
                    """
                ).fetchall()

            self.assertEqual(len(rows), 2)
            for row in rows:
                self.assertEqual(row["probable_pitcher_known"], 1)
                self.assertIn(row["probable_pitcher_name"], {"Fallback Home", "Fallback Away"})
                self.assertEqual(row["season_era"], 4.5)
                self.assertEqual(row["season_whip"], 1.3)
                self.assertEqual(row["season_stats_scope"], "full_season_year_aggregate")
                self.assertEqual(row["season_stats_leakage_risk"], 1)
                self.assertEqual(row["stats_source"], "statsapi.player_stat_data(type=yearByYear,career)+lookup_player")
                self.assertIsNone(row["stats_as_of_date"])

    def test_backfill_pitcher_context_repair_mode_fails_before_writes_when_fallback_is_broad(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game_id, game_date in ((3011, "2021-08-01"), (3012, "2021-08-02")):
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
                [
                    "--db",
                    str(db_path),
                    "backfill-pitcher-context",
                    "--season",
                    str(season),
                    "--repair-mode",
                    "--max-null-safe-fallback-share",
                    "0.5",
                ]
            )

            schedule_rows = [
                {
                    "game_id": 3011,
                    "season": season,
                    "game_date": "2021-08-01",
                    "home_probable_pitcher": "Home Starter",
                    "away_probable_pitcher": "Away Starter",
                },
                {
                    "game_id": 3012,
                    "season": season,
                    "game_date": "2021-08-02",
                    "home_probable_pitcher": "Home Starter",
                    "away_probable_pitcher": "Away Starter",
                },
            ]

            def fake_lookup_player(name, season=None, *args, **kwargs):
                if name == "Home Starter":
                    return [{"id": 501}]
                if name == "Away Starter":
                    return [{"id": 502}]
                return []

            with patch("scripts.history_ingest.fetch_schedule_bounded", return_value=schedule_rows):
                with patch("scripts.history_ingest.fetch_lookup_player_bounded", side_effect=fake_lookup_player):
                    with patch("scripts.history_ingest.fetch_boxscore_bounded", side_effect=RuntimeError("api down")):
                        with self.assertRaises(RuntimeError):
                            args.func(args)

            with connect_db(str(db_path)) as conn:
                row_count = conn.execute("SELECT COUNT(*) AS c FROM game_pitcher_context").fetchone()["c"]
                checkpoint = conn.execute(
                    """
                    SELECT status, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='pitcher-context-2021' AND partition_key='season=2021'
                    """
                ).fetchone()

            self.assertEqual(row_count, 0)
            self.assertEqual(checkpoint["status"], "failed")
            checkpoint_cursor = json.loads(checkpoint["cursor_json"])
            self.assertEqual(checkpoint_cursor["season"], season)

    def test_build_pitcher_context_quality_report_flags_broad_null_safe_fallback_damage(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 3013,
                        "season": season,
                        "game_date": "2021-08-03",
                        "status": "Final",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                )
                for side, pitcher_id in (("home", 611), ("away", 612)):
                    upsert_game_pitcher_context(
                        conn,
                        {
                            "game_id": 3013,
                            "side": side,
                            "pitcher_id": pitcher_id,
                            "pitcher_name": f"{side} starter",
                            "probable_pitcher_id": pitcher_id,
                            "probable_pitcher_name": f"{side} starter",
                            "probable_pitcher_known": 1,
                            "stats_source": "leakage_safe_null_fallback(probable_pitcher_identity_without_prior_completed_pitching)",
                            "season_stats_scope": "season_to_date_prior_completed_games",
                            "season_stats_leakage_risk": 0,
                        },
                    )
                report = build_pitcher_context_quality_report(conn, season=season)

            self.assertEqual(report["null_safe_fallback_rows"], 2)
            self.assertEqual(report["rows_with_stats"], 0)
            self.assertGreater(report["null_safe_fallback_share"], DEFAULT_MAX_NULL_SAFE_FALLBACK_SHARE)
            self.assertFalse(report["safe_for_canonical_write"])

    def test_backfill_pitcher_context_refreshes_missing_probable_pitcher_handedness(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2024
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 3020,
                        "season": season,
                        "game_date": "2024-04-02",
                        "status": "Scheduled",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                )
                upsert_player_handedness(
                    conn,
                    {
                        "player_id": 701,
                        "player_name": "Known Home Starter",
                        "primary_position_code": "P",
                    },
                )
                upsert_player_handedness(
                    conn,
                    {
                        "player_id": 702,
                        "player_name": "Known Away Starter",
                        "primary_position_code": "P",
                    },
                )

            parser = build_parser()
            args = parser.parse_args(["--db", str(db_path), "backfill-pitcher-context", "--season", str(season)])

            schedule_rows = [
                {
                    "game_id": 3020,
                    "season": season,
                    "game_date": "2024-04-02",
                    "home_probable_pitcher": "Known Home Starter",
                    "away_probable_pitcher": "Known Away Starter",
                }
            ]

            def fake_lookup_player(name, season=None):
                if name == "Known Home Starter":
                    return [{"id": 701}]
                if name == "Known Away Starter":
                    return [{"id": 702}]
                return []

            def fake_get(endpoint, params):
                self.assertEqual(endpoint, "people")
                person_ids = sorted(int(player_id) for player_id in params["personIds"].split(","))
                self.assertEqual(person_ids, [701, 702])
                return {
                    "people": [
                        {
                            "id": 701,
                            "fullName": "Known Home Starter",
                            "pitchHand": {"code": "R"},
                            "batSide": {"code": "L"},
                            "primaryPosition": {"abbreviation": "P"},
                        },
                        {
                            "id": 702,
                            "fullName": "Known Away Starter",
                            "pitchHand": {"code": "L"},
                            "batSide": {"code": "R"},
                            "primaryPosition": {"abbreviation": "P"},
                        },
                    ]
                }

            stub_statsapi = types.SimpleNamespace(
                lookup_player=fake_lookup_player,
                get=fake_get,
            )

            with patch("scripts.history_ingest.fetch_schedule_bounded", return_value=schedule_rows):
                with patch("scripts.history_ingest.statsapi", stub_statsapi):
                    args.func(args)

            with connect_db(str(db_path)) as conn:
                handedness_rows = conn.execute(
                    """
                    SELECT player_id, bat_side, pitch_hand
                    FROM player_handedness_dim
                    WHERE player_id IN (701, 702)
                    ORDER BY player_id
                    """
                ).fetchall()

            self.assertEqual(
                [(row["player_id"], row["bat_side"], row["pitch_hand"]) for row in handedness_rows],
                [(701, "L", "R"), (702, "R", "L")],
            )

    def test_backfill_pitcher_context_legacy_alias_defaults_to_2020(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["backfill-pitcher-context-2020"])
        self.assertEqual(args.season, 2020)

    def test_rebuild_history_parser_defaults_to_all_stages_and_safe_feature_version(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["rebuild-history"])

        self.assertEqual(args.stages, ["all"])
        self.assertEqual(args.feature_version, "v1")
        self.assertEqual(args.season_start, 2020)
        self.assertEqual(args.season_end, 2025)

    def test_canonical_write_guard_requires_explicit_opt_in_for_mutating_commands(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--db", str(DEFAULT_DB_PATH), "materialize-feature-rows", "--season", "2024"])

        with self.assertRaises(SystemExit) as exc:
            enforce_canonical_write_guard(args, parser)

        self.assertEqual(exc.exception.code, 2)

    def test_canonical_write_guard_allows_read_only_commands_without_opt_in(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--db", str(DEFAULT_DB_PATH), "audit-pitcher-context", "--season", "2024"])

        enforce_canonical_write_guard(args, parser)

    def test_canonical_write_guard_allows_mutating_commands_with_explicit_opt_in(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "--db",
                str(DEFAULT_DB_PATH),
                "--allow-canonical-writes",
                "materialize-feature-rows",
                "--season",
                "2024",
            ]
        )

        enforce_canonical_write_guard(args, parser)

    def test_canonical_write_guard_requires_explicit_opt_in_for_rebuild_history(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--db", str(DEFAULT_DB_PATH), "rebuild-history", "--season", "2024"])

        with self.assertRaises(SystemExit) as exc:
            enforce_canonical_write_guard(args, parser)

        self.assertEqual(exc.exception.code, 2)

    def test_rebuild_history_runs_selected_stages_for_each_season_in_stage_order(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "rebuild-history",
                    "--season-start",
                    "2024",
                    "--season-end",
                    "2025",
                    "--stages",
                    "feature-rows",
                    "base",
                    "venues",
                ]
            )
            calls: list[tuple[str, int]] = []

            def record(stage_name: str):
                def _inner(stage_args):
                    calls.append((stage_name, int(stage_args.season)))

                return _inner

            with patch("scripts.history_ingest.cmd_backfill", side_effect=record("base")):
                with patch("scripts.history_ingest.cmd_sync_venues", side_effect=record("venues")):
                    with patch("scripts.history_ingest.cmd_materialize_feature_rows", side_effect=record("feature-rows")):
                        args.func(args)

            self.assertEqual(
                calls,
                [
                    ("base", 2024),
                    ("venues", 2024),
                    ("feature-rows", 2024),
                    ("base", 2025),
                    ("venues", 2025),
                    ("feature-rows", 2025),
                ],
            )

            with connect_db(str(db_path)) as conn:
                run = conn.execute(
                    """
                    SELECT status, note
                    FROM ingestion_runs
                    WHERE partition_key='rebuild-history:range=2024-2025'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ).fetchone()

            self.assertEqual(run["status"], "success")
            self.assertEqual(
                json.loads(run["note"]),
                {
                    "destructive_replace": "disabled",
                    "job": "rebuild-history",
                    "seasons": [2024, 2025],
                    "stages": ["base", "venues", "feature-rows"],
                },
            )

    def test_rebuild_history_rejects_all_plus_explicit_stage_mix(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "rebuild-history",
                    "--season",
                    "2024",
                    "--stages",
                    "all",
                    "base",
                ]
            )

            with self.assertRaises(ValueError) as exc:
                args.func(args)

        self.assertIn("cannot be combined", str(exc.exception))

    def test_sync_venues_populates_venue_dim_from_statsapi_metadata(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 8101,
                        "season": 2024,
                        "game_date": "2024-06-01",
                        "scheduled_datetime": "2024-06-01T23:10:00Z",
                        "status": "Scheduled",
                        "venue_id": 5001,
                    },
                )

            parser = build_parser()
            args = parser.parse_args(["--db", str(db_path), "sync-venues", "--season", "2024"])

            with patch(
                "scripts.history_ingest.fetch_statsapi_venue_details_bounded",
                return_value={
                    "venue_id": 5001,
                    "venue_name": "Example Park",
                    "city": "Example City",
                    "state": "EX",
                    "country": "USA",
                    "timezone": "America/New_York",
                    "latitude": 40.0,
                    "longitude": -73.0,
                    "roof_type": "open",
                    "statsapi_venue_name": "Example Park",
                },
            ):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                row = conn.execute(
                    "SELECT venue_name, timezone, roof_type, weather_exposure_default FROM venue_dim WHERE venue_id=5001"
                ).fetchone()

            self.assertEqual(row["venue_name"], "Example Park")
            self.assertEqual(row["timezone"], "America/New_York")
            self.assertEqual(row["roof_type"], "open")
            self.assertEqual(row["weather_exposure_default"], 1)

    def test_sync_venues_uses_known_fallback_metadata_when_statsapi_lookup_is_missing(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 8102,
                        "season": 2024,
                        "game_date": "2024-04-27",
                        "scheduled_datetime": "2024-04-27T22:05:00Z",
                        "status": "Final",
                        "venue_id": 5340,
                    },
                )

            parser = build_parser()
            args = parser.parse_args(["--db", str(db_path), "sync-venues", "--season", "2024"])

            with patch("scripts.history_ingest.fetch_statsapi_venue_details_bounded", return_value=None):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                row = conn.execute(
                    """
                    SELECT venue_name, city, state, country, timezone, roof_type
                    FROM venue_dim
                    WHERE venue_id=5340
                    """
                ).fetchone()

            self.assertEqual(row["venue_name"], "Estadio Alfredo Harp Helu")
            self.assertEqual(row["city"], "Mexico City")
            self.assertEqual(row["state"], "CMX")
            self.assertEqual(row["country"], "MEX")
            self.assertEqual(row["timezone"], "America/Mexico_City")
            self.assertEqual(row["roof_type"], "open")

    def test_backfill_game_metadata_repairs_existing_games_with_venue_id_and_day_night(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 8401,
                        "season": 2024,
                        "game_date": "2024-06-01",
                        "scheduled_datetime": "2024-06-01T23:10:00Z",
                        "status": "Scheduled",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                )

            parser = build_parser()
            args = parser.parse_args(["--db", str(db_path), "backfill-game-metadata", "--season", "2024"])

            with patch(
                "scripts.history_ingest.fetch_schedule_bounded",
                return_value=[
                    {
                        "game_id": 8401,
                        "season": 2024,
                        "game_date": "2024-06-01",
                        "game_datetime": "2024-06-01T23:10:00Z",
                        "game_type": "R",
                        "status": "Scheduled",
                        "home_id": 147,
                        "away_id": 121,
                        "venue_id": 5001,
                        "dayNight": "night",
                        "venue": {
                            "id": 5001,
                            "name": "Example Park",
                            "location": {
                                "city": "Example City",
                                "stateAbbrev": "EX",
                                "country": "USA",
                                "defaultCoordinates": {"latitude": 40.0, "longitude": -73.0},
                                "timeZone": {"id": "America/New_York"},
                            },
                        },
                    }
                ],
            ):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                game_row = conn.execute("SELECT venue_id, day_night FROM games WHERE game_id = 8401").fetchone()
                venue_row = conn.execute("SELECT venue_name, timezone FROM venue_dim WHERE venue_id = 5001").fetchone()

            self.assertEqual(game_row["venue_id"], 5001)
            self.assertEqual(game_row["day_night"], "night")
            self.assertEqual(venue_row["venue_name"], "Example Park")
            self.assertEqual(venue_row["timezone"], "America/New_York")

    def test_backfill_game_weather_writes_observed_archive_snapshot(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game_id, scheduled_datetime in (
                    (8201, "2024-06-01T23:10:00Z"),
                    (8202, "2024-06-02T23:10:00Z"),
                ):
                    upsert_game(
                        conn,
                        {
                            "game_id": game_id,
                            "season": 2024,
                            "game_date": scheduled_datetime[:10],
                            "scheduled_datetime": scheduled_datetime,
                            "status": "Final",
                            "venue_id": 5001,
                            "day_night": "night",
                        },
                    )
                upsert_venue_dim(
                    conn,
                    {
                        "venue_id": 5001,
                        "venue_name": "Example Park",
                        "city": "Example City",
                        "state": "EX",
                        "country": "USA",
                        "timezone": "UTC",
                        "latitude": 40.0,
                        "longitude": -73.0,
                        "roof_type": "open",
                    },
                )

            parser = build_parser()
            args = parser.parse_args(
                ["--db", str(db_path), "backfill-game-weather", "--season", "2024", "--game-id", "8201"]
            )

            open_meteo_payload = {
                "hourly": {
                    "time": ["2024-06-01T22:00", "2024-06-01T23:00", "2024-06-02T00:00"],
                    "temperature_2m": [70.0, 72.0, 68.0],
                    "relative_humidity_2m": [40, 50, 60],
                    "surface_pressure": [1012, 1011, 1010],
                    "precipitation": [0.0, 0.2, 0.0],
                    "precipitation_probability": [5, 15, 10],
                    "wind_speed_10m": [8.0, 9.0, 7.0],
                    "wind_gusts_10m": [11.0, 13.0, 9.0],
                    "wind_direction_10m": [170, 180, 190],
                    "weather_code": [0, 1, 0],
                    "cloud_cover": [5, 15, 10],
                    "is_day": [1, 0, 0],
                }
            }

            with patch("scripts.history_ingest.fetch_open_meteo_hourly_bounded", return_value=open_meteo_payload):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                row = conn.execute(
                    """
                    SELECT snapshot_type, source, as_of_ts, target_game_ts, temperature_f, wind_speed_mph,
                           precipitation_probability,
                           weather_exposure_flag, day_night_source
                    FROM game_weather_snapshots
                    WHERE game_id=8201
                    """
                ).fetchone()
                snapshot_count = conn.execute("SELECT COUNT(*) AS c FROM game_weather_snapshots").fetchone()["c"]

            self.assertEqual(row["snapshot_type"], "observed_archive")
            self.assertEqual(row["source"], "open_meteo_archive")
            self.assertEqual(row["as_of_ts"], "2024-06-01T23:10:00Z")
            self.assertEqual(row["target_game_ts"], "2024-06-01T23:10:00Z")
            self.assertEqual(row["temperature_f"], 72.0)
            self.assertEqual(row["wind_speed_mph"], 9.0)
            self.assertIsNone(row["precipitation_probability"])
            self.assertEqual(row["weather_exposure_flag"], 1)
            self.assertEqual(row["day_night_source"], "games.day_night")
            self.assertEqual(snapshot_count, 1)

    def test_update_game_weather_forecasts_skips_started_games_and_writes_future_snapshot(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game in (
                    {
                        "game_id": 8301,
                        "season": 2026,
                        "game_date": "2026-03-10",
                        "scheduled_datetime": "2026-03-10T16:00:00Z",
                        "status": "Scheduled",
                        "venue_id": 5001,
                        "day_night": "day",
                    },
                    {
                        "game_id": 8302,
                        "season": 2026,
                        "game_date": "2026-03-10",
                        "scheduled_datetime": "2026-03-10T23:00:00Z",
                        "status": "Scheduled",
                        "venue_id": 5001,
                        "day_night": "night",
                    },
                ):
                    upsert_game(conn, game)
                upsert_venue_dim(
                    conn,
                    {
                        "venue_id": 5001,
                        "venue_name": "Example Park",
                        "city": "Example City",
                        "state": "EX",
                        "country": "USA",
                        "timezone": "UTC",
                        "latitude": 40.0,
                        "longitude": -73.0,
                        "roof_type": "open",
                    },
                )

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "update-game-weather-forecasts",
                    "--date",
                    "2026-03-10",
                    "--as-of-ts",
                    "2026-03-10T18:00:00Z",
                ]
            )
            open_meteo_payload = {
                "hourly": {
                    "time": ["2026-03-10T22:00", "2026-03-10T23:00", "2026-03-11T00:00"],
                    "temperature_2m": [61.0, 60.0, 58.0],
                    "relative_humidity_2m": [45, 50, 55],
                    "surface_pressure": [1018, 1017, 1016],
                    "precipitation": [0.0, 0.0, 0.0],
                    "precipitation_probability": [0, 5, 10],
                    "wind_speed_10m": [6.0, 7.0, 8.0],
                    "wind_gusts_10m": [9.0, 10.0, 11.0],
                    "wind_direction_10m": [150, 160, 170],
                    "weather_code": [0, 0, 1],
                    "cloud_cover": [10, 15, 20],
                    "is_day": [0, 0, 0],
                }
            }

            with patch("scripts.history_ingest.fetch_open_meteo_hourly_bounded", return_value=open_meteo_payload):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                rows = conn.execute(
                    """
                    SELECT game_id, snapshot_type, source, as_of_ts, precipitation_probability
                    FROM game_weather_snapshots
                    ORDER BY game_id
                    """
                ).fetchall()
                checkpoint = conn.execute(
                    """
                    SELECT cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='weather-forecast-2026-03-10' AND partition_key='weather-date=2026-03-10'
                    """
                ).fetchone()

            self.assertEqual([row["game_id"] for row in rows], [8302])
            self.assertEqual(rows[0]["snapshot_type"], "forecast")
            self.assertEqual(rows[0]["source"], "open_meteo_forecast")
            self.assertEqual(rows[0]["as_of_ts"], "2026-03-10T18:00:00Z")
            self.assertEqual(rows[0]["precipitation_probability"], 5.0)
            self.assertEqual(json.loads(checkpoint["cursor_json"])["skipped_started_games"], 1)

    def test_backfill_pitcher_appearances_for_season_is_idempotent(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game_id, game_date in ((4001, "2021-08-01"), (4002, "2021-08-05")):
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
                ["--db", str(db_path), "--checkpoint-every", "1", "backfill-pitcher-appearances", "--season", "2021"]
            )

            def fake_game_boxscore(_endpoint, params):
                game_id = int(params["gamePk"])
                if game_id == 4001:
                    return {
                        "teams": {
                            "home": {
                                "team": {"id": 147},
                                "pitchers": [501, 503],
                                "players": {
                                    "ID501": {
                                        "person": {"id": 501, "fullName": "Home Starter"},
                                        "stats": {"pitching": {"inningsPitched": "5.0", "hits": 4, "baseOnBalls": 2, "strikeOuts": 6, "earnedRuns": 2, "runs": 2, "strikes": 50, "numberOfPitches": 78}},
                                    },
                                    "ID503": {
                                        "person": {"id": 503, "fullName": "Home Reliever"},
                                        "stats": {"pitching": {"inningsPitched": "4.0", "hits": 1, "baseOnBalls": 0, "strikeOuts": 5, "earnedRuns": 0, "runs": 0, "holds": 1, "strikes": 30, "numberOfPitches": 42}},
                                    },
                                },
                            },
                            "away": {
                                "team": {"id": 121},
                                "pitchers": [502],
                                "players": {
                                    "ID502": {
                                        "person": {"id": 502, "fullName": "Away Starter"},
                                        "stats": {"pitching": {"inningsPitched": "6.0", "hits": 5, "baseOnBalls": 1, "strikeOuts": 7, "earnedRuns": 3, "runs": 3, "strikes": 61, "numberOfPitches": 88}},
                                    }
                                },
                            },
                        }
                    }
                return {
                    "teams": {
                        "home": {
                            "team": {"id": 147},
                            "pitchers": [504],
                            "players": {
                                "ID504": {
                                    "person": {"id": 504, "fullName": "Home Starter Two"},
                                    "stats": {"pitching": {"inningsPitched": "7.0", "hits": 3, "baseOnBalls": 1, "strikeOuts": 8, "earnedRuns": 1, "runs": 1, "strikes": 67, "numberOfPitches": 96}},
                                }
                            },
                        },
                        "away": {
                            "team": {"id": 121},
                            "pitchers": [505, 506],
                            "players": {
                                "ID505": {
                                    "person": {"id": 505, "fullName": "Away Starter Two"},
                                    "stats": {"pitching": {"inningsPitched": "5.0", "hits": 6, "baseOnBalls": 3, "strikeOuts": 4, "earnedRuns": 4, "runs": 4, "strikes": 45, "numberOfPitches": 83, "blownSaves": 1}},
                                },
                                "ID506": {
                                    "person": {"id": 506, "fullName": "Away Reliever Two"},
                                    "stats": {"pitching": {"inningsPitched": "3.0", "hits": 0, "baseOnBalls": 0, "strikeOuts": 3, "earnedRuns": 0, "runs": 0, "saves": 1, "strikes": 22, "numberOfPitches": 29}},
                                },
                            }
                        },
                    }
                }

            stub_statsapi = types.SimpleNamespace(get=fake_game_boxscore)
            with patch("scripts.history_ingest.statsapi", stub_statsapi):
                args.func(args)
                args.func(args)

            with connect_db(str(db_path)) as conn:
                row_count = conn.execute("SELECT COUNT(*) AS c FROM game_pitcher_appearances").fetchone()["c"]
                reliever_row = conn.execute(
                    """
                    SELECT appearance_order, is_starter, is_reliever, holds, save_flag, blown_save_flag
                    FROM game_pitcher_appearances
                    WHERE game_id=4002 AND pitcher_id=506
                    """
                ).fetchone()
                checkpoint = conn.execute(
                    """
                    SELECT status, attempts, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='pitcher-appearances-2021' AND partition_key='season=2021'
                    """
                ).fetchone()

            self.assertEqual(row_count, 6)
            self.assertEqual(reliever_row["appearance_order"], 2)
            self.assertEqual(reliever_row["is_starter"], 0)
            self.assertEqual(reliever_row["is_reliever"], 1)
            self.assertEqual(reliever_row["holds"], 0)
            self.assertEqual(reliever_row["save_flag"], 1)
            self.assertEqual(reliever_row["blown_save_flag"], 0)
            self.assertEqual(checkpoint["status"], "success")
            self.assertGreaterEqual(checkpoint["attempts"], 2)
            checkpoint_cursor = json.loads(checkpoint["cursor_json"])
            self.assertEqual(checkpoint_cursor["games_processed"], 2)
            self.assertEqual(checkpoint_cursor["rows_upserted"], 6)
            self.assertEqual(checkpoint_cursor["rows_inserted"], 0)
            self.assertEqual(checkpoint_cursor["rows_updated"], 6)

    def test_backfill_bullpen_support_uses_prior_local_pitcher_appearances_only(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "--checkpoint-every",
                    "1",
                    "backfill-bullpen-support",
                    "--season",
                    str(season),
                    "--top-n-values",
                    "2,3",
                ]
            )

            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game in (
                    {
                        "game_id": 7001,
                        "season": season,
                        "game_date": "2021-04-01",
                        "scheduled_datetime": "2021-04-01T17:05:00Z",
                        "status": "Final",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                    {
                        "game_id": 7002,
                        "season": season,
                        "game_date": "2021-04-03",
                        "scheduled_datetime": "2021-04-03T18:10:00Z",
                        "status": "Final",
                        "home_team_id": 121,
                        "away_team_id": 147,
                    },
                    {
                        "game_id": 7003,
                        "season": season,
                        "game_date": "2021-04-05",
                        "scheduled_datetime": "2021-04-05T19:10:00Z",
                        "status": "Scheduled",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                    {
                        "game_id": 7004,
                        "season": season,
                        "game_date": "2021-04-06",
                        "scheduled_datetime": "2021-04-06T19:10:00Z",
                        "status": "Final",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                ):
                    upsert_game(conn, game)

                for row in (
                    {
                        "game_id": 7001,
                        "team_id": 147,
                        "side": "home",
                        "pitcher_id": 501,
                        "pitcher_name": "Alpha",
                        "appearance_order": 2,
                        "is_starter": 0,
                        "is_reliever": 1,
                        "outs_recorded": 6,
                        "batters_faced": 8,
                        "pitches": 20,
                        "hits": 1,
                        "walks": 0,
                        "strikeouts": 3,
                        "runs": 0,
                        "earned_runs": 0,
                        "home_runs": 0,
                    },
                    {
                        "game_id": 7001,
                        "team_id": 147,
                        "side": "home",
                        "pitcher_id": 502,
                        "pitcher_name": "Bravo",
                        "appearance_order": 3,
                        "is_starter": 0,
                        "is_reliever": 1,
                        "outs_recorded": 3,
                        "batters_faced": 5,
                        "pitches": 16,
                        "hits": 2,
                        "walks": 1,
                        "strikeouts": 1,
                        "runs": 1,
                        "earned_runs": 1,
                        "home_runs": 1,
                    },
                    {
                        "game_id": 7001,
                        "team_id": 121,
                        "side": "away",
                        "pitcher_id": 601,
                        "pitcher_name": "Away One",
                        "appearance_order": 2,
                        "is_starter": 0,
                        "is_reliever": 1,
                        "outs_recorded": 3,
                        "batters_faced": 4,
                        "pitches": 12,
                        "hits": 0,
                        "walks": 0,
                        "strikeouts": 1,
                        "runs": 0,
                        "earned_runs": 0,
                        "home_runs": 0,
                    },
                    {
                        "game_id": 7002,
                        "team_id": 147,
                        "side": "away",
                        "pitcher_id": 503,
                        "pitcher_name": "Charlie",
                        "appearance_order": 2,
                        "is_starter": 0,
                        "is_reliever": 1,
                        "outs_recorded": 6,
                        "batters_faced": 8,
                        "pitches": 18,
                        "hits": 0,
                        "walks": 1,
                        "strikeouts": 4,
                        "runs": 0,
                        "earned_runs": 0,
                        "home_runs": 0,
                    },
                    {
                        "game_id": 7002,
                        "team_id": 147,
                        "side": "away",
                        "pitcher_id": 502,
                        "pitcher_name": "Bravo",
                        "appearance_order": 3,
                        "is_starter": 0,
                        "is_reliever": 1,
                        "outs_recorded": 3,
                        "batters_faced": 5,
                        "pitches": 28,
                        "hits": 1,
                        "walks": 1,
                        "strikeouts": 0,
                        "runs": 1,
                        "earned_runs": 1,
                        "home_runs": 0,
                    },
                    {
                        "game_id": 7002,
                        "team_id": 121,
                        "side": "home",
                        "pitcher_id": 602,
                        "pitcher_name": "Away Two",
                        "appearance_order": 2,
                        "is_starter": 0,
                        "is_reliever": 1,
                        "outs_recorded": 6,
                        "batters_faced": 7,
                        "pitches": 21,
                        "hits": 1,
                        "walks": 0,
                        "strikeouts": 2,
                        "runs": 0,
                        "earned_runs": 0,
                        "home_runs": 0,
                    },
                    {
                        "game_id": 7004,
                        "team_id": 147,
                        "side": "home",
                        "pitcher_id": 504,
                        "pitcher_name": "Future Reliever",
                        "appearance_order": 2,
                        "is_starter": 0,
                        "is_reliever": 1,
                        "outs_recorded": 3,
                        "batters_faced": 5,
                        "pitches": 24,
                        "hits": 0,
                        "walks": 0,
                        "strikeouts": 1,
                        "runs": 0,
                        "earned_runs": 0,
                        "home_runs": 0,
                    },
                ):
                    upsert_game_pitcher_appearance(conn, row)

            args.func(args)
            args.func(args)

            with connect_db(str(db_path)) as conn:
                state_row = conn.execute(
                    """
                    SELECT bullpen_appearances_season, bullpen_outs_season, bullpen_pitches_last3d,
                           relievers_used_last3d_count, relievers_back_to_back_count, freshness_score
                    FROM team_bullpen_game_state
                    WHERE game_id=7003 AND side='home'
                    """
                ).fetchone()
                top2_row = conn.execute(
                    """
                    SELECT n_available, selected_pitcher_ids_json, topn_outs_season, topn_k_minus_bb_rate_season,
                           topn_outs_last3d, topn_back_to_back_count, quality_dropoff_vs_team
                    FROM team_bullpen_top_relievers
                    WHERE game_id=7003 AND side='home' AND top_n=2
                    """
                ).fetchone()
                top3_row = conn.execute(
                    """
                    SELECT n_available, selected_pitcher_ids_json
                    FROM team_bullpen_top_relievers
                    WHERE game_id=7003 AND side='home' AND top_n=3
                    """
                ).fetchone()
                future_state_row = conn.execute(
                    """
                    SELECT bullpen_appearances_season
                    FROM team_bullpen_game_state
                    WHERE game_id=7003 AND side='home'
                    """
                ).fetchone()
                checkpoint = conn.execute(
                    """
                    SELECT status, attempts, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='bullpen-support-2021' AND partition_key='season=2021'
                    """
                ).fetchone()

            self.assertEqual(state_row["bullpen_appearances_season"], 4)
            self.assertEqual(state_row["bullpen_outs_season"], 18)
            self.assertEqual(state_row["bullpen_pitches_last3d"], 46)
            self.assertEqual(state_row["relievers_used_last3d_count"], 2)
            self.assertEqual(state_row["relievers_back_to_back_count"], 0)
            self.assertIsNotNone(state_row["freshness_score"])
            self.assertEqual(top2_row["n_available"], 2)
            self.assertEqual(json.loads(top2_row["selected_pitcher_ids_json"]), [501, 503])
            self.assertEqual(top2_row["topn_outs_season"], 12)
            self.assertEqual(top2_row["topn_outs_last3d"], 6)
            self.assertEqual(top2_row["topn_back_to_back_count"], 0)
            self.assertIsNotNone(top2_row["topn_k_minus_bb_rate_season"])
            self.assertIsNotNone(top2_row["quality_dropoff_vs_team"])
            self.assertEqual(top3_row["n_available"], 3)
            self.assertEqual(json.loads(top3_row["selected_pitcher_ids_json"]), [501, 503, 502])
            self.assertEqual(future_state_row["bullpen_appearances_season"], 4)
            self.assertEqual(checkpoint["status"], "success")
            self.assertGreaterEqual(checkpoint["attempts"], 2)
            self.assertEqual(json.loads(checkpoint["cursor_json"])["top_n_values"], [2, 3])

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
            with patch("scripts.history_ingest.fetch_schedule_bounded", return_value=schedule_rows):
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
            self.assertEqual(request_counts, [0, 0])
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

            def fake_schedule(*_policy, **kwargs):
                captured_kwargs.update(kwargs)
                return schedule_rows

            with patch("scripts.history_ingest.fetch_schedule_bounded", side_effect=fake_schedule):
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
            self.assertEqual(run["request_count"], 0)
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
            with patch(
                "scripts.history_ingest.fetch_schedule_bounded",
                side_effect=RuntimeError("boom"),
            ):
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
            self.assertEqual(run["request_count"], 0)
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

    def test_materialize_feature_rows_2020_keeps_null_starter_stats_valid(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2020
            parser = build_parser()
            args = parser.parse_args(["--db", str(db_path), "materialize-feature-rows", "--season", str(season)])

            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 6001,
                        "season": season,
                        "game_date": "2020-07-24",
                        "scheduled_datetime": "2020-07-24T23:05:00Z",
                        "status": "Final",
                        "home_team_id": 147,
                        "away_team_id": 121,
                        "home_score": 5,
                        "away_score": 3,
                        "winning_team_id": 147,
                    },
                )
                conn.execute(
                    """
                    INSERT INTO labels (game_id, did_home_win, home_score, away_score, run_differential, total_runs)
                    VALUES (6001, 1, 5, 3, 2, 8)
                    """
                )
                for payload in (
                    {"game_id": 6001, "team_id": 147, "side": "home", "hits": 9, "batting_avg": 0.281, "obp": 0.340, "ops": 0.790},
                    {"game_id": 6001, "team_id": 121, "side": "away", "hits": 7, "batting_avg": 0.245, "obp": 0.300, "ops": 0.680},
                ):
                    upsert_game_team_stats(conn, payload)
                for side, pitcher_id in (("home", 501), ("away", 502)):
                    upsert_game_pitcher_context(
                        conn,
                        {
                            "game_id": 6001,
                            "side": side,
                            "probable_pitcher_id": pitcher_id,
                            "probable_pitcher_name": f"{side} starter",
                            "probable_pitcher_known": 1,
                            "season_stats_scope": "season_to_date_prior_completed_games",
                            "season_stats_leakage_risk": 0,
                        },
                    )

            args.func(args)

            with connect_db(str(db_path)) as conn:
                row = conn.execute(
                    """
                    SELECT source_contract_status, source_contract_issues_json, feature_payload_json
                    FROM feature_rows
                    WHERE game_id=6001 AND feature_version='v1'
                    """
                ).fetchone()

            payload = json.loads(row["feature_payload_json"])
            self.assertEqual(row["source_contract_status"], "valid")
            self.assertIsNone(row["source_contract_issues_json"])
            self.assertEqual(payload["home_starter_known"], 1)
            self.assertEqual(payload["away_starter_known"], 1)
            self.assertEqual(payload["home_starter_stats_available"], 0)
            self.assertEqual(payload["away_starter_stats_available"], 0)

    def test_materialize_feature_rows_v2_phase1_blocks_when_pitcher_context_is_unsafe(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "materialize-feature-rows",
                    "--season",
                    str(season),
                    "--feature-version",
                    "v2_phase1",
                ]
            )

            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 6002,
                        "season": season,
                        "game_date": "2021-07-24",
                        "scheduled_datetime": "2021-07-24T23:05:00Z",
                        "status": "Final",
                        "home_team_id": 147,
                        "away_team_id": 121,
                        "home_score": 5,
                        "away_score": 3,
                        "winning_team_id": 147,
                    },
                )
                conn.execute(
                    """
                    INSERT INTO labels (game_id, did_home_win, home_score, away_score, run_differential, total_runs)
                    VALUES (6002, 1, 5, 3, 2, 8)
                    """
                )
                for payload in (
                    {"game_id": 6002, "team_id": 147, "side": "home", "hits": 9, "batting_avg": 0.281, "obp": 0.340, "ops": 0.790},
                    {"game_id": 6002, "team_id": 121, "side": "away", "hits": 7, "batting_avg": 0.245, "obp": 0.300, "ops": 0.680},
                ):
                    upsert_game_team_stats(conn, payload)
                for side, pitcher_id in (("home", 501), ("away", 502)):
                    upsert_game_pitcher_context(
                        conn,
                        {
                            "game_id": 6002,
                            "side": side,
                            "probable_pitcher_id": pitcher_id,
                            "probable_pitcher_name": f"{side} starter",
                            "probable_pitcher_known": 1,
                            "stats_source": "leakage_safe_null_fallback(probable_pitcher_identity_without_prior_completed_pitching)",
                            "season_stats_scope": "season_to_date_prior_completed_games",
                            "season_stats_leakage_risk": 0,
                        },
                    )

            with self.assertRaises(RuntimeError):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                row_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM feature_rows WHERE feature_version='v2_phase1'"
                ).fetchone()["c"]

            self.assertEqual(row_count, 0)

    def test_materialize_feature_rows_v2_phase1_includes_integrated_support_and_explicit_degradation(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "materialize-feature-rows",
                    "--season",
                    str(season),
                    "--feature-version",
                    "v2_phase1",
                ]
            )

            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game_id, game_date, venue_id, home_score, away_score in (
                    (7001, "2021-07-24", 5001, 5, 3),
                    (7002, "2021-07-25", 5001, 4, 2),
                ):
                    upsert_game(
                        conn,
                        {
                            "game_id": game_id,
                            "season": season,
                            "game_date": game_date,
                            "scheduled_datetime": f"{game_date}T23:05:00Z",
                            "status": "Final",
                            "venue_id": venue_id,
                            "home_team_id": 147,
                            "away_team_id": 121,
                            "home_score": home_score,
                            "away_score": away_score,
                            "winning_team_id": 147,
                        },
                    )
                conn.execute(
                    """
                    INSERT INTO labels (game_id, did_home_win, home_score, away_score, run_differential, total_runs)
                    VALUES
                      (7001, 1, 5, 3, 2, 8),
                      (7002, 1, 4, 2, 2, 6)
                    """
                )
                for payload in (
                    {"game_id": 7001, "team_id": 147, "side": "home", "runs": 5, "hits": 8, "batting_avg": 0.275, "obp": 0.341, "slg": 0.470, "ops": 0.811, "strikeouts": 7, "walks": 3},
                    {"game_id": 7001, "team_id": 121, "side": "away", "runs": 3, "hits": 6, "batting_avg": 0.240, "obp": 0.301, "slg": 0.390, "ops": 0.691, "strikeouts": 9, "walks": 2},
                    {"game_id": 7002, "team_id": 147, "side": "home", "runs": 4, "hits": 9, "batting_avg": 0.286, "obp": 0.352, "slg": 0.480, "ops": 0.832, "strikeouts": 6, "walks": 4},
                    {"game_id": 7002, "team_id": 121, "side": "away", "runs": 2, "hits": 5, "batting_avg": 0.220, "obp": 0.295, "slg": 0.360, "ops": 0.655, "strikeouts": 10, "walks": 2},
                ):
                    upsert_game_team_stats(conn, payload)
                for game_id, side, pitcher_id, era, whip in (
                    (7001, "home", 501, 3.1, 1.08),
                    (7001, "away", 502, 4.2, 1.22),
                    (7002, "home", 503, 2.9, 1.05),
                    (7002, "away", 504, 4.6, 1.28),
                ):
                    upsert_game_pitcher_context(
                        conn,
                        {
                            "game_id": game_id,
                            "side": side,
                            "pitcher_id": pitcher_id,
                            "probable_pitcher_id": pitcher_id,
                            "probable_pitcher_name": f"{side}-{pitcher_id}",
                            "probable_pitcher_known": 1,
                            "season_era": era,
                            "season_whip": whip,
                            "season_avg_allowed": 0.240,
                            "season_runs_per_9": era,
                            "season_strike_pct": 0.660,
                            "season_win_pct": 0.600,
                            "season_stats_scope": "season_to_date_prior_completed_games",
                            "season_stats_leakage_risk": 0,
                        },
                    )
                for pitcher_id, pitch_hand in ((501, "R"), (502, "L"), (503, "R"), (504, "L")):
                    upsert_player_handedness(
                        conn,
                        {
                            "player_id": pitcher_id,
                            "player_name": f"Pitcher {pitcher_id}",
                            "pitch_hand": pitch_hand,
                        },
                    )
                upsert_venue_dim(
                    conn,
                    {
                        "venue_id": 5001,
                        "venue_name": "Example Park",
                        "city": "Queens",
                        "state": "NY",
                        "country": "USA",
                        "timezone": "America/New_York",
                        "latitude": 40.75,
                        "longitude": -73.84,
                        "roof_type": "open",
                        "weather_exposure_default": 1,
                    },
                )
                for game_id, temperature_f, wind_speed_mph in (
                    (7001, 74.0, 11.0),
                    (7002, 88.0, 14.0),
                ):
                    upsert_game_weather_snapshot(
                        conn,
                        {
                            "game_id": game_id,
                            "venue_id": 5001,
                            "as_of_ts": f"{'2021-07-24' if game_id == 7001 else '2021-07-25'}T23:05:00Z",
                            "target_game_ts": f"{'2021-07-24' if game_id == 7001 else '2021-07-25'}T23:05:00Z",
                            "snapshot_type": "observed_archive",
                            "source": "open_meteo_archive",
                            "source_priority": 1,
                            "hour_offset_from_first_pitch": 0.0,
                            "temperature_f": temperature_f,
                            "humidity_pct": 55.0,
                            "pressure_hpa": 1012.0,
                            "precipitation_mm": 0.0,
                            "wind_speed_mph": wind_speed_mph,
                            "wind_gust_mph": wind_speed_mph + 3.0,
                            "wind_direction_deg": 180.0,
                            "weather_code": 1,
                            "cloud_cover_pct": 25.0,
                            "is_day": 1,
                            "weather_exposure_flag": 1,
                        },
                    )
                for game_id, side, team_id, era, whip, kbb, hr_rate, freshness, outs_last3d, pitches_last3d, back_to_back, high_usage in (
                    (7001, "home", 147, 3.20, 1.11, 0.180, 0.030, 0.81, 12, 44, 1, 0),
                    (7001, "away", 121, 4.05, 1.29, 0.120, 0.045, 0.52, 16, 61, 2, 1),
                    (7002, "home", 147, 3.05, 1.08, 0.190, 0.028, 0.77, 10, 40, 0, 0),
                ):
                    upsert_team_bullpen_game_state(
                        conn,
                        {
                            "game_id": game_id,
                            "team_id": team_id,
                            "side": side,
                            "as_of_ts": f"{'2021-07-24' if game_id == 7001 else '2021-07-25'}T23:05:00Z",
                            "season_games_in_sample": 20,
                            "bullpen_pitchers_in_sample": 6,
                            "bullpen_appearances_season": 60,
                            "bullpen_outs_season": 180,
                            "bullpen_era_season": era,
                            "bullpen_whip_season": whip,
                            "bullpen_runs_per_9_season": era,
                            "bullpen_k_rate_season": 0.250,
                            "bullpen_bb_rate_season": 0.070,
                            "bullpen_k_minus_bb_rate_season": kbb,
                            "bullpen_hr_rate_season": hr_rate,
                            "bullpen_outs_last3d": outs_last3d,
                            "bullpen_pitches_last3d": pitches_last3d,
                            "relievers_back_to_back_count": back_to_back,
                            "high_usage_relievers_last3d_count": high_usage,
                            "freshness_score": freshness,
                        },
                    )
                for game_id, side, team_id, kbb, freshness, dropoff, n_available in (
                    (7001, "home", 147, 0.220, 0.84, 0.040, 3),
                    (7001, "away", 121, 0.140, 0.48, -0.030, 3),
                    (7002, "home", 147, 0.210, 0.79, 0.030, 3),
                ):
                    upsert_team_bullpen_top_relievers(
                        conn,
                        {
                            "game_id": game_id,
                            "team_id": team_id,
                            "side": side,
                            "as_of_ts": f"{'2021-07-24' if game_id == 7001 else '2021-07-25'}T23:05:00Z",
                            "top_n": 3,
                            "n_available": n_available,
                            "selected_pitcher_ids_json": json.dumps([1, 2, 3]),
                            "topn_appearances_season": 30,
                            "topn_outs_season": 90,
                            "topn_era_season": 3.00,
                            "topn_whip_season": 1.05,
                            "topn_runs_per_9_season": 3.00,
                            "topn_k_rate_season": 0.280,
                            "topn_bb_rate_season": 0.060,
                            "topn_k_minus_bb_rate_season": kbb,
                            "topn_outs_last3d": 6,
                            "topn_pitches_last3d": 24,
                            "topn_appearances_last3d": 3,
                            "topn_back_to_back_count": 1,
                            "topn_freshness_score": freshness,
                            "quality_dropoff_vs_team": dropoff,
                        },
                    )
                for game_id, side, team_id, lineup_status, lineup_known_flag, announced_count, lefty_share, righty_share, balance, quality_mean, top3_quality, vs_rhp, vs_lhp in (
                    (7001, "home", 147, "full", 1, 9, 0.44, 0.44, 1.00, 0.610, 0.640, 0.590, 0.630),
                    (7001, "away", 121, "partial", 1, 7, 0.29, 0.57, 0.800, 0.560, 0.590, 0.540, 0.580),
                    (7002, "away", 121, "full", 1, 9, 0.33, 0.56, 0.890, 0.570, 0.600, 0.550, 0.590),
                ):
                    upsert_team_lineup_game_state(
                        conn,
                        {
                            "game_id": game_id,
                            "team_id": team_id,
                            "side": side,
                            "as_of_ts": f"{'2021-07-24' if game_id == 7001 else '2021-07-25'}T23:05:00Z",
                            "snapshot_type": "announced",
                            "lineup_status": lineup_status,
                            "lineup_known_flag": lineup_known_flag,
                            "announced_lineup_count": announced_count,
                            "lineup_l_count": 4,
                            "lineup_r_count": 4,
                            "lineup_s_count": 1,
                            "top3_l_count": 2,
                            "top3_r_count": 1,
                            "top3_s_count": 0,
                            "top5_l_count": 2,
                            "top5_r_count": 2,
                            "top5_s_count": 1,
                            "lineup_lefty_pa_share_proxy": lefty_share,
                            "lineup_righty_pa_share_proxy": righty_share,
                            "lineup_switch_pa_share_proxy": 0.11,
                            "lineup_balance_score": balance,
                            "lineup_quality_metric": "handedness_affinity_proxy_v1",
                            "lineup_quality_mean": quality_mean,
                            "top3_lineup_quality_mean": top3_quality,
                            "top5_lineup_quality_mean": quality_mean,
                            "lineup_vs_rhp_quality": vs_rhp,
                            "lineup_vs_lhp_quality": vs_lhp,
                        },
                    )
                for game_id, side, team_id, vs_pitch_hand, ops, runs_per_game, strikeout_rate, walk_rate, games_in_sample, plate_appearances in (
                    (7001, "home", 147, "L", 0.790, 4.80, 0.210, 0.090, 24, 810),
                    (7001, "away", 121, "R", 0.710, 3.90, 0.250, 0.070, 22, 760),
                    (7002, "away", 121, "R", 0.700, 3.80, 0.260, 0.065, 24, 820),
                ):
                    upsert_team_platoon_split(
                        conn,
                        {
                            "game_id": game_id,
                            "team_id": team_id,
                            "side": side,
                            "as_of_ts": f"{'2021-07-24' if game_id == 7001 else '2021-07-25'}T23:05:00Z",
                            "vs_pitch_hand": vs_pitch_hand,
                            "games_in_sample": games_in_sample,
                            "plate_appearances": plate_appearances,
                            "batting_avg": 0.255,
                            "obp": 0.330,
                            "slg": 0.430,
                            "ops": ops,
                            "runs_per_game": runs_per_game,
                            "strikeout_rate": strikeout_rate,
                            "walk_rate": walk_rate,
                        },
                    )

            args.func(args)

            with connect_db(str(db_path)) as conn:
                first_row = conn.execute(
                    """
                    SELECT source_contract_status, source_contract_issues_json, feature_payload_json
                    FROM feature_rows
                    WHERE game_id=7001 AND feature_version='v2_phase1'
                    """
                ).fetchone()
                second_row = conn.execute(
                    """
                    SELECT source_contract_status, source_contract_issues_json, feature_payload_json
                    FROM feature_rows
                    WHERE game_id=7002 AND feature_version='v2_phase1'
                    """
                ).fetchone()

            first_payload = json.loads(first_row["feature_payload_json"])
            second_payload = json.loads(second_row["feature_payload_json"])

            self.assertEqual(first_row["source_contract_status"], "valid")
            self.assertIsNone(first_row["source_contract_issues_json"])
            self.assertEqual(first_payload["home_bullpen_available_flag"], 1)
            self.assertEqual(first_payload["away_bullpen_available_flag"], 1)
            self.assertAlmostEqual(first_payload["bullpen_era_delta"], 0.85, places=3)
            self.assertAlmostEqual(first_payload["bullpen_freshness_delta"], 0.29, places=3)
            self.assertAlmostEqual(first_payload["lineup_vs_opp_hand_ops_delta"], 0.08, places=3)
            self.assertEqual(first_payload["weather_available_flag"], 1)
            self.assertEqual(first_payload["weather_observed_archive_flag"], 1)
            self.assertEqual(first_payload["windy_flag"], 0)
            self.assertEqual(first_payload["extreme_temp_flag"], 0)
            self.assertNotIn("precipitation_probability", first_payload)
            self.assertNotIn("precip_risk_flag", first_payload)

            self.assertEqual(second_row["source_contract_status"], "degraded")
            self.assertEqual(
                json.loads(second_row["source_contract_issues_json"]),
                ["away_bullpen_support_missing", "home_lineup_platoon_support_missing"],
            )
            self.assertEqual(second_payload["away_bullpen_available_flag"], 0)
            self.assertEqual(second_payload["home_lineup_known_flag"], 0)
            self.assertEqual(second_payload["home_platoon_available_flag"], 0)
            self.assertEqual(second_payload["weather_available_flag"], 1)
            self.assertEqual(second_payload["windy_flag"], 1)
            self.assertEqual(second_payload["extreme_temp_flag"], 1)
            self.assertNotIn("precipitation_probability", second_payload)
            self.assertNotIn("precip_risk_flag", second_payload)


class TestLineupSupport(unittest.TestCase):
    def test_build_game_lineup_snapshot_rows_extracts_order_and_handedness(self) -> None:
        game_row = {
            "game_id": 9001,
            "game_date": "2024-04-01",
            "scheduled_datetime": "2024-04-01T23:05:00Z",
            "home_team_id": 147,
            "away_team_id": 121,
        }
        boxscore = {
            "teams": {
                "home": {
                    "players": {
                        "ID11": {
                            "person": {"id": 11, "fullName": "Lead Off"},
                            "position": {"abbreviation": "CF"},
                            "batSide": {"code": "L"},
                            "pitchHand": {"code": "R"},
                        },
                        "ID12": {
                            "person": {"id": 12, "fullName": "Second"},
                            "position": {"abbreviation": "SS"},
                            "batSide": {"code": "S"},
                            "pitchHand": {"code": "R"},
                        },
                    },
                    "battingOrder": [11, 12],
                },
                "away": {"players": {}, "battingOrder": []},
            }
        }

        rows = build_game_lineup_snapshot_rows(game_row, boxscore, snapshot_type="announced")

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["player_id"], 11)
        self.assertEqual(rows[0]["batting_order"], 1)
        self.assertEqual(rows[0]["bat_side"], "L")
        self.assertEqual(rows[1]["bat_side"], "S")
        self.assertEqual(rows[1]["position_code"], "SS")
        self.assertEqual(rows[0]["snapshot_type"], "announced")
        self.assertEqual(rows[0]["lineup_status"], "partial")

    def test_build_game_lineup_snapshot_rows_uses_handedness_lookup_when_boxscore_omits_hands(self) -> None:
        game_row = {
            "game_id": 9002,
            "game_date": "2024-04-02",
            "scheduled_datetime": "2024-04-02T23:05:00Z",
            "home_team_id": 147,
            "away_team_id": 121,
        }
        boxscore = {
            "teams": {
                "home": {
                    "players": {
                        "ID21": {
                            "person": {"id": 21, "fullName": "Lookup Lead Off"},
                            "position": {"abbreviation": "CF"},
                        }
                    },
                    "battingOrder": [21],
                },
                "away": {"players": {}, "battingOrder": []},
            }
        }

        rows = build_game_lineup_snapshot_rows(
            game_row,
            boxscore,
            snapshot_type="announced",
            handedness_by_player={21: {"player_id": 21, "bat_side": "L", "pitch_hand": "R"}},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["bat_side"], "L")
        self.assertEqual(rows[0]["pitch_hand"], "R")

    def test_backfill_lineup_support_builds_lineup_and_prior_only_platoon_rows(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            season = 2021
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "--checkpoint-every",
                    "1",
                    "backfill-lineup-support",
                    "--season",
                    str(season),
                ]
            )

            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game in (
                    {
                        "game_id": 8101,
                        "season": season,
                        "game_date": "2021-04-01",
                        "scheduled_datetime": "2021-04-01T17:05:00Z",
                        "status": "Final",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                    {
                        "game_id": 8102,
                        "season": season,
                        "game_date": "2021-04-03",
                        "scheduled_datetime": "2021-04-03T18:10:00Z",
                        "status": "Scheduled",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                ):
                    upsert_game(conn, game)

                upsert_game_team_stats(
                    conn,
                    {
                        "game_id": 8101,
                        "team_id": 147,
                        "side": "home",
                        "runs": 6,
                        "hits": 9,
                        "batting_avg": 0.300,
                        "obp": 0.360,
                        "slg": 0.500,
                        "ops": 0.860,
                        "strikeouts": 8,
                        "walks": 3,
                    },
                )
                upsert_game_team_stats(
                    conn,
                    {
                        "game_id": 8101,
                        "team_id": 121,
                        "side": "away",
                        "runs": 2,
                        "hits": 6,
                        "batting_avg": 0.220,
                        "obp": 0.280,
                        "slg": 0.340,
                        "ops": 0.620,
                        "strikeouts": 10,
                        "walks": 2,
                    },
                )
                upsert_game_pitcher_context(
                    conn,
                    {
                        "game_id": 8101,
                        "side": "away",
                        "pitcher_id": 701,
                        "probable_pitcher_id": 701,
                        "probable_pitcher_name": "Righty Starter",
                        "probable_pitcher_known": 1,
                    },
                )
                upsert_game_pitcher_context(
                    conn,
                    {
                        "game_id": 8101,
                        "side": "home",
                        "pitcher_id": 702,
                        "probable_pitcher_id": 702,
                        "probable_pitcher_name": "Lefty Starter",
                        "probable_pitcher_known": 1,
                    },
                )

            historical_boxscores = {
                8101: {
                    "teams": {
                        "home": {
                            "players": {
                                **{
                                    f"ID{100 + idx}": {
                                        "person": {"id": 100 + idx, "fullName": f"Home {idx}"},
                                        "position": {"abbreviation": "1B"},
                                    }
                                    for idx in range(1, 10)
                                },
                                "ID702": {
                                    "person": {"id": 702, "fullName": "Lefty Starter"},
                                    "position": {"abbreviation": "P"},
                                },
                            },
                            "battingOrder": [101, 102, 103, 104, 105, 106, 107, 108, 109],
                        },
                        "away": {
                            "players": {
                                **{
                                    f"ID{200 + idx}": {
                                        "person": {"id": 200 + idx, "fullName": f"Away {idx}"},
                                        "position": {"abbreviation": "2B"},
                                    }
                                    for idx in range(1, 10)
                                },
                                "ID701": {
                                    "person": {"id": 701, "fullName": "Righty Starter"},
                                    "position": {"abbreviation": "P"},
                                },
                            },
                            "battingOrder": [201, 202, 203, 204, 205, 206, 207, 208, 209],
                        },
                    }
                },
                8102: {
                    "teams": {
                        "home": {
                            "players": {
                                **{
                                    f"ID{300 + idx}": {
                                        "person": {"id": 300 + idx, "fullName": f"Future Home {idx}"},
                                        "position": {"abbreviation": "LF"},
                                    }
                                    for idx in range(1, 10)
                                }
                            },
                            "battingOrder": [301, 302, 303, 304, 305, 306, 307, 308, 309],
                        },
                        "away": {
                            "players": {
                                **{
                                    f"ID{400 + idx}": {
                                        "person": {"id": 400 + idx, "fullName": f"Future Away {idx}"},
                                        "position": {"abbreviation": "RF"},
                                    }
                                    for idx in range(1, 10)
                                }
                            },
                            "battingOrder": [401, 402, 403, 404, 405, 406, 407, 408, 409],
                        },
                    }
                },
            }

            def fake_statsapi_get(endpoint, params):
                self.assertEqual(endpoint, "people")
                ids = {int(token) for token in str(params["personIds"]).split(",")}
                people = []
                for player_id in sorted(ids):
                    if 101 <= player_id <= 105:
                        bat_side = "L"
                        pitch_hand = "R"
                    elif 106 <= player_id <= 109:
                        bat_side = "R"
                        pitch_hand = "R"
                    elif 201 <= player_id <= 209:
                        bat_side = "R"
                        pitch_hand = "R"
                    elif 301 <= player_id <= 304:
                        bat_side = "L"
                        pitch_hand = "R"
                    elif 305 <= player_id <= 309:
                        bat_side = "R"
                        pitch_hand = "R"
                    elif 401 <= player_id <= 404:
                        bat_side = "R"
                        pitch_hand = "L"
                    elif 405 <= player_id <= 409:
                        bat_side = "L"
                        pitch_hand = "L"
                    elif player_id == 701:
                        bat_side = "R"
                        pitch_hand = "R"
                    elif player_id == 702:
                        bat_side = "L"
                        pitch_hand = "L"
                    else:
                        raise AssertionError(f"unexpected player id {player_id}")
                    people.append(
                        {
                            "id": player_id,
                            "fullName": f"Player {player_id}",
                            "batSide": {"code": bat_side},
                            "pitchHand": {"code": pitch_hand},
                            "primaryPosition": {"abbreviation": "P" if player_id in {701, 702} else "1B"},
                        }
                    )
                return {"people": people}

            stub_statsapi = types.SimpleNamespace(get=fake_statsapi_get)
            with patch("scripts.history_ingest.fetch_game_boxscore_bounded", side_effect=lambda game_id, *_: historical_boxscores[game_id]):
                with patch("scripts.history_ingest.statsapi", stub_statsapi):
                    args.func(args)
                    args.func(args)

            with connect_db(str(db_path)) as conn:
                lineup_row = conn.execute(
                    """
                    SELECT lineup_known_flag, announced_lineup_count, lineup_l_count, lineup_r_count,
                           lineup_quality_metric, lineup_vs_rhp_quality, lineup_vs_lhp_quality
                    FROM team_lineup_game_state
                    WHERE game_id=8102 AND side='home'
                    """
                ).fetchone()
                platoon_row = conn.execute(
                    """
                    SELECT games_in_sample, plate_appearances, batting_avg, obp, slg, ops, runs_per_game
                    FROM team_platoon_splits
                    WHERE game_id=8102 AND side='home' AND vs_pitch_hand='R'
                    """
                ).fetchone()
                platoon_l_row = conn.execute(
                    """
                    SELECT games_in_sample
                    FROM team_platoon_splits
                    WHERE game_id=8102 AND side='home' AND vs_pitch_hand='L'
                    """
                ).fetchone()
                snapshot_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM game_lineup_snapshots WHERE game_id=8102 AND side='home'"
                ).fetchone()["c"]
                handedness_row = conn.execute(
                    "SELECT pitch_hand FROM player_handedness_dim WHERE player_id=701"
                ).fetchone()
                checkpoint = conn.execute(
                    """
                    SELECT status, attempts, cursor_json
                    FROM ingestion_checkpoints
                    WHERE job_name='lineup-support-2021' AND partition_key='season=2021'
                    """
                ).fetchone()

            self.assertEqual(lineup_row["lineup_known_flag"], 1)
            self.assertEqual(lineup_row["announced_lineup_count"], 9)
            self.assertEqual(lineup_row["lineup_l_count"], 4)
            self.assertEqual(lineup_row["lineup_r_count"], 5)
            self.assertEqual(lineup_row["lineup_quality_metric"], "handedness_affinity_proxy_v1")
            self.assertAlmostEqual(lineup_row["lineup_vs_rhp_quality"], 0.444, places=3)
            self.assertAlmostEqual(lineup_row["lineup_vs_lhp_quality"], 0.556, places=3)
            self.assertEqual(platoon_row["games_in_sample"], 1)
            self.assertEqual(platoon_l_row["games_in_sample"], 0)
            self.assertEqual(platoon_row["plate_appearances"], 33)
            self.assertAlmostEqual(platoon_row["batting_avg"], 0.3, places=3)
            self.assertAlmostEqual(platoon_row["ops"], 0.86, places=3)
            self.assertAlmostEqual(platoon_row["runs_per_game"], 6.0, places=3)
            self.assertEqual(snapshot_count, 9)
            self.assertEqual(handedness_row["pitch_hand"], "R")
            self.assertEqual(checkpoint["status"], "success")
            self.assertGreaterEqual(checkpoint["attempts"], 2)
            self.assertEqual(json.loads(checkpoint["cursor_json"])["team_platoon_splits_rows_upserted"], 8)

    def test_update_lineup_support_builds_missing_rows_when_lineup_unavailable(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--db",
                    str(db_path),
                    "update-lineup-support",
                    "--date",
                    "2021-04-05",
                ]
            )

            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                upsert_game(
                    conn,
                    {
                        "game_id": 8201,
                        "season": 2021,
                        "game_date": "2021-04-05",
                        "scheduled_datetime": "2021-04-05T19:10:00Z",
                        "status": "Scheduled",
                        "home_team_id": 147,
                        "away_team_id": 121,
                    },
                )
                upsert_player_handedness(
                    conn,
                    {
                        "player_id": 900,
                        "player_name": "Known Starter",
                        "pitch_hand": "L",
                    },
                )

            with patch("scripts.history_ingest.fetch_game_boxscore_bounded", return_value={"teams": {"home": {"players": {}}, "away": {"players": {}}}}):
                args.func(args)

            with connect_db(str(db_path)) as conn:
                home_row = conn.execute(
                    """
                    SELECT lineup_known_flag, lineup_status, announced_lineup_count, lineup_quality_metric
                    FROM team_lineup_game_state
                    WHERE game_id=8201 AND side='home'
                    """
                ).fetchone()
                platoon_row = conn.execute(
                    """
                    SELECT games_in_sample
                    FROM team_platoon_splits
                    WHERE game_id=8201 AND side='home' AND vs_pitch_hand='R'
                    """
                ).fetchone()
                checkpoint = conn.execute(
                    """
                    SELECT status
                    FROM ingestion_checkpoints
                    WHERE job_name='lineup-support-incremental' AND partition_key='date=2021-04-05'
                    """
                ).fetchone()

            self.assertEqual(home_row["lineup_known_flag"], 0)
            self.assertEqual(home_row["lineup_status"], "missing")
            self.assertEqual(home_row["announced_lineup_count"], 0)
            self.assertEqual(home_row["lineup_quality_metric"], "unavailable__player_offense_support_not_built")
            self.assertEqual(platoon_row["games_in_sample"], 0)
            self.assertEqual(checkpoint["status"], "success")

    def test_build_support_coverage_report_surfaces_residual_gaps_and_missing_v2_materialization(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "history.db"
            with connect_db(str(db_path)) as conn:
                ensure_schema(conn)
                for game_id, season, game_date, status, venue_id in (
                    (9001, 2020, "2020-08-04", "Postponed", 2394),
                    (9002, 2023, "2023-04-29", "Final", 5340),
                    (9003, 2023, "2023-04-30", "Final", 5340),
                ):
                    upsert_game(
                        conn,
                        {
                            "game_id": game_id,
                            "season": season,
                            "game_date": game_date,
                            "scheduled_datetime": f"{game_date}T22:05:00Z",
                            "status": status,
                            "venue_id": venue_id,
                            "home_team_id": 147,
                            "away_team_id": 121,
                        },
                    )
                upsert_venue_dim(
                    conn,
                    {
                        "venue_id": 2394,
                        "venue_name": "Comerica Park",
                        "city": "Detroit",
                        "state": "MI",
                        "country": "USA",
                        "timezone": "America/Detroit",
                        "latitude": 42.339,
                        "longitude": -83.0485,
                        "roof_type": "open",
                        "weather_exposure_default": 1,
                    },
                )
                upsert_venue_dim(
                    conn,
                    {
                        "venue_id": 5340,
                        "venue_name": "Estadio Alfredo Harp Helu",
                        "city": "Mexico City",
                        "state": "CMX",
                        "country": "MEX",
                        "timezone": "America/Mexico_City",
                        "latitude": 19.404,
                        "longitude": -99.0855,
                        "roof_type": "open",
                        "weather_exposure_default": 1,
                    },
                )
                for side, team_id in (("home", 147), ("away", 121)):
                    upsert_team_lineup_game_state(
                        conn,
                        {
                            "game_id": 9001,
                            "team_id": team_id,
                            "side": side,
                            "as_of_ts": "2020-08-04T22:05:00Z",
                            "snapshot_type": "fallback",
                            "lineup_status": "missing",
                            "lineup_known_flag": 0,
                            "announced_lineup_count": 0,
                            "lineup_quality_metric": "unavailable__player_offense_support_not_built",
                        },
                    )
                for game_id, game_date in ((9002, "2023-04-29"), (9003, "2023-04-30")):
                    for order, side, team_id in ((1, "home", 147), (1, "away", 121)):
                        conn.execute(
                            """
                            INSERT INTO game_lineup_snapshots (
                              game_id, team_id, side, as_of_ts, snapshot_type, lineup_status, player_id, batting_order
                            )
                            VALUES (?, ?, ?, ?, 'fallback', 'full', ?, ?)
                            """,
                            (game_id, team_id, side, f"{game_date}T22:05:00Z", game_id + 100 + order, order),
                        )
                upsert_game_weather_snapshot(
                    conn,
                    {
                        "game_id": 9001,
                        "venue_id": 2394,
                        "as_of_ts": "2020-08-04T22:05:00Z",
                        "target_game_ts": "2020-08-04T22:05:00Z",
                        "snapshot_type": "observed_archive",
                        "source": "open_meteo_archive",
                        "source_priority": 1,
                        "hour_offset_from_first_pitch": 0.0,
                        "temperature_f": 75.0,
                        "humidity_pct": 50.0,
                        "pressure_hpa": 1013.0,
                        "precipitation_mm": 0.0,
                        "wind_speed_mph": 10.0,
                        "wind_gust_mph": 12.0,
                        "wind_direction_deg": 180.0,
                        "weather_code": 1,
                        "cloud_cover_pct": 20.0,
                        "is_day": 1,
                        "weather_exposure_flag": 1,
                    },
                )

                report = build_support_coverage_report(conn, feature_version="v2_phase1")

            by_season = {item["season"]: item for item in report["by_season"]}
            self.assertEqual(by_season[2020]["lineup_snapshot_missing_games"], 1)
            self.assertEqual(by_season[2020]["lineup_snapshot_completed_coverage"], 1.0)
            self.assertEqual(by_season[2020]["integrated_feature_rows"], 0)
            self.assertEqual(by_season[2023]["weather_missing_games"], 2)
            self.assertEqual(by_season[2023]["weather_completed_coverage"], 0.0)
            self.assertEqual(by_season[2023]["integrated_feature_missing_games"], 2)

            lineup_gap = report["missing_games"]["lineup_snapshot"][0]
            self.assertEqual(lineup_gap["game_id"], 9001)
            self.assertEqual(lineup_gap["reason"], "non_completed_game_status_postponed")
            self.assertEqual(lineup_gap["lineup_state_team_rows"], 2)

            weather_gaps = {item["game_id"]: item for item in report["missing_games"]["weather"]}
            self.assertEqual(weather_gaps[9002]["reason"], "completed_game_missing_weather_snapshot")
            self.assertEqual(weather_gaps[9002]["venue_name"], "Estadio Alfredo Harp Helu")
            self.assertEqual(weather_gaps[9003]["reason"], "completed_game_missing_weather_snapshot")


if __name__ == "__main__":
    unittest.main()
