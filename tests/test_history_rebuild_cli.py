from __future__ import annotations

import argparse
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.history_ingest import (
    REBUILD_STAGE_BASE,
    REBUILD_STAGE_BULLPEN_SUPPORT,
    REBUILD_STAGE_FEATURE_ROWS,
    REBUILD_STAGE_LINEUP_SUPPORT,
    REBUILD_STAGE_ORDER,
    REBUILD_STAGE_PITCHER_APPEARANCES,
    REBUILD_STAGE_PITCHER_CONTEXT,
    REBUILD_STAGE_TEAM_STATS,
    REBUILD_STAGE_VENUES,
    REBUILD_STAGE_WEATHER,
    _rebuild_selected_seasons,
    _rebuild_selected_stages,
    build_parser,
    cmd_rebuild_history,
)


class TestHistoryRebuildHelpers(unittest.TestCase):
    def test_rebuild_selected_stages_defaults_to_full_order(self) -> None:
        self.assertEqual(_rebuild_selected_stages(["all"]), list(REBUILD_STAGE_ORDER))

    def test_rebuild_selected_stages_normalizes_to_canonical_order(self) -> None:
        selected = _rebuild_selected_stages([
            REBUILD_STAGE_FEATURE_ROWS,
            REBUILD_STAGE_BASE,
            REBUILD_STAGE_WEATHER,
            REBUILD_STAGE_BASE,
        ])
        self.assertEqual(
            selected,
            [REBUILD_STAGE_BASE, REBUILD_STAGE_WEATHER, REBUILD_STAGE_FEATURE_ROWS],
        )

    def test_rebuild_selected_stages_rejects_all_plus_explicit_stage(self) -> None:
        with self.assertRaises(ValueError):
            _rebuild_selected_stages(["all", REBUILD_STAGE_BASE])

    def test_rebuild_selected_seasons_uses_explicit_single_season(self) -> None:
        args = argparse.Namespace(season=2024, season_start=2020, season_end=2025)
        self.assertEqual(_rebuild_selected_seasons(args), [2024])

    def test_rebuild_selected_seasons_builds_inclusive_range(self) -> None:
        args = argparse.Namespace(season=None, season_start=2022, season_end=2024)
        self.assertEqual(_rebuild_selected_seasons(args), [2022, 2023, 2024])

    def test_rebuild_selected_seasons_rejects_inverted_range(self) -> None:
        args = argparse.Namespace(season=None, season_start=2025, season_end=2024)
        with self.assertRaises(ValueError):
            _rebuild_selected_seasons(args)


class TestHistoryRebuildCli(unittest.TestCase):
    def test_parser_exposes_safe_rebuild_command(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["rebuild-history", "--season", "2024"])
        self.assertEqual(args.command, "rebuild-history")
        self.assertEqual(args.stages, ["all"])
        self.assertEqual(args.season, 2024)
        self.assertIs(args.func, cmd_rebuild_history)

    def test_rebuild_history_formatter_strings_are_present(self) -> None:
        parser = build_parser()
        rebuild_parser = next(
            action.choices["rebuild-history"]
            for action in parser._actions
            if hasattr(action, "choices") and action.choices
        )
        help_text = rebuild_parser.format_help()
        self.assertIn("This command never deletes or", help_text)
        self.assertIn("replaces the target DB.", help_text)
        self.assertIn("Prefer this command for reproducible multi-stage", help_text)
        self.assertIn("Explicit selections are normalized into", help_text)
        self.assertIn("canonical execution order.", help_text)

    def test_rebuild_history_orchestrates_requested_stages_per_season(self) -> None:
        parser = build_parser()
        with TemporaryDirectory() as td:
            db_path = f"{td}/scratch.db"
            args = parser.parse_args(
                [
                    "--db",
                    db_path,
                    "rebuild-history",
                    "--season-start",
                    "2023",
                    "--season-end",
                    "2024",
                    "--stages",
                    REBUILD_STAGE_BASE,
                    REBUILD_STAGE_TEAM_STATS,
                    REBUILD_STAGE_PITCHER_CONTEXT,
                    REBUILD_STAGE_PITCHER_APPEARANCES,
                    REBUILD_STAGE_BULLPEN_SUPPORT,
                    REBUILD_STAGE_LINEUP_SUPPORT,
                    REBUILD_STAGE_VENUES,
                    REBUILD_STAGE_WEATHER,
                    REBUILD_STAGE_FEATURE_ROWS,
                ]
            )

            calls: list[tuple[str, int]] = []

            def _record(name: str):
                def inner(stage_args):
                    calls.append((name, stage_args.season))
                return inner

            with patch("scripts.history_ingest.cmd_backfill", side_effect=_record("base")):
                with patch("scripts.history_ingest.cmd_backfill_team_stats", side_effect=_record("team-stats")):
                    with patch("scripts.history_ingest.cmd_backfill_pitcher_context", side_effect=_record("pitcher-context")):
                        with patch("scripts.history_ingest.cmd_backfill_pitcher_appearances", side_effect=_record("pitcher-appearances")):
                            with patch("scripts.history_ingest.cmd_backfill_bullpen_support", side_effect=_record("bullpen-support")):
                                with patch("scripts.history_ingest.cmd_backfill_lineup_support", side_effect=_record("lineup-support")):
                                    with patch("scripts.history_ingest.cmd_sync_venues", side_effect=_record("venues")):
                                        with patch("scripts.history_ingest.cmd_backfill_game_weather", side_effect=_record("weather")):
                                            with patch("scripts.history_ingest.cmd_materialize_feature_rows", side_effect=_record("feature-rows")):
                                                args.func(args)

        self.assertEqual(
            calls,
            [
                ("base", 2023),
                ("team-stats", 2023),
                ("pitcher-context", 2023),
                ("pitcher-appearances", 2023),
                ("bullpen-support", 2023),
                ("lineup-support", 2023),
                ("venues", 2023),
                ("weather", 2023),
                ("feature-rows", 2023),
                ("base", 2024),
                ("team-stats", 2024),
                ("pitcher-context", 2024),
                ("pitcher-appearances", 2024),
                ("bullpen-support", 2024),
                ("lineup-support", 2024),
                ("venues", 2024),
                ("weather", 2024),
                ("feature-rows", 2024),
            ],
        )


if __name__ == "__main__":
    unittest.main()
