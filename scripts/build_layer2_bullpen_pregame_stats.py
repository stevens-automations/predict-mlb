#!/usr/bin/env python3
"""Build the Layer 2 bullpen_pregame_stats table."""

from __future__ import annotations

import sqlite3
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "mlb_history.db"
TABLE_NAME = "bullpen_pregame_stats"
SEASONS = tuple(range(2020, 2026))
INSERT_SQL = f"""
    INSERT OR REPLACE INTO {TABLE_NAME} (
      game_id,
      side,
      season,
      season_bullpen_era,
      season_bullpen_whip,
      season_bullpen_k_pct,
      season_bullpen_bb_pct,
      season_bullpen_hr_per_9,
      season_appearances,
      bullpen_outs_last1d,
      bullpen_outs_last3d,
      bullpen_outs_last5d,
      bullpen_pitches_last1d,
      bullpen_pitches_last3d,
      relievers_used_last3d_count,
      high_usage_relievers_last3d,
      back_to_back_relievers_count,
      computed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass(slots=True)
class RelieverAppearance:
    pitcher_id: int
    innings_pitched: float
    batters_faced: int
    pitches: int
    hits: int
    walks: int
    strikeouts: int
    earned_runs: int
    home_runs: int
    outs_recorded: int


@dataclass(slots=True)
class BullpenGameUsage:
    game_date: date
    outs_recorded: int = 0
    pitches: int = 0
    reliever_ids: set[int] = field(default_factory=set)
    high_usage_reliever_ids: set[int] = field(default_factory=set)

    def add_appearance(self, appearance: RelieverAppearance) -> None:
        self.outs_recorded += appearance.outs_recorded
        self.pitches += appearance.pitches
        self.reliever_ids.add(appearance.pitcher_id)
        if appearance.pitches > 20:
            self.high_usage_reliever_ids.add(appearance.pitcher_id)


@dataclass(slots=True)
class TeamBullpenSeasonState:
    appearances: int = 0
    innings_pitched: float = 0.0
    batters_faced: int = 0
    pitches: int = 0
    hits: int = 0
    walks: int = 0
    strikeouts: int = 0
    earned_runs: int = 0
    home_runs: int = 0
    recent_games: deque[BullpenGameUsage] = field(default_factory=deque)

    def apply_appearance(self, appearance: RelieverAppearance) -> None:
        self.appearances += 1
        self.innings_pitched += appearance.innings_pitched
        self.batters_faced += appearance.batters_faced
        self.pitches += appearance.pitches
        self.hits += appearance.hits
        self.walks += appearance.walks
        self.strikeouts += appearance.strikeouts
        self.earned_runs += appearance.earned_runs
        self.home_runs += appearance.home_runs

    def append_recent_game(self, usage: BullpenGameUsage) -> None:
        self.recent_games.append(usage)


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 60000")
    return conn


def create_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
          game_id INTEGER NOT NULL,
          side TEXT NOT NULL CHECK(side IN ('home', 'away')),
          season INTEGER NOT NULL,
          season_bullpen_era REAL,
          season_bullpen_whip REAL,
          season_bullpen_k_pct REAL,
          season_bullpen_bb_pct REAL,
          season_bullpen_hr_per_9 REAL,
          season_appearances INTEGER,
          bullpen_outs_last1d INTEGER,
          bullpen_outs_last3d INTEGER,
          bullpen_outs_last5d INTEGER,
          bullpen_pitches_last1d INTEGER,
          bullpen_pitches_last3d INTEGER,
          relievers_used_last3d_count INTEGER,
          high_usage_relievers_last3d INTEGER,
          back_to_back_relievers_count INTEGER,
          computed_at TEXT,
          PRIMARY KEY (game_id, side),
          FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


def to_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def to_float(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def safe_divide(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def parse_game_date(value: str) -> date:
    return date.fromisoformat(value)


def fetch_games(conn: sqlite3.Connection, season: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT game_id, season, game_date, scheduled_datetime, status, home_team_id, away_team_id
        FROM games
        WHERE season = ?
        ORDER BY game_date, COALESCE(scheduled_datetime, ''), game_id
        """,
        (season,),
    ).fetchall()


def fetch_reliever_appearances(
    conn: sqlite3.Connection, season: int
) -> dict[tuple[int, str], list[RelieverAppearance]]:
    rows = conn.execute(
        """
        SELECT
          g.game_id,
          gpa.side,
          gpa.pitcher_id,
          gpa.innings_pitched,
          gpa.batters_faced,
          gpa.pitches,
          gpa.hits,
          gpa.walks,
          gpa.strikeouts,
          gpa.earned_runs,
          gpa.home_runs,
          gpa.outs_recorded
        FROM games g
        INNER JOIN game_pitcher_appearances gpa
          ON gpa.game_id = g.game_id
        WHERE g.season = ?
          AND g.status = 'Final'
          AND gpa.is_reliever = 1
        ORDER BY g.game_date, COALESCE(g.scheduled_datetime, ''), g.game_id, gpa.side, gpa.appearance_order, gpa.pitcher_id
        """,
        (season,),
    ).fetchall()

    appearances_by_game_side: dict[tuple[int, str], list[RelieverAppearance]] = {}
    for row in rows:
        key = (int(row["game_id"]), str(row["side"]))
        appearances_by_game_side.setdefault(key, []).append(
            RelieverAppearance(
                pitcher_id=int(row["pitcher_id"]),
                innings_pitched=to_float(row["innings_pitched"]),
                batters_faced=to_int(row["batters_faced"]),
                pitches=to_int(row["pitches"]),
                hits=to_int(row["hits"]),
                walks=to_int(row["walks"]),
                strikeouts=to_int(row["strikeouts"]),
                earned_runs=to_int(row["earned_runs"]),
                home_runs=to_int(row["home_runs"]),
                outs_recorded=to_int(row["outs_recorded"]),
            )
        )
    return appearances_by_game_side


def build_recent_metrics(state: TeamBullpenSeasonState, target_date: date) -> dict[str, int]:
    cutoff_5d = target_date.toordinal() - 5
    while state.recent_games and state.recent_games[0].game_date.toordinal() < cutoff_5d:
        state.recent_games.popleft()

    yesterday = target_date.toordinal() - 1
    two_days_ago = target_date.toordinal() - 2
    three_days_ago = target_date.toordinal() - 3

    outs_last1d = 0
    outs_last3d = 0
    outs_last5d = 0
    pitches_last1d = 0
    pitches_last3d = 0
    relievers_used_last3d: set[int] = set()
    high_usage_last3d: set[int] = set()
    yesterday_relievers: set[int] = set()
    two_days_ago_relievers: set[int] = set()

    for usage in state.recent_games:
        ordinal = usage.game_date.toordinal()
        if ordinal == yesterday:
            outs_last1d += usage.outs_recorded
            pitches_last1d += usage.pitches
            yesterday_relievers.update(usage.reliever_ids)
        if three_days_ago <= ordinal < target_date.toordinal():
            outs_last3d += usage.outs_recorded
            pitches_last3d += usage.pitches
            relievers_used_last3d.update(usage.reliever_ids)
            high_usage_last3d.update(usage.high_usage_reliever_ids)
        if cutoff_5d <= ordinal < target_date.toordinal():
            outs_last5d += usage.outs_recorded
        if ordinal == two_days_ago:
            two_days_ago_relievers.update(usage.reliever_ids)

    return {
        "bullpen_outs_last1d": outs_last1d,
        "bullpen_outs_last3d": outs_last3d,
        "bullpen_outs_last5d": outs_last5d,
        "bullpen_pitches_last1d": pitches_last1d,
        "bullpen_pitches_last3d": pitches_last3d,
        "relievers_used_last3d_count": len(relievers_used_last3d),
        "high_usage_relievers_last3d": len(high_usage_last3d),
        "back_to_back_relievers_count": len(yesterday_relievers & two_days_ago_relievers),
    }


def build_row(
    *,
    game: sqlite3.Row,
    side: str,
    state: TeamBullpenSeasonState | None,
    computed_at: str,
) -> tuple[Any, ...]:
    if state is None or state.appearances == 0:
        season_bullpen_era = None
        season_bullpen_whip = None
        season_bullpen_k_pct = None
        season_bullpen_bb_pct = None
        season_bullpen_hr_per_9 = None
        season_appearances = None
        recent_metrics = {
            "bullpen_outs_last1d": 0,
            "bullpen_outs_last3d": 0,
            "bullpen_outs_last5d": 0,
            "bullpen_pitches_last1d": 0,
            "bullpen_pitches_last3d": 0,
            "relievers_used_last3d_count": 0,
            "high_usage_relievers_last3d": 0,
            "back_to_back_relievers_count": 0,
        }
    else:
        season_bullpen_era = safe_divide(9.0 * state.earned_runs, state.innings_pitched)
        season_bullpen_whip = safe_divide(state.walks + state.hits, state.innings_pitched)
        season_bullpen_k_pct = safe_divide(state.strikeouts, state.batters_faced)
        season_bullpen_bb_pct = safe_divide(state.walks, state.batters_faced)
        season_bullpen_hr_per_9 = safe_divide(9.0 * state.home_runs, state.innings_pitched)
        season_appearances = state.appearances
        recent_metrics = build_recent_metrics(state, parse_game_date(str(game["game_date"])))

    return (
        int(game["game_id"]),
        side,
        int(game["season"]),
        season_bullpen_era,
        season_bullpen_whip,
        season_bullpen_k_pct,
        season_bullpen_bb_pct,
        season_bullpen_hr_per_9,
        season_appearances,
        recent_metrics["bullpen_outs_last1d"],
        recent_metrics["bullpen_outs_last3d"],
        recent_metrics["bullpen_outs_last5d"],
        recent_metrics["bullpen_pitches_last1d"],
        recent_metrics["bullpen_pitches_last3d"],
        recent_metrics["relievers_used_last3d_count"],
        recent_metrics["high_usage_relievers_last3d"],
        recent_metrics["back_to_back_relievers_count"],
        computed_at,
    )


def process_season(conn: sqlite3.Connection, season: int, computed_at: str) -> int:
    games = fetch_games(conn, season)
    reliever_appearances = fetch_reliever_appearances(conn, season)
    season_states: dict[int, TeamBullpenSeasonState] = {}
    rows_to_upsert: list[tuple[Any, ...]] = []

    index = 0
    while index < len(games):
        current_date = str(games[index]["game_date"])
        day_games: list[sqlite3.Row] = []
        while index < len(games) and str(games[index]["game_date"]) == current_date:
            day_games.append(games[index])
            index += 1

        pending_updates: list[tuple[int, list[RelieverAppearance], BullpenGameUsage]] = []
        for game in day_games:
            for side, team_key in (("home", "home_team_id"), ("away", "away_team_id")):
                team_id = game[team_key]
                if team_id is None:
                    continue
                rows_to_upsert.append(
                    build_row(
                        game=game,
                        side=side,
                        state=season_states.get(int(team_id)),
                        computed_at=computed_at,
                    )
                )

            if str(game["status"]) != "Final":
                continue

            game_date = parse_game_date(str(game["game_date"]))
            game_id = int(game["game_id"])
            for side, team_key in (("home", "home_team_id"), ("away", "away_team_id")):
                team_id = game[team_key]
                if team_id is None:
                    continue
                appearances = reliever_appearances.get((game_id, side), [])
                usage = BullpenGameUsage(game_date=game_date)
                for appearance in appearances:
                    usage.add_appearance(appearance)
                pending_updates.append((int(team_id), appearances, usage))

        for team_id, appearances, usage in pending_updates:
            state = season_states.setdefault(team_id, TeamBullpenSeasonState())
            for appearance in appearances:
                state.apply_appearance(appearance)
            state.append_recent_game(usage)

    conn.executemany(INSERT_SQL, rows_to_upsert)
    conn.commit()
    return len(rows_to_upsert)


def print_verification(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE game_id = 661199
          AND side = 'away'
        """
    ).fetchone()
    print("Verification row for game_id=661199, side='away':")
    if row is None:
        print("  <missing>")
        return
    for key in row.keys():
        print(f"  {key}: {row[key]}")


def build_table() -> int:
    computed_at = datetime.now(timezone.utc).isoformat()
    with connect_db() as conn:
        create_table(conn)
        total_rows = 0
        for season in SEASONS:
            print(f"Processing season {season}...")
            rows = process_season(conn, season, computed_at)
            total_rows += rows
            print(f"Season {season}: upserted {rows} rows.")
        total_count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(f"Finished {TABLE_NAME}: processed {total_rows} season rows, table count={total_count}.")
        print_verification(conn)
    return 0


if __name__ == "__main__":
    raise SystemExit(build_table())
