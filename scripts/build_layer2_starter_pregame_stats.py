#!/usr/bin/env python3
"""Build the Layer 2 starter_pregame_stats table."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "mlb_history.db"
TABLE_NAME = "starter_pregame_stats"
SEASONS = tuple(range(2020, 2026))
INSERT_SQL = f"""
    INSERT OR REPLACE INTO {TABLE_NAME} (
      game_id,
      side,
      season,
      probable_pitcher_id,
      probable_pitcher_known,
      pitcher_hand,
      season_starts,
      season_era,
      season_whip,
      season_k_pct,
      season_bb_pct,
      season_hr_per_9,
      season_avg_allowed,
      season_strike_pct,
      season_win_pct,
      stats_available_flag,
      computed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass(slots=True)
class StarterAppearance:
    game_id: int
    side: str
    season: int
    game_date: str
    status: str | None
    pitcher_id: int
    pitcher_hand: str | None
    innings_pitched: float
    batters_faced: int
    pitches: int
    strikes: int
    hits: int
    walks: int
    strikeouts: int
    earned_runs: int
    home_runs: int
    win: int


@dataclass(slots=True)
class PitcherSeasonState:
    starts: int = 0
    wins: int = 0
    innings_pitched: float = 0.0
    batters_faced: int = 0
    pitches: int = 0
    strikes: int = 0
    hits: int = 0
    walks: int = 0
    strikeouts: int = 0
    earned_runs: int = 0
    home_runs: int = 0

    def apply(self, appearance: StarterAppearance) -> None:
        self.starts += 1
        self.wins += appearance.win
        self.innings_pitched += appearance.innings_pitched
        self.batters_faced += appearance.batters_faced
        self.pitches += appearance.pitches
        self.strikes += appearance.strikes
        self.hits += appearance.hits
        self.walks += appearance.walks
        self.strikeouts += appearance.strikeouts
        self.earned_runs += appearance.earned_runs
        self.home_runs += appearance.home_runs


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
          probable_pitcher_id INTEGER,
          probable_pitcher_known INTEGER DEFAULT 0,
          pitcher_hand TEXT,
          season_starts INTEGER,
          season_era REAL,
          season_whip REAL,
          season_k_pct REAL,
          season_bb_pct REAL,
          season_hr_per_9 REAL,
          season_avg_allowed REAL,
          season_strike_pct REAL,
          season_win_pct REAL,
          stats_available_flag INTEGER DEFAULT 0,
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


def fetch_games(conn: sqlite3.Connection, season: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT game_id, season, game_date, scheduled_datetime, status
        FROM games
        WHERE season = ?
        ORDER BY game_date, COALESCE(scheduled_datetime, ''), game_id
        """,
        (season,),
    ).fetchall()


def fetch_starter_appearances(
    conn: sqlite3.Connection, season: int
) -> dict[tuple[int, str], StarterAppearance]:
    rows = conn.execute(
        """
        SELECT
          g.game_id,
          g.season,
          g.game_date,
          g.status,
          g.home_team_id,
          g.away_team_id,
          g.home_score,
          g.away_score,
          g.winning_team_id,
          gpa.side,
          gpa.pitcher_id,
          phd.pitch_hand,
          gpa.innings_pitched,
          gpa.batters_faced,
          gpa.pitches,
          gpa.strikes,
          gpa.hits,
          gpa.walks,
          gpa.strikeouts,
          gpa.earned_runs,
          gpa.home_runs
        FROM games g
        INNER JOIN game_pitcher_appearances gpa
          ON gpa.game_id = g.game_id
        LEFT JOIN player_handedness_dim phd
          ON phd.player_id = gpa.pitcher_id
        WHERE g.season = ?
          AND gpa.is_starter = 1
        ORDER BY g.game_date, COALESCE(g.scheduled_datetime, ''), g.game_id, gpa.side
        """,
        (season,),
    ).fetchall()

    appearances: dict[tuple[int, str], StarterAppearance] = {}
    for row in rows:
        side = str(row["side"])
        team_won = compute_team_win(
            side=side,
            home_team_id=row["home_team_id"],
            away_team_id=row["away_team_id"],
            home_score=row["home_score"],
            away_score=row["away_score"],
            winning_team_id=row["winning_team_id"],
        )
        appearances[(int(row["game_id"]), side)] = StarterAppearance(
            game_id=int(row["game_id"]),
            side=side,
            season=int(row["season"]),
            game_date=str(row["game_date"]),
            status=row["status"],
            pitcher_id=int(row["pitcher_id"]),
            pitcher_hand=row["pitch_hand"],
            innings_pitched=to_float(row["innings_pitched"]),
            batters_faced=to_int(row["batters_faced"]),
            pitches=to_int(row["pitches"]),
            strikes=to_int(row["strikes"]),
            hits=to_int(row["hits"]),
            walks=to_int(row["walks"]),
            strikeouts=to_int(row["strikeouts"]),
            earned_runs=to_int(row["earned_runs"]),
            home_runs=to_int(row["home_runs"]),
            win=team_won,
        )
    return appearances


def compute_team_win(
    *,
    side: str,
    home_team_id: Any,
    away_team_id: Any,
    home_score: Any,
    away_score: Any,
    winning_team_id: Any,
) -> int:
    if winning_team_id is not None:
        team_id = home_team_id if side == "home" else away_team_id
        return 1 if int(winning_team_id) == int(team_id) else 0
    if home_score is None or away_score is None:
        return 0
    if side == "home":
        return 1 if int(home_score) > int(away_score) else 0
    return 1 if int(away_score) > int(home_score) else 0


def build_row(
    *,
    game: sqlite3.Row,
    side: str,
    starter: StarterAppearance | None,
    state: PitcherSeasonState | None,
    computed_at: str,
) -> tuple[Any, ...]:
    probable_pitcher_id = starter.pitcher_id if starter is not None else None
    probable_pitcher_known = 1 if starter is not None else 0
    pitcher_hand = starter.pitcher_hand if starter is not None else None

    if starter is None or state is None or state.starts == 0:
        season_starts = None
        season_era = None
        season_whip = None
        season_k_pct = None
        season_bb_pct = None
        season_hr_per_9 = None
        season_avg_allowed = None
        season_strike_pct = None
        season_win_pct = None
        stats_available_flag = 0
    else:
        season_starts = state.starts
        season_era = safe_divide(9.0 * state.earned_runs, state.innings_pitched)
        season_whip = safe_divide(state.walks + state.hits, state.innings_pitched)
        season_k_pct = safe_divide(state.strikeouts, state.batters_faced)
        season_bb_pct = safe_divide(state.walks, state.batters_faced)
        season_hr_per_9 = safe_divide(9.0 * state.home_runs, state.innings_pitched)
        season_avg_allowed = safe_divide(state.hits, state.batters_faced - state.walks)
        season_strike_pct = safe_divide(state.strikes, state.pitches)
        season_win_pct = safe_divide(state.wins, state.starts)
        stats_available_flag = 1

    return (
        int(game["game_id"]),
        side,
        int(game["season"]),
        probable_pitcher_id,
        probable_pitcher_known,
        pitcher_hand,
        season_starts,
        season_era,
        season_whip,
        season_k_pct,
        season_bb_pct,
        season_hr_per_9,
        season_avg_allowed,
        season_strike_pct,
        season_win_pct,
        stats_available_flag,
        computed_at,
    )


def process_season(conn: sqlite3.Connection, season: int, computed_at: str) -> int:
    games = fetch_games(conn, season)
    starters_by_game_side = fetch_starter_appearances(conn, season)
    season_states: dict[int, PitcherSeasonState] = {}
    rows_to_upsert: list[tuple[Any, ...]] = []

    index = 0
    while index < len(games):
        current_date = str(games[index]["game_date"])
        day_games: list[sqlite3.Row] = []
        while index < len(games) and str(games[index]["game_date"]) == current_date:
            day_games.append(games[index])
            index += 1

        pending_updates: list[StarterAppearance] = []
        for game in day_games:
            game_id = int(game["game_id"])
            for side in ("home", "away"):
                starter = starters_by_game_side.get((game_id, side))
                state = None
                if starter is not None:
                    state = season_states.get(starter.pitcher_id)
                rows_to_upsert.append(
                    build_row(
                        game=game,
                        side=side,
                        starter=starter,
                        state=state,
                        computed_at=computed_at,
                    )
                )

                if starter is not None and str(game["status"]) == "Final":
                    pending_updates.append(starter)

        for starter in pending_updates:
            season_states.setdefault(starter.pitcher_id, PitcherSeasonState()).apply(starter)

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
