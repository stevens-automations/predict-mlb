#!/usr/bin/env python3
"""Build the Layer 2 team_vs_hand_pregame_stats table."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "mlb_history.db"
TABLE_NAME = "team_vs_hand_pregame_stats"
SEASONS = tuple(range(2020, 2026))
INSERT_SQL = f"""
    INSERT OR REPLACE INTO {TABLE_NAME} (
      game_id,
      side,
      season,
      vs_rhp_games,
      vs_lhp_games,
      vs_rhp_ops,
      vs_lhp_ops,
      vs_rhp_batting_avg,
      vs_lhp_batting_avg,
      vs_rhp_runs_per_game,
      vs_lhp_runs_per_game,
      computed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass(slots=True)
class TeamGameSplitResult:
    team_id: int
    opponent_starter_hand: str
    ops: float | None
    batting_avg: float | None
    runs: int | None


@dataclass(slots=True)
class HandSplitState:
    games: int = 0
    sum_ops: float = 0.0
    sum_batting_avg: float = 0.0
    sum_runs: float = 0.0

    def apply(self, result: TeamGameSplitResult) -> None:
        self.games += 1
        self.sum_ops += result.ops or 0.0
        self.sum_batting_avg += result.batting_avg or 0.0
        self.sum_runs += float(result.runs or 0)


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
          vs_rhp_games INTEGER,
          vs_lhp_games INTEGER,
          vs_rhp_ops REAL,
          vs_lhp_ops REAL,
          vs_rhp_batting_avg REAL,
          vs_lhp_batting_avg REAL,
          vs_rhp_runs_per_game REAL,
          vs_lhp_runs_per_game REAL,
          computed_at TEXT,
          PRIMARY KEY (game_id, side),
          FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


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


def fetch_final_split_results(conn: sqlite3.Connection, season: int) -> dict[tuple[int, str], TeamGameSplitResult]:
    rows = conn.execute(
        """
        SELECT
          g.game_id,
          gts.side,
          gts.team_id,
          gts.ops,
          gts.batting_avg,
          gts.runs,
          phd.pitch_hand AS opponent_starter_hand
        FROM games g
        INNER JOIN game_team_stats gts
          ON gts.game_id = g.game_id
        INNER JOIN game_pitcher_appearances gpa
          ON gpa.game_id = g.game_id
         AND gpa.is_starter = 1
         AND gpa.side = CASE gts.side WHEN 'home' THEN 'away' ELSE 'home' END
        LEFT JOIN player_handedness_dim phd
          ON phd.player_id = gpa.pitcher_id
        WHERE g.season = ?
          AND g.status = 'Final'
        ORDER BY g.game_date, COALESCE(g.scheduled_datetime, ''), g.game_id, gts.side
        """,
        (season,),
    ).fetchall()

    results: dict[tuple[int, str], TeamGameSplitResult] = {}
    for row in rows:
        pitch_hand = str(row["opponent_starter_hand"] or "").upper()
        if pitch_hand not in {"L", "R"}:
            continue
        results[(int(row["game_id"]), str(row["side"]))] = TeamGameSplitResult(
            team_id=int(row["team_id"]),
            opponent_starter_hand=pitch_hand,
            ops=to_float(row["ops"]),
            batting_avg=to_float(row["batting_avg"]),
            runs=to_int(row["runs"]),
        )
    return results


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def mean_or_none(total: float, count: int) -> float | None:
    if count <= 0:
        return None
    return total / count


def build_row(
    *,
    game: sqlite3.Row,
    side: str,
    rhp_state: HandSplitState | None,
    lhp_state: HandSplitState | None,
    computed_at: str,
) -> tuple[Any, ...]:
    vs_rhp_games = rhp_state.games if rhp_state and rhp_state.games > 0 else None
    vs_lhp_games = lhp_state.games if lhp_state and lhp_state.games > 0 else None

    return (
        int(game["game_id"]),
        side,
        int(game["season"]),
        vs_rhp_games,
        vs_lhp_games,
        mean_or_none(rhp_state.sum_ops, rhp_state.games) if rhp_state and rhp_state.games > 0 else None,
        mean_or_none(lhp_state.sum_ops, lhp_state.games) if lhp_state and lhp_state.games > 0 else None,
        mean_or_none(rhp_state.sum_batting_avg, rhp_state.games) if rhp_state and rhp_state.games > 0 else None,
        mean_or_none(lhp_state.sum_batting_avg, lhp_state.games) if lhp_state and lhp_state.games > 0 else None,
        mean_or_none(rhp_state.sum_runs, rhp_state.games) if rhp_state and rhp_state.games > 0 else None,
        mean_or_none(lhp_state.sum_runs, lhp_state.games) if lhp_state and lhp_state.games > 0 else None,
        computed_at,
    )


def process_season(conn: sqlite3.Connection, season: int, computed_at: str) -> int:
    games = fetch_games(conn, season)
    final_split_results = fetch_final_split_results(conn, season)
    split_states_by_team: dict[int, dict[str, HandSplitState]] = {}
    rows_to_upsert: list[tuple[Any, ...]] = []

    index = 0
    while index < len(games):
        current_date = str(games[index]["game_date"])
        day_games: list[sqlite3.Row] = []
        while index < len(games) and str(games[index]["game_date"]) == current_date:
            day_games.append(games[index])
            index += 1

        pending_updates: list[TeamGameSplitResult] = []
        for game in day_games:
            for side, team_key in (("home", "home_team_id"), ("away", "away_team_id")):
                team_id = to_int(game[team_key])
                if team_id is None:
                    continue
                team_states = split_states_by_team.get(team_id, {})
                rows_to_upsert.append(
                    build_row(
                        game=game,
                        side=side,
                        rhp_state=team_states.get("R"),
                        lhp_state=team_states.get("L"),
                        computed_at=computed_at,
                    )
                )

            if str(game["status"]) != "Final":
                continue

            game_id = int(game["game_id"])
            for side in ("home", "away"):
                result = final_split_results.get((game_id, side))
                if result is not None:
                    pending_updates.append(result)

        for result in pending_updates:
            split_states = split_states_by_team.setdefault(result.team_id, {})
            split_states.setdefault(result.opponent_starter_hand, HandSplitState()).apply(result)

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
