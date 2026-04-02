#!/usr/bin/env python3
"""
Predict today's MLB games.

Function: predict_today(conn, date_str=None)

- Load today's schedule from today_schedule table
- For each game: call feature_builder.build_feature_row(game_id, conn)
                 → scorer.score_game(features)
- Write to daily_predictions table (create if not exists)
- Skip games already in daily_predictions for today (idempotent)
- Log to pipeline_log table

Args:
    conn: SQLite3 connection (caller manages lifecycle).
    date_str: Date string 'YYYY-MM-DD'. Defaults to today ET.
"""

from __future__ import annotations

import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pytz

ROOT = Path(__file__).resolve().parents[2]
ET_TZ = pytz.timezone("America/New_York")

# Add project root to path for imports
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.inference.feature_builder import build_feature_row
from scripts.inference.scorer import score_game, _load_model
from scripts.inference.explainer import explain_prediction
from scripts.jobs.schedule_tweets import score_game_interestingness
from scripts.jobs.fetch_odds import prob_to_american_ml

CREATE_DAILY_PREDICTIONS_SQL = """
CREATE TABLE IF NOT EXISTS daily_predictions (
    game_id             INTEGER PRIMARY KEY,
    game_date           TEXT NOT NULL,
    home_team           TEXT,
    away_team           TEXT,
    home_team_id        INTEGER,
    away_team_id        INTEGER,
    first_pitch_et      TEXT,
    predicted_winner    TEXT,
    home_win_prob       REAL,
    confidence_tier     TEXT,
    home_odds           TEXT,
    away_odds           TEXT,
    best_odds_bookmaker TEXT,
    implied_home_ml     INTEGER,
    odds_gap            INTEGER,
    shap_reasons_json   TEXT,
    tweet_score         INTEGER,
    tweet_eligible      INTEGER DEFAULT 0,
    tweet_text          TEXT,
    tweet_scheduled_at  TEXT,
    tweeted             INTEGER DEFAULT 0,
    actual_winner       TEXT,
    home_score          INTEGER,
    away_score          INTEGER,
    did_predict_correct INTEGER,
    result_tweeted      INTEGER DEFAULT 0,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
)
"""

MIGRATE_DAILY_PREDICTIONS_COLS = [
    ("implied_home_ml", "INTEGER"),
    ("odds_gap", "INTEGER"),
    ("shap_reasons_json", "TEXT"),
    ("tweet_score", "INTEGER"),
    ("tweet_eligible", "INTEGER DEFAULT 0"),
    ("tweet_text", "TEXT"),
]

CREATE_PIPELINE_LOG_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT DEFAULT (datetime('now')),
    job         TEXT,
    status      TEXT,
    message     TEXT,
    duration_s  REAL
)
"""


def _log(conn: sqlite3.Connection, job: str, status: str, message: str, duration_s: float = 0.0):
    conn.execute(
        "INSERT INTO pipeline_log (job, status, message, duration_s) VALUES (?, ?, ?, ?)",
        (job, status, message, duration_s),
    )
    conn.commit()


def predict_today(
    conn: sqlite3.Connection,
    date_str: Optional[str] = None,
) -> list:
    """
    Run predictions for all games in today_schedule.

    Args:
        conn: SQLite3 connection.
        date_str: Date string 'YYYY-MM-DD'. Defaults to today ET.

    Returns:
        List of prediction result dicts.
    """
    t0 = time.time()
    JOB = "predict_today"

    # Ensure tables exist
    conn.execute(CREATE_DAILY_PREDICTIONS_SQL)
    conn.execute(CREATE_PIPELINE_LOG_SQL)
    conn.commit()

    # Migrate: add new columns if not present (idempotent)
    for col, typedef in MIGRATE_DAILY_PREDICTIONS_COLS:
        try:
            conn.execute(f"ALTER TABLE daily_predictions ADD COLUMN {col} {typedef}")
            conn.commit()
        except Exception:
            pass  # Column already exists

    # Load model bundle once for explainer
    try:
        _model, _feature_cols = _load_model()
        model_bundle = {"model": _model, "feature_cols": _feature_cols}
    except Exception as e:
        model_bundle = None
        _log(conn, JOB, "warn", f"Could not load model bundle for explainer: {e}")

    if date_str is None:
        date_str = datetime.now(ET_TZ).strftime("%Y-%m-%d")

    _log(conn, JOB, "started", f"date={date_str}")

    try:
        # Load today's schedule
        schedule_rows = conn.execute(
            """
            SELECT game_id, game_date, home_team, away_team, home_team_id, away_team_id,
                   first_pitch_et, home_odds, away_odds, odds_bookmaker
            FROM today_schedule
            WHERE game_date = ?
            """,
            (date_str,),
        ).fetchall()

        if not schedule_rows:
            _log(conn, JOB, "completed", f"no games found for {date_str}", time.time() - t0)
            return []

        # Get game_ids already in daily_predictions for today (idempotency)
        existing_ids = set(
            row[0]
            for row in conn.execute(
                "SELECT game_id FROM daily_predictions WHERE game_date = ?",
                (date_str,),
            ).fetchall()
        )

        results = []
        skipped = 0
        errors = 0

        for row in schedule_rows:
            # Handle both Row and tuple
            if hasattr(row, "keys"):
                r = dict(row)
            else:
                cols = ["game_id", "game_date", "home_team", "away_team", "home_team_id",
                        "away_team_id", "first_pitch_et", "home_odds", "away_odds", "odds_bookmaker"]
                r = dict(zip(cols, row))

            game_id = r["game_id"]

            # Skip if already predicted
            if game_id in existing_ids:
                skipped += 1
                continue

            try:
                # Build features
                features = build_feature_row(game_id, conn)

                # Score
                score = score_game(features)

                now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                # Compute implied ML and odds gap
                home_win_prob = score["home_win_prob"]
                implied_home_ml = prob_to_american_ml(home_win_prob)
                home_odds_str = r.get("home_odds")
                market_home_ml = None
                odds_gap = None
                if home_odds_str:
                    try:
                        market_home_ml = int(str(home_odds_str).replace("+", ""))
                        odds_gap = market_home_ml - implied_home_ml
                    except (TypeError, ValueError):
                        pass

                # Run SHAP explainer
                shap_reasons = []
                shap_reasons_json = None
                if model_bundle is not None:
                    try:
                        import json
                        shap_reasons = explain_prediction(features, model_bundle)
                        shap_reasons_json = json.dumps(shap_reasons)
                    except Exception as ex:
                        _log(conn, JOB, "warn", f"game_id={game_id}: explainer failed: {ex}")

                # Write to daily_predictions
                conn.execute(
                    """
                    INSERT OR IGNORE INTO daily_predictions
                        (game_id, game_date, home_team, away_team, home_team_id, away_team_id,
                         first_pitch_et, predicted_winner, home_win_prob, confidence_tier,
                         home_odds, away_odds, best_odds_bookmaker,
                         implied_home_ml, odds_gap, shap_reasons_json,
                         created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        game_id,
                        date_str,
                        r.get("home_team"),
                        r.get("away_team"),
                        r.get("home_team_id"),
                        r.get("away_team_id"),
                        r.get("first_pitch_et"),
                        score["predicted_winner"],
                        home_win_prob,
                        score["confidence_tier"],
                        r.get("home_odds"),
                        r.get("away_odds"),
                        r.get("odds_bookmaker"),
                        implied_home_ml,
                        odds_gap,
                        shap_reasons_json,
                        now,
                        now,
                    ),
                )
                conn.commit()

                # Build full prediction row for tweet scoring
                prediction_row = {
                    "game_id": game_id,
                    "home_team": r.get("home_team"),
                    "away_team": r.get("away_team"),
                    "predicted_winner": score["predicted_winner"],
                    "home_win_prob": home_win_prob,
                    "confidence_tier": score["confidence_tier"],
                    "home_odds": r.get("home_odds"),
                    "away_odds": r.get("away_odds"),
                    "odds_gap": odds_gap,
                    "implied_home_ml": implied_home_ml,
                    "shap_reasons_json": shap_reasons_json,
                }

                # Tweet scoring
                tweet_score = score_game_interestingness(prediction_row)
                confidence_tier = score["confidence_tier"]
                # tier gate: only medium/high eligible
                tweet_eligible = 1 if (tweet_score >= 2 and confidence_tier in ("medium", "high")) else 0
                tweet_text = None

                # Generate tweet for eligible games
                if tweet_eligible:
                    try:
                        from server.tweet_generator_llm import generate_tweet
                        tweet_text = generate_tweet(prediction_row, shap_reasons, feature_dict=features)
                    except Exception as ex:
                        _log(conn, JOB, "warn", f"game_id={game_id}: tweet gen failed: {ex}")

                # Update tweet fields
                conn.execute(
                    """
                    UPDATE daily_predictions
                    SET tweet_score = ?, tweet_eligible = ?, tweet_text = ?, updated_at = datetime('now')
                    WHERE game_id = ?
                    """,
                    (tweet_score, tweet_eligible, tweet_text, game_id),
                )
                conn.commit()

                result = {
                    "game_id": game_id,
                    "home_team": r.get("home_team"),
                    "away_team": r.get("away_team"),
                    "predicted_winner": score["predicted_winner"],
                    "home_win_prob": home_win_prob,
                    "away_win_prob": score["away_win_prob"],
                    "confidence_tier": score["confidence_tier"],
                    "cold_start": features.get("cold_start", False),
                    "implied_home_ml": implied_home_ml,
                    "odds_gap": odds_gap,
                    "tweet_score": tweet_score,
                    "tweet_eligible": tweet_eligible,
                    "tweet_text": tweet_text,
                    "shap_reasons": shap_reasons,
                }
                results.append(result)

            except ValueError as e:
                # Game not in games table (e.g. future game not yet ingested)
                errors += 1
                _log(conn, JOB, "failed",
                     f"game_id={game_id}: {e} (skipping)", 0.0)
            except Exception as e:
                errors += 1
                _log(conn, JOB, "failed",
                     f"game_id={game_id}: {type(e).__name__}: {e}", 0.0)

        # DB integrity check: no low-tier games should be tweet_eligible
        bad_count = conn.execute(
            """
            SELECT COUNT(*) FROM daily_predictions
            WHERE game_date = ? AND tweet_eligible = 1
              AND confidence_tier NOT IN ('medium', 'high')
            """,
            (date_str,),
        ).fetchone()[0]
        if bad_count:
            _log(conn, JOB, "warn",
                 f"tier gate violation: {bad_count} low-tier rows had tweet_eligible=1, forcing to 0")
            conn.execute(
                """
                UPDATE daily_predictions
                SET tweet_eligible = 0, updated_at = datetime('now')
                WHERE game_date = ? AND tweet_eligible = 1
                  AND confidence_tier NOT IN ('medium', 'high')
                """,
                (date_str,),
            )
            conn.commit()

        duration = time.time() - t0
        _log(
            conn,
            JOB,
            "completed",
            f"predicted={len(results)}, skipped={skipped}, errors={errors} for {date_str}",
            duration,
        )
        return results

    except Exception as e:
        duration = time.time() - t0
        _log(conn, JOB, "failed", str(e), duration)
        raise


if __name__ == "__main__":
    import json

    db_path = ROOT / "data" / "mlb_history.db"
    date_str = sys.argv[1] if len(sys.argv) > 1 else None

    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        results = predict_today(conn, date_str)
        print(json.dumps(results, indent=2, default=str))
        print(f"\n{len(results)} predictions written.")
    finally:
        conn.close()
