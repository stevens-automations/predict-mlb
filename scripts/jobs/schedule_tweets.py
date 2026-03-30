#!/usr/bin/env python3
"""
Tweet scheduling and scoring for predict-mlb.

Functions:
    score_game_interestingness(prediction_row) -> int
    select_games_to_tweet(conn, date_str=None) -> list[dict]
    schedule_tweets(conn, date_str=None) -> list[dict]
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pytz

ROOT = Path(__file__).resolve().parents[2]
ET_TZ = pytz.timezone("America/New_York")

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def score_game_interestingness(prediction_row: dict) -> int:
    """
    Score a game prediction for tweet-worthiness.

    Scoring:
        +3 if |odds_gap| >= 30 and gap favors predicted winner
        +2 if confidence_tier == 'high' (win_prob >= 0.65)
        +2 if any SHAP reason has |shap| >= 0.04 (strong single factor)
        +1 if confidence_tier == 'medium'
        +1 if predicted winner is market underdog (positive ML odds)

    Args:
        prediction_row: dict from daily_predictions. Expected keys:
            predicted_winner, home_win_prob, confidence_tier,
            odds_gap (optional), home_odds (optional), away_odds (optional),
            shap_reasons_json (optional JSON string).

    Returns:
        Integer score.
    """
    score = 0

    # Confidence tier scoring
    tier = prediction_row.get("confidence_tier", "low")
    if tier == "high":
        score += 2
    elif tier == "medium":
        score += 1

    # Odds gap scoring
    odds_gap = prediction_row.get("odds_gap")
    if odds_gap is not None:
        try:
            gap = int(odds_gap)
            predicted = prediction_row.get("predicted_winner", "home")
            # Positive gap = market favors home more than we do
            # Negative gap = we like home more than market does
            # We want: gap favors predicted winner
            # If predicted=home and gap<0 (we like home more): value edge for home ✓
            # If predicted=away and gap>0 (market likes home more than us): value edge for away ✓
            gap_favors_pick = (predicted == "home" and gap < 0) or (predicted == "away" and gap > 0)
            if abs(gap) >= 30 and gap_favors_pick:
                score += 3
        except (TypeError, ValueError):
            pass

    # SHAP scoring — strong single factor
    shap_json = prediction_row.get("shap_reasons_json")
    if shap_json:
        try:
            reasons = json.loads(shap_json)
            if any(abs(r.get("shap", 0)) >= 0.04 for r in reasons):
                score += 2
        except Exception:
            pass

    # Underdog pick scoring
    predicted = prediction_row.get("predicted_winner", "home")
    if predicted == "home":
        odds_str = prediction_row.get("home_odds")
    else:
        odds_str = prediction_row.get("away_odds")

    if odds_str:
        try:
            odds_val = int(str(odds_str).replace("+", ""))
            if odds_val > 0:  # positive ML = underdog
                score += 1
        except (TypeError, ValueError):
            pass

    return score


def select_games_to_tweet(conn: sqlite3.Connection, date_str: Optional[str] = None) -> list[dict]:
    """
    Select top games to tweet for a given date.

    Returns up to 3 games with tweet_score >= 2 AND confidence_tier in (medium, high), sorted by score descending.
    """
    if date_str is None:
        date_str = datetime.now(ET_TZ).strftime("%Y-%m-%d")

    rows = conn.execute(
        """
        SELECT game_id, home_team, away_team, predicted_winner, home_win_prob,
               confidence_tier, home_odds, away_odds, odds_gap, implied_home_ml,
               shap_reasons_json, tweet_score, tweet_eligible, tweet_text
        FROM daily_predictions
        WHERE game_date = ? AND tweet_eligible = 1
        ORDER BY tweet_score DESC
        LIMIT 3
        """,
        (date_str,),
    ).fetchall()

    result = []
    for row in rows:
        if hasattr(row, "keys"):
            result.append(dict(row))
        else:
            cols = [
                "game_id", "home_team", "away_team", "predicted_winner", "home_win_prob",
                "confidence_tier", "home_odds", "away_odds", "odds_gap", "implied_home_ml",
                "shap_reasons_json", "tweet_score", "tweet_eligible", "tweet_text",
            ]
            result.append(dict(zip(cols, row)))
    return result


def schedule_tweets(conn: sqlite3.Connection, date_str: Optional[str] = None) -> list[dict]:
    """
    Score all predictions for a date, mark tweet-eligible games, return selected games.

    This is called after predict_today() has run and daily_predictions is populated.
    Only medium/high confidence_tier games are eligible for tweeting, regardless of score.
    """
    if date_str is None:
        date_str = datetime.now(ET_TZ).strftime("%Y-%m-%d")

    rows = conn.execute(
        """
        SELECT game_id, home_team, away_team, predicted_winner, home_win_prob,
               confidence_tier, home_odds, away_odds, odds_gap, implied_home_ml,
               shap_reasons_json
        FROM daily_predictions
        WHERE game_date = ?
        """,
        (date_str,),
    ).fetchall()

    scored = []
    for row in rows:
        if hasattr(row, "keys"):
            r = dict(row)
        else:
            cols = [
                "game_id", "home_team", "away_team", "predicted_winner", "home_win_prob",
                "confidence_tier", "home_odds", "away_odds", "odds_gap", "implied_home_ml",
                "shap_reasons_json",
            ]
            r = dict(zip(cols, row))

        s = score_game_interestingness(r)
        eligible = 1 if (s >= 2 and r.get("confidence_tier", "low") in ("medium", "high")) else 0
        r["tweet_score"] = s
        r["tweet_eligible"] = eligible

        conn.execute(
            """
            UPDATE daily_predictions
            SET tweet_score = ?, tweet_eligible = ?, updated_at = datetime('now')
            WHERE game_id = ?
            """,
            (s, eligible, r["game_id"]),
        )
        if eligible:
            scored.append(r)

    conn.commit()

    # Return top 3 by score
    scored.sort(key=lambda x: x["tweet_score"], reverse=True)
    return scored[:3]


if __name__ == "__main__":
    import json as _json

    db_path = ROOT / "data" / "mlb_history.db"
    date_str = sys.argv[1] if len(sys.argv) > 1 else None

    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        selected = schedule_tweets(conn, date_str)
        print(_json.dumps(selected, indent=2, default=str))
        print(f"\n{len(selected)} games selected for tweeting.")
    finally:
        conn.close()
