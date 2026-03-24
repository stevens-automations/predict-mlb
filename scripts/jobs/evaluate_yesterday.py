#!/usr/bin/env python3
"""
Score yesterday's game predictions against actual results.

Compares daily_predictions to outcomes in the labels table.
Writes did_predict_correct, actual_winner, home_score, away_score.
Logs overall and per-tier accuracy summary to pipeline_log.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Optional

import pytz

ET_TZ = pytz.timezone("America/New_York")
logger = logging.getLogger(__name__)


def _log(
    conn: sqlite3.Connection,
    job: str,
    status: str,
    message: str,
    duration_s: float = 0.0,
) -> None:
    try:
        conn.execute(
            "INSERT INTO pipeline_log (job, status, message, duration_s) VALUES (?, ?, ?, ?)",
            (job, status, message, duration_s),
        )
        conn.commit()
    except Exception as exc:
        logger.warning(f"pipeline_log write failed: {exc}")


def evaluate_yesterday(
    conn: sqlite3.Connection, date_str: Optional[str] = None
) -> None:
    """Score yesterday's predictions vs actual results.

    Steps:
      1. Get yesterday's date (or use date_str).
      2. Load daily_predictions rows where game_date = yesterday AND did_predict_correct IS NULL.
      3. For each, look up result in labels table by game_id.
      4. Update did_predict_correct (1/0), actual_winner, home_score, away_score.
      5. Compute total/correct/accuracy overall + by confidence_tier.
      6. Log summary to pipeline_log.

    Args:
        conn: SQLite3 connection (caller manages lifecycle). Should have
              row_factory = sqlite3.Row set.
        date_str: Override date as 'YYYY-MM-DD'. Defaults to yesterday ET.
    """
    JOB = "evaluate_yesterday"
    t0 = time.time()

    if date_str:
        yesterday = date_str
    else:
        now_et = datetime.now(ET_TZ)
        yesterday = (now_et.date() - timedelta(days=1)).isoformat()

    _log(conn, JOB, "started", f"Evaluating predictions for {yesterday}")
    logger.info(f"[{JOB}] evaluating predictions for {yesterday}")

    try:
        # Load unevaluated predictions
        preds = conn.execute(
            """
            SELECT game_id, predicted_winner, home_team, away_team,
                   home_win_prob, confidence_tier
            FROM daily_predictions
            WHERE game_date = ? AND did_predict_correct IS NULL
            """,
            (yesterday,),
        ).fetchall()

        if not preds:
            msg = f"No unevaluated predictions found for {yesterday}"
            elapsed = time.time() - t0
            _log(conn, JOB, "completed", msg, elapsed)
            logger.info(f"[{JOB}] {msg}")
            return

        total = 0
        correct = 0
        skipped = 0
        # tier_counts: tier -> {"total": int, "correct": int}
        tier_counts: dict[str, dict[str, int]] = {}

        for pred in preds:
            game_id = pred["game_id"] if hasattr(pred, "__getitem__") else pred[0]

            # Helper to fetch field by name or index
            def _get(row: Any, name: str, idx: int) -> Any:
                try:
                    return row[name]
                except (KeyError, TypeError, IndexError):
                    return row[idx]

            game_id = _get(pred, "game_id", 0)
            predicted_winner = _get(pred, "predicted_winner", 1)
            home_team = _get(pred, "home_team", 2)
            away_team = _get(pred, "away_team", 3)
            confidence_tier = _get(pred, "confidence_tier", 5) or "unknown"

            # Look up actual result in labels
            label = conn.execute(
                "SELECT did_home_win, home_score, away_score FROM labels WHERE game_id = ?",
                (game_id,),
            ).fetchone()

            if label is None:
                logger.debug(f"[{JOB}] no label yet for game {game_id}, skipping")
                skipped += 1
                continue

            try:
                did_home_win = label["did_home_win"]
                home_score = label["home_score"]
                away_score = label["away_score"]
            except (KeyError, TypeError):
                did_home_win = label[0]
                home_score = label[1]
                away_score = label[2]

            actual_winner = home_team if did_home_win else away_team
            did_correct = 1 if predicted_winner == actual_winner else 0

            # Update prediction row
            conn.execute(
                """
                UPDATE daily_predictions
                SET did_predict_correct = ?,
                    actual_winner = ?,
                    home_score = ?,
                    away_score = ?,
                    updated_at = datetime('now')
                WHERE game_id = ?
                """,
                (did_correct, actual_winner, home_score, away_score, game_id),
            )

            total += 1
            correct += did_correct

            # Track by tier
            tier_counts.setdefault(confidence_tier, {"total": 0, "correct": 0})
            tier_counts[confidence_tier]["total"] += 1
            tier_counts[confidence_tier]["correct"] += did_correct

        conn.commit()

        # Build summary
        accuracy = correct / total if total > 0 else 0.0
        tier_parts: list[str] = []
        for tier in ("high", "medium", "low", "unknown"):
            counts = tier_counts.get(tier)
            if counts and counts["total"] > 0:
                t_acc = counts["correct"] / counts["total"]
                tier_parts.append(
                    f"{tier}: {counts['correct']}/{counts['total']} ({t_acc:.1%})"
                )
        # Include any tiers we didn't enumerate above
        for tier, counts in tier_counts.items():
            if tier not in ("high", "medium", "low", "unknown") and counts["total"] > 0:
                t_acc = counts["correct"] / counts["total"]
                tier_parts.append(
                    f"{tier}: {counts['correct']}/{counts['total']} ({t_acc:.1%})"
                )

        elapsed = time.time() - t0
        summary = (
            f"date={yesterday} evaluated={total} correct={correct} skipped={skipped} "
            f"accuracy={accuracy:.1%}"
        )
        if tier_parts:
            summary += " | " + " | ".join(tier_parts)

        _log(conn, JOB, "completed", summary, elapsed)
        logger.info(f"[{JOB}] {summary}")

        # Results tweet summary — season-to-date record
        try:
            season_row = conn.execute(
                """
                SELECT COUNT(*) as total, SUM(did_predict_correct) as correct
                FROM daily_predictions
                WHERE did_predict_correct IS NOT NULL
                """
            ).fetchone()
            season_total = season_row[0] if season_row else 0
            season_correct = int(season_row[1] or 0) if season_row else 0
            season_acc = season_correct / season_total if season_total > 0 else 0.0
            results_summary = (
                f"Yesterday: {correct}/{total} ({accuracy:.0%}) on full slate. "
                f"Season: {season_correct}/{season_total} ({season_acc:.0%})"
            )
            _log(conn, JOB, "results_summary", results_summary, 0.0)
            logger.info(f"[{JOB}] results_summary: {results_summary}")
        except Exception as exc:
            logger.warning(f"[{JOB}] results_summary failed: {exc}")

    except Exception as exc:
        elapsed = time.time() - t0
        _log(conn, JOB, "failed", str(exc), elapsed)
        logger.error(f"[{JOB}] failed: {exc}")
        raise


def generate_weekly_recap(conn) -> str:
    """
    Generate weekly recap tweet text. Called every Monday morning.
    Includes: season W/L record, best upset correctly predicted this week.
    """
    from datetime import datetime, timedelta
    import pytz
    ET = pytz.timezone('America/New_York')
    today = datetime.now(ET).date()
    week_start = today - timedelta(days=7)

    # Season totals
    season_row = conn.execute('''
        SELECT COUNT(*) as total, SUM(did_predict_correct) as correct
        FROM daily_predictions
        WHERE did_predict_correct IS NOT NULL
    ''').fetchone()
    total = season_row['total'] or 0
    correct = int(season_row['correct'] or 0)
    season_pct = int(100 * correct / total) if total else 0

    # This week's totals
    week_row = conn.execute('''
        SELECT COUNT(*) as total, SUM(did_predict_correct) as correct
        FROM daily_predictions
        WHERE did_predict_correct IS NOT NULL AND game_date >= ?
    ''', (str(week_start),)).fetchone()
    w_total = week_row['total'] or 0
    w_correct = int(week_row['correct'] or 0)

    # Best upset correctly predicted this week
    upset_row = conn.execute('''
        SELECT home_team, away_team, predicted_winner, home_win_prob,
               home_odds, away_odds, game_date
        FROM daily_predictions
        WHERE did_predict_correct = 1
        AND game_date >= ?
        AND (
            (predicted_winner = 'home' AND CAST(home_odds AS INTEGER) > 0)
            OR
            (predicted_winner = 'away' AND CAST(away_odds AS INTEGER) > 0)
        )
        ORDER BY ABS(CAST(
            CASE WHEN predicted_winner='home' THEN home_odds ELSE away_odds END
        AS INTEGER)) DESC
        LIMIT 1
    ''', (str(week_start),)).fetchone()

    # Build tweet
    lines = []
    lines.append(f'Weekly recap: {w_correct}/{w_total} this week.')
    lines.append(f'Season record: {correct}/{total} ({season_pct}%)')

    if upset_row:
        winner_name = upset_row['home_team'] if upset_row['predicted_winner'] == 'home' else upset_row['away_team']
        upset_odds = upset_row['home_odds'] if upset_row['predicted_winner'] == 'home' else upset_row['away_odds']
        lines.append(f'Best pick: {winner_name} ({upset_odds}) \u2713')

    return ' | '.join(lines)


# Type alias for row access compatibility
from typing import Any  # noqa: E402 (moved import for use in nested function)
