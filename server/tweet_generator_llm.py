#!/usr/bin/env python3
"""
LLM-based tweet generator for predict-mlb.

Uses local Qwen via Ollama. Falls back to deterministic scaffold if Ollama unavailable.
"""
from __future__ import annotations

import requests

OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
DEFAULT_MODEL = "qwen3.5:4b"


def generate_tweet(game: dict, shap_reasons: list[dict], model: str = DEFAULT_MODEL) -> str:
    """
    Generate a tweet using local Qwen via Ollama.
    Falls back to deterministic scaffold if Ollama unavailable.

    Args:
        game: dict with keys: home_team, away_team, predicted_winner, home_win_prob,
              confidence_tier, home_odds, away_odds, odds_gap (optional),
              implied_home_ml (optional).
        shap_reasons: list of SHAP reason dicts from explainer.explain_prediction().
        model: Ollama model name.

    Returns:
        Tweet string (<= 280 chars).
    """
    winner = game["home_team"] if game["predicted_winner"] == "home" else game["away_team"]
    loser = game["away_team"] if game["predicted_winner"] == "home" else game["home_team"]
    prob = game["home_win_prob"] if game["predicted_winner"] == "home" else 1 - game["home_win_prob"]
    win_pct = int(prob * 100)

    # Value note — only when odds gap is meaningful
    value_note = ""
    if game.get("odds_gap") and abs(game["odds_gap"]) >= 30:
        direction = "underdogs" if game.get("implied_home_ml", 0) > 0 else "favorites"
        value_note = f"Market has them as {direction} but we disagree."

    # Top 2-3 readable SHAP reasons
    reasons = [r["human_summary"] for r in shap_reasons[:3] if r.get("human_summary")]
    reasons_text = "\n".join(f"- {r}" for r in reasons) if reasons else "(no specific factors available)"

    # Market odds line
    if game.get("home_odds"):
        winner_odds = game.get("home_odds") if game["predicted_winner"] == "home" else game.get("away_odds", "N/A")
        odds_line = f"Market odds: {winner} at {winner_odds}"
    else:
        odds_line = ""

    prompt = f"""You write short MLB prediction tweets. Be specific and confident. No hashtags. No emojis unless one fits naturally. Max 240 characters. Sound like a sharp sports bettor, not a bot. Do not mention AI, models, or algorithms.

Game: {game['away_team']} (away) @ {game['home_team']} (home)
Our pick: {winner} to win ({win_pct}% confidence)
{odds_line}
{value_note}

Key reasons we like {winner}:
{reasons_text}

Write one tweet. Be direct and specific. Make sure all stats and reasons clearly refer to {winner}."""

    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "think": False,  # disable Qwen3.5 extended thinking mode
                "options": {"temperature": 0.75, "num_predict": 120},
            },
            timeout=15,
        )
        if resp.status_code == 200:
            import re
            data = resp.json()
            tweet = data.get("response", "").strip()
            # Strip <think>...</think> blocks that Qwen sometimes emits
            tweet = re.sub(r"<think>.*?</think>", "", tweet, flags=re.DOTALL).strip()
            if tweet and len(tweet) <= 280:
                return tweet
    except Exception:
        pass

    # Fallback to deterministic scaffold
    try:
        from server.tweet_scaffold import format_prediction_tweet
        lines = format_prediction_tweet([game])
        return lines[0] if lines else f"{winner} ({win_pct}%) over {loser}"
    except Exception:
        return f"{winner} ({win_pct}%) over {loser}"
