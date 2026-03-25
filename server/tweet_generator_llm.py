#!/usr/bin/env python3
"""
LLM-based tweet generator for predict-mlb.

Uses local Qwen via Ollama. Falls back to deterministic scaffold if Ollama unavailable.
Model preference: qwen3.5:9b → qwen3.5:4b → deterministic scaffold.
"""
from __future__ import annotations

import re

import requests

OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
DEFAULT_MODEL = "qwen3.5:9b"
FALLBACK_MODEL = "qwen3.5:4b"


def _enforce_char_limit(text: str, limit: int = 275) -> str:
    """Truncate at word boundary if over limit."""
    if len(text) <= limit:
        return text
    truncated = text[:limit].rsplit(' ', 1)[0]
    return truncated.rstrip('.,;:') + '…'


def _call_ollama(prompt: str, model: str, timeout: int = 15) -> str | None:
    """
    Call Ollama API. Returns stripped tweet text or None on failure.
    """
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
            timeout=timeout,
        )
        if resp.status_code == 200:
            data = resp.json()
            tweet = data.get("response", "").strip()
            # Strip <think>...</think> blocks that Qwen sometimes emits
            tweet = re.sub(r"<think>.*?</think>", "", tweet, flags=re.DOTALL).strip()
            if tweet:
                return tweet
    except Exception:
        pass
    return None


def generate_tweet(game: dict, shap_reasons: list[dict], model: str = DEFAULT_MODEL) -> str:
    """
    Generate a tweet using local Qwen via Ollama.
    Falls back: 9B → 4B → deterministic scaffold.

    Args:
        game: dict with keys: home_team, away_team, predicted_winner, home_win_prob,
              confidence_tier, home_odds, away_odds, odds_gap (optional),
              implied_home_ml (optional).
        shap_reasons: list of SHAP reason dicts from explainer.explain_prediction().
        model: Ollama model name (default: qwen3.5:9b).

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

    # Use humanize_reasons to replace generic "home"/"away" with actual team names
    from scripts.inference.explainer import humanize_reasons
    supporting = [r for r in shap_reasons if r.get("direction") == game["predicted_winner"] and r.get("human_summary")]
    named_reasons = humanize_reasons(supporting[:3], game["home_team"], game["away_team"])
    reasons_text = "\n".join(f"- {r}" for r in named_reasons) if named_reasons else ""

    # Build team labels with odds inline, e.g. "New York Mets (-120)"
    home_odds = game.get("home_odds") or ""
    away_odds = game.get("away_odds") or ""
    home_label = f"{game['home_team']} ({home_odds})" if home_odds else game['home_team']
    away_label = f"{game['away_team']} ({away_odds})" if away_odds else game['away_team']
    winner_label = home_label if game["predicted_winner"] == "home" else away_label
    loser_label = away_label if game["predicted_winner"] == "home" else home_label

    reasons_block = f"\nKey reasons:\n{reasons_text}" if reasons_text else ""

    prompt = f"""Write one MLB prediction tweet. Be factual, clear, and direct. No hashtags. No emojis. Max 240 characters. Do not mention AI or models.

Our pick: {winner_label} to defeat {loser_label} ({win_pct}% confidence).
{value_note}{reasons_block}

Write a single tweet that states the pick clearly (with odds) and briefly explains why. Every stat mentioned must come from the "Key reasons" above — do not invent anything. Use the team names exactly as written."""

    # Try primary model first (9B), then fallback (4B)
    models_to_try = [model]
    if model != FALLBACK_MODEL:
        models_to_try.append(FALLBACK_MODEL)

    for m in models_to_try:
        tweet = _call_ollama(prompt, m)
        if tweet and len(tweet) <= 280:
            return _enforce_char_limit(tweet)
        elif tweet:
            # Over limit — truncate and return
            return _enforce_char_limit(tweet)

    # Fallback to deterministic scaffold
    try:
        from server.tweet_scaffold import format_prediction_tweet
        lines = format_prediction_tweet([game])
        result = lines[0] if lines else f"{winner} ({win_pct}%) over {loser}"
        return _enforce_char_limit(result)
    except Exception:
        return _enforce_char_limit(f"{winner} ({win_pct}%) over {loser}")
