#!/usr/bin/env python3
"""
LLM-based tweet generator for predict-mlb.

Uses local Qwen via Ollama. Falls back to deterministic scaffold if Ollama unavailable.
Model preference: qwen3.5:9b → qwen3.5:4b → deterministic scaffold.
"""
from __future__ import annotations

import re

import requests
USE_LLM = False  # Set to True mid-season when Qwen tweets are tuned

OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
DEFAULT_MODEL = "qwen3.5:9b"
FALLBACK_MODEL = "qwen3.5:4b"

SYSTEM_PROMPT = """You write MLB game prediction tweets for a data-driven prediction account.

VOICE: Analytical sports fan. Not a betting service — use "prediction" or "taking" not "pick" or "bet".
FORMAT: One tweet. Max 240 characters. No hashtags. No emojis. No filler phrases. No "Stay tuned" or similar.
STATS: Use only the stats provided. Do not add stats, records, or claims you weren't given.
STRUCTURE: Lead with the prediction (team + odds). Then give 1-2 specific reasons why. Keep it tight.

STAT DEFINITIONS (for context — do not explain these in the tweet):
- Win rate: % of games won this season
- Run differential: average runs scored minus runs allowed per game (positive = good)
- ERA: Earned Run Average — runs a pitcher allows per 9 innings (lower is better; league avg ~4.20)
- Last 10: win-loss record over last 10 games

EXAMPLE TWEETS:
Example 1:
Prediction context: Cleveland Guardians (-152) over Kansas City Royals (+130), 62% confidence.
Cleveland: win rate 59%, run diff +1.4/game, last 10: 7-3. KC starter ERA 5.02 this season (above league avg).
Tweet: Taking the Cleveland Guardians (-152) tonight. They're 7-3 in their last 10 and outscoring opponents by +1.4 runs/game. KC's starter has a 5.02 ERA — well above average.

Example 2:
Prediction context: New York Mets (-142) over Pittsburgh Pirates (+122), 66% confidence.
Mets: starter ERA 2.80 this season (well below league avg), run diff +1.8/game. Pirates: win rate 41%.
Tweet: New York Mets (-142) at home tonight. Their starter is posting a 2.80 ERA this season and they're outscoring opponents by +1.8 runs/game — solid edge over Pittsburgh (+122).

Now write one tweet for the game below. Follow the same format."""


def _build_user_prompt(ctx: dict) -> str:
    w_stats = "\n".join(f"  - {s}" for s in ctx["winner_stats"]) or "  - (limited data available)"
    l_stats = "\n".join(f"  - {s}" for s in ctx["loser_stats"]) or "  - (limited data available)"

    winner_label = f"{ctx['winner']} ({ctx['winner_odds']})" if ctx['winner_odds'] else ctx['winner']
    loser_label = f"{ctx['loser']} ({ctx['loser_odds']})" if ctx['loser_odds'] else ctx['loser']
    value = f"\nNote: {ctx['value_note']}" if ctx['value_note'] else ""

    return f"""Prediction context: {winner_label} over {loser_label}, {ctx['win_pct']}% confidence.{value}

{ctx['winner']} stats:
{w_stats}

{ctx['loser']} stats:
{l_stats}

Tweet:"""


def _enforce_char_limit(text: str, limit: int = 275) -> str:
    """Truncate at word boundary if over limit."""
    if len(text) <= limit:
        return text
    truncated = text[:limit].rsplit(' ', 1)[0]
    return truncated.rstrip('.,;:') + '…'


def _call_ollama(prompt: str, model: str, timeout: int = 15, system: str | None = None) -> str | None:
    """
    Call Ollama API. Returns stripped tweet text or None on failure.
    """
    try:
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "think": False,  # disable Qwen3.5 extended thinking mode
            "options": {"temperature": 0.75, "num_predict": 120},
        }
        if system:
            payload["system"] = system
        resp = requests.post(
            OLLAMA_URL,
            json=payload,
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


def _deterministic_tweet(game: dict, shap_reasons: list, feature_dict: dict = None) -> str:
    """
    Generate a clean, factual, deterministic tweet. No LLM involved.
    Format: "Prediction: [Winner] ([odds]) to defeat [Loser] ([odds]). [1-2 stat reasons]."
    """
    pred = game["predicted_winner"]
    winner = game["home_team"] if pred == "home" else game["away_team"]
    loser = game["away_team"] if pred == "home" else game["home_team"]
    prob = game["home_win_prob"] if pred == "home" else 1 - game["home_win_prob"]
    win_pct = int(prob * 100)

    home_odds = game.get("home_odds") or ""
    away_odds = game.get("away_odds") or ""
    winner_odds = home_odds if pred == "home" else away_odds
    loser_odds = away_odds if pred == "home" else home_odds

    winner_label = f"{winner} ({winner_odds})" if winner_odds else winner
    loser_label = f"{loser} ({loser_odds})" if loser_odds else loser

    reasons = []
    if feature_dict:
        wp = "home" if pred == "home" else "away"
        lp = "away" if pred == "home" else "home"

        def _fget(key):
            v = feature_dict.get(key)
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        w_win = _fget(f"{wp}_team_season_win_pct")
        l_win = _fget(f"{lp}_team_season_win_pct")
        if w_win is not None and l_win is not None and w_win > l_win:
            loser_poss = f"{loser}'" if loser.endswith("s") else f"{loser}'s"
            reasons.append(f"{winner} holds a {w_win:.0%} win rate vs {loser_poss} {l_win:.0%}")

        w_rd = _fget(f"{wp}_team_season_run_diff_per_game")
        l_rd = _fget(f"{lp}_team_season_run_diff_per_game")
        if w_rd is not None and l_rd is not None and w_rd > l_rd:
            reasons.append(f"Run differential: {winner} {w_rd:+.1f} vs {loser} {l_rd:+.1f} per game")

        w_era = _fget(f"{wp}_starter_era")
        l_era = _fget(f"{lp}_starter_era")
        if w_era is not None and l_era is not None and w_era < l_era:
            reasons.append(f"Starter ERA: {winner} {w_era:.2f} vs {loser} {l_era:.2f}")
        elif w_era is None and l_era is None:
            w_cera = _fget(f"{wp}_starter_career_era")
            l_cera = _fget(f"{lp}_starter_career_era")
            if w_cera is not None and l_cera is not None and w_cera < l_cera:
                reasons.append(f"Career starter ERA: {winner} {w_cera:.2f} vs {loser} {l_cera:.2f}")

        w_r10 = _fget(f"{wp}_team_rolling_last10_win_pct")
        l_r10 = _fget(f"{lp}_team_rolling_last10_win_pct")
        if w_r10 is not None and l_r10 is not None and w_r10 > l_r10:
            w10 = round(w_r10 * 10)
            l10 = round(l_r10 * 10)
            reasons.append(f"Last 10 games: {winner} {w10}-{10-w10} vs {loser} {l10}-{10-l10}")

    reason_strs = reasons[:2]

    tweet = f"Prediction: {winner_label} to defeat {loser_label} ({win_pct}% confidence)."
    if reason_strs:
        tweet += " " + "; ".join(r.capitalize() if not r[0].isupper() else r for r in reason_strs) + "."

    return _enforce_char_limit(tweet)


def generate_tweet(
    game: dict,
    shap_reasons: list[dict],
    feature_dict: dict | None = None,
    model: str = DEFAULT_MODEL,
) -> str:
    """
    Generate a tweet using local Qwen via Ollama.
    Falls back: 9B → 4B → deterministic scaffold.

    Args:
        game: dict with keys: home_team, away_team, predicted_winner, home_win_prob,
              confidence_tier, home_odds, away_odds, odds_gap (optional),
              implied_home_ml (optional).
        shap_reasons: list of SHAP reason dicts from explainer.explain_prediction().
        feature_dict: optional feature dict; if provided, uses build_tweet_context
                      for deterministic context building and structured system prompt.
        model: Ollama model name (default: qwen3.5:9b).

    Returns:
        Tweet string (<= 280 chars).
    """
    if not USE_LLM or feature_dict is None:
        return _deterministic_tweet(game, shap_reasons, feature_dict)

    # Build prompt using deterministic context if feature_dict is available
    if feature_dict is not None:
        from scripts.inference.explainer import build_tweet_context
        ctx = build_tweet_context(game, feature_dict, shap_reasons)
        prompt = _build_user_prompt(ctx)
        system = SYSTEM_PROMPT
    else:
        # Legacy path: build prompt from SHAP reasons directly
        winner = game["home_team"] if game["predicted_winner"] == "home" else game["away_team"]
        loser = game["away_team"] if game["predicted_winner"] == "home" else game["home_team"]
        prob = game["home_win_prob"] if game["predicted_winner"] == "home" else 1 - game["home_win_prob"]
        win_pct = int(prob * 100)

        value_note = ""
        if game.get("odds_gap") and abs(game["odds_gap"]) >= 30:
            direction = "underdogs" if game.get("implied_home_ml", 0) > 0 else "favorites"
            value_note = f"Market has them as {direction} but we disagree."

        from scripts.inference.explainer import humanize_reasons
        supporting = [r for r in shap_reasons if r.get("direction") == game["predicted_winner"] and r.get("human_summary")]
        named_reasons = humanize_reasons(supporting[:3], game["home_team"], game["away_team"])
        reasons_text = "\n".join(f"- {r}" for r in named_reasons) if named_reasons else ""

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
        system = None

    # Try primary model first (9B), then fallback (4B)
    models_to_try = [model]
    if model != FALLBACK_MODEL:
        models_to_try.append(FALLBACK_MODEL)

    for m in models_to_try:
        tweet = _call_ollama(prompt, m, system=system)
        if tweet and len(tweet) <= 280:
            return _enforce_char_limit(tweet)
        elif tweet:
            # Over limit — truncate and return
            return _enforce_char_limit(tweet)

    # Fallback to deterministic scaffold
    try:
        from server.tweet_scaffold import format_prediction_tweet
        lines = format_prediction_tweet([game])
        result = lines[0] if lines else f"{winner if feature_dict is None else game['home_team']} ({win_pct if feature_dict is None else int((game['home_win_prob'] if game['predicted_winner'] == 'home' else 1 - game['home_win_prob']) * 100)}%) over {loser if feature_dict is None else game['away_team']}"
        return _enforce_char_limit(result)
    except Exception:
        winner_fb = game["home_team"] if game["predicted_winner"] == "home" else game["away_team"]
        prob_fb = game["home_win_prob"] if game["predicted_winner"] == "home" else 1 - game["home_win_prob"]
        loser_fb = game["away_team"] if game["predicted_winner"] == "home" else game["home_team"]
        return _enforce_char_limit(f"{winner_fb} ({int(prob_fb * 100)}%) over {loser_fb}")
