"""
Tweet scaffold for predict-mlb.
Current format: deterministic, probability-based.
Future intent: replace with local LLM (Qwen 3.5 via Ollama) for nondeterministic,
more engaging tweet generation. See docs/TODO.md for this roadmap item.
"""


def format_prediction_tweet(games: list) -> list[str]:
    """
    Format a list of game predictions into tweet strings.
    games: list of dicts with keys: home_team, away_team, predicted_winner,
           home_win_prob, confidence_tier, home_odds, away_odds, first_pitch_et
    Returns: list of tweet strings (split across multiple if > 280 chars)
    """
    lines = []
    for g in games:
        prob = g['home_win_prob'] if g['predicted_winner'] == 'home' else 1 - g['home_win_prob']
        winner = g['home_team'] if g['predicted_winner'] == 'home' else g['away_team']
        loser = g['away_team'] if g['predicted_winner'] == 'home' else g['home_team']
        tier_emoji = '🔥' if g['confidence_tier'] == 'high' else ('⚡' if g['confidence_tier'] == 'medium' else '')
        odds = g.get('home_odds') if g['predicted_winner'] == 'home' else g.get('away_odds')
        odds_str = f' ({odds})' if odds else ''
        lines.append(f"{tier_emoji} {winner}{odds_str} ({int(prob*100)}%) over {loser}")

    # Split into tweets respecting 280 char limit
    tweets = []
    current = []
    for line in lines:
        block = '\n'.join(current + [line])
        if len(block) > 260 and current:
            tweets.append('\n'.join(current))
            current = [line]
        else:
            current.append(line)
    if current:
        tweets.append('\n'.join(current))
    return tweets


def format_results_tweet(date_str: str, total: int, correct: int, best_pick: dict = None) -> str:
    """Format yesterday's results tweet."""
    pct = int(100 * correct / total) if total else 0
    msg = f"Yesterday's MLB predictions: {correct}/{total} ({pct}%) ✅"
    if best_pick:
        msg += f"\nBest pick: {best_pick['predicted_winner']} ({int(best_pick['home_win_prob']*100)}%) ✅"
    return msg
