# Tweet Content Strategy
Last updated: 2026-03-30

## Current Configuration

- **Tweet filter:** medium/high confidence tier only, max 3/day, tweet_score >= 2
- **Tweet format:** deterministic (factual, no hashtags, no emojis, clean prose)
- **LLM path:** available but disabled (`USE_LLM=False` in `server/tweet_generator_llm.py`) — enable mid-season once 2026 data is richer and Qwen prompts are tuned
- **Posting status:** pending X developer portal fix (app must be in a Project for v2 API access)

## Confidence Tiers
- **High:** win_prob >= 0.65
- **Medium:** win_prob 0.60–0.65
- **Low:** win_prob < 0.60 (never tweeted)

## Interestingness Scoring

Each game is scored for tweet-worthiness:

| Points | Condition |
|--------|-----------|
| +3 | Odds gap >= 30 ML points favoring our pick |
| +2 | High confidence tier |
| +2 | Strong SHAP factor (\|shap\| >= 0.04) |
| +1 | Medium confidence tier |
| +1 | Predicted winner is market underdog |

Top 3 games by score are tweeted each day. Minimum score: 2.

## Deterministic Tweet Format

The default tweet builder (`server/tweet_generator_llm.py` with `USE_LLM=False`) produces factual, structured tweets:

- Team matchup and predicted winner with win probability
- Top SHAP-derived reason(s) for the pick (ERA edge, momentum, bullpen, matchup)
- Market odds when available and interesting (value edge callout)
- No hashtags, no emojis, no hype language

## SHAP Integration

SHAP values from LightGBM identify the top contributing features per prediction. These are translated to human-readable reasons:
- Starter ERA/quality edge
- Team momentum (rolling win%)
- Bullpen ERA advantage
- Handedness matchup (lineup OPS vs starter hand)
- Run differential edge

Only reasons that support the predicted winner are included (contradicting reasons filtered out).

## LLM Path (Disabled)

When `USE_LLM=True`:
- Calls Qwen 3.5 9B via local Ollama
- Structured system prompt with stat definitions and examples
- Deterministic data passed as context; LLM provides natural language variation
- Falls back to deterministic format if Ollama unavailable

Enable mid-season when:
1. 2026 data has accumulated (teams past cold-start phase)
2. Tweet text has been reviewed for quality
3. X developer portal access is resolved

## What NOT to Do
- Don't tweet low-confidence games
- Don't include hashtags (looks spammy)
- Don't mention the model or ML — use "we think" / "we like"
- Don't fabricate stats — all values come from the feature row and SHAP
- Don't let LLM invent numbers — structured prompt constrains output
