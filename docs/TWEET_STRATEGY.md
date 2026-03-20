# Tweet Content Strategy
Last updated: 2026-03-20

## Philosophy
We tweet 2-3 games per day, not all of them. Every tweet should say something specific and interesting — not just "we pick Team X to win." The goal is to build credibility by showing our reasoning, surface genuine edges where our model disagrees with the market, and use Qwen to make the language feel human each time.

---

## Deterministic Data Signals (the interesting stuff we can pull)

### 1. Implied Odds vs Market Odds — Value Edge
Our model outputs a win probability (e.g. 64% home win). Convert to implied American odds:
- `implied_odds = -(prob / (1 - prob)) * 100` if prob > 0.5 → e.g. 64% = -178
- Compare against market odds we pull from The Odds API

**When it's interesting:** We say home team wins 64% (implied -178) but market has them at +110 (underdog). That's a massive disagreement — either we see something the market doesn't, or the market knows something we don't. Worth calling out.

**Tweet angle:** "Our model has NYY at 64% tonight — market has them as +115 underdogs. We see value on the Yankees."

**Filter:** Only tweet when the gap between our implied odds and market odds is meaningful (e.g. >30 moneyline points in the same direction as our pick). This is the highest-signal case.

### 2. SHAP-Based "Why We Like This Pick"
LightGBM supports SHAP values natively via `booster.predict(X, pred_contrib=True)`. For any prediction we can extract the top 3 contributors — the specific features that pushed the model toward one team.

**Examples of what SHAP might surface:**
- "Their starter has a career ERA 1.2 runs lower than the opponent's" → pitcher quality edge
- "Dodgers are +0.9 run differential/game better over the last 10 games" → hot team
- "Home team bullpen ERA is 0.8 runs better this season" → bullpen edge
- "Visiting lineup is 0.040 OPS lower vs LHP, and they're facing an LHP tonight" → handedness matchup

**Tweet angle:** "We like the Dodgers tonight (67%). Their bullpen ERA is 0.8 runs better than SF's this season, and the Giants are facing a tough lefty matchup."

**Filter:** Only use SHAP angles that are human-readable (deltas, ERA, win%, OPS). Skip features like `humidity_pct` even if they have high SHAP (not interesting to readers).

### 3. Upset Alert — High Confidence Pick on an Underdog
When our model gives 60%+ to a team the market has as a significant underdog (e.g. +150 or longer).

**Tweet angle:** "Upset alert: we like the A's (+160) tonight at 61% confidence. Their starter has been dominant — 2.1 ERA in last 5 starts."

### 4. Dominant Starter Spotlight
When the biggest single SHAP contributor is starter quality (career or season ERA differential), and it's a significant gap (e.g. >1.5 ERA difference).

**Tweet angle:** "Gerrit Cole vs. a 5.1 ERA starter tonight. Our model loves the Yankees here (72%)."

### 5. Hot Team Momentum
When `rolling_last10_win_pct_delta` is the dominant SHAP contributor and one team is significantly hotter (e.g. 8/10 in last 10 vs 3/10).

**Tweet angle:** "Cubs have won 8 of their last 10, visiting a Reds team that's 3-7 over the same stretch. Model gives Chicago 65% tonight."

### 6. Mismatch Alert — Extreme Feature Delta
When a single comparative feature is very extreme (e.g. one team's OPS is 0.100+ higher, or run differential delta is >2.0 per game).

**Tweet angle:** "Yankees outscoring opponents by 2.3 runs/game this season vs Tigers at -0.9. Hard to fade that kind of gap."

---

## Tweet Selection Logic (which games to tweet)

Score each game on "interestingness":
1. **+3 pts:** Odds gap ≥ 30 ML points in our direction (model disagrees with market)
2. **+2 pts:** Confidence ≥ 65% (high tier)
3. **+2 pts:** Top SHAP feature is interpretable and has a strong delta (>1 ERA, >0.8 bullpen ERA, >0.060 OPS)
4. **+1 pt:** Confidence 60-65% (medium tier)
5. **+1 pt:** Underdog pick (predicted winner is market underdog)

Tweet top 2-3 games by score each day. Never tweet a game scoring 0.

---

## Qwen Integration

Both Qwen 3.5 4B and 9B are running locally on this machine via Ollama.

**API call:**
```python
import requests
response = requests.post("http://127.0.0.1:11434/api/generate", json={
    "model": "qwen3.5:4b",  # 4B is fast enough, 9B for better quality
    "prompt": <structured prompt>,
    "stream": False,
    "options": {"temperature": 0.7, "max_tokens": 120}
})
tweet = response.json()["response"].strip()
```

**Latency:** Qwen 3.5 4B generates ~120 tokens in ~3-5 seconds locally. Fast enough for a tweet that gets sent 1 hour before game time. Use 9B if we have time.

**Structured prompt template:**
```
You write short MLB prediction tweets. Be specific and confident, not hype-y. No hashtags. No emojis unless one fits naturally. Max 240 characters.

Game: {away_team} @ {home_team}
Our pick: {predicted_winner} ({win_pct}% confidence)
Market odds: {predicted_winner} at {market_odds}
{value_note}  ← e.g. "We see value — market has them as underdogs" or ""

Top reasons we like this pick:
{shap_reason_1}
{shap_reason_2}
{shap_reason_3}

Write one tweet about this pick. Be direct. Sound like a sharp sports bettor, not a bot.
```

**SHAP reason formatting (human-readable translations):**
Map feature names to natural language before passing to Qwen:
```python
FEATURE_LABELS = {
    "away_starter_career_era": "away starter career ERA",
    "home_starter_career_era": "home starter career ERA",
    "starter_era_delta": "starter ERA edge",
    "run_diff_per_game_delta": "run differential edge",
    "bullpen_era_delta": "bullpen ERA edge",
    "starter_k_pct_delta": "strikeout rate edge",
    "win_pct_delta": "win% edge",
    "rolling_last10_win_pct_delta": "recent form edge",
    "vs_starter_hand_ops_delta": "lineup-starter handedness matchup",
    ...
}
```

Only pass the top 2-3 SHAP contributors that have interpretable labels and meaningful magnitude (|SHAP| > 0.01). Skip weather, doubleheader flags, etc.

---

## What NOT to do
- Don't tweet games where confidence is < 60% with nothing else interesting
- Don't include hashtags (looks spammy)
- Don't let Qwen go off-script into general baseball commentary — the prompt is tight and specific
- Don't mention the model or ML — just "we think" / "we like" — keeps it approachable
- Don't fabricate stats — always pass real values from the feature row, never let Qwen invent them

---

## Implementation Plan

### Phase 1 (now — during season): SHAP extraction
- Add `scripts/inference/explainer.py` — takes feature dict + model, returns top-N human-readable SHAP reasons
- Add to `predict_today.py`: after scoring, run explainer, store SHAP reasons in `daily_predictions` table

### Phase 2 (early April): Odds comparison
- In `fetch_odds.py`: compute `implied_odds` from our `home_win_prob` and store `odds_gap` in `daily_predictions`
- Add tweet scoring logic to `schedule_tweets.py`

### Phase 3 (mid-April): Qwen generation
- Add `server/tweet_generator_llm.py` — wraps Ollama API call with structured prompt
- Wire into tweet scheduling: for top-scored games, call Qwen to generate tweet text
- Fall back to deterministic scaffold (`server/tweet_scaffold.py`) if Ollama is unavailable

### Phase 4 (ongoing): Tune and iterate
- Log tweet text + engagement (if Twitter API exposes it)
- Adjust Qwen prompt based on what reads well vs feels robotic
- Experiment with 4B vs 9B quality tradeoff

---

## Example Tweets (illustrative)

**Odds value pick:**
> We have the Cubs at 63% tonight, market has them at +125. Their starter's career ERA is 0.9 runs better than Milwaukee's, and they've won 7 of their last 10. Taking Chicago.

**Dominant starter:**
> Gerrit Cole (2.8 career ERA) vs a 5.1 ERA starter. Model gives the Yankees 71%. Hard to fade that pitching gap.

**Hot team:**
> Dodgers have outscored opponents by 2.1 runs/game over the last 10. Visiting a Cubs team that's 3-7 in the same stretch. LA at 66%.

**Handedness matchup:**
> Giants lineup has been 40 points of OPS worse vs left-handed starters this season. They're facing a lefty tonight. Taking the Phillies at 62%.
