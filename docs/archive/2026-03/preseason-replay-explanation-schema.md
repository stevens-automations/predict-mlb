# Preseason Replay Explanation Schema (Guardrailed)

Purpose: prevent unsupported claims in LLM-generated explanation text during preseason replay mode.

## Required output schema

```json
{
  "cause": "short reason for the pick",
  "confidence": {
    "label": "low|medium|high",
    "score": 0.0
  },
  "evidence": [
    {"source": "odds_snapshot", "detail": "specific verifiable fact"}
  ],
  "caveats": ["uncertainty or limitation"]
}
```

## Guardrails

- Output must parse as a JSON object (no free-form text).
- `cause` required and length-capped.
- `confidence.label` restricted to `low|medium|high`.
- `confidence.score` must be within `[0.0, 1.0]`.
- `evidence[*].source` must match allowed source list from runtime context.
- Unsupported/unknown evidence entries are dropped.
- If no evidence survives validation, the explanation is rejected.
- `caveats` must be a list (entries length-capped).

## Example (valid)

```json
{
  "cause": "Model sees a modest edge on home pitching + market mispricing.",
  "confidence": {"label": "medium", "score": 0.63},
  "evidence": [
    {"source": "odds_snapshot", "detail": "Market -115 vs model-implied -129."},
    {"source": "model_features", "detail": "Bullpen run-prevention features favor home."}
  ],
  "caveats": ["Lineup cards are not final yet."]
}
```

## Example (rejected)

```json
{
  "cause": "Huge edge because away starter is hurt.",
  "confidence": {"label": "high", "score": 0.91},
  "evidence": [
    {"source": "twitter_rumor", "detail": "Insider says injury."}
  ],
  "caveats": ["No official report yet."]
}
```

Reason: unsupported evidence source leads to empty validated evidence set.
