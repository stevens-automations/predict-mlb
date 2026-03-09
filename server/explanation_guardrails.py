from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json


ALLOWED_CONFIDENCE_LABELS = {"low", "medium", "high"}
MAX_CAUSE_LENGTH = 220
MAX_CAVEAT_LENGTH = 180
MAX_EVIDENCE_ITEMS = 5
MAX_CAVEATS = 4


@dataclass(frozen=True)
class ExplanationValidationResult:
    valid: bool
    explanation: Optional[Dict[str, Any]]
    errors: List[str]
    dropped_evidence_items: int


def build_replay_explanation_prompt(context: Dict[str, Any]) -> str:
    """Return a strict prompt that asks the model for schema-only JSON.

    This is intentionally rigid for preseason replay mode where we want
    deterministic, auditable explanation output.
    """
    compact_context = json.dumps(context, sort_keys=True)
    return (
        "You are generating a PRESEASON REPLAY explanation. "
        "Output JSON only (no markdown, no prose before/after).\n"
        "Schema:\n"
        "{\n"
        '  "cause": "string <= 220 chars, direct reason for pick",\n'
        '  "confidence": {"label": "low|medium|high", "score": 0.0-1.0},\n'
        '  "evidence": [\n'
        '    {"source": "<must match an allowed source>", "detail": "specific fact from source"}\n'
        "  ],\n"
        '  "caveats": ["string <= 180 chars"]\n'
        "}\n"
        "Rules:"
        " (1) Never invent injuries, line moves, weather, or stats."
        " (2) Every evidence.source must come from allowed sources in context."
        " (3) Keep caveats explicit about uncertainty."
        " (4) If evidence is weak, lower confidence.\n"
        f"Context: {compact_context}"
    )


def _as_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return None
    return None


def _clean_text(value: Any, max_len: int) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:max_len]


def _normalize_confidence(confidence: Any) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    errors: List[str] = []
    if not isinstance(confidence, dict):
        return None, ["confidence_missing_or_invalid"]

    label = str(confidence.get("label", "")).strip().lower()
    if label not in ALLOWED_CONFIDENCE_LABELS:
        errors.append("confidence_label_invalid")

    try:
        score = float(confidence.get("score"))
    except (TypeError, ValueError):
        score = -1.0
    if not (0.0 <= score <= 1.0):
        errors.append("confidence_score_out_of_range")

    if errors:
        return None, errors
    return {"label": label, "score": round(score, 3)}, []


def _normalize_evidence(evidence: Any, allowed_sources: Iterable[str]) -> Tuple[List[Dict[str, str]], int, List[str]]:
    allowed = {str(s).strip() for s in allowed_sources if str(s).strip()}
    dropped = 0
    errors: List[str] = []

    if not isinstance(evidence, list):
        return [], 0, ["evidence_missing_or_invalid"]

    normalized: List[Dict[str, str]] = []
    for item in evidence[:MAX_EVIDENCE_ITEMS]:
        if not isinstance(item, dict):
            dropped += 1
            continue
        source = _clean_text(item.get("source"), 80)
        detail = _clean_text(item.get("detail"), 220)
        if not source or not detail:
            dropped += 1
            continue
        if allowed and source not in allowed:
            dropped += 1
            continue
        normalized.append({"source": source, "detail": detail})

    if not normalized:
        errors.append("evidence_empty_after_validation")
    return normalized, dropped, errors


def validate_replay_explanation_output(
    raw_output: Any,
    *,
    allowed_sources: Iterable[str],
) -> ExplanationValidationResult:
    """Validate/sanitize model output for preseason replay explanation mode."""
    payload = _as_json_dict(raw_output)
    if payload is None:
        return ExplanationValidationResult(False, None, ["output_not_json_object"], 0)

    errors: List[str] = []
    cause = _clean_text(payload.get("cause"), MAX_CAUSE_LENGTH)
    if not cause:
        errors.append("cause_missing_or_invalid")

    confidence, conf_errors = _normalize_confidence(payload.get("confidence"))
    errors.extend(conf_errors)

    evidence, dropped, evidence_errors = _normalize_evidence(payload.get("evidence"), allowed_sources)
    errors.extend(evidence_errors)

    caveats_raw = payload.get("caveats")
    caveats: List[str] = []
    if isinstance(caveats_raw, list):
        for item in caveats_raw[:MAX_CAVEATS]:
            cleaned = _clean_text(item, MAX_CAVEAT_LENGTH)
            if cleaned:
                caveats.append(cleaned)
    else:
        errors.append("caveats_missing_or_invalid")

    if errors:
        return ExplanationValidationResult(False, None, sorted(set(errors)), dropped)

    return ExplanationValidationResult(
        True,
        {
            "cause": cause,
            "confidence": confidence,
            "evidence": evidence,
            "caveats": caveats,
        },
        [],
        dropped,
    )
