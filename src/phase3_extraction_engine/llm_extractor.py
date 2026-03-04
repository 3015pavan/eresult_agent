"""
LLM Structured Extraction — Phase 3.

Uses Groq (Llama-3.3-70b) or any OpenAI-compatible API to extract
student result data as structured JSON from ambiguous text layouts.

Falls back gracefully when no API key is configured.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

_GROQ_KEY  = os.getenv("GROQ_API_KEY", "")
_OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")

_EXTRACTION_SYSTEM_PROMPT = """You are an academic result extraction engine.
Extract student result data from the provided email or document text.

Return ONLY a JSON array of objects, no explanation. Each object must have:
{
  "usn": "string (student unique registration number, e.g. 1MS21CS042)",
  "name": "string (student name, or empty string)",
  "semester": int (1-8),
  "sgpa": float or null,
  "cgpa": float or null,
  "academic_year": "string e.g. 2023-24 or empty",
  "exam_type": "regular|supplementary|improvement",
  "subjects": [
    {
      "subject_code": "string",
      "subject_name": "string",
      "total_marks": int,
      "max_marks": int,
      "grade": "string (A+/A/B/C/D/F) or empty",
      "grade_points": float or null,
      "status": "PASS|FAIL|ABSENT"
    }
  ]
}

Rules:
- CRITICAL: If the email contains results for MULTIPLE semesters, return ONE separate
  object per semester in the array. Never merge subjects from different semesters.
- Each semester block (e.g. "Semester 1", "Semester 2") must become its own JSON object
  with its own semester number, SGPA, and subjects list.
- If no USN is found, still extract subject data if present (usn = "UNKNOWN").
- grade must be letter like A+, A, B, C, D, F — derive from marks if not explicit.
- status: PASS if marks >= 40% of max_marks, else FAIL.
- Return [] if no result data is present.
"""


def _call_groq(prompt: str, model: str = "llama-3.3-70b-versatile") -> str:
    """Call Groq API and return the raw response text."""
    import httpx
    headers = {
        "Authorization": f"Bearer {_GROQ_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _EXTRACTION_SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
    }
    resp = httpx.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers=headers,
        json=payload,
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _call_openai(prompt: str, model: str = "gpt-4o-mini") -> str:
    """Call OpenAI API as fallback."""
    import httpx
    headers = {
        "Authorization": f"Bearer {_OPENAI_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _EXTRACTION_SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 2048,
    }
    resp = httpx.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=payload,
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _parse_json_response(raw: str) -> list[dict]:
    """Extract JSON array from LLM response (handles markdown code blocks)."""
    # Strip markdown fences
    cleaned = re.sub(r"```(?:json)?\s*", "", raw).strip()
    cleaned = cleaned.rstrip("```").strip()

    # Try direct parse
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            # LLM might wrap in {"results": [...]}
            for key in ("results", "data", "students", "records"):
                if isinstance(parsed.get(key), list):
                    return parsed[key]
            return [parsed]
    except json.JSONDecodeError:
        pass

    # Try to extract JSON array with regex
    m = re.search(r"\[.*\]", cleaned, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass

    return []


def _normalise_record(rec: dict) -> dict:
    """Normalise a single extracted record to match our schema."""
    subjects = []
    for s in (rec.get("subjects") or []):
        if not isinstance(s, dict):
            continue
        marks    = int(s.get("total_marks") or s.get("marks") or 0)
        max_m    = int(s.get("max_marks") or 100)
        grade_raw = str(s.get("grade") or "")
        gp_raw   = s.get("grade_points")

        # Derive status from marks if not explicit
        status_raw = str(s.get("status") or "")
        if "PASS" in status_raw.upper():
            status = "PASS"
        elif "FAIL" in status_raw.upper() or "ABSENT" in status_raw.upper():
            status = "FAIL"
        else:
            status = "PASS" if marks >= int(max_m * 0.4) else "FAIL"

        subjects.append({
            "subject_code": str(s.get("subject_code") or "").strip().upper(),
            "subject_name": str(s.get("subject_name") or "").strip(),
            "total_marks":  marks,
            "max_marks":    max_m,
            "grade":        grade_raw.strip().upper(),
            "grade_points": float(gp_raw) if gp_raw is not None else None,
            "status":       status,
        })

    exam_type = str(rec.get("exam_type") or "regular").lower()
    if exam_type not in ("regular", "supplementary", "improvement"):
        exam_type = "regular"

    return {
        "usn":              str(rec.get("usn") or "UNKNOWN").strip().upper(),
        "name":             str(rec.get("name") or "").strip(),
        "semester":         int(rec.get("semester") or 1),
        "sgpa":             float(rec["sgpa"]) if rec.get("sgpa") else None,
        "cgpa":             float(rec["cgpa"]) if rec.get("cgpa") else None,
        "academic_year":    str(rec.get("academic_year") or ""),
        "exam_type":        exam_type,
        "subjects":         subjects,
        "overall_confidence": 0.85,
        "extraction_strategy": "llm",
    }


def llm_extract(text: str, max_retries: int = 2) -> list[dict]:
    """
    Extract student result records from text using an LLM.

    Args:
        text:        Email body or document text (max ~4000 chars).
        max_retries: Number of re-extraction attempts on parse failure.

    Returns:
        List of normalised record dicts (same schema as regex extractor).
        Returns [] when no API key is configured or extraction fails.
    """
    if not _GROQ_KEY and not _OPENAI_KEY:
        logger.debug("llm_extract: no API key configured — skipping LLM extraction")
        return []

    prompt = f"Extract student result data from this text:\n\n{text[:4000]}"

    for attempt in range(max_retries + 1):
        try:
            if _GROQ_KEY:
                raw = _call_groq(prompt)
            else:
                raw = _call_openai(prompt)

            records = _parse_json_response(raw)
            if not records:
                logger.debug("llm_extract: empty response on attempt %d", attempt + 1)
                continue

            normalised = [_normalise_record(r) for r in records]
            logger.info("llm_extract: extracted %d record(s)", len(normalised))
            return normalised

        except Exception as exc:
            logger.warning("llm_extract attempt %d failed: %s", attempt + 1, exc)
            if attempt < max_retries:
                # Add targeted retry prompt if first attempt returned nothing
                prompt = (
                    f"The previous extraction returned no data. "
                    f"Look more carefully for USN (format: 1MS23CS042), "
                    f"subject names, marks, and grades in this text:\n\n{text[:4000]}"
                )

    return []
