"""
LLM Structured Extraction — Phase 3.

Uses Groq (Llama-3.3-70b) → OpenAI → Gemini (in order of preference) to
extract student result data as structured JSON from ambiguous text layouts.

Falls back gracefully when no API key is configured.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

_GROQ_KEY   = os.getenv("GROQ_API_KEY", "")
_OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
_GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")

# ── VTU grade scale (used for normalisation) ──────────────────────────────────
_GRADE_TO_GP: dict[str, float] = {
    "O":  10.0, "A+": 9.0, "A": 8.0, "B+": 7.0,
    "B":   6.0, "C":  5.0, "P": 4.0, "F":  0.0,
}
_VALID_GRADES: set[str] = set(_GRADE_TO_GP.keys())

_EXTRACTION_SYSTEM_PROMPT = """You are an academic result extraction engine specialised in VTU/MSRIT grade reports.
Extract student result data from the provided text.

Return ONLY a JSON array of objects, no explanation. Each object must have:
{
  "usn": "string (student unique registration number, e.g. 1MS21CS042)",
  "name": "string (student name, or empty string)",
  "semester": integer (1-8),
  "sgpa": float or null (range 0.0–10.0),
  "cgpa": float or null (range 0.0–10.0),
  "academic_year": "string e.g. 2023-24 or empty",
  "exam_type": "regular|supplementary|improvement",
  "subjects": [
    {
      "subject_code": "string (e.g. 21CS51)",
      "subject_name": "string",
      "internal_marks": integer or null,
      "external_marks": integer or null,
      "total_marks": integer (sum of internal + external when both present),
      "max_marks": integer (default 100),
      "grade": "O|A+|A|B+|B|C|P|F  — MUST be one of these VTU symbols, never a number",
      "grade_points": float (O=10, A+=9, A=8, B+=7, B=6, C=5, P=4, F=0),
      "credits": integer (default 3),
      "status": "PASS|FAIL|ABSENT"
    }
  ]
}

STRICT RULES — violations will cause downstream errors:
1. MULTIPLE SEMESTERS: If results for multiple semesters appear, return ONE object per semester.
   Never merge subjects from different semesters into one object.
2. GRADES: grade must be exactly one of: O, A+, A, B+, B, C, P, F
   — NEVER put a number in the grade field.
   — Derive grade from marks percentage if not printed:
     ≥90% → O, ≥80% → A+, ≥70% → A, ≥60% → B+, ≥55% → B, ≥50% → C, ≥40% → P, <40% → F
3. GRADE POINTS: must be the exact VTU value for the grade (O=10.0, A+=9.0, A=8.0,
   B+=7.0, B=6.0, C=5.0, P=4.0, F=0.0). Do NOT invent intermediate values.
4. MARKS: total_marks must be an integer within [0, max_marks]. Never hallucinate marks.
   If the value is unclear, omit the subject rather than inventing a number.
5. SGPA/CGPA: must be a float in [0.0, 10.0]. Compute as Σ(grade_points × credits) / Σ(credits).
6. STATUS: PASS if total_marks >= 40% of max_marks, else FAIL.
7. USN format: [1-4][A-Z]{2}[0-9]{2}[A-Z]{2,4}[0-9]{3} e.g. 1MS21CS042
8. Return [] if no result data is present.
"""


def _call_groq(prompt: str, model: str = "llama-3.3-70b-versatile") -> str:
    """Call Groq API and return the raw response text."""
    import httpx
    try:
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
    except Exception as exc:
        logger.error(f"Groq LLM extraction failed: {exc}")
        return ""


def _call_openai(prompt: str, model: str = "gpt-4o-mini") -> str:
    """Call OpenAI API as fallback."""
    import httpx
    try:
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
    except Exception as exc:
        logger.error(f"OpenAI LLM extraction failed: {exc}")
        return ""


def _call_gemini(prompt: str, model: str = "gemini-2.0-flash") -> str:
    """Call Google Gemini REST API as last-resort fallback."""
    import httpx
    try:
        full_prompt = _EXTRACTION_SYSTEM_PROMPT + "\n\n" + prompt
        payload = {
            "contents": [{"parts": [{"text": full_prompt}]}],
            "generationConfig": {"temperature": 0.0, "maxOutputTokens": 2048},
        }
        resp = httpx.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            params={"key": _GEMINI_KEY},
            json=payload,
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as exc:
        logger.error(f"Gemini LLM extraction failed: {exc}")
        return ""


def _parse_json_response(raw: str) -> list[dict]:
    """Extract JSON array from LLM response (handles markdown code blocks)."""
    cleaned = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            for key in ("results", "data", "students", "records"):
                if isinstance(parsed.get(key), list):
                    return parsed[key]
            return [parsed]
    except json.JSONDecodeError:
        pass

    m = re.search(r"\[.*\]", cleaned, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass

    return []


def _coerce_int(value) -> Optional[int]:
    """Coerce a value to int; return None on failure."""
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _coerce_float(value) -> Optional[float]:
    """Coerce a value to float; return None on failure."""
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _marks_pct_to_grade(marks: int, max_marks: int) -> str:
    """Derive VTU letter grade from marks/max_marks ratio."""
    if max_marks <= 0:
        return "F"
    pct = (marks / max_marks) * 100.0
    for threshold, grade in [
        (90.0, "O"), (80.0, "A+"), (70.0, "A"), (60.0, "B+"),
        (55.0, "B"), (50.0, "C"), (40.0, "P"), (0.0, "F"),
    ]:
        if pct >= threshold:
            return grade
    return "F"


def _normalise_subject(s: dict) -> Optional[dict]:
    """
    Normalise a raw LLM subject dict to a clean validated schema.
    Returns None if the subject lacks both marks and grade data.
    """
    if not isinstance(s, dict):
        return None

    max_marks  = _coerce_int(s.get("max_marks")) or 100
    internal   = _coerce_int(s.get("internal_marks") or s.get("internal"))
    external   = _coerce_int(s.get("external_marks") or s.get("external"))

    # Synthesise total from internal+external if not explicit
    total_raw  = s.get("total_marks") or s.get("marks")
    total      = _coerce_int(total_raw)
    if total is None and internal is not None and external is not None:
        total = internal + external

    # Clamp marks to valid range
    if total is not None:
        total = max(0, min(total, max_marks))

    # Normalise grade — strip digits and validate against VTU scale
    grade_raw = str(s.get("grade") or "").strip().upper()
    if re.search(r"\d", grade_raw):
        # Grade contains a digit — it's actually a grade_points value, derive grade
        gp_candidate = _coerce_float(grade_raw)
        if gp_candidate is not None and 0.0 <= gp_candidate <= 10.0:
            # Map to nearest VTU grade
            for g, gp in sorted(_GRADE_TO_GP.items(), key=lambda x: -x[1]):
                if gp_candidate >= gp:
                    grade_raw = g
                    break
        elif total is not None:
            grade_raw = _marks_pct_to_grade(total, max_marks)
        else:
            grade_raw = ""

    if grade_raw not in _VALID_GRADES:
        if total is not None:
            grade_raw = _marks_pct_to_grade(total, max_marks)
        else:
            grade_raw = ""

    # Grade points: use VTU lookup, then LLM value, then None
    expected_gp = _GRADE_TO_GP.get(grade_raw) if grade_raw else None
    gp_llm      = _coerce_float(s.get("grade_points"))
    if expected_gp is not None:
        # Use computed value; accept LLM value only if it's exact
        grade_points: Optional[float] = (
            expected_gp if gp_llm is None or abs(gp_llm - expected_gp) > 0.5
            else gp_llm
        )
    else:
        grade_points = gp_llm if (gp_llm is not None and 0.0 <= gp_llm <= 10.0) else None

    # Status: deterministic from marks
    if total is not None:
        status = "PASS" if total >= int(max_marks * 0.40) else "FAIL"
    else:
        status_raw = str(s.get("status") or "").upper()
        status     = status_raw if status_raw in ("PASS", "FAIL", "ABSENT", "WITHHELD") else "FAIL"

    # Skip subjects with no meaningful data
    if total is None and not grade_raw:
        return None

    credits = _coerce_int(s.get("credits")) or 3

    return {
        "subject_code":   str(s.get("subject_code") or "").strip().upper(),
        "subject_name":   str(s.get("subject_name") or "").strip(),
        "internal_marks": internal,
        "external_marks": external,
        "total_marks":    total,
        "max_marks":      max_marks,
        "grade":          grade_raw,
        "grade_points":   grade_points,
        "credits":        credits,
        "status":         status,
    }


def _normalise_record(rec: dict) -> dict:
    """Normalise a single extracted record to match our schema."""
    subjects = [
        ns for s in (rec.get("subjects") or [])
        if (ns := _normalise_subject(s)) is not None
    ]

    # Clamp GPA values
    sgpa = _coerce_float(rec.get("sgpa"))
    cgpa = _coerce_float(rec.get("cgpa"))
    if sgpa is not None:
        sgpa = round(min(10.0, max(0.0, sgpa)), 2)
    if cgpa is not None:
        cgpa = round(min(10.0, max(0.0, cgpa)), 2)

    exam_type = str(rec.get("exam_type") or "regular").lower()
    if exam_type not in ("regular", "supplementary", "improvement"):
        exam_type = "regular"

    return {
        "usn":              str(rec.get("usn") or "UNKNOWN").strip().upper(),
        "name":             str(rec.get("name") or "").strip(),
        "semester":         max(1, min(8, _coerce_int(rec.get("semester")) or 1)),
        "sgpa":             sgpa,
        "cgpa":             cgpa,
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
    if not _GROQ_KEY and not _OPENAI_KEY and not _GEMINI_KEY:
        logger.debug("llm_extract: no API key configured — skipping LLM extraction")
        return []

    prompt = (
        "Extract ALL student result records from the text below.\n"
        "Return each row in the table as one subject entry.\n"
        "Use only VTU grade symbols (O/A+/A/B+/B/C/P/F) — never put a number in the grade field.\n\n"
        f"{text[:4000]}"
    )

    # Build ordered provider list; each entry is (name, callable)
    _providers: list[tuple[str, Any]] = []
    if _GROQ_KEY:
        _providers.append(("groq",   _call_groq))
    if _OPENAI_KEY:
        _providers.append(("openai", _call_openai))
    if _GEMINI_KEY:
        _providers.append(("gemini", _call_gemini))

    for attempt in range(max_retries + 1):
        raw: Optional[str] = None

        # Waterfall: try each provider in turn; stop at first success
        for provider_name, call_fn in _providers:
            try:
                raw = call_fn(prompt)
                logger.debug("llm_extract: provider=%s succeeded (attempt %d)", provider_name, attempt + 1)
                break
            except Exception as exc:
                logger.warning(
                    "llm_extract: provider=%s failed (attempt %d): %s",
                    provider_name, attempt + 1, exc,
                )

        if raw is None:
            # All providers failed on this attempt
            if attempt < max_retries:
                prompt = (
                    "The previous extraction returned no data or an error occurred.\n"
                    "Look more carefully for USN (format: 1MS23CS042), subject names, "
                    "marks (integers), and VTU grades (O/A+/A/B+/B/C/P/F).\n\n"
                    f"{text[:4000]}"
                )
            continue

        records = _parse_json_response(raw)
        if not records:
            logger.debug("llm_extract: empty response on attempt %d", attempt + 1)
            if attempt < max_retries:
                prompt = (
                    "The previous extraction returned no data or an error occurred.\n"
                    "Look more carefully for USN (format: 1MS23CS042), subject names, "
                    "marks (integers), and VTU grades (O/A+/A/B+/B/C/P/F).\n\n"
                    f"{text[:4000]}"
                )
            continue

        normalised = [_normalise_record(r) for r in records]
        # Filter out completely empty records
        normalised = [r for r in normalised if r["subjects"] or r["usn"] != "UNKNOWN"]
        if normalised:
            logger.info("llm_extract: extracted %d record(s)", len(normalised))
            return normalised

    return []
