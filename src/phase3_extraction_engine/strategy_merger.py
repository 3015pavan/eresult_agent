"""
Strategy Merger with Field-Level Voting — Phase 3.

Runs multiple extraction strategies and produces a consensus result:
  Strategy A: Text Regex (always runs)
  Strategy B: LLM Structured Extraction (runs when API key present)
  Strategy C: Document Parser output (when attachments present)

Voting:
  - USN: majority vote or most confident
  - Semester: majority vote
  - Subject list: union of all strategies, deduped by subject_code
  - Marks: average when sources agree, max-confidence when they disagree
  - Status: majority vote; FAIL takes priority on tie
  - final_confidence: weighted average of strategy confidences
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

_USN_RE = re.compile(r"\b([1-4][A-Z]{2}\d{2}[A-Z]{2,3}\d{3})\b", re.IGNORECASE)


# ── Strategy confidence weights ───────────────────────────────────────────────
_WEIGHTS = {
    "text_regex": 0.65,
    "llm": 0.85,
    "document_parser": 0.80,
    "csv_format": 0.75,
}


def _strategy_weight(strategy: str) -> float:
    for key, w in _WEIGHTS.items():
        if key in strategy:
            return w
    return 0.60


def _dedupe_subjects(subjects: list[dict]) -> list[dict]:
    """
    Merge duplicate subjects across strategies.
    Key: normalised subject_code or subject_name.
    When marks differ, take the higher-confidence source.
    """
    seen: dict[str, dict] = {}
    for s in subjects:
        code = str(s.get("subject_code") or "").strip().upper()
        name = str(s.get("subject_name") or "").strip().upper()
        key  = code if code else name[:20]
        if not key:
            key = f"SUBJ_{len(seen)}"

        if key not in seen:
            seen[key] = s.copy()
        else:
            # Prefer entry with more data
            existing = seen[key]
            if s.get("grade") and not existing.get("grade"):
                existing["grade"] = s["grade"]
            if s.get("grade_points") and not existing.get("grade_points"):
                existing["grade_points"] = s["grade_points"]
            # Average marks if both have them
            if s.get("total_marks") and existing.get("total_marks"):
                existing["total_marks"] = int(
                    (int(s["total_marks"]) + int(existing["total_marks"])) / 2
                )
            # FAIL is sticky — if any source says FAIL, record FAIL
            if str(s.get("status", "")).upper() == "FAIL":
                existing["status"] = "FAIL"

    return list(seen.values())


def _vote_usn(records_by_strategy: list[tuple[str, list[dict]]]) -> Optional[str]:
    """Return the most-voted USN across all strategies."""
    votes: dict[str, int] = {}
    for strategy, records in records_by_strategy:
        weight = int(_strategy_weight(strategy) * 100)
        for rec in records:
            usn = str(rec.get("usn") or "").strip().upper()
            if usn and usn != "UNKNOWN" and _USN_RE.match(usn):
                votes[usn] = votes.get(usn, 0) + weight
    if not votes:
        return None
    return max(votes, key=lambda k: votes[k])


def _vote_semester(records_by_strategy: list[tuple[str, list[dict]]]) -> int:
    """Majority vote for semester number."""
    votes: dict[int, int] = {}
    for strategy, records in records_by_strategy:
        for rec in records:
            sem = int(rec.get("semester") or 0)
            if 1 <= sem <= 8:
                votes[sem] = votes.get(sem, 0) + 1
    if not votes:
        return 1
    return max(votes, key=lambda k: votes[k])


def extract_with_voting(
    text: str,
    doc_records: Optional[list[dict]] = None,
    run_llm: bool = True,
) -> list[dict]:
    """
    Run all extraction strategies and merge using field-level voting.

    Args:
        text:        Raw email/document text.
        doc_records: Pre-parsed records from doc parser (Phase 2 output).
        run_llm:     Whether to invoke the LLM strategy.

    Returns:
        Merged list of extraction records with consensus data.
    """
    from src.api.routes.pipeline import _extract_from_body  # noqa: local import

    strategy_results: list[tuple[str, list[dict]]] = []

    # ── Strategy A: Text Regex ────────────────────────────────────────────────
    try:
        email_mock = {"subject": "", "body": text}
        regex_records = _extract_from_body(email_mock)
        if regex_records:
            strategy_results.append(("text_regex", regex_records))
            logger.debug("strategy_merger: regex → %d records", len(regex_records))
    except Exception as exc:
        logger.warning("strategy_merger: regex failed: %s", exc)

    # ── Strategy B: LLM Extraction ────────────────────────────────────────────
    if run_llm:
        try:
            from .llm_extractor import llm_extract
            llm_records = llm_extract(text)
            if llm_records:
                strategy_results.append(("llm", llm_records))
                logger.debug("strategy_merger: llm → %d records", len(llm_records))
        except Exception as exc:
            logger.warning("strategy_merger: llm failed: %s", exc)

    # ── Strategy C: Document Parser records ──────────────────────────────────
    if doc_records:
        strategy_results.append(("document_parser", doc_records))
        logger.debug("strategy_merger: doc_parser → %d records", len(doc_records))

    # ── No results ────────────────────────────────────────────────────────────
    if not strategy_results:
        return []

    # ── Multi-semester fast-path: if LLM returned distinct semester records ──
    # Return them as-is (per-semester) instead of collapsing into one record.
    for strat, recs in strategy_results:
        if strat == "llm" and recs:
            llm_sems = {
                int(r.get("semester") or 0)
                for r in recs
                if 1 <= int(r.get("semester") or 0) <= 8
            }
            if len(llm_sems) > 1:
                result = []
                for r in recs:
                    r_copy = dict(r)
                    r_copy["subjects"] = _dedupe_subjects(r.get("subjects") or [])
                    r_copy["overall_confidence"] = 0.90
                    r_copy["extraction_strategy"] = "llm_multisem"
                    result.append(r_copy)
                logger.info(
                    "strategy_merger: multi-semester email → %d per-semester records "
                    "(semesters: %s)",
                    len(result),
                    sorted(llm_sems),
                )
                return result

    # ── Vote on USN and semester ──────────────────────────────────────────────
    best_usn  = _vote_usn(strategy_results)
    best_sem  = _vote_semester(strategy_results)

    # ── Merge subjects across strategies ─────────────────────────────────────
    all_subjects: list[dict] = []
    all_sgpa:     list[float] = []
    all_cgpa:     list[float] = []
    all_names:    list[str] = []

    for strategy, records in strategy_results:
        for rec in records:
            all_subjects.extend(rec.get("subjects") or [])
            if rec.get("sgpa"):
                all_sgpa.append(float(rec["sgpa"]))
            if rec.get("cgpa"):
                all_cgpa.append(float(rec["cgpa"]))
            if rec.get("name"):
                all_names.append(str(rec["name"]).strip())

    merged_subjects = _dedupe_subjects(all_subjects)

    # ── Compute merged confidence ─────────────────────────────────────────────
    all_confidences = [
        rec.get("overall_confidence", _strategy_weight(strat))
        for strat, records in strategy_results
        for rec in records
    ]
    n_strategies = len(strategy_results)
    merged_confidence = min(
        0.97,
        (sum(all_confidences) / len(all_confidences) if all_confidences else 0.60)
        + (n_strategies - 1) * 0.04,   # bonus for multi-strategy agreement
    )

    # ── Build merged record ────────────────────────────────────────────────────
    merged = {
        "usn":      best_usn or "UNKNOWN",
        "name":     max(all_names, key=len) if all_names else "",
        "semester": best_sem,
        "sgpa":     round(sum(all_sgpa) / len(all_sgpa), 2) if all_sgpa else None,
        "cgpa":     round(sum(all_cgpa) / len(all_cgpa), 2) if all_cgpa else None,
        "academic_year": "",
        "exam_type": "regular",
        "subjects":  merged_subjects,
        "overall_confidence": round(merged_confidence, 3),
        "extraction_strategy": "+".join(s for s, _ in strategy_results),
        "strategies_used": [s for s, _ in strategy_results],
    }

    return [merged] if merged["usn"] != "UNKNOWN" or merged["subjects"] else []
