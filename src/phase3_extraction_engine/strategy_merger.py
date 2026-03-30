"""
Strategy Merger with Field-Level Voting — Phase 3.

Runs multiple extraction strategies and produces a consensus result:
  Strategy A: Text Regex (always runs)
  Strategy B: LLM Structured Extraction (runs when API key present)
  Strategy C: Document Parser output (when attachments present)

Voting:
  - USN: weighted majority vote
  - Semester: majority vote
  - Subject list: union of all strategies, deduped by subject_code
  - Marks: prefer higher-confidence source; average only when confidence equal
  - Status: FAIL is sticky on tie; else majority
  - Grade / grade_points: prefer LLM > regex > doc_parser
  - final_confidence: weighted average of strategy confidences
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ── VTU grade scale (used in table normalisation) ────────────────────────────
_GRADE_TO_GP: dict[str, float] = {
    "O": 10.0, "A+": 9.0, "A": 8.0 , "B+": 7.0,
    "B":  6.0, "C":  5.0, "P": 4.0 , "F":  0.0,
    "NE": 0.0, "W": 0.0,  # MSRIT-specific grades
}
_VALID_GRADES: set[str] = set(_GRADE_TO_GP.keys())
_DIGIT_RE = re.compile(r"\d")
# USN: [1-4][AA]##[code 2-4 alpha]###  — widened dept to {2,4} for AIML/CSBS
_USN_RE = re.compile(r"\b([1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3})\b", re.IGNORECASE)


# ── Strategy confidence weights ───────────────────────────────────────────────
_WEIGHTS: dict[str, float] = {
    "universal_llm":   0.90,  # Primary: Universal extraction
    "enhanced_llm":   0.85,  # MSRIT-specific
    "llm":            0.80,  # Generic LLM
    "document_parser": 0.75,
    "text_regex":      0.65,
    "csv_format":      0.70,
}


def _strategy_weight(strategy: str) -> float:
    for key, w in _WEIGHTS.items():
        if key in strategy:
            return w
    return 0.60


# ── Table-row normalisation ───────────────────────────────────────────────────

def _marks_pct_to_grade(marks: int, max_marks: int) -> str:
    """Derive VTU letter grade from marks percentage."""
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


def _normalise_table_subject(row: dict) -> Optional[dict]:
    """
    Normalise a raw parsed-table row dict into a subject record.

    Handles wide-format VTU tables where individual cells may represent
    marks/grades/grade_points extracted deterministically (no LLM inference).
    Returns None if the row has no usable marks or grade data.
    """
    def _int(v) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(float(str(v).strip()))
        except (TypeError, ValueError):
            return None

    def _flt(v) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(str(v).strip())
        except (TypeError, ValueError):
            return None

    max_marks = _int(row.get("max_marks")) or 100
    internal  = _int(row.get("internal_marks") or row.get("internal"))
    external  = _int(row.get("external_marks") or row.get("external"))
    total     = _int(row.get("total_marks") or row.get("marks"))

    # Synthesise total
    if total is None and internal is not None and external is not None:
        total = internal + external
    if total is not None:
        total = max(0, min(total, max_marks))

    # Grade
    grade_raw = str(row.get("grade") or "").strip().upper()
    if _DIGIT_RE.search(grade_raw):
        # Digit in grade — it's actually a grade_points value
        gp_candidate = _flt(grade_raw)
        if gp_candidate is not None:
            for g, gp in sorted(_GRADE_TO_GP.items(), key=lambda x: -x[1]):
                if gp_candidate >= gp:
                    grade_raw = g
                    break
        else:
            grade_raw = ""
    if grade_raw not in _VALID_GRADES:
        grade_raw = _marks_pct_to_grade(total, max_marks) if total is not None else ""

    # Grade points: deterministic lookup
    expected_gp = _GRADE_TO_GP.get(grade_raw) if grade_raw else None
    gp_raw      = _flt(row.get("grade_points"))
    if expected_gp is not None:
        grade_points: Optional[float] = (
            expected_gp
            if gp_raw is None or abs(gp_raw - expected_gp) > 0.5
            else gp_raw
        )
    else:
        grade_points = gp_raw if (gp_raw is not None and 0.0 <= gp_raw <= 10.0) else None

    # Status
    if total is not None:
        status = "PASS" if total >= int(max_marks * 0.40) else "FAIL"
    else:
        status_raw = str(row.get("status") or "").upper()
        status     = status_raw if status_raw in ("PASS", "FAIL", "ABSENT", "WITHHELD") else "FAIL"

    if total is None and not grade_raw:
        return None

    return {
        "subject_code":   str(row.get("subject_code") or "").strip().upper(),
        "subject_name":   str(row.get("subject_name") or "").strip(),
        "internal_marks": internal,
        "external_marks": external,
        "total_marks":    total,
        "max_marks":      max_marks,
        "grade":          grade_raw,
        "grade_points":   grade_points,
        "credits":        int(row.get("credits") or 3),
        "status":         status,
    }


def _normalise_doc_records(raw_rows: list[dict]) -> list[dict]:
    """
    Convert raw parsed-table rows (Phase 2 output) into proper extraction records.
    Groups rows by USN; each USN becomes one record per semester.
    """
    from collections import defaultdict
    grouped: dict[tuple[str, int], list[dict]] = defaultdict(list)

    for row in raw_rows:
        usn = str(row.get("usn") or "UNKNOWN").strip().upper()
        sem = max(1, min(8, int(row.get("semester") or 1)))
        subj = _normalise_table_subject(row)
        if subj:
            grouped[(usn, sem)].append(subj)

    records: list[dict] = []
    for (usn, sem), subjects in grouped.items():
        records.append({
            "usn":      usn,
            "name":     "",
            "semester": sem,
            "sgpa":     None,
            "cgpa":     None,
            "academic_year": "",
            "exam_type": "regular",
            "subjects": subjects,
            "overall_confidence": 0.80,
            "extraction_strategy": "document_parser",
        })
    return records


# ── Subject deduplication with confidence-aware merging ──────────────────────

def _dedupe_subjects(subjects: list[dict]) -> list[dict]:
    """
    Merge duplicate subjects across strategies.
    Key: normalised subject_code, or first 20 chars of subject_name.
    Merging priorities (highest wins):
      - marks: prefer the source with an explicit max_marks; else higher value
      - grade: non-empty non-numeric value wins
      - grade_points: VTU lookup; else higher-confidence source
      - status: FAIL is sticky
    """
    seen: dict[str, dict] = {}
    for s in subjects:
        code = str(s.get("subject_code") or "").strip().upper()
        name = str(s.get("subject_name") or "").strip().upper()
        key  = code if code else (name[:20] if name else f"SUBJ_{len(seen)}")

        if key not in seen:
            seen[key] = dict(s)
        else:
            ex = seen[key]

            # Marks: prefer entry with explicit max_marks and within-range total
            ex_total  = ex.get("total_marks")
            new_total = s.get("total_marks")
            ex_max    = int(ex.get("max_marks") or 100)
            new_max   = int(s.get("max_marks") or 100)

            if new_total is not None and ex_total is None:
                ex["total_marks"] = new_total
                ex["max_marks"]   = new_max
            elif new_total is not None and ex_total is not None:
                # Both have marks — prefer whichever has explicit max_marks != 100
                if new_max != 100 and ex_max == 100:
                    ex["total_marks"] = new_total
                    ex["max_marks"]   = new_max
                # If both have real max_marks, prefer higher-confidence (LLM)
                # (already ordered: LLM subjects come after regex in strategy_results)

            # Grade: prefer valid non-numeric VTU grade
            new_grade = str(s.get("grade") or "").strip().upper()
            if new_grade in _VALID_GRADES and ex.get("grade") not in _VALID_GRADES:
                ex["grade"] = new_grade

            # Grade points: prefer matching VTU lookup
            new_gp = s.get("grade_points")
            ex_gp  = ex.get("grade_points")
            g      = ex.get("grade", "")
            expected = _GRADE_TO_GP.get(g) if g else None
            if expected is not None:
                if ex_gp is None or abs(float(ex_gp) - expected) > 0.5:
                    ex["grade_points"] = expected
            elif new_gp is not None and ex_gp is None:
                ex["grade_points"] = new_gp

            # internal/external marks
            if s.get("internal_marks") is not None and ex.get("internal_marks") is None:
                ex["internal_marks"] = s["internal_marks"]
            if s.get("external_marks") is not None and ex.get("external_marks") is None:
                ex["external_marks"] = s["external_marks"]

            # FAIL is sticky
            if str(s.get("status", "")).upper() == "FAIL":
                ex["status"] = "FAIL"

    return list(seen.values())


# ── USN and semester voting ───────────────────────────────────────────────────

def _vote_usn(records_by_strategy: list[tuple[str, list[dict]]]) -> Optional[str]:
    """Return the most-voted USN across all strategies."""
    votes: dict[str, int] = {}
    for strategy, records in records_by_strategy:
        weight = int(_strategy_weight(strategy) * 100)
        for rec in records:
            usn = str(rec.get("usn") or "").strip().upper()
            if usn and usn != "UNKNOWN" and _USN_RE.fullmatch(usn):
                votes[usn] = votes.get(usn, 0) + weight
    return max(votes, key=lambda k: votes[k]) if votes else None


def _vote_semester(records_by_strategy: list[tuple[str, list[dict]]]) -> int:
    """Majority vote for semester number."""
    votes: dict[int, int] = {}
    for _, records in records_by_strategy:
        for rec in records:
            sem = int(rec.get("semester") or 0)
            if 1 <= sem <= 8:
                votes[sem] = votes.get(sem, 0) + 1
    return max(votes, key=lambda k: votes[k]) if votes else 1


# ── Phase 2 raw-table → structured-records converter ────────────────────────

# Keyword → field name mapping for header detection
_HEADER_KEYWORDS: list[tuple[list[str], str]] = [
    (["usn", "register", "reg no", "roll no"],                      "usn"),
    (["student name", "name"],                                       "name"),
    (["sub code", "subject code", "course code", "code"],           "subject_code"),
    (["subject name", "subject", "course", "paper"],                "subject_name"),
    (["credits", "credit"],                                         "credits"),
    (["internal", "cie", "ia", "int"],                              "internal_marks"),
    (["external", "see", "ext", "theory"],                         "external_marks"),
    (["total marks", "total", "marks obtained", "scored"],          "total_marks"),
    (["max marks", "maximum", "out of"],                            "max_marks"),
    (["grade pts", "grade point", "gp"],                            "grade_points"),
    (["grade"],                                                      "grade"),
    (["status", "result"],                                          "status"),
    (["sgpa"],                                                       "sgpa"),
    (["cgpa"],                                                       "cgpa"),
    (["sem", "semester"],                                            "semester"),
]


def _match_header_cell(cell: str) -> Optional[str]:
    """Return the field name for a header cell, or None."""
    c = cell.lower().strip()
    for keywords, field in _HEADER_KEYWORDS:
        if any(kw in c for kw in keywords):
            return field
    return None


def raw_tables_to_doc_records(tables: list[list[list[str]]]) -> list[dict]:
    """
    Convert raw Phase 2 table output (list of tables, each a list of string rows)
    into structured row-level dicts suitable for _normalise_doc_records().

    Handles simple tabular layouts; returns [] for wide-format VTU tables where
    LLM strategy will cover extraction instead.
    """
    all_rows: list[dict] = []

    for table in tables:
        if not table or len(table) < 2:
            continue

        # Detect header row (first 10 rows)
        header_idx = -1
        field_map: dict[int, str] = {}  # col_index → field_name
        for i, row in enumerate(table[:10]):
            mapping = {}
            for j, cell in enumerate(row):
                field = _match_header_cell(cell)
                if field:
                    mapping[j] = field
            # Require at least 3 recognisable columns
            if len(mapping) >= 3:
                header_idx = i
                field_map  = mapping
                break

        if header_idx < 0 or not field_map:
            # No recognisable header — skip this table (LLM handles via text)
            continue

        usn_col     = next((j for j, f in field_map.items() if f == "usn"),  None)
        name_col    = next((j for j, f in field_map.items() if f == "name"), None)
        has_usn_col = usn_col is not None

        current_usn  = ""
        current_name = ""

        for row in table[header_idx + 1:]:
            if not any(cell.strip() for cell in row):
                continue

            row_dict: dict = {}
            for j, field in field_map.items():
                if j < len(row):
                    row_dict[field] = row[j].strip()

            # Carry forward USN/name if this row is a continuation
            if has_usn_col:
                usn_val = row_dict.get("usn", "").strip()
                if _USN_RE.match(usn_val):
                    current_usn  = usn_val.upper()
                    current_name = row_dict.get("name", current_name)
            if current_usn:
                row_dict["usn"]  = current_usn
                row_dict["name"] = current_name

            # Skip rows with no subject data
            if not row_dict.get("subject_code") and not row_dict.get("subject_name"):
                continue
            if not row_dict.get("total_marks") and not row_dict.get("grade"):
                continue

            all_rows.append(row_dict)

    return all_rows


# ── Main entry point ─────────────────────────────────────────────────────────

def extract_with_voting(
    text: str,
    doc_records: Optional[list[dict]] = None,
    run_llm: bool = True,
) -> list[dict]:
    """
    Run all extraction strategies and merge using field-level voting.

    Args:
        text:        Raw email/document text.
        doc_records: Pre-parsed rows from Phase 2 doc parser.
        run_llm:     Whether to invoke the LLM strategy.

    Returns:
        Merged list of extraction records with consensus data.
    """
    from src.api.routes.pipeline import _extract_from_body  # noqa: local import

    strategy_results: list[tuple[str, list[dict]]] = []

    # ── Strategy A: Text Regex ────────────────────────────────────────────────
    try:
        regex_records = _extract_from_body({"subject": "", "body": text})
        if regex_records:
            strategy_results.append(("text_regex", regex_records))
            logger.debug("strategy_merger: regex → %d records", len(regex_records))
    except Exception as exc:
        logger.warning("strategy_merger: regex failed: %s", exc)

    # ── Strategy B: Universal LLM Extraction ────────────────────────────────────
    if run_llm:
        try:
            from .universal_extractor import create_universal_extractor
            universal_extractor = create_universal_extractor()
            llm_records = universal_extractor.extract_with_fallback(text)
            if llm_records:
                strategy_results.append(("universal_llm", llm_records))
                logger.debug("strategy_merger: universal_llm → %d records", len(llm_records))
        except Exception as exc:
            logger.warning("strategy_merger: universal_llm failed: %s", exc)
            # Fallback to enhanced MSRIT
            try:
                from .enhanced_llm_extractor import create_enhanced_extractor
                enhanced_extractor = create_enhanced_extractor()
                llm_records = enhanced_extractor.extract_with_fallback(text)
                if llm_records:
                    strategy_results.append(("enhanced_llm", llm_records))
                    logger.debug("strategy_merger: enhanced_llm_fallback → %d records", len(llm_records))
            except Exception as fallback_exc:
                logger.warning("strategy_merger: enhanced_llm_fallback failed: %s", fallback_exc)
                # Final fallback to original LLM
                try:
                    from .llm_extractor import llm_extract
                    llm_records = llm_extract(text)
                    if llm_records:
                        strategy_results.append(("llm", llm_records))
                        logger.debug("strategy_merger: llm_final_fallback → %d records", len(llm_records))
                except Exception as final_exc:
                    logger.warning("strategy_merger: all llm strategies failed: %s", final_exc)

    # ── Strategy C: Document Parser rows (normalised from Phase 2 tables) ─────
    if doc_records:
        # doc_records are raw table rows — normalise them first
        norm = _normalise_doc_records(doc_records)
        if norm:
            strategy_results.append(("document_parser", norm))
            logger.debug("strategy_merger: doc_parser → %d records", len(norm))

    if not strategy_results:
        return []

    # ── Multi-semester fast-path ──────────────────────────────────────────────
    # If universal LLM returned distinct per-semester records, use them directly
    for strat, recs in strategy_results:
        if strat in ("universal_llm", "enhanced_llm", "llm") and recs:
            llm_sems = {
                int(r.get("semester") or 0)
                for r in recs
                if 1 <= int(r.get("semester") or 0) <= 8
            }
            if len(llm_sems) > 1:
                # Multi-semester - highest confidence
                result = []
                for r in recs:
                    r_copy = dict(r)
                    r_copy["subjects"] = _dedupe_subjects(r.get("subjects") or [])
                    confidence_map = {"universal_llm": 0.95, "enhanced_llm": 0.90, "llm": 0.85}
                    r_copy["overall_confidence"] = confidence_map.get(strat, 0.85)
                    r_copy["extraction_strategy"] = f"{strat}_multisem"
                    result.append(r_copy)
                logger.info(
                    "strategy_merger: multi-semester → %d records (sems: %s, strategy: %s)",
                    len(result), sorted(llm_sems), strat,
                )
                return result

    # ── Vote on USN and semester ──────────────────────────────────────────────
    best_usn = _vote_usn(strategy_results)
    best_sem = _vote_semester(strategy_results)

    # ── Merge subjects and GPA across strategies (LLM data takes priority) ───
    all_subjects: list[dict] = []
    all_sgpa:     list[float] = []
    all_cgpa:     list[float] = []
    all_names:    list[str]   = []

    # Process in ascending confidence order so LLM values overwrite lower ones
    ordered = sorted(strategy_results, key=lambda t: _strategy_weight(t[0]))
    for strategy, records in ordered:
        for rec in records:
            all_subjects.extend(rec.get("subjects") or [])
            sgpa = rec.get("sgpa")
            cgpa = rec.get("cgpa")
            if sgpa is not None:
                try:
                    v = float(sgpa)
                    if 0.0 <= v <= 10.0:
                        all_sgpa.append(v)
                except (TypeError, ValueError):
                    pass
            if cgpa is not None:
                try:
                    v = float(cgpa)
                    if 0.0 <= v <= 10.0:
                        all_cgpa.append(v)
                except (TypeError, ValueError):
                    pass
            if rec.get("name"):
                all_names.append(str(rec["name"]).strip())

    merged_subjects = _dedupe_subjects(all_subjects)

    # ── Compute merged confidence ─────────────────────────────────────────────
    all_confs = [
        rec.get("overall_confidence", _strategy_weight(strat))
        for strat, records in strategy_results
        for rec in records
    ]
    n = len(strategy_results)
    merged_confidence = min(
        0.97,
        (sum(all_confs) / len(all_confs) if all_confs else 0.60) + (n - 1) * 0.04,
    )

    # ── Deterministic SGPA from merged subjects ───────────────────────────────
    # Compute from grade_points × credits; fall back to averaged extracted value
    from .validator import _compute_sgpa  # noqa: local import
    computed_sgpa = _compute_sgpa(merged_subjects)

    if computed_sgpa is not None:
        merged_sgpa: Optional[float] = computed_sgpa
    elif all_sgpa:
        candidate = round(sum(all_sgpa) / len(all_sgpa), 2)
        merged_sgpa = candidate if 0.0 <= candidate <= 10.0 else None
    else:
        merged_sgpa = None

    merged_cgpa = (
        round(sum(all_cgpa) / len(all_cgpa), 2) if all_cgpa else None
    )

    merged = {
        "usn":      best_usn or "UNKNOWN",
        "name":     max(all_names, key=len) if all_names else "",
        "semester": best_sem,
        "sgpa":     merged_sgpa,
        "cgpa":     merged_cgpa,
        "academic_year": "",
        "exam_type": "regular",
        "subjects":  merged_subjects,
        "overall_confidence": round(merged_confidence, 3),
        "extraction_strategy": "+".join(s for s, _ in strategy_results),
        "strategies_used": [s for s, _ in strategy_results],
    }

    return [merged] if merged["usn"] != "UNKNOWN" or merged["subjects"] else []
