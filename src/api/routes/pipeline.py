"""
Pipeline Orchestration Endpoint.

Classify → Extract → Store

Dedup   : Redis SHA-256 exact dedup
Storage : MinIO raw email JSON
Database: PostgreSQL via psycopg2
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor

from src.common.observability import get_logger
from src.common import database as db
from src.common.storage import get_storage
from src.common.cache import get_cache

logger = get_logger(__name__)
router = APIRouter()

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
EMAILS_CACHE = PROJECT_ROOT / "data" / "emails_cache.json"
PIPELINE_STATE_FILE = PROJECT_ROOT / "data" / "state" / "pipeline_state.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_emails_cache() -> list[dict]:
    if not EMAILS_CACHE.exists():
        return []
    try:
        return json.loads(EMAILS_CACHE.read_text(encoding="utf-8"))
    except Exception:
        return []


def _load_pipeline_state() -> dict:
    cache = get_cache()
    state = cache.get_pipeline_state()
    if state:
        return state
    if PIPELINE_STATE_FILE.exists():
        try:
            return json.loads(PIPELINE_STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "last_run": None,
        "emails_processed": 0,
        "records_extracted": 0,
        "status": "idle",
    }


def _save_pipeline_state(state: dict) -> None:
    get_cache().set_pipeline_state(state)
    PIPELINE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    PIPELINE_STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


# ---------------------------------------------------------------------------
# Classify – inline keyword heuristic (no ML model needed)
# ---------------------------------------------------------------------------

import re as _cls_re

# Strongly academic — these words almost never appear outside result/marksheet emails
_STRONG_KEYWORDS = frozenset([
    "sgpa", "cgpa", "marksheet", "scorecard", "grade card", "grade sheet",
    "semester result", "exam result", "internal marks", "grade points",
    "marks obtained", "total marks", "subject code", "subject wise",
    "result declared", "result published", "university result",
    "cie marks", "see marks", "semester gpa", "cumulative gpa",
])

# Weaker — common in student emails but also appear in other contexts
_WEAK_KEYWORDS = frozenset([
    "result", "marks", "grade", "usn", "pass", "fail",
    "subject", "semester", "1ms", "2ms", "3ms", "4ms",
    "backlog", "arrear", "revaluation", "re-appear",
])

# Negative signals — strongly indicate marketing / job / newsletter emails
_NEGATIVE_KEYWORDS = frozenset([
    "unsubscribe", "click here to unsubscribe", "opt-out", "opt out",
    "internship opportunity", "job opening", "hiring", "we are hiring",
    "job alert", "apply now", "career", "recruitment",
    "newsletter", "promotional", "discount", "offer expires",
    "congratulations on your purchase", "your order", "invoice",
    "meeting invite", "calendar event", "zoom link",
    "follow us on", "social media", "linkedin", "twitter",
    "click to unsubscribe", "marketing", "sponsored",
    "limited time", "act now", "free trial", "sign up",
])

# USN pattern for classification check
_USN_CLS_RE = _cls_re.compile(r"\b[1-4][a-z]{2}\d{2}[a-z]{2,4}\d{3}\b", _cls_re.I)

_ATTACHMENT_EXTS = frozenset([".pdf", ".xlsx", ".xls", ".csv"])


def _classify_email(email: dict) -> tuple[str, float]:
    """Return (classification_label, confidence) using keyword heuristics."""
    subject = (email.get("subject") or "").lower()
    body    = (email.get("body") or email.get("snippet") or "").lower()
    text    = f"{subject} {body}"

    attachments = email.get("attachments") or []
    has_result_attachment = any(
        any(str(a.get("filename", "")).lower().endswith(ext) for ext in _ATTACHMENT_EXTS)
        for a in attachments
    )

    # ── Negative override: clearly not a result email ────────────────
    neg_hits = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in text)
    if neg_hits >= 2:
        return "other", 0.1

    # ── Count strong and weak keyword hits ───────────────────────────
    strong_hits = sum(1 for kw in _STRONG_KEYWORDS if kw in text)
    weak_hits   = sum(1 for kw in _WEAK_KEYWORDS   if kw in text)
    has_usn     = bool(_USN_CLS_RE.search(text))

    # ── Classification rules (strictest first) ────────────────────────
    # Rule 1: Any strong keyword → almost certainly a result email
    if strong_hits >= 1:
        confidence = min(0.95, 0.70 + strong_hits * 0.08 + weak_hits * 0.03)
        return "result_email", round(confidence, 2)

    # Rule 2: USN pattern present + at least 2 weak keywords
    if has_usn and weak_hits >= 2:
        confidence = min(0.90, 0.65 + weak_hits * 0.05)
        return "result_email", round(confidence, 2)

    # Rule 3: USN present + result-focused attachment
    if has_usn and has_result_attachment:
        return "result_email", 0.85

    # Rule 4: Attachment with ≥2 weak keywords (e.g. marksheet PDF without USN in body)
    if has_result_attachment and weak_hits >= 2:
        return "result_email", 0.75

    # Rule 5: Exactly "result" or "marks" in subject with weak keyword support
    subj_has_result = any(kw in subject for kw in ("result", "marks", "marksheet", "grade", "sgpa", "cgpa"))
    if subj_has_result and weak_hits >= 2:
        confidence = min(0.85, 0.60 + weak_hits * 0.04)
        return "result_email", round(confidence, 2)

    return "other", 0.1


# ---------------------------------------------------------------------------
# Phase 3 – extraction regexes
# ---------------------------------------------------------------------------
import re as _re

# Core USN pattern: [1-4][institution-2chars][year-2digits][dept-2-4chars][roll-3digits]
# Handles: 1MS23CS001, 1MS23CS147, 1RV22IS042, 4BM21CS200, 1MS23EC001, etc.
_USN_RE   = _re.compile(r"\b([1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3})\b", _re.I)

# Also match explicit "USN:" and "USN : XXX" annotations in subject/body
_USN_LABEL_RE = _re.compile(r"(?:USN|Reg(?:istration)?\s*No\.?)[\s:]+([1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3})\b", _re.I)

_SGPA_RE  = _re.compile(r"(?:s\.?g\.?p\.?a|semester\s+gpa|grade\s+point)[^\d]*(\d{1,2}\.\d{1,2})", _re.I)
_CGPA_RE  = _re.compile(r"(?:c\.?g\.?p\.?a|cumulative\s+gpa|overall\s+gpa)[^\d]*(\d{1,2}\.\d{1,2})", _re.I)

# Numeric semester
_SEM_RE   = _re.compile(r"(?:semester|sem(?:ester)?|term)[^\d]*(\d{1,2})", _re.I)
# Ordinal word semester: "fifth semester", "3rd semester", "II semester" etc.
_SEM_ORD_RE = _re.compile(
    r"(?:semester|sem)[\s:]+"
    r"(I{1,3}V?|VI{0,3}|IV|VIII?|IX?X?|first|second|third|fourth|fifth|sixth|seventh|eighth|\d{1,2}(?:st|nd|rd|th)?)",
    _re.I,
)
_ORDINAL_MAP = {
    "i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6, "vii": 7, "viii": 8,
    "first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5,
    "sixth": 6, "seventh": 7, "eighth": 8,
    "1st": 1, "2nd": 2, "3rd": 3, "4th": 4, "5th": 5, "6th": 6, "7th": 7, "8th": 8,
}

def _parse_semester(text: str) -> int:
    """Find semester number from text, handling ordinals and Roman numerals."""
    m = _SEM_RE.search(text)
    if m:
        return max(1, min(8, int(m.group(1))))
    m2 = _SEM_ORD_RE.search(text)
    if m2:
        raw = m2.group(1).strip().lower().rstrip(".")
        if raw in _ORDINAL_MAP:
            return _ORDINAL_MAP[raw]
        try:
            return max(1, min(8, int(raw)))
        except ValueError:
            pass
    return 1

_NAME_RE  = _re.compile(
    r"(?:student\s*(?:name)?|name|dear)\s*[:\s,]+"
    r"([A-Z][a-z]+(?:[ \t]+[A-Z][a-z]+){0,4})",
    _re.I,
)

# Format A: subjectcode - Subject Name : 88/100 - PASS   (with optional max_marks)
_SUBJ_RE  = _re.compile(
    r"^([A-Z0-9]{4,12})\s*[-\u2013:]\s*([^:\n]{3,60}?)[:\s]+(\d{1,3})(?:[/ ](\d{1,3}))?\s*(?:marks?)?\s*[-\u2013]?\s*(PASS|FAIL|P|F)\b",
    _re.I | _re.MULTILINE,
)
# Format B: bullet/dash list:  - Subject Name: 88/100 - PASS
_SUBJ_RE2 = _re.compile(
    r"(?:[-\u2022*]\s*)([^:|\n]{3,50}):\s*(\d{1,3})(?:/(\d{1,3}))?\s*[-\u2013]?\s*(PASS|FAIL)",
    _re.I,
)
# Format C: Subject: Name | Marks: 88 | Status: PASS
_SUBJ_RE3 = _re.compile(
    r"Subject[:\s]+([\w\s&-]+?)\s*\|.*?Marks[:\s]+(\d{1,3}).*?Status[:\s]+(PASS|FAIL)",
    _re.I,
)
# Format D: VTU pipe table: code | name | cie | see | total | max | grade | status
# e.g.  21CS51 | Software Engineering | 40 | 62 | 78 | 100 | B | PASS
_SUBJ_PIPE_RE = _re.compile(
    r"([A-Z0-9]{4,10})\s*\|\s*([^|\n]{3,50}?)\s*\|[^|]*\|[^|]*\|\s*(\d{1,3})\s*\|\s*(\d{1,3})\s*\|\s*([A-Za-z][+\-]?)\s*\|\s*(PASS|FAIL|P|F)",
    _re.I,
)
# Format E: plain pipe: name | marks | grade | pass/fail
_SUBJ_PIPE2_RE = _re.compile(
    r"([A-Za-z][\w\s&/()'\-]{2,50}?)\s*\|\s*(\d{1,3})\s*(?:\|[^|]*)?\|\s*(PASS|FAIL|P|F)",
    _re.I | _re.MULTILINE,
)# Format G: VTU grade-only format (no PASS/FAIL — just Grade letter)
#   "Engineering Mathematics I – 92 – Grade O"
#   "Data Structures – 92 – Grade O"
_VTU_GRADE_RE = _re.compile(
    r"^(.{3,60}?)\s*[\u2013\-]\s*(\d{1,3})\s*[\u2013\-]\s*Grade\s+([A-Za-z][+\-]?)\s*$",
    _re.MULTILINE,
)
_VTU_FAIL_GRADES = frozenset(["F", "AB", "W", "X"])

# Multi-semester block splitter: "Semester N" or "Semester N Results"
# Ends at a --- separator line OR the start of the next Semester block.
_SEM_BLOCK_RE = _re.compile(
    r"Semester\s+(\d{1,2})(?:\s+Results?)?\b[^\n]*\n(.*?)(?=\n[-\u2014]{3,}|\nSemester\s+\d|\Z)",
    _re.DOTALL | _re.I,
)
# Per-block SGPA: "SGPA: 8.7"
_BLOCK_SGPA_RE = _re.compile(r"SGPA\s*:\s*(\d{1,2}\.\d{1,2})", _re.I)
_TRAILING_JUNK_RE = _re.compile(r"\s*\n.*$", _re.DOTALL)

# CSV / tabular: any_id, subject_name, score, letter_grade
# Handles formats like: STU001, English, 88, A   or   1, Mathematics, 72, B+
_CSV_SUBJ_RE = _re.compile(
    r"^[A-Za-z0-9_\-]+\s*,\s*([A-Za-z][A-Za-z0-9\s&/()'\-]{1,50}?)\s*,\s*(\d{1,3})\s*,\s*([A-Za-z][+\-]?)\s*$",
    _re.MULTILINE,
)
# Letter grades that represent failure (Indian university grading)
_LETTER_FAIL = frozenset(["F", "E", "U", "AB", "W"])


def _letter_grade_to_status(grade: str) -> str:
    """Return 'PASS' or 'FAIL' for a letter grade."""
    return "FAIL" if grade.upper().strip() in _LETTER_FAIL else "PASS"


def _extract_from_body(email: dict) -> list[dict]:
    """
    Multi-pattern extraction: handles
      A. structured code-name-marks-status lines
      B. bullet-list subject:marks - PASS/FAIL
      C. Subject|Marks|Status keyword format
      D. VTU pipe table: code|name|cie|see|total|max|grade|status
      E. plain pipe: name|marks|PASS/FAIL
      F. CSV: id, subject, score, grade
    USN can be anywhere in subject or body including "USN: xxx" annotations.
    Semester resolved from digits, ordinals (fifth) and Roman numerals (V).
    """
    body  = email.get("body", email.get("snippet", ""))
    subj  = email.get("subject", "")
    text  = f"{subj}\n{body}"
    if not text.strip():
        return []

    # ── USN extraction: standard regex + explicit "USN:" label ────────────
    usn_set: dict[str, None] = {}
    for m in _USN_LABEL_RE.finditer(text):
        usn_set[m.group(1).upper()] = None
    for m in _USN_RE.finditer(text):
        usn_set[m.group(1).upper()] = None
    usns = list(usn_set.keys())
    if not usns:
        return []

    sgpa_m  = _SGPA_RE.search(text)
    cgpa_m  = _CGPA_RE.search(text)
    raw_sgpa = float(sgpa_m.group(1)) if sgpa_m else None
    raw_cgpa = float(cgpa_m.group(1)) if cgpa_m else None
    semester = _parse_semester(text)

    raw_name = ""
    name_m = _NAME_RE.search(text)
    if name_m:
        raw_name = _TRAILING_JUNK_RE.sub("", name_m.group(1)).strip()
        raw_name = _re.sub(r"\s+(USN|No|Number|ID|Code|Register|Student)\s*$", "", raw_name, flags=_re.I).strip()
        # Drop if captured word is a USN itself
        if _USN_RE.match(raw_name.replace(" ", "")):
            raw_name = ""

    subjects: list[dict] = []

    # ── Format A: code - name : marks - PASS/FAIL  ─────────────────────────
    for m in _SUBJ_RE.finditer(text):
        code  = m.group(1).strip().upper()
        sname = m.group(2).strip(" -\u2013:")
        marks = int(m.group(3))
        max_m = int(m.group(4)) if m.group(4) else 100
        status = "PASS" if m.group(5).upper().startswith("P") else "FAIL"
        if code in usn_set or _USN_RE.match(code):
            continue
        if not any(c.isdigit() for c in code):
            continue
        if 0 <= marks <= 200:
            subjects.append({"subject_code": code, "subject_name": sname,
                             "total_marks": marks, "max_marks": max_m,
                             "status": status, "grade": "", "grade_points": None})

    # ── Format D: VTU pipe table ─────────────────────────────────────────────
    if not subjects:
        for m in _SUBJ_PIPE_RE.finditer(text):
            code  = m.group(1).strip().upper()
            sname = m.group(2).strip()
            marks = int(m.group(3))
            max_m = int(m.group(4)) if m.group(4) else 100
            grade = m.group(5).strip().upper()
            status = "PASS" if m.group(6).upper().startswith("P") else "FAIL"
            if code in usn_set or _USN_RE.match(code):
                continue
            if 0 <= marks <= 200:
                subjects.append({"subject_code": code, "subject_name": sname,
                                 "total_marks": marks, "max_marks": max_m,
                                 "status": status, "grade": grade, "grade_points": None})

    # ── Format E: plain pipe: name | marks | PASS/FAIL ──────────────────────
    if not subjects:
        for m in _SUBJ_PIPE2_RE.finditer(text):
            sname = m.group(1).strip(" -\u2013*\u2022|")
            marks = int(m.group(2))
            status = "PASS" if m.group(3).upper().startswith("P") else "FAIL"
            # Skip header rows
            if sname.lower() in ("subject", "paper", "course", "name"):
                continue
            if 0 <= marks <= 200:
                subjects.append({"subject_code": "", "subject_name": sname,
                                 "total_marks": marks, "max_marks": 100,
                                 "status": status, "grade": "", "grade_points": None})

    # ── Format B: bullet list - name : marks - PASS/FAIL ────────────────────
    if not subjects:
        for m in _SUBJ_RE2.finditer(text):
            sname = m.group(1).strip(" -\u2013*\u2022")
            marks = int(m.group(2))
            max_m = int(m.group(3)) if m.group(3) else 100
            status = "PASS" if m.group(4).upper().startswith("P") else "FAIL"
            if 0 <= marks <= 200:
                subjects.append({"subject_code": "", "subject_name": sname,
                                 "total_marks": marks, "max_marks": max_m,
                                 "status": status, "grade": "", "grade_points": None})

    # ── Format C: Subject: name | Marks: n | Status: PASS ───────────────────
    if not subjects:
        for m in _SUBJ_RE3.finditer(text):
            sname = m.group(1).strip()
            marks = int(m.group(2))
            status = "PASS" if m.group(3).upper().startswith("P") else "FAIL"
            if 0 <= marks <= 200:
                subjects.append({"subject_code": "", "subject_name": sname,
                                 "total_marks": marks, "max_marks": 100,
                                 "status": status, "grade": "", "grade_points": None})

    # ── Format F: CSV id,name,score,grade ────────────────────────────────────
    # Handles: STU001, English, 88, A   or  1MS23CS001, Mathematics, 72, B+
    # When multiple student IDs appear in CSV but only one USN found in subject,
    # all rows belong to that USN.
    if not subjects:
        for m in _CSV_SUBJ_RE.finditer(text):
            sname = m.group(1).strip()
            try:
                marks = int(m.group(2))
            except ValueError:
                continue
            grade = m.group(3).strip().upper()
            status = _letter_grade_to_status(grade)
            if 0 <= marks <= 200:
                subjects.append({"subject_code": "", "subject_name": sname,
                                 "total_marks": marks, "max_marks": 100,
                                 "status": status, "grade": grade, "grade_points": None})

    # ── Format G: VTU grade-only "Subject Name – marks – Grade X" ─────────────
    # Also handles multi-semester by splitting on "Semester N Results" blocks.
    sem_blocks = list(_SEM_BLOCK_RE.finditer(text))

    if sem_blocks:
        # Multi-semester: one record per block
        multi_records: list[dict] = []
        for blk in sem_blocks:
            blk_sem = int(blk.group(1))
            blk_text = blk.group(2)
            blk_sgpa_m = _BLOCK_SGPA_RE.search(blk_text)
            blk_sgpa = float(blk_sgpa_m.group(1)) if blk_sgpa_m else None

            blk_subjects: list[dict] = []
            for m in _VTU_GRADE_RE.finditer(blk_text):
                sname = m.group(1).strip(" -\u2013")
                marks_raw = int(m.group(2))
                grade = m.group(3).strip().upper()
                status = "FAIL" if grade in _VTU_FAIL_GRADES else "PASS"
                if 0 <= marks_raw <= 200:
                    blk_subjects.append({
                        "subject_code": "", "subject_name": sname,
                        "total_marks": marks_raw, "max_marks": 100,
                        "status": status, "grade": grade, "grade_points": None,
                    })

            if blk_subjects:
                for usn in usns:
                    multi_records.append({
                        "usn": usn, "name": raw_name, "semester": blk_sem,
                        "sgpa": blk_sgpa, "cgpa": raw_cgpa,
                        "subjects": blk_subjects,
                        "overall_confidence": 0.90,
                        "extraction_strategy": "text_regex_vtu_multisem",
                        "academic_year": "", "exam_type": "regular",
                    })
                    logger.info("extracted_record", usn=usn,
                                subjects=len(blk_subjects), sgpa=blk_sgpa,
                                semester=blk_sem)
        if multi_records:
            return multi_records
        # Fall through to single-block VTU extraction

    if not subjects:
        for m in _VTU_GRADE_RE.finditer(text):
            sname = m.group(1).strip(" -\u2013")
            marks_raw = int(m.group(2))
            grade = m.group(3).strip().upper()
            status = "FAIL" if grade in _VTU_FAIL_GRADES else "PASS"
            if 0 <= marks_raw <= 200:
                subjects.append({
                    "subject_code": "", "subject_name": sname,
                    "total_marks": marks_raw, "max_marks": 100,
                    "status": status, "grade": grade, "grade_points": None,
                })

    # Deduplicate subjects by name/code — first occurrence wins
    seen_names: set[str] = set()
    unique_subjects: list[dict] = []
    for s in subjects:
        key = (s.get("subject_code") or s.get("subject_name", "")).upper().strip()
        if key and key not in seen_names:
            seen_names.add(key)
            unique_subjects.append(s)

    records = []
    for usn in usns:
        records.append({
            "usn": usn, "name": raw_name, "semester": semester,
            "sgpa": raw_sgpa, "cgpa": raw_cgpa, "subjects": unique_subjects,
            "overall_confidence": 0.80,
            "extraction_strategy": "text_regex",
            "academic_year": "", "exam_type": "regular",
        })
        logger.info("extracted_record", usn=usn, subjects=len(unique_subjects), sgpa=raw_sgpa)
    return records


# ---------------------------------------------------------------------------
# Phase 4 – persist to PostgreSQL
# ---------------------------------------------------------------------------

def _save_records_to_db(
    records: list[dict],
    email_id: str,
    extraction_id: str,
    pre_clean: bool = False,
) -> int:
    """
    Persist extracted student records to PostgreSQL.

    When pre_clean=True (force reprocess), purge all existing student_results
    for each (student_id, semester) pair before up-inserting fresh data.
    This prevents stale results from a previously bad extraction from lingering.
    """
    institution_id = db.get_default_institution_id()
    saved = 0
    _cleaned: set[tuple] = set()  # (student_id, semester) pairs already cleaned

    for rec in records:
        usn  = str(rec.get("usn") or "").strip().upper()
        name = str(rec.get("name") or "").strip()
        if not usn or len(usn) < 5:
            continue

        student_id = db.upsert_student(usn, name or usn, institution_id=institution_id)
        semester   = int(rec.get("semester") or 1)

        # Purge stale results for this (student, semester) so old bad-extraction
        # rows don't coexist with fresh data after a force reprocess.
        if pre_clean and (student_id, semester) not in _cleaned:
            try:
                with db.get_connection() as _conn:
                    with _conn.cursor() as _cur:
                        _cur.execute(
                            "DELETE FROM student_results WHERE student_id = %s AND semester = %s",
                            (student_id, semester),
                        )
                        _deleted = _cur.rowcount
                _cleaned.add((student_id, semester))
                if _deleted:
                    logger.info(
                        "pre_clean_deleted",
                        usn=usn, semester=semester, rows=_deleted,
                    )
            except Exception as _exc:
                logger.warning("pre_clean_failed: %s", _exc)

        for subj in (rec.get("subjects") or []):
            if not isinstance(subj, dict):
                continue
            marks     = subj.get("total_marks", 0) or 0
            status_raw = str(subj.get("status", "PASS"))
            status    = "pass" if "PASS" in status_raw.upper() else "fail"
            gp        = float(subj.get("grade_points") or 0) or None
            grade     = str(subj.get("grade") or "")

            subj_code = str(subj.get("subject_code") or "").strip().upper()
            subj_name = str(subj.get("subject_name") or "").strip()
            if not subj_code:
                if subj_name:
                    words    = subj_name.upper().split()
                    initials = "".join(w[0] for w in words[:3]).ljust(2, "X")
                    h        = hashlib.md5(subj_name.encode()).hexdigest()[:4].upper()
                    subj_code = f"{initials}{h}"
                else:
                    subj_code = f"SUBJ{abs(hash(str(marks))) % 1000:03d}"

            try:
                subject_id = db.get_or_create_subject(
                    institution_id, subj_code, subj_name or subj_code, semester
                )
                db.upsert_result(
                    student_id=student_id,
                    subject_id=subject_id,
                    semester=semester,
                    marks_obtained=float(marks) if marks else None,
                    max_marks=float(subj.get("max_marks") or 100),
                    grade=grade,
                    grade_points=gp,
                    status=status,
                    exam_type="SEE",
                )
                saved += 1
            except Exception as exc:
                logger.debug("upsert_result_error", error=str(exc), usn=usn)

        if rec.get("sgpa"):
            try:
                db.store_semester_aggregate(
                    student_id=student_id,
                    semester=semester,
                    sgpa=float(rec["sgpa"]),
                    backlogs=sum(
                        1 for s in (rec.get("subjects") or [])
                        if "FAIL" in str(s.get("status", "")).upper()
                    ),
                )
            except Exception:
                pass

        db.compute_and_store_cgpa(student_id)

    return saved


# ---------------------------------------------------------------------------
# Background pipeline task
# ---------------------------------------------------------------------------

_pipeline_running = False
_pipeline_log: list[str] = []


def _clear_dedup_cache() -> int:
    """Delete all dedup:sha256 and dedup:simhash keys from Redis. Returns count deleted."""
    try:
        cache = get_cache()
        r = cache.r
        deleted = 0
        for pattern in ("dedup:sha256:*", "dedup:simhash:*"):
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor, match=pattern, count=200)
                if keys:
                    r.delete(*keys)
                    deleted += len(keys)
                if cursor == 0:
                    break
        return deleted
    except Exception as exc:
        logger.warning("dedup_clear_failed", error=str(exc))
        return 0


def _run_pipeline_sync(force: bool = False) -> dict:
    global _pipeline_running, _pipeline_log

    if _pipeline_running:
        return {"status": "already_running"}

    _pipeline_running = True
    _pipeline_log = []

    def log(msg: str):
        _pipeline_log.append(msg)
        logger.info("pipeline", msg=msg)

    try:
        db.init_db()
        storage = get_storage()
        cache   = get_cache()

        # Force mode: clear Redis dedup so all cached emails are re-processed
        if force:
            n_cleared = _clear_dedup_cache()
            log(f"[FORCE] Cleared {n_cleared} dedup keys from Redis. Re-processing all emails.")

        # Ensure MinIO buckets exist
        try:
            storage.ensure_buckets()
        except Exception as exc:
            log(f"[WARN] MinIO bucket setup failed: {exc}. Continuing without object storage.")

        emails = _load_emails_cache()
        if not emails:
            log("No emails in cache. Run Sync first.")
            _save_pipeline_state({
                "last_run": datetime.now(timezone.utc).isoformat(),
                "status": "no_emails",
                "emails_processed": 0,
                "records_extracted": 0,
                "log": _pipeline_log,
            })
            return {"status": "no_emails", "emails_processed": 0, "records_extracted": 0}

        log(f"Starting pipeline on {len(emails)} cached emails …")

        emails_processed = 0
        result_emails    = 0
        records_extracted = 0
        skipped_dedup    = 0

        for email in emails:
            msg_id  = email.get("id", str(uuid4()))
            subject = email.get("subject", "(no subject)")
            sender  = email.get("from", "")
            date    = email.get("date", "")
            body    = email.get("body", "")

            # ── HTML body → clean text (“email body with HTML tags”) ────────────
            _html_hints = ("<html", "<table", "<br", "<div", "<p>",
                           "&lt;", "&gt;", "&#")
            if body and any(h in body[:500].lower() for h in _html_hints):
                try:
                    from src.phase2_document_intelligence import convert_html_body
                    _html_doc = convert_html_body(body, subject)
                    if _html_doc.text and len(_html_doc.text) > 20:
                        body = _html_doc.text
                        email = {**email, "body": body}
                except Exception:
                    pass  # keep raw body on failure

            # ── Dedup (skipped in force mode — Redis already cleared) ────────
            if not force:
                # 1. DB check — source of truth; survives Redis flushes/restarts
                try:
                    db_status = db.get_email_db_status(msg_id)
                    if db_status in ("completed", "skipped"):
                        skipped_dedup += 1
                        log(
                            f"  [ALREADY PROCESSED] '{subject[:60]}' — "
                            f"status={db_status!r} in DB. "
                            f"Use force=true to reprocess."
                        )
                        continue
                except Exception:
                    pass

                # 2. Redis fast-dedup — catches emails seen but not yet in DB
                try:
                    if cache.is_duplicate_sha256(msg_id, sender, date, subject):
                        skipped_dedup += 1
                        log(f"  [DEDUP] Skipping duplicate: {subject[:50]}")
                        continue
                except Exception:
                    pass  # Redis unavailable — continue without dedup

            # ── Phase 1: classify ────────────────────────────────────
            classification, confidence = _classify_email(email)

            # ── Store raw email in MinIO ─────────────────────────────
            raw_path: str | None = None
            try:
                raw_path = storage.store_email(msg_id, email)
            except Exception as exc:
                logger.debug("minio_store_failed", error=str(exc))

            # ── Security scan (body) ──────────────────────────────────
            try:
                from src.common.security import is_safe
                body_bytes = body.encode("utf-8", errors="replace")
                if not is_safe(body_bytes):
                    log(f"  [SECURITY] Unsafe content in {subject[:50]} — quarantined")
                    continue
            except Exception:
                pass  # security module unavailable — continue

            # ── Persist email metadata to PostgreSQL ─────────────────
            email_db_id = db.upsert_email(
                message_id=msg_id,
                subject=subject,
                sender=sender,
                received_at=date or None,
                raw_path=raw_path,
                body=body,
            )
            db.update_email_classification(email_db_id, classification, confidence)

            emails_processed += 1
            log(f"  [{emails_processed}/{len(emails)}] {subject[:60]} → {classification} ({confidence:.0%})")

            if classification not in ("result_email",):
                db.update_email_status(email_db_id, "skipped")
                # Still mark as seen so we don't reprocess
                try:
                    cache.mark_seen_sha256(msg_id, sender, date, subject)
                except Exception:
                    pass
                continue

            result_emails += 1

            # ── Attachment extraction (PDF / image / DOCX / ODF, etc.) ─────────
            _att_texts: list[str] = []
            for att in (email.get("attachments") or []):
                att_fname = att.get("filename", "")
                att_mime  = att.get("mimeType", "")
                if not att.get("attachmentId"):
                    continue
                log(f"    → Fetching attachment: {att_fname} ({att_mime})")
                try:
                    from src.phase2_document_intelligence import convert_gmail_attachment
                    _att_doc = convert_gmail_attachment(att, msg_id)
                    if _att_doc and _att_doc.text:
                        _att_texts.append(
                            f"[Attachment: {att_fname}]\n{_att_doc.flat_text()}"
                        )
                        log(
                            f"      → {len(_att_doc.text)} chars extracted "
                            f"via {_att_doc.parse_strategy}"
                        )
                    else:
                        log(f"      → No text extracted from {att_fname}")
                except Exception as _att_exc:
                    logger.warning("attachment_parse_failed %s: %s", att_fname, _att_exc)

            if _att_texts:
                body = body + "\n\n" + "\n\n".join(_att_texts)
                email = {**email, "body": body}  # update for downstream extractors

            # ── Phase 3: multi-strategy extract ────────────────────────
            regex_records = _extract_from_body(email)
            try:
                from src.phase3_extraction_engine.strategy_merger import extract_with_voting
                from src.phase3_extraction_engine.validator import validate_and_correct
                from src.phase3_extraction_engine.review_queue import (
                    enqueue_for_review, REVIEW_THRESHOLD
                )
                # Pass subject + body so strategy merger can find USN-in-subject-only emails
                full_text = f"{subject}\n{body}"

                # For multi-semester emails, regex records already have per-semester data —
                # bypass the merger's flattening and use regex records directly.
                _sems = {r.get("semester") for r in regex_records if r.get("semester")}
                if len(_sems) > 1:
                    raw_records = regex_records  # already structured per-semester
                else:
                    raw_records = extract_with_voting(
                        full_text, regex_records, run_llm=True
                    )
                # validate_and_correct returns (records, ValidationResult) tuple
                raw_records, _vr = validate_and_correct(raw_records, full_text)
            except Exception as ph3_exc:
                logger.debug("phase3_fallback", error=str(ph3_exc))
                raw_records = regex_records

            final_confidence = (
                raw_records[0].get("overall_confidence",
                    raw_records[0].get("confidence", 0.8))
                if raw_records
                else 0.3
            )
            extraction_id = db.save_extraction(
                email_id=email_db_id,
                records=raw_records,
                confidence=final_confidence,
                strategy="multi_strategy",
            )

            # Route low-confidence extractions to human review
            try:
                if raw_records and final_confidence < REVIEW_THRESHOLD:
                    queue_id = enqueue_for_review(
                        email_id=msg_id,
                        subject=subject,
                        sender=sender,
                        body=body,
                        records=raw_records,
                        confidence=final_confidence,
                    )
                    log(f"    → Low confidence ({final_confidence:.0%}) → review queue {queue_id[:8]}")
            except Exception:
                pass

            # ── Phase 4: persist to student/result tables ─────────────
            n_saved = _save_records_to_db(raw_records, email_db_id, extraction_id, pre_clean=force)
            records_extracted += n_saved
            db.update_email_status(email_db_id, "completed" if n_saved > 0 else "processed_no_records")

            if raw_records:
                log(f"    → Extracted {len(raw_records)} record(s), {n_saved} results saved ({final_confidence:.0%} conf)")

            # ── Post-save: generate pgvector embeddings ───────────────
            try:
                from src.common.embeddings import store_student_embedding
                usns = {r.get("usn") for r in raw_records if r.get("usn")}
                if usns:
                    with db.get_connection() as _conn:
                        with _conn.cursor() as _cur:
                            for usn_val in usns:
                                _cur.execute(
                                    "SELECT id, usn, name FROM students WHERE usn = %s",
                                    (usn_val,),
                                )
                                row = _cur.fetchone()
                                if row:
                                    store_student_embedding(str(row[0]), row[1], row[2] or "")
            except Exception as emb_exc:
                logger.debug("embedding_gen_failed", error=str(emb_exc))

            # ── Mark as seen in Redis ─────────────────────────────────
            try:
                cache.mark_seen_sha256(msg_id, sender, date, subject)
            except Exception:
                pass

        log(
            f"Pipeline complete. {emails_processed} emails, "
            f"{result_emails} result emails, "
            f"{records_extracted} results saved, "
            f"{skipped_dedup} duplicates skipped."
        )

        # Build a human-readable message so callers understand the outcome
        if records_extracted == 0 and skipped_dedup > 0 and emails_processed == 0:
            run_message = (
                f"All {skipped_dedup} email(s) were already processed and stored. "
                "Send force=true to reprocess from scratch."
            )
        elif records_extracted == 0 and result_emails == 0:
            run_message = "No result emails found in this batch."
        else:
            run_message = (
                f"Extracted {records_extracted} record(s) from "
                f"{result_emails} result email(s). "
                + (f"{skipped_dedup} already-processed email(s) skipped." if skipped_dedup else "")
            ).strip()

        state = {
            "last_run": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "emails_processed": emails_processed,
            "result_emails": result_emails,
            "records_extracted": records_extracted,
            "skipped_dedup": skipped_dedup,
            "message": run_message,
            "log": _pipeline_log,
        }
        _save_pipeline_state(state)
        return state

    except Exception as exc:
        logger.error("pipeline_error", error=str(exc))
        _save_pipeline_state({"status": "error", "error": str(exc), "log": _pipeline_log})
        raise
    finally:
        _pipeline_running = False


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

class PipelineRunRequest(BaseModel):
    force: bool = False  # If True, clears Redis dedup cache before running


class PipelineRunResponse(BaseModel):
    status: str
    emails_processed: int = 0
    result_emails: int = 0
    records_extracted: int = 0
    skipped_dedup: int = 0
    message: str = ""
    log: list[str] = []


@router.post("/pipeline/run", response_model=PipelineRunResponse)
async def run_pipeline(request: PipelineRunRequest = PipelineRunRequest()) -> dict:
    """Run the full pipeline: classify → extract → store.

    Set ``force=true`` in the request body to clear Redis dedup cache and
    re-process all cached emails from scratch.
    """
    import asyncio

    if _pipeline_running:
        raise HTTPException(status_code=409, detail="Pipeline is already running")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, lambda: _run_pipeline_sync(force=request.force))

    # Append current DB totals to the log so the UI can display them
    try:
        db.init_db()
        stats = db.get_pipeline_stats()
        result.setdefault("log", []).append(
            f"\nDatabase totals: {stats.get('total_students', 0)} students | "
            f"{stats.get('total_results', 0)} result records | "
            f"{stats.get('emails_processed', 0)} emails | "
            f"Avg CGPA: {float(stats.get('average_cgpa') or 0):.2f}"
        )
    except Exception:
        pass

    return result


@router.get("/pipeline/status")
async def pipeline_status() -> dict:
    """Current pipeline state + database statistics."""
    db.init_db()
    state = _load_pipeline_state()
    stats = db.get_pipeline_stats()
    return {
        "pipeline": state,
        "database": stats,
        "running": _pipeline_running,
        "log_tail": _pipeline_log[-10:] if _pipeline_log else [],
    }


@router.get("/pipeline/records")
async def list_records(usn: str = "", limit: int = 100) -> dict:
    """List extracted student records."""
    db.init_db()
    if usn:
        student = db.get_student(usn)
        students = [student] if student else []
    else:
        students = db.get_all_students(limit=limit)

    result = []
    for s in students:
        if not s:
            continue
        results = db.get_student_results(s["usn"])
        # Convert UUIDs / dates to strings for JSON serialisation
        row = {k: str(v) if not isinstance(v, (str, int, float, bool, type(None), list, dict)) else v
               for k, v in s.items()}
        row["results"] = [
            {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
             for k, v in r.items()}
            for r in results
        ]
        result.append(row)

    return {"total": len(result), "students": result}


@router.get("/pipeline/emails")
async def list_processed_emails(limit: int = 100) -> dict:
    """List emails with their pipeline classification status."""
    db.init_db()
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM email_metadata ORDER BY created_at DESC LIMIT %s", (limit,)
            )
            emails = [
                {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
                 for k, v in row.items()}
                for row in cur.fetchall()
            ]
    return {"total": len(emails), "emails": emails}


@router.delete("/pipeline/reset")
async def reset_pipeline() -> dict:
    """Clear all extracted data and dedup cache for full re-processing."""
    db.init_db()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            for tbl in (
                "extractions", "attachments", "email_metadata",
                "semester_aggregates", "student_results", "students",
            ):
                cur.execute(f"DELETE FROM {tbl}")
    # Clear Redis pipeline state + dedup keys
    try:
        get_cache().clear_pipeline_state()
    except Exception:
        pass
    _clear_dedup_cache()
    if PIPELINE_STATE_FILE.exists():
        PIPELINE_STATE_FILE.unlink()
    return {"reset": True, "dedup_cleared": True}


@router.delete("/pipeline/clear-seeds")
async def clear_seed_data() -> dict:
    """Remove only seeded test students and their results (keeps real pipeline data intact)."""
    db.init_db()
    deleted = 0
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            # Cascade: delete results/aggregates for seed students first
            cur.execute("""
                DELETE FROM semester_aggregates
                WHERE student_id IN (
                    SELECT id FROM students WHERE metadata->>'source' = 'seed'
                )
            """)
            cur.execute("""
                DELETE FROM student_results
                WHERE student_id IN (
                    SELECT id FROM students WHERE metadata->>'source' = 'seed'
                )
            """)
            cur.execute("DELETE FROM students WHERE metadata->>'source' = 'seed'")
            deleted = cur.rowcount
    return {"cleared": True, "seed_students_removed": deleted}
