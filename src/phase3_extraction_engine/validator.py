"""
Extraction Validator & Correction Loop — Phase 3.

Validates extracted records against strict VTU/MSRIT academic rules.
Runs up to MAX_RETRIES re-extraction with targeted prompts when validation fails.

Validation rules enforced:
  - USN format: [1-4][A-Z]{2}[0-9]{2}[A-Z]{2,4}[0-9]{3}
  - Semester in [1..8]
  - Marks in [0, max_marks] (never negative or above stated max)
  - Grade must be one of: O, A+, A, B+, B, C, P, F  (no digits allowed)
  - Grade points must match VTU scale (O=10, A+=9, ..., P=4, F=0) within ±0.5
  - SGPA and CGPA must be within [0.0, 10.0]
  - SGPA is recomputed deterministically from grade_points × credits;
    if extracted value differs by >0.5 the computed value replaces it
  - Status is consistent with marks (>= 40% of max_marks → PASS)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

MAX_RETRIES = 3

# ── VTU 10-point grade scale ──────────────────────────────────────────────────
_VALID_GRADES: set[str] = {"O", "A+", "A", "B+", "B", "C", "P", "F"}
_GRADE_TO_GP:  dict[str, float] = {
    "O":  10.0,
    "A+":  9.0,
    "A":   8.0,
    "B+":  7.0,
    "B":   6.0,
    "C":   5.0,
    "P":   4.0,
    "F":   0.0,
}
# Marks-to-grade heuristic for VTU (marks are percentage-of-max)
_MARKS_PCT_TO_GRADE: list[tuple[float, str]] = [
    (90.0, "O"),
    (80.0, "A+"),
    (70.0, "A"),
    (60.0, "B+"),
    (55.0, "B"),
    (50.0, "C"),
    (40.0, "P"),
    (0.0,  "F"),
]

_USN_PATTERN = re.compile(r"^[1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3}$", re.IGNORECASE)
_DIGIT_RE    = re.compile(r"\d")


# ── Helpers ───────────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    valid:    bool
    errors:   list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    fixed:    bool = False

    def log(self) -> None:
        if not self.valid:
            logger.error("Validation failed: %s", self.errors)
        elif self.warnings:
            logger.warning("Validation warnings: %s", self.warnings)


def _grade_from_marks(marks: int, max_marks: int) -> str:
    """Derive VTU letter grade from marks/max_marks ratio."""
    if max_marks <= 0:
        return "F"
    pct = (marks / max_marks) * 100.0
    for threshold, grade in _MARKS_PCT_TO_GRADE:
        if pct >= threshold:
            return grade
    return "F"


def _gp_from_grade(grade: str) -> Optional[float]:
    """Return grade points for a valid VTU grade symbol."""
    return _GRADE_TO_GP.get(grade.strip().upper())


def _compute_sgpa(subjects: list[dict]) -> Optional[float]:
    """
    Deterministically compute SGPA = Σ(grade_points × credits) / Σ(credits).
    Uses subject-level credits when present; defaults to 3 credits if missing.
    Returns None when no grade-point data is available.
    """
    total_weighted = 0.0
    total_credits  = 0
    for s in subjects:
        gp = s.get("grade_points")
        if gp is None:
            continue
        try:
            credits = int(s.get("credits") or 3)
            if credits <= 0:
                credits = 3
            total_weighted += float(gp) * credits
            total_credits  += credits
        except (TypeError, ValueError):
            continue
    if total_credits == 0:
        return None
    return round(total_weighted / total_credits, 2)


def _coerce_marks(value) -> Optional[int]:
    """Safely coerce marks to int; return None on failure."""
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _validate_usn(usn: str) -> Optional[str]:
    """Return error string or None if OK."""
    if not usn or usn.upper() == "UNKNOWN":
        return "missing_usn"
    if not _USN_PATTERN.match(usn.strip()):
        return f"invalid_usn_format:{usn}"
    return None


def _validate_subject(s: dict, idx: int) -> list[str]:
    """Validate a single subject dict. Returns list of error strings."""
    errors:   list[str] = []
    max_marks = _coerce_marks(s.get("max_marks")) or 100
    marks     = _coerce_marks(s.get("total_marks"))
    grade     = str(s.get("grade") or "").strip().upper()
    gp        = s.get("grade_points")
    status    = str(s.get("status") or "").upper()

    # ── Marks ─────────────────────────────────────────────────────────────────
    if marks is None:
        errors.append(f"subject[{idx}]:marks_not_numeric:{s.get('total_marks')!r}")
    else:
        if not (0 <= marks <= max_marks):
            errors.append(f"subject[{idx}]:marks_out_of_range:{marks}/{max_marks}")

    # ── Grade symbol ──────────────────────────────────────────────────────────
    if grade:
        if _DIGIT_RE.search(grade):
            errors.append(f"subject[{idx}]:grade_contains_digit:{grade!r}")
        elif grade not in _VALID_GRADES:
            errors.append(f"subject[{idx}]:invalid_grade:{grade!r}")

    # ── Grade points range & consistency ─────────────────────────────────────
    if gp is not None:
        try:
            gp_f = float(gp)
            if not (0.0 <= gp_f <= 10.0):
                errors.append(f"subject[{idx}]:grade_points_out_of_range:{gp_f}")
            elif grade in _GRADE_TO_GP:
                expected = _GRADE_TO_GP[grade]
                if abs(gp_f - expected) > 0.5:
                    errors.append(
                        f"subject[{idx}]:grade_points_mismatch:"
                        f"grade={grade},gp={gp_f},expected={expected}"
                    )
        except (TypeError, ValueError):
            errors.append(f"subject[{idx}]:grade_points_not_numeric:{gp!r}")

    # ── Status ────────────────────────────────────────────────────────────────
    if status not in ("PASS", "FAIL", "ABSENT", "WITHHELD"):
        errors.append(f"subject[{idx}]:invalid_status:{status!r}")
    elif marks is not None and status in ("PASS", "FAIL"):
        pass_marks   = _coerce_marks(s.get("pass_marks")) or int(max_marks * 0.40)
        derived      = "PASS" if marks >= pass_marks else "FAIL"
        if derived != status:
            errors.append(
                f"subject[{idx}]:status_inconsistency:"
                f"marks={marks}/{max_marks},status={status},expected={derived}"
            )

    return errors


def _autofix_subject(s: dict, idx: int) -> bool:
    """
    Apply deterministic corrections to a subject dict in-place.
    Returns True if any fix was applied.
    """
    fixed = False
    max_marks = _coerce_marks(s.get("max_marks")) or 100
    marks     = _coerce_marks(s.get("total_marks"))
    grade     = str(s.get("grade") or "").strip().upper()
    gp_raw    = s.get("grade_points")

    # Coerce marks to int
    if s.get("total_marks") is not None and marks is not None:
        if s["total_marks"] != marks:
            s["total_marks"] = marks
            fixed = True

    # Clamp marks to [0, max_marks]
    if marks is not None and not (0 <= marks <= max_marks):
        s["total_marks"] = max(0, min(marks, max_marks))
        marks = s["total_marks"]
        fixed = True

    # Fix grade: if it contains a digit, derive from marks
    if grade and _DIGIT_RE.search(grade):
        if marks is not None:
            s["grade"] = _grade_from_marks(marks, max_marks)
            grade = s["grade"]
            fixed = True
        else:
            s["grade"] = ""
            grade = ""
            fixed = True

    # Validate/normalise grade
    if grade and grade not in _VALID_GRADES:
        if marks is not None:
            s["grade"] = _grade_from_marks(marks, max_marks)
            grade = s["grade"]
            fixed = True
        else:
            s["grade"] = ""
            grade = ""
            fixed = True

    # Derive grade from marks if grade is still missing
    if not grade and marks is not None:
        s["grade"] = _grade_from_marks(marks, max_marks)
        grade = s["grade"]
        fixed = True

    # Fix grade_points: if missing, blank, or out of range, look up from grade
    expected_gp = _gp_from_grade(grade) if grade else None
    if expected_gp is not None:
        try:
            gp_f = float(gp_raw) if gp_raw is not None else None
        except (TypeError, ValueError):
            gp_f = None

        if gp_f is None or not (0.0 <= gp_f <= 10.0) or abs(gp_f - expected_gp) > 0.5:
            s["grade_points"] = expected_gp
            fixed = True

    # Fix status from marks
    if marks is not None:
        pass_marks     = _coerce_marks(s.get("pass_marks")) or int(max_marks * 0.40)
        correct_status = "PASS" if marks >= pass_marks else "FAIL"
        if str(s.get("status", "")).upper() != correct_status:
            s["status"] = correct_status
            fixed = True

    return fixed


def validate_record(record: dict) -> ValidationResult:
    """Validate a single extracted record. Auto-fixes where possible."""
    errors:   list[str] = []
    warnings: list[str] = []
    fixed = False

    # ── USN ───────────────────────────────────────────────────────────────────
    usn_err = _validate_usn(str(record.get("usn") or ""))
    if usn_err:
        warnings.append(usn_err)

    # ── Semester ─────────────────────────────────────────────────────────────
    sem = record.get("semester")
    try:
        sem_i = int(sem or 1)
    except (TypeError, ValueError):
        sem_i = 1
    if not (1 <= sem_i <= 8):
        warnings.append(f"semester_out_of_range:{sem}")
        record["semester"] = max(1, min(8, sem_i))
        fixed = True

    # ── Subjects ─────────────────────────────────────────────────────────────
    subjects = record.get("subjects") or []
    if not subjects:
        warnings.append("no_subjects_found")

    seen_codes: set[str] = set()
    for i, s in enumerate(subjects):
        # Auto-fix before validation
        if _autofix_subject(s, i):
            fixed = True
        # Re-validate after fix
        subj_errors = _validate_subject(s, i)
        errors.extend(subj_errors)
        # Dedup check
        code = str(s.get("subject_code") or "").strip().upper()
        if code and code in seen_codes:
            warnings.append(f"duplicate_subject_code:{code}")
        if code:
            seen_codes.add(code)

    # ── SGPA: deterministic recomputation ────────────────────────────────────
    computed_sgpa = _compute_sgpa(subjects)
    extracted_sgpa = record.get("sgpa")

    if computed_sgpa is not None:
        if extracted_sgpa is not None:
            try:
                ex = float(extracted_sgpa)
                if not (0.0 <= ex <= 10.0):
                    warnings.append(f"sgpa_out_of_range:{ex}")
                    record["sgpa"] = computed_sgpa
                    fixed = True
                elif abs(ex - computed_sgpa) > 0.5:
                    warnings.append(
                        f"sgpa_inconsistency:extracted={ex},computed={computed_sgpa}"
                    )
                    record["sgpa"] = computed_sgpa
                    fixed = True
            except (TypeError, ValueError):
                warnings.append(f"sgpa_not_numeric:{extracted_sgpa}")
                record["sgpa"] = computed_sgpa
                fixed = True
        else:
            record["sgpa"] = computed_sgpa
            fixed = True
    elif extracted_sgpa is not None:
        try:
            ex = float(extracted_sgpa)
            if not (0.0 <= ex <= 10.0):
                warnings.append(f"sgpa_out_of_range:{ex}")
                record["sgpa"] = min(10.0, max(0.0, ex))
                fixed = True
        except (TypeError, ValueError):
            warnings.append(f"sgpa_not_numeric:{extracted_sgpa}")
            record["sgpa"] = None
            fixed = True

    # ── CGPA: range check & clamp ────────────────────────────────────────────
    cgpa = record.get("cgpa")
    if cgpa is not None:
        try:
            c = float(cgpa)
            if not (0.0 <= c <= 10.0):
                warnings.append(f"cgpa_out_of_range:{c}")
                record["cgpa"] = min(10.0, max(0.0, c))
                fixed = True
        except (TypeError, ValueError):
            warnings.append(f"cgpa_not_numeric:{cgpa}")
            record["cgpa"] = None
            fixed = True

    return ValidationResult(
        valid=(len(errors) == 0),
        errors=errors,
        warnings=warnings,
        fixed=fixed,
    )


def validate_and_correct(
    records: list[dict],
    text: str,
    max_iterations: int = MAX_RETRIES,
) -> tuple[list[dict], ValidationResult]:
    """
    Validate records, apply auto-corrections, and optionally re-extract via LLM.

    Args:
        records:        Initial extracted records.
        text:           Original source text (for re-extraction).
        max_iterations: Max re-extraction attempts when LLM is available.

    Returns:
        (corrected_records, final_validation_result)
    """
    if not records:
        return [], ValidationResult(valid=False, errors=["no_records"])

    # First pass: auto-correct each record
    all_errors:   list[str] = []
    all_warnings: list[str] = []
    any_fixed = False

    for rec in records:
        vr = validate_record(rec)
        all_errors.extend(vr.errors)
        all_warnings.extend(vr.warnings)
        any_fixed = any_fixed or vr.fixed

    if not all_errors:
        return records, ValidationResult(valid=True, warnings=all_warnings, fixed=any_fixed)

    # Re-extraction loop via LLM when hard errors remain
    try:
        from .llm_extractor import llm_extract, _GROQ_KEY, _OPENAI_KEY
        has_llm = bool(_GROQ_KEY or _OPENAI_KEY)
    except ImportError:
        has_llm = False

    if not has_llm:
        return records, ValidationResult(
            valid=len(all_errors) == 0,
            errors=all_errors,
            warnings=all_warnings,
            fixed=any_fixed,
        )

    for iteration in range(max_iterations):
        logger.info(
            "validation_correction_loop: iteration %d, errors=%d",
            iteration + 1, len(all_errors),
        )

        error_context   = "; ".join(all_errors[:5])
        targeted_prompt = (
            f"Previous extraction had issues: {error_context}.\n"
            f"Please re-extract carefully, ensuring:\n"
            f"- Grades are VTU symbols (O/A+/A/B+/B/C/P/F), not numbers\n"
            f"- Marks are integers within [0, max_marks]\n"
            f"- SGPA and CGPA are floats within [0.0, 10.0]\n"
            f"- Grade points follow VTU scale (O=10, A+=9, A=8, B+=7, B=6, C=5, P=4, F=0)\n"
            f"\nText:\n\n{text[:3000]}"
        )

        try:
            from .llm_extractor import llm_extract
            new_records = llm_extract(targeted_prompt)
        except Exception as exc:
            logger.warning("re_extraction_failed: %s", exc)
            break

        if not new_records:
            break

        new_errors:   list[str] = []
        new_warnings: list[str] = []
        for rec in new_records:
            vr = validate_record(rec)
            new_errors.extend(vr.errors)
            new_warnings.extend(vr.warnings)

        if len(new_errors) < len(all_errors):
            records      = new_records
            all_errors   = new_errors
            all_warnings = new_warnings
            logger.info("re_extraction_improved: errors=%d", len(all_errors))

        if not all_errors:
            break

    return records, ValidationResult(
        valid=(len(all_errors) == 0),
        errors=all_errors,
        warnings=all_warnings,
        fixed=True,
    )
