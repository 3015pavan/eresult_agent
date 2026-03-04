"""
Extraction Validator & Correction Loop — Phase 3.

Validates extracted records against business rules.
Runs up to MAX_RETRIES re-extraction with targeted prompts when validation fails.

Validation checks:
  - USN format: [1-4][A-Z]{2}[0-9]{2}[A-Z]{2,3}[0-9]{3}
  - Semester in [1..8]
  - Marks in [0..150]
  - At least one subject (if supposedly a result email)
  - No duplicate subject codes within a single record
  - Status is consistent with marks (≥40% = PASS)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

MAX_RETRIES = 3

_USN_PATTERN = re.compile(r"^[1-4][A-Z]{2}\d{2}[A-Z]{2,3}\d{3}$", re.IGNORECASE)


@dataclass
class ValidationResult:
    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    fixed: bool = False   # True if auto-corrections were applied


def _validate_usn(usn: str) -> Optional[str]:
    """Return error string or None if OK."""
    if not usn or usn == "UNKNOWN":
        return "missing_usn"
    if not _USN_PATTERN.match(usn.strip()):
        return f"invalid_usn_format:{usn}"
    return None


def _validate_subject(s: dict, idx: int) -> list[str]:
    errors: list[str] = []
    marks     = s.get("total_marks")
    max_marks = s.get("max_marks") or 100
    status    = str(s.get("status") or "").upper()

    if marks is None:
        errors.append(f"subject[{idx}]:missing_marks")
    elif not (0 <= int(marks) <= 200):
        errors.append(f"subject[{idx}]:marks_out_of_range:{marks}")

    if status not in ("PASS", "FAIL", "ABSENT", "WITHHELD"):
        errors.append(f"subject[{idx}]:invalid_status:{status}")

    # Check consistency: marks vs status
    if marks is not None and status in ("PASS", "FAIL"):
        threshold = int(max_marks) * 0.40
        derived_status = "PASS" if int(marks) >= threshold else "FAIL"
        if derived_status != status:
            errors.append(
                f"subject[{idx}]:status_inconsistency:marks={marks},status={status}"
            )

    return errors


def validate_record(record: dict) -> ValidationResult:
    """Validate a single extracted record."""
    errors:   list[str] = []
    warnings: list[str] = []
    fixed = False

    # USN
    usn_err = _validate_usn(str(record.get("usn") or ""))
    if usn_err:
        warnings.append(usn_err)  # Warning not error — we can still store without USN

    # Semester
    sem = record.get("semester")
    if sem is None or not (1 <= int(sem) <= 8):
        warnings.append(f"semester_out_of_range:{sem}")
        record["semester"] = max(1, min(8, int(sem or 1)))
        fixed = True

    # Subjects
    subjects = record.get("subjects") or []
    if not subjects:
        warnings.append("no_subjects_found")

    seen_codes: set[str] = set()
    for i, s in enumerate(subjects):
        subj_errors = _validate_subject(s, i)
        errors.extend(subj_errors)

        # Auto-fix: derive status from marks
        marks    = s.get("total_marks")
        max_m    = int(s.get("max_marks") or 100)
        if marks is not None and subj_errors:
            correct_status = "PASS" if int(marks) >= int(max_m * 0.40) else "FAIL"
            if str(s.get("status", "")).upper() != correct_status:
                s["status"] = correct_status
                fixed = True
                errors = [e for e in errors if "status_inconsistency" not in e]

        # Dedup by code
        code = str(s.get("subject_code") or "").strip().upper()
        if code and code in seen_codes:
            warnings.append(f"duplicate_subject_code:{code}")
        if code:
            seen_codes.add(code)

    # SGPA / CGPA range
    for field_name in ("sgpa", "cgpa"):
        val = record.get(field_name)
        if val is not None:
            try:
                fval = float(val)
                if not (0.0 <= fval <= 10.0):
                    warnings.append(f"{field_name}_out_of_range:{fval}")
                    record[field_name] = min(10.0, max(0.0, fval))
                    fixed = True
            except (TypeError, ValueError):
                warnings.append(f"{field_name}_not_numeric:{val}")
                record[field_name] = None
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
    Validate records, perform auto-corrections, and optionally re-extract via LLM.

    Args:
        records:        Initial extracted records.
        text:           Original source text (for re-extraction).
        max_iterations: Max re-extraction attempts when LLM is available.

    Returns:
        (corrected_records, final_validation_result)
    """
    if not records:
        return [], ValidationResult(valid=False, errors=["no_records"])

    # First pass: auto-correct
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
        # Return auto-corrected records even with warnings
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

        # Build targeted re-extraction prompt
        error_context = "; ".join(all_errors[:5])
        targeted_prompt = (
            f"Previous extraction had issues: {error_context}.\n"
            f"Please re-extract more carefully from this text:\n\n{text[:3000]}"
        )

        try:
            from .llm_extractor import llm_extract
            new_records = llm_extract(targeted_prompt)
        except Exception as exc:
            logger.warning("re_extraction_failed: %s", exc)
            break

        if not new_records:
            break

        # Validate new records
        new_errors:   list[str] = []
        new_warnings: list[str] = []
        for rec in new_records:
            vr = validate_record(rec)
            new_errors.extend(vr.errors)
            new_warnings.extend(vr.warnings)

        # Accept if improved
        if len(new_errors) < len(all_errors):
            records     = new_records
            all_errors  = new_errors
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
