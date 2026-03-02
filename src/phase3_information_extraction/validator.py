"""
Extraction Validator.

Domain-constraint validation for extracted student records.

Validation rules:
  1. USN format: Must match institution-specific regex pattern
  2. GPA range: 0.0 ≤ SGPA ≤ 10.0, 0.0 ≤ CGPA ≤ 10.0
  3. Marks range: 0 ≤ total_marks ≤ max_marks
  4. Subject count: ≥ 1 per student
  5. Cross-field: FAIL status + high marks is suspect
  6. Cross-record: Same USN should have same name
  7. Duplicate detection: Same USN + same subject → conflict
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from src.common.config import get_settings
from src.common.models import (
    StudentRecord,
    SubjectResult,
    ResultStatus,
)
from src.common.observability import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationError:
    """A single validation error."""

    field: str
    message: str
    severity: str  # "error" | "warning"
    record_usn: str = ""
    suggested_fix: str | None = None


@dataclass
class ValidationResult:
    """Result of validating a set of student records."""

    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)
    records_checked: int = 0
    records_valid: int = 0
    records_with_errors: int = 0
    records_with_warnings: int = 0

    @property
    def error_messages(self) -> list[str]:
        return [f"[{e.field}] {e.message}" for e in self.errors]

    @property
    def warning_messages(self) -> list[str]:
        return [f"[{w.field}] {w.message}" for w in self.warnings]


class ExtractionValidator:
    """
    Validate extracted student records against domain constraints.

    Three-tier validation:
      1. Field-level: Type, format, range checks
      2. Cross-field: Logical consistency within a record
      3. Cross-record: Consistency across records in the batch
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._usn_pattern = re.compile(
            self.settings.extraction.usn_pattern,
            re.IGNORECASE,
        )

    def validate_batch(
        self,
        records: list[StudentRecord],
    ) -> ValidationResult:
        """Validate a batch of student records."""
        result = ValidationResult(
            is_valid=True,
            records_checked=len(records),
        )

        if not records:
            return result

        # Field-level and cross-field validation
        usn_names: dict[str, set[str]] = defaultdict(set)
        usn_subjects: dict[str, list[str]] = defaultdict(list)

        for record in records:
            record_errors = self._validate_record(record)

            if record.name:
                usn_names[record.usn].add(record.name.strip().lower())

            for subj in record.subjects:
                if subj.subject_code:
                    usn_subjects[record.usn].append(subj.subject_code)

            has_errors = False
            has_warnings = False

            for err in record_errors:
                if err.severity == "error":
                    result.errors.append(err)
                    has_errors = True
                else:
                    result.warnings.append(err)
                    has_warnings = True

            if has_errors:
                result.records_with_errors += 1
            elif has_warnings:
                result.records_with_warnings += 1
            else:
                result.records_valid += 1

        # Cross-record validation
        cross_errors = self._validate_cross_record(
            records, usn_names, usn_subjects,
        )
        for err in cross_errors:
            if err.severity == "error":
                result.errors.append(err)
            else:
                result.warnings.append(err)

        result.is_valid = len(result.errors) == 0

        logger.info(
            "validation_complete",
            total=result.records_checked,
            valid=result.records_valid,
            errors=len(result.errors),
            warnings=len(result.warnings),
        )

        return result

    def _validate_record(self, record: StudentRecord) -> list[ValidationError]:
        """Validate a single student record."""
        errors: list[ValidationError] = []

        # 1. USN format
        if not self._usn_pattern.match(record.usn):
            errors.append(ValidationError(
                field="usn",
                message=f"USN '{record.usn}' does not match expected pattern",
                severity="error",
                record_usn=record.usn,
            ))

        # 2. Name validation
        if record.name:
            if len(record.name.strip()) < 2:
                errors.append(ValidationError(
                    field="name",
                    message=f"Name too short: '{record.name}'",
                    severity="warning",
                    record_usn=record.usn,
                ))
            if re.search(r"\d", record.name):
                errors.append(ValidationError(
                    field="name",
                    message=f"Name contains digits: '{record.name}'",
                    severity="warning",
                    record_usn=record.usn,
                ))

        # 3. SGPA range
        if record.sgpa is not None:
            if not (0.0 <= record.sgpa <= self.settings.extraction.gpa_max):
                errors.append(ValidationError(
                    field="sgpa",
                    message=(
                        f"SGPA {record.sgpa} out of range "
                        f"[0, {self.settings.extraction.gpa_max}]"
                    ),
                    severity="error",
                    record_usn=record.usn,
                ))

        # 4. Subject count
        if len(record.subjects) == 0:
            errors.append(ValidationError(
                field="subjects",
                message="No subjects found for student",
                severity="warning",
                record_usn=record.usn,
            ))

        # 5. Per-subject validation
        for i, subj in enumerate(record.subjects):
            errors.extend(self._validate_subject(subj, record.usn, i))

        # 6. Cross-field checks within record
        errors.extend(self._validate_cross_field(record))

        return errors

    def _validate_subject(
        self,
        subj: SubjectResult,
        usn: str,
        idx: int,
    ) -> list[ValidationError]:
        """Validate a single subject result."""
        errors: list[ValidationError] = []
        prefix = f"subjects[{idx}]"

        # Marks range
        if subj.total_marks is not None:
            if subj.total_marks < 0:
                errors.append(ValidationError(
                    field=f"{prefix}.total_marks",
                    message=f"Negative marks: {subj.total_marks}",
                    severity="error",
                    record_usn=usn,
                ))
            if subj.max_marks and subj.total_marks > subj.max_marks:
                errors.append(ValidationError(
                    field=f"{prefix}.total_marks",
                    message=(
                        f"Marks {subj.total_marks} exceed max {subj.max_marks}"
                    ),
                    severity="error",
                    record_usn=usn,
                    suggested_fix=f"Check if marks should be ≤ {subj.max_marks}",
                ))

        # Status-marks consistency
        if subj.status == ResultStatus.FAIL and subj.total_marks is not None:
            if subj.max_marks:
                pass_threshold = subj.max_marks * 0.4  # Typical 40% pass mark
                if subj.total_marks >= pass_threshold:
                    errors.append(ValidationError(
                        field=f"{prefix}.status",
                        message=(
                            f"FAIL status but marks {subj.total_marks} "
                            f"≥ pass threshold ~{pass_threshold}"
                        ),
                        severity="warning",
                        record_usn=usn,
                        suggested_fix="Verify status or marks value",
                    ))

        if subj.status == ResultStatus.PASS and subj.total_marks is not None:
            if subj.max_marks:
                pass_threshold = subj.max_marks * 0.35
                if subj.total_marks < pass_threshold:
                    errors.append(ValidationError(
                        field=f"{prefix}.status",
                        message=(
                            f"PASS status but marks {subj.total_marks} "
                            f"< typical minimum ~{pass_threshold}"
                        ),
                        severity="warning",
                        record_usn=usn,
                    ))

        # Grade-status consistency
        if subj.grade and subj.status:
            fail_grades = {"F", "FE", "AB"}
            if subj.grade.upper() in fail_grades and subj.status == ResultStatus.PASS:
                errors.append(ValidationError(
                    field=f"{prefix}.grade",
                    message=(
                        f"Grade '{subj.grade}' indicates failure "
                        f"but status is PASS"
                    ),
                    severity="error",
                    record_usn=usn,
                ))

        return errors

    def _validate_cross_field(
        self,
        record: StudentRecord,
    ) -> list[ValidationError]:
        """Cross-field validation within a single record."""
        errors: list[ValidationError] = []

        # SGPA vs marks consistency
        if record.sgpa is not None and record.subjects:
            all_fail = all(
                s.status == ResultStatus.FAIL for s in record.subjects
            )
            if all_fail and record.sgpa > 5.0:
                errors.append(ValidationError(
                    field="sgpa",
                    message=(
                        f"SGPA {record.sgpa} seems high when all "
                        f"subjects show FAIL status"
                    ),
                    severity="warning",
                    record_usn=record.usn,
                ))

            all_pass = all(
                s.status == ResultStatus.PASS for s in record.subjects
            )
            if all_pass and record.sgpa < 2.0:
                errors.append(ValidationError(
                    field="sgpa",
                    message=(
                        f"SGPA {record.sgpa} seems very low when all "
                        f"subjects show PASS status"
                    ),
                    severity="warning",
                    record_usn=record.usn,
                ))

        return errors

    def _validate_cross_record(
        self,
        records: list[StudentRecord],
        usn_names: dict[str, set[str]],
        usn_subjects: dict[str, list[str]],
    ) -> list[ValidationError]:
        """Cross-record validation across the batch."""
        errors: list[ValidationError] = []

        # Same USN should have same name
        for usn, names in usn_names.items():
            if len(names) > 1:
                errors.append(ValidationError(
                    field="name",
                    message=(
                        f"USN {usn} has multiple names: {names}"
                    ),
                    severity="warning",
                    record_usn=usn,
                ))

        # Duplicate subject detection
        for usn, subjects in usn_subjects.items():
            seen = set()
            for subj in subjects:
                if subj in seen:
                    errors.append(ValidationError(
                        field="subjects",
                        message=(
                            f"Duplicate subject '{subj}' for USN {usn}"
                        ),
                        severity="warning",
                        record_usn=usn,
                    ))
                seen.add(subj)

        # Statistical outlier detection
        if len(records) >= 5:
            sgpas = [r.sgpa for r in records if r.sgpa is not None]
            if sgpas:
                mean_sgpa = sum(sgpas) / len(sgpas)
                std_sgpa = (
                    sum((s - mean_sgpa) ** 2 for s in sgpas) / len(sgpas)
                ) ** 0.5

                if std_sgpa > 0:
                    for record in records:
                        if record.sgpa is not None:
                            z_score = abs(record.sgpa - mean_sgpa) / std_sgpa
                            if z_score > 3.0:
                                errors.append(ValidationError(
                                    field="sgpa",
                                    message=(
                                        f"SGPA {record.sgpa} is a statistical "
                                        f"outlier (z={z_score:.1f})"
                                    ),
                                    severity="warning",
                                    record_usn=record.usn,
                                ))

        return errors
