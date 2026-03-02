"""
Rule-Based Extractor.

Highest precision strategy for structured tables where column headers
can be reliably mapped to our canonical schema.

Works best when:
  - Table has clear, recognizable column headers
  - Data is well-structured (one student per row, or one subject per row)
  - Known university format (pre-configured schema mappings)

Produces confidence 0.95 for exact header matches, 0.80 for fuzzy matches.
"""

from __future__ import annotations

from src.common.config import get_settings
from src.common.models import (
    ExtractedTable,
    StudentRecord,
    SubjectResult,
    ResultStatus,
    ExamType,
    ExtractionStrategy,
)
from src.common.observability import get_logger

logger = get_logger(__name__)

# Known column name variants (lowercase)
COLUMN_VARIANTS = {
    "usn": {"usn", "reg no", "registration number", "reg. no.", "enrollment no",
            "roll no", "hall ticket", "university seat number"},
    "name": {"name", "student name", "name of the student", "candidate name", "student"},
    "subject_code": {"sub code", "subject code", "sub. code", "course code", "code"},
    "subject_name": {"subject", "subject name", "sub name", "course name", "course title"},
    "internal_marks": {"internal", "int marks", "ia marks", "cia", "internal marks",
                       "sessional", "internal assessment"},
    "external_marks": {"external", "ext marks", "univ marks", "external marks",
                       "university marks", "see", "semester end exam"},
    "total_marks": {"total", "total marks", "marks obtained", "marks", "tot"},
    "max_marks": {"max marks", "maximum marks", "max", "out of"},
    "grade": {"grade", "letter grade", "grade obtained"},
    "grade_points": {"grade points", "gp", "grade point"},
    "credits": {"credits", "credit", "cr"},
    "status": {"result", "status", "pass/fail", "p/f", "outcome"},
    "sgpa": {"sgpa", "gpa", "semester gpa"},
    "semester": {"semester", "sem", "sem no"},
}


class RuleBasedExtractor:
    """
    Extracts student records using header-to-schema mapping rules.

    Algorithm:
      1. Map table headers to canonical column names
      2. Determine table layout:
         a. Subject-per-row: each row has USN, subject, marks (common in marksheets)
         b. Student-per-row: each row has all subjects for one student
         c. Mixed: USN in header rows, subjects in sub-rows
      3. Extract records based on detected layout
      4. Assign confidence based on header match quality
    """

    def __init__(self) -> None:
        self.settings = get_settings()

    def extract(self, table: ExtractedTable) -> list[StudentRecord]:
        """Extract student records from a table using rule-based approach."""
        # Step 1: Map headers
        column_map = self._map_headers(table.headers)

        if not column_map:
            logger.debug("rule_no_header_match", headers=table.headers)
            return []

        # Determine match confidence
        exact_matches = len(column_map)
        total_headers = len(table.headers)
        header_confidence = min(0.95, 0.5 + (exact_matches / max(total_headers, 1)) * 0.5)

        # Step 2: Detect layout
        layout = self._detect_layout(column_map, table)

        # Step 3: Extract based on layout
        if layout == "subject_per_row":
            records = self._extract_subject_per_row(table, column_map, header_confidence)
        elif layout == "student_per_row":
            records = self._extract_student_per_row(table, column_map, header_confidence)
        else:
            records = self._extract_generic(table, column_map, header_confidence)

        logger.info(
            "rule_extraction_complete",
            layout=layout,
            records=len(records),
            header_confidence=round(header_confidence, 3),
            mapped_columns=list(column_map.keys()),
        )

        return records

    def _map_headers(self, headers: list[str]) -> dict[str, int]:
        """
        Map table headers to canonical column names.

        Returns dict of {canonical_name: column_index}.
        """
        column_map: dict[str, int] = {}

        for idx, header in enumerate(headers):
            normalized = header.lower().strip()

            for canonical, variants in COLUMN_VARIANTS.items():
                if normalized in variants:
                    column_map[canonical] = idx
                    break

        return column_map

    def _detect_layout(
        self,
        column_map: dict[str, int],
        table: ExtractedTable,
    ) -> str:
        """
        Detect table layout type.

        - subject_per_row: has USN + subject_code/name columns
        - student_per_row: has USN but no subject columns (subjects in headers)
        - generic: fallback
        """
        has_usn = "usn" in column_map
        has_subject = "subject_code" in column_map or "subject_name" in column_map
        has_marks = "total_marks" in column_map or "grade" in column_map

        if has_usn and has_subject and has_marks:
            return "subject_per_row"
        elif has_usn and has_marks and not has_subject:
            return "student_per_row"
        else:
            return "generic"

    def _extract_subject_per_row(
        self,
        table: ExtractedTable,
        column_map: dict[str, int],
        confidence: float,
    ) -> list[StudentRecord]:
        """
        Extract records where each row is one subject for one student.

        Multiple rows share the same USN → group into one StudentRecord.
        """
        usn_col = column_map.get("usn")
        if usn_col is None:
            return []

        # Group rows by USN
        student_rows: dict[str, list[list[str]]] = {}
        for row in table.rows:
            if usn_col >= len(row):
                continue
            usn = row[usn_col].strip()
            if not usn:
                continue
            student_rows.setdefault(usn, []).append(row)

        records: list[StudentRecord] = []
        for usn, rows in student_rows.items():
            subjects: list[SubjectResult] = []

            for row in rows:
                subject = self._extract_subject_from_row(row, column_map)
                if subject:
                    subjects.append(subject)

            # Get name from first row
            name_col = column_map.get("name")
            name = rows[0][name_col].strip() if name_col is not None and name_col < len(rows[0]) else ""

            # Get semester
            sem_col = column_map.get("semester")
            semester = None
            if sem_col is not None and sem_col < len(rows[0]):
                try:
                    semester = int(rows[0][sem_col].strip())
                except (ValueError, IndexError):
                    pass

            # Get SGPA
            sgpa_col = column_map.get("sgpa")
            sgpa = None
            if sgpa_col is not None and sgpa_col < len(rows[0]):
                try:
                    sgpa = float(rows[0][sgpa_col].strip())
                except (ValueError, IndexError):
                    pass

            record = StudentRecord(
                usn=usn,
                name=name,
                semester=semester,
                subjects=subjects,
                sgpa=sgpa,
                extraction_strategy=ExtractionStrategy.RULE_BASED,
                overall_confidence=confidence,
                field_confidences={
                    "usn": confidence,
                    "name": confidence * 0.95 if name else 0.0,
                    "subjects": confidence * 0.9,
                    "sgpa": confidence if sgpa else 0.0,
                },
            )
            records.append(record)

        return records

    def _extract_student_per_row(
        self,
        table: ExtractedTable,
        column_map: dict[str, int],
        confidence: float,
    ) -> list[StudentRecord]:
        """
        Extract records where each row is one complete student.

        Common in summary result sheets where subjects aren't listed individually.
        """
        usn_col = column_map.get("usn")
        if usn_col is None:
            return []

        records: list[StudentRecord] = []
        for row in table.rows:
            if usn_col >= len(row):
                continue
            usn = row[usn_col].strip()
            if not usn:
                continue

            name_col = column_map.get("name")
            name = row[name_col].strip() if name_col is not None and name_col < len(row) else ""

            sgpa_col = column_map.get("sgpa")
            sgpa = None
            if sgpa_col is not None and sgpa_col < len(row):
                try:
                    sgpa = float(row[sgpa_col].strip())
                except ValueError:
                    pass

            record = StudentRecord(
                usn=usn,
                name=name,
                sgpa=sgpa,
                extraction_strategy=ExtractionStrategy.RULE_BASED,
                overall_confidence=confidence,
            )
            records.append(record)

        return records

    def _extract_generic(
        self,
        table: ExtractedTable,
        column_map: dict[str, int],
        confidence: float,
    ) -> list[StudentRecord]:
        """Fallback extraction for unknown layouts."""
        return self._extract_student_per_row(table, column_map, confidence * 0.8)

    def _extract_subject_from_row(
        self,
        row: list[str],
        column_map: dict[str, int],
    ) -> SubjectResult | None:
        """Extract a SubjectResult from a single row."""
        try:
            # Subject code
            code_col = column_map.get("subject_code")
            code = row[code_col].strip() if code_col is not None and code_col < len(row) else None

            # Subject name
            name_col = column_map.get("subject_name")
            subj_name = row[name_col].strip() if name_col is not None and name_col < len(row) else None

            # Marks
            total_col = column_map.get("total_marks")
            total_marks = 0
            if total_col is not None and total_col < len(row):
                try:
                    total_marks = int(float(row[total_col].strip()))
                except (ValueError, IndexError):
                    return None

            max_col = column_map.get("max_marks")
            max_marks = self.settings.extraction.marks_max_default
            if max_col is not None and max_col < len(row):
                try:
                    max_marks = int(float(row[max_col].strip()))
                except (ValueError, IndexError):
                    pass

            # Internal/External marks
            int_col = column_map.get("internal_marks")
            internal = None
            if int_col is not None and int_col < len(row):
                try:
                    internal = int(float(row[int_col].strip()))
                except (ValueError, IndexError):
                    pass

            ext_col = column_map.get("external_marks")
            external = None
            if ext_col is not None and ext_col < len(row):
                try:
                    external = int(float(row[ext_col].strip()))
                except (ValueError, IndexError):
                    pass

            # Grade
            grade_col = column_map.get("grade")
            grade = row[grade_col].strip() if grade_col is not None and grade_col < len(row) else None

            # Grade points
            gp_col = column_map.get("grade_points")
            gp = None
            if gp_col is not None and gp_col < len(row):
                try:
                    gp = float(row[gp_col].strip())
                except (ValueError, IndexError):
                    pass

            # Credits
            cr_col = column_map.get("credits")
            credits = None
            if cr_col is not None and cr_col < len(row):
                try:
                    credits = int(float(row[cr_col].strip()))
                except (ValueError, IndexError):
                    pass

            # Status
            status_col = column_map.get("status")
            status = ResultStatus.PASS
            if status_col is not None and status_col < len(row):
                status_text = row[status_col].strip().upper()
                if status_text in ("F", "FAIL", "FAILED", "AB", "ABSENT"):
                    status = ResultStatus.FAIL
                elif status_text in ("AB", "ABSENT"):
                    status = ResultStatus.ABSENT
                elif status_text in ("W", "WITHHELD"):
                    status = ResultStatus.WITHHELD

            return SubjectResult(
                subject_code=code,
                subject_name=subj_name,
                internal_marks=internal,
                external_marks=external,
                total_marks=total_marks,
                max_marks=max_marks,
                grade=grade,
                grade_points=gp,
                credits=credits,
                status=status,
            )

        except Exception as e:
            logger.debug("subject_extraction_failed", error=str(e))
            return None
