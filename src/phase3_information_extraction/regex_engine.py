"""
Regex-Based Extractor.

Secondary strategy for semi-structured data where headers may be
partially recognizable or data follows known patterns.

Pattern library:
  - USN: r'[1-4][A-Z]{2}\d{2}[A-Z]{2,3}\d{3}'
  - GPA: r'\b\d{1,2}\.\d{1,2}\b' with constraint ≤ 10.0
  - Marks: r'\b\d{1,3}\b' with constraint ≤ max_marks
  - Name: Capitalized word sequences near USN
  - Status: r'\b(PASS|FAIL|P|F|ABSENT|AB)\b'
"""

from __future__ import annotations

import re
from typing import Any

from src.common.config import get_settings
from src.common.models import (
    ExtractedTable,
    StudentRecord,
    SubjectResult,
    ResultStatus,
    ExtractionStrategy,
)
from src.common.observability import get_logger

logger = get_logger(__name__)


class RegexExtractor:
    """
    Extract student records using pattern matching.

    This extractor works on the raw text representation of tables,
    not on structured column data. Useful when:
      - Column headers can't be mapped
      - Table structure is irregular
      - Data is in semi-structured text blocks

    Pattern matching priorities:
      1. USN detection (anchor pattern — highly distinctive)
      2. Name detection (capitalized sequence near USN)
      3. Marks/GPA detection (numeric patterns with constraints)
      4. Status detection (PASS/FAIL keywords)
    """

    # Compiled regex patterns
    PATTERNS = {
        # Indian university USN formats
        # VTU: 1BM21CS001, 4VV22IS123
        # Others: variations with 2-3 letter department codes
        "usn": re.compile(
            r"\b([1-4][A-Z]{2}\d{2}[A-Z]{2,3}\d{3})\b",
            re.IGNORECASE,
        ),

        # GPA: 1-2 digits, dot, 1-2 digits (e.g., 8.5, 9.12, 10.0)
        "gpa": re.compile(
            r"\b(\d{1,2}\.\d{1,2})\b",
        ),

        # Marks: 1-3 digit number (constrained to ≤ max_marks in validation)
        "marks": re.compile(
            r"\b(\d{1,3})\b",
        ),

        # Status indicators
        "status_pass": re.compile(
            r"\b(PASS|PASSED|P|ELIGIBLE)\b",
            re.IGNORECASE,
        ),
        "status_fail": re.compile(
            r"\b(FAIL|FAILED|F|NOT\s*ELIGIBLE)\b",
            re.IGNORECASE,
        ),
        "status_absent": re.compile(
            r"\b(ABSENT|AB)\b",
            re.IGNORECASE,
        ),

        # Name: sequence of capitalized words
        "name": re.compile(
            r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,4})\b",
        ),

        # Subject code: uppercase letters + digits (e.g., 21CS51, MAT201)
        "subject_code": re.compile(
            r"\b(\d{2}[A-Z]{2,4}\d{2,3})\b",
        ),

        # Grade: single or double letter grades
        "grade": re.compile(
            r"\b(A\+|A|B\+|B|C\+|C|D|E|F|FE|O|S)\b",
            re.IGNORECASE,
        ),
    }

    def __init__(self) -> None:
        self.settings = get_settings()

    def extract(self, table: ExtractedTable) -> list[StudentRecord]:
        """Extract student records from table using regex patterns."""
        # Convert table to text blocks for pattern matching
        text_blocks = self._table_to_text_blocks(table)

        if not text_blocks:
            return []

        records = self._extract_from_blocks(text_blocks)

        logger.info(
            "regex_extraction_complete",
            records=len(records),
            text_blocks=len(text_blocks),
        )

        return records

    def _table_to_text_blocks(self, table: ExtractedTable) -> list[str]:
        """Convert table rows to text blocks for pattern matching."""
        blocks = []
        for row in table.rows:
            block = " | ".join(cell.strip() for cell in row if cell.strip())
            if block:
                blocks.append(block)
        return blocks

    def _extract_from_blocks(
        self,
        text_blocks: list[str],
    ) -> list[StudentRecord]:
        """
        Extract records from text blocks using anchored pattern matching.

        Strategy: USN as anchor
          1. Find all USN patterns
          2. For each USN, extract surrounding context
          3. From context, extract name, marks, GPA, status
        """
        records: list[StudentRecord] = []
        usn_records: dict[str, list[dict[str, Any]]] = {}

        for block in text_blocks:
            # Find USN (anchor)
            usn_match = self.PATTERNS["usn"].search(block)
            if not usn_match:
                continue

            usn = usn_match.group(1).upper()
            extracted = self._extract_fields_from_block(block, usn)

            usn_records.setdefault(usn, []).append(extracted)

        # Build StudentRecords from grouped data
        for usn, field_list in usn_records.items():
            subjects: list[SubjectResult] = []
            name = ""
            sgpa = None

            for fields in field_list:
                if fields.get("name") and not name:
                    name = fields["name"]

                if fields.get("sgpa") is not None:
                    sgpa = fields["sgpa"]

                if fields.get("total_marks") is not None:
                    subject = SubjectResult(
                        subject_code=fields.get("subject_code"),
                        subject_name=None,
                        total_marks=fields["total_marks"],
                        max_marks=fields.get("max_marks", self.settings.extraction.marks_max_default),
                        grade=fields.get("grade"),
                        status=fields.get("status", ResultStatus.PASS),
                    )
                    subjects.append(subject)

            record = StudentRecord(
                usn=usn,
                name=name,
                subjects=subjects,
                sgpa=sgpa,
                extraction_strategy=ExtractionStrategy.REGEX,
                overall_confidence=0.75,  # Regex confidence is moderate
                field_confidences={
                    "usn": 0.95,  # USN regex is very precise
                    "name": 0.70 if name else 0.0,
                    "subjects": 0.70 if subjects else 0.0,
                    "sgpa": 0.80 if sgpa else 0.0,
                },
            )
            records.append(record)

        return records

    def _extract_fields_from_block(
        self,
        block: str,
        usn: str,
    ) -> dict[str, Any]:
        """Extract all available fields from a text block."""
        fields: dict[str, Any] = {"usn": usn}

        # Name: capitalized words NOT matching USN
        name_matches = self.PATTERNS["name"].findall(block)
        for name in name_matches:
            # Heuristic: name is near USN but not a column header
            if name.upper() != usn and len(name) > 3:
                fields["name"] = name
                break

        # Subject code
        subj_match = self.PATTERNS["subject_code"].search(block)
        if subj_match:
            fields["subject_code"] = subj_match.group(1)

        # GPA/SGPA
        gpa_matches = self.PATTERNS["gpa"].findall(block)
        for gpa_str in gpa_matches:
            gpa_val = float(gpa_str)
            if 0 < gpa_val <= self.settings.extraction.gpa_max:
                fields["sgpa"] = gpa_val
                break

        # Marks (all numeric values, filtered by range)
        marks_matches = self.PATTERNS["marks"].findall(block)
        numeric_values = [int(m) for m in marks_matches if m.isdigit()]
        # Filter to plausible marks range
        plausible_marks = [
            m for m in numeric_values
            if 0 <= m <= self.settings.extraction.marks_max_default
            and m != int(usn[-3:])  # Don't confuse USN suffix with marks
        ]
        if plausible_marks:
            fields["total_marks"] = plausible_marks[0]

        # Grade
        grade_match = self.PATTERNS["grade"].search(block)
        if grade_match:
            fields["grade"] = grade_match.group(1).upper()

        # Status
        if self.PATTERNS["status_fail"].search(block):
            fields["status"] = ResultStatus.FAIL
        elif self.PATTERNS["status_absent"].search(block):
            fields["status"] = ResultStatus.ABSENT
        elif self.PATTERNS["status_pass"].search(block):
            fields["status"] = ResultStatus.PASS

        return fields
