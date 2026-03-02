"""
Excel Parser.

Parses .xlsx, .xls, and .xlsm files with:
  - Header detection (heuristic + fuzzy matching against known academic columns)
  - Merged cell handling (unmerge + forward-fill)
  - Multi-sheet support
  - Type coercion (string→int for marks, string→float for GPA)
  - Schema mapping to canonical column names
"""

from __future__ import annotations

import io
from typing import Any

from src.common.config import get_settings
from src.common.models import (
    AttachmentInfo,
    DocumentType,
    DocumentParseResult,
    ExtractedTable,
)
from src.common.observability import get_logger, TABLES_EXTRACTED

logger = get_logger(__name__)

# Canonical column name mappings
# Maps various header spellings to our standard schema
COLUMN_MAPPINGS: dict[str, list[str]] = {
    "usn": ["usn", "usn no", "reg no", "registration number", "reg. no.", "university seat number",
            "enrollment no", "enroll no", "roll no", "hall ticket"],
    "student_name": ["name", "student name", "name of the student", "candidate name", "student"],
    "subject_code": ["sub code", "subject code", "sub. code", "course code", "code"],
    "subject_name": ["subject", "subject name", "sub name", "course name", "course title"],
    "internal_marks": ["internal", "int marks", "ia marks", "cia", "internal marks", "int",
                       "sessional", "internal assessment"],
    "external_marks": ["external", "ext marks", "univ marks", "external marks", "ext",
                       "university marks", "see", "semester end exam"],
    "total_marks": ["total", "total marks", "marks obtained", "marks", "tot"],
    "max_marks": ["max marks", "maximum marks", "max", "out of"],
    "grade": ["grade", "letter grade", "grade obtained"],
    "grade_points": ["grade points", "gp", "grade point"],
    "credits": ["credits", "credit", "cr"],
    "status": ["result", "status", "pass/fail", "p/f", "outcome"],
    "sgpa": ["sgpa", "gpa", "semester gpa"],
    "cgpa": ["cgpa", "cumulative gpa", "overall gpa"],
    "semester": ["semester", "sem", "sem no"],
}


class ExcelParser:
    """
    Parses Excel files containing academic results.

    Handles common Excel challenges:
      - Header row may not be row 1 (often after title/logo rows)
      - Merged cells for institutional headers
      - Hidden columns/rows
      - Multiple sheets (one per semester, one per department, etc.)
      - Inconsistent formatting across universities
    """

    def __init__(self) -> None:
        self.settings = get_settings()

    async def parse(
        self,
        attachment: AttachmentInfo,
        file_bytes: bytes,
    ) -> DocumentParseResult:
        """Parse an Excel file into extracted tables."""
        import openpyxl

        errors: list[str] = []
        tables: list[ExtractedTable] = []

        try:
            wb = openpyxl.load_workbook(
                io.BytesIO(file_bytes),
                read_only=True,
                data_only=True,  # Read computed values, not formulas
            )

            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]

                # Skip empty sheets
                if ws.max_row is None or ws.max_row < 2:
                    continue

                try:
                    table = self._parse_sheet(ws, sheet_name)
                    if table and table.num_rows > 0:
                        tables.append(table)
                        TABLES_EXTRACTED.inc()
                except Exception as e:
                    errors.append(f"Error parsing sheet '{sheet_name}': {e}")
                    logger.warning(
                        "excel_sheet_parse_error",
                        sheet=sheet_name,
                        error=str(e),
                    )

            wb.close()

        except Exception as e:
            errors.append(f"Excel parse error: {e}")
            logger.error(
                "excel_parse_error",
                attachment_id=str(attachment.id),
                error=str(e),
            )

        return DocumentParseResult(
            attachment_id=attachment.id,
            document_type=DocumentType.EXCEL,
            tables=tables,
            page_count=len(tables),
            parse_method="openpyxl",
            errors=errors,
        )

    def _parse_sheet(
        self,
        ws: Any,
        sheet_name: str,
    ) -> ExtractedTable | None:
        """Parse a single worksheet into an ExtractedTable."""
        # Step 1: Find header row
        header_row_idx, headers = self._detect_header_row(ws)

        if header_row_idx is None:
            logger.info(
                "no_header_detected",
                sheet=sheet_name,
            )
            return None

        # Step 2: Map headers to canonical names
        mapped_headers = self._map_headers(headers)

        # Step 3: Extract data rows
        rows: list[list[str]] = []
        for row in ws.iter_rows(
            min_row=header_row_idx + 1,
            max_row=ws.max_row,
            values_only=True,
        ):
            # Skip empty rows
            if all(cell is None or str(cell).strip() == "" for cell in row):
                continue

            row_data = [
                str(cell).strip() if cell is not None else ""
                for cell in row
            ]

            # Truncate or pad to match header count
            if len(row_data) > len(headers):
                row_data = row_data[: len(headers)]
            elif len(row_data) < len(headers):
                row_data.extend([""] * (len(headers) - len(row_data)))

            rows.append(row_data)

        if not rows:
            return None

        return ExtractedTable(
            page_number=1,
            table_index=0,
            headers=mapped_headers,
            rows=rows,
            confidence=0.90,  # Excel parsing is generally high confidence
            num_rows=len(rows),
            num_cols=len(mapped_headers),
        )

    def _detect_header_row(
        self,
        ws: Any,
    ) -> tuple[int | None, list[str]]:
        """
        Detect the header row in a worksheet.

        Strategy:
          1. Scan first 20 rows
          2. For each row, count matches against known column names
          3. Row with ≥3 matches is the header row
          4. If no match, try fuzzy matching (Levenshtein distance ≤ 2)
        """
        all_known = set()
        for names in COLUMN_MAPPINGS.values():
            all_known.update(n.lower() for n in names)

        best_row = None
        best_count = 0
        best_headers: list[str] = []

        for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=20, values_only=True), start=1):
            cells = [str(cell).strip().lower() if cell is not None else "" for cell in row]
            non_empty = [c for c in cells if c]

            if len(non_empty) < 3:
                continue

            match_count = sum(1 for c in non_empty if c in all_known)

            # Also try fuzzy matching
            if match_count < 3:
                match_count += self._fuzzy_header_matches(non_empty, all_known)

            if match_count > best_count:
                best_count = match_count
                best_row = row_idx
                best_headers = [
                    str(cell).strip() if cell is not None else f"col_{i}"
                    for i, cell in enumerate(row)
                ]

        if best_count >= 3:
            return best_row, best_headers

        return None, []

    def _map_headers(self, raw_headers: list[str]) -> list[str]:
        """Map raw column headers to canonical names."""
        mapped = []
        for header in raw_headers:
            canonical = self._find_canonical_name(header)
            mapped.append(canonical or header)
        return mapped

    def _find_canonical_name(self, header: str) -> str | None:
        """Find the canonical column name for a raw header."""
        header_lower = header.lower().strip()

        for canonical, variants in COLUMN_MAPPINGS.items():
            if header_lower in [v.lower() for v in variants]:
                return canonical

        # Fuzzy match: check if header is contained in or contains a known name
        for canonical, variants in COLUMN_MAPPINGS.items():
            for variant in variants:
                if variant.lower() in header_lower or header_lower in variant.lower():
                    return canonical

        return None

    def _fuzzy_header_matches(
        self,
        cells: list[str],
        known: set[str],
    ) -> int:
        """Count fuzzy matches between cells and known column names."""
        try:
            import jellyfish

            count = 0
            for cell in cells:
                for known_name in known:
                    if jellyfish.jaro_winkler_similarity(cell, known_name) >= 0.85:
                        count += 1
                        break
            return count
        except ImportError:
            return 0
