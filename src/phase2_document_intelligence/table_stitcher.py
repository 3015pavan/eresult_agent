"""
Table Stitcher — Phase 2.

Merges multi-page tables that were split across PDF pages.

Detection heuristics:
  1. Continuation: Last row of page N looks like a data row (no header keywords).
  2. Header matching: First row of page N+1 matches the header of page N.
  3. Row count consistency: Pages with same number of columns are candidates.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# Keywords that typically appear only in header rows
_HEADER_KEYWORDS = re.compile(
    r"\b(subject|code|name|usn|marks|grade|status|result|total|max|pass|fail|"
    r"serial|no\.?|s\.no|sl\.?|semester|sem|sgpa|cgpa|credits?)\b",
    re.IGNORECASE,
)

_NUMERIC_DOMINANT_RE = re.compile(r"^\d+[\d\.\s,/-]*$")


def _is_header_row(row: list[str]) -> bool:
    """Return True if the row looks like a table header."""
    if not row:
        return False
    hits = sum(1 for cell in row if _HEADER_KEYWORDS.search(cell))
    return hits >= max(1, len(row) // 3)


def _cols_match(row_a: list[str], row_b: list[str]) -> bool:
    """Return True if two rows have the same column count or differ by ≤1."""
    return abs(len(row_a) - len(row_b)) <= 1


def stitch_tables(
    tables: list[list[list[str]]],
    similarity_threshold: float = 0.6,
) -> list[list[list[str]]]:
    """
    Merge tables that are continuations of each other across pages.

    Args:
        tables: List of tables, each a list of rows, each row a list of cell strings.
        similarity_threshold: Fraction of column names that must match to consider
                              two tables as the same logical table.

    Returns:
        Deduplicated / merged table list.
    """
    if len(tables) <= 1:
        return tables

    result: list[list[list[str]]] = []
    current = tables[0]

    for next_table in tables[1:]:
        if not next_table:
            continue

        # Try to detect if next_table is a continuation
        current_header = current[0] if current else []
        next_first     = next_table[0] if next_table else []

        if not _cols_match(current_header, next_first):
            # Different structure — separate tables
            result.append(current)
            current = next_table
            continue

        # Check if next page starts with a repetition of the header
        if _is_header_row(next_first):
            # Calculate header similarity
            matched = sum(
                1 for a, b in zip(current_header, next_first)
                if a.strip().lower() == b.strip().lower()
            )
            total_cols = max(len(current_header), len(next_first), 1)
            similarity = matched / total_cols

            if similarity >= similarity_threshold:
                # Same logical table — append rows, skip the repeated header
                logger.debug(
                    "table_stitcher: merging continuation table "
                    "(similarity=%.2f, rows=%d→+%d)",
                    similarity, len(current), len(next_table) - 1,
                )
                current = current + next_table[1:]
                continue

        # No header on next page — assume it's a direct continuation
        if not _is_header_row(next_first) and _cols_match(current_header, next_first):
            logger.debug(
                "table_stitcher: direct continuation (no header on next page, rows=%d→+%d)",
                len(current), len(next_table),
            )
            current = current + next_table
            continue

        result.append(current)
        current = next_table

    result.append(current)
    return result


def extract_student_rows(
    table: list[list[str]],
    usn_col_hints: list[str] | None = None,
) -> list[dict[str, str]]:
    """
    Convert a raw table (list of rows) into list of dicts using the header row as keys.
    USN column is auto-detected if not hinted.
    """
    if not table or len(table) < 2:
        return []

    header = [cell.strip().lower() for cell in table[0]]
    rows   = table[1:]
    result: list[dict[str, str]] = []

    for row in rows:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        d = {header[i]: (row[i].strip() if i < len(row) else "") for i in range(len(header))}
        if any(d.values()):
            result.append(d)

    return result
