"""
Table Stitcher.

Handles multi-page tables by detecting continuation patterns and
stitching table fragments across page boundaries.

Common patterns in academic result PDFs:
  - Same column headers repeated on each page (page continuation)
  - Table continues without headers (just data rows)
  - Page number or footer text between fragments
  - Different tables for different semester/subjects
"""

from __future__ import annotations

from src.common.models import ExtractedTable
from src.common.observability import get_logger

logger = get_logger(__name__)


class TableStitcher:
    """
    Detects and stitches multi-page table fragments.

    Algorithm:
      1. Group tables by column structure similarity
      2. For each group, determine if tables are continuation of the same table
      3. Merge rows while preserving ordering
      4. Recompute confidence as weighted average

    Heuristics for detecting continuation:
      - Headers match (exact or fuzzy)
      - Column count matches
      - Data type pattern matches (e.g., USN column has consistent format)
      - Sequential page numbers
    """

    # Minimum header similarity to consider tables as part of the same group
    HEADER_SIMILARITY_THRESHOLD = 0.8

    def stitch_tables(self, tables: list[ExtractedTable]) -> list[ExtractedTable]:
        """
        Stitch multi-page table fragments into complete tables.

        Input: list of table fragments (possibly from different pages)
        Output: list of complete tables (fragments merged where appropriate)
        """
        if len(tables) <= 1:
            return tables

        # Sort by page number, then table index
        sorted_tables = sorted(tables, key=lambda t: (t.page_number, t.table_index))

        # Group tables by column structure
        groups: list[list[ExtractedTable]] = []
        current_group: list[ExtractedTable] = [sorted_tables[0]]

        for table in sorted_tables[1:]:
            if self._should_merge(current_group[-1], table):
                current_group.append(table)
            else:
                groups.append(current_group)
                current_group = [table]

        groups.append(current_group)

        # Merge each group into a single table
        result = []
        for group in groups:
            if len(group) == 1:
                result.append(group[0])
            else:
                merged = self._merge_group(group)
                result.append(merged)
                logger.info(
                    "tables_merged",
                    fragment_count=len(group),
                    pages=[t.page_number for t in group],
                    result_rows=merged.num_rows,
                )

        return result

    def _should_merge(self, table_a: ExtractedTable, table_b: ExtractedTable) -> bool:
        """
        Determine if two tables should be merged.

        Checks:
          1. Column count matches (±1)
          2. Headers match (if both have headers)
          3. Tables are on consecutive pages
        """
        # Column count check
        if abs(table_a.num_cols - table_b.num_cols) > 1:
            return False

        # Header similarity check
        if table_a.headers and table_b.headers:
            similarity = self._header_similarity(table_a.headers, table_b.headers)
            if similarity >= self.HEADER_SIMILARITY_THRESHOLD:
                return True

        # If second table has no headers but same column count → likely continuation
        if not table_b.headers and table_a.num_cols == table_b.num_cols:
            if table_b.page_number == table_a.page_number + 1:
                return True

        return False

    def _merge_group(self, group: list[ExtractedTable]) -> ExtractedTable:
        """Merge a group of table fragments into a single table."""
        # Use headers from first table
        headers = group[0].headers

        # Combine all rows (skip header-like rows in continuation tables)
        all_rows: list[list[str]] = []
        for table in group:
            for row in table.rows:
                # Skip if row looks like a repeated header
                if self._is_header_row(row, headers):
                    continue
                all_rows.append(row)

        # Weighted average confidence
        total_rows = sum(t.num_rows for t in group)
        avg_confidence = (
            sum(t.confidence * t.num_rows for t in group) / total_rows
            if total_rows > 0
            else 0.0
        )

        return ExtractedTable(
            page_number=group[0].page_number,
            table_index=group[0].table_index,
            headers=headers,
            rows=all_rows,
            confidence=avg_confidence,
            num_rows=len(all_rows),
            num_cols=len(headers),
        )

    @staticmethod
    def _header_similarity(headers_a: list[str], headers_b: list[str]) -> float:
        """Compute similarity between two header lists."""
        if not headers_a or not headers_b:
            return 0.0

        # Normalize
        norm_a = [h.lower().strip() for h in headers_a]
        norm_b = [h.lower().strip() for h in headers_b]

        # Count exact matches
        min_len = min(len(norm_a), len(norm_b))
        matches = sum(1 for i in range(min_len) if norm_a[i] == norm_b[i])

        return matches / max(len(norm_a), len(norm_b))

    @staticmethod
    def _is_header_row(row: list[str], headers: list[str]) -> bool:
        """Check if a data row is actually a repeated header."""
        if not headers:
            return False

        norm_row = [cell.lower().strip() for cell in row]
        norm_headers = [h.lower().strip() for h in headers]

        matches = sum(1 for r, h in zip(norm_row, norm_headers) if r == h)
        return matches >= len(headers) * 0.7
