"""
Native PDF Parser.

Extracts tables and text from PDFs that have a text layer (not scanned).

Parser chain:
  1. pdfplumber — text extraction and basic table detection
  2. camelot (lattice mode) — high-precision table extraction for ruled tables
  3. camelot (stream mode) — fallback for tables without visible borders
  4. tabula-py — secondary fallback using Java-based extraction

Table detection confidence:
  - camelot provides accuracy scores per table
  - Score < threshold → try alternative method
  - Score mapping: camelot accuracy → our confidence ∈ [0, 1]
"""

from __future__ import annotations

import io
from typing import Any
from uuid import UUID

from src.common.config import get_settings
from src.common.models import (
    AttachmentInfo,
    DocumentType,
    DocumentParseResult,
    ExtractedTable,
)
from src.common.observability import get_logger, TABLES_EXTRACTED

logger = get_logger(__name__)


class NativePDFParser:
    """
    Extracts tables from native (text-layer) PDFs.

    Architecture:
      - pdfplumber for text extraction (preserves layout positioning)
      - camelot for table detection and extraction
      - Two-pass strategy: lattice (ruled tables) → stream (borderless tables)
      - Per-table confidence scoring based on camelot's accuracy metric

    Handles:
      - Single and multi-page tables
      - Merged cells
      - Multi-column layouts
      - Header row detection
    """

    def __init__(self) -> None:
        self.settings = get_settings()

    async def parse(
        self,
        attachment: AttachmentInfo,
        file_bytes: bytes,
    ) -> DocumentParseResult:
        """Extract tables and text from a native PDF."""
        errors: list[str] = []
        tables: list[ExtractedTable] = []
        raw_text = ""
        page_count = 0

        try:
            # Step 1: Extract full text using pdfplumber
            raw_text, page_count = self._extract_text(file_bytes)

            # Step 2: Extract tables using camelot
            tables = self._extract_tables_camelot(file_bytes, page_count)

            if not tables:
                # Step 3: Try tabula as fallback
                logger.info("camelot_no_tables_trying_tabula", attachment_id=str(attachment.id))
                tables = self._extract_tables_tabula(file_bytes)

        except Exception as e:
            errors.append(f"Native PDF parsing error: {e}")
            logger.error(
                "native_pdf_parse_error",
                attachment_id=str(attachment.id),
                error=str(e),
            )

        TABLES_EXTRACTED.inc(len(tables))

        return DocumentParseResult(
            attachment_id=attachment.id,
            document_type=DocumentType.PDF_NATIVE,
            tables=tables,
            raw_text=raw_text,
            page_count=page_count,
            ocr_used=False,
            parse_method="pdfplumber+camelot",
            errors=errors,
        )

    def _extract_text(self, file_bytes: bytes) -> tuple[str, int]:
        """Extract all text from PDF pages using pdfplumber."""
        import pdfplumber

        all_text = []
        page_count = 0

        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            page_count = len(pdf.pages)

            if page_count > self.settings.document.max_pages_per_document:
                logger.warning(
                    "pdf_page_limit_exceeded",
                    page_count=page_count,
                    max_pages=self.settings.document.max_pages_per_document,
                )
                page_count = self.settings.document.max_pages_per_document

            for page in pdf.pages[:page_count]:
                text = page.extract_text()
                if text:
                    all_text.append(text)

        return "\n".join(all_text), page_count

    def _extract_tables_camelot(
        self,
        file_bytes: bytes,
        page_count: int,
    ) -> list[ExtractedTable]:
        """
        Extract tables using camelot with two-pass strategy.

        Pass 1 — Lattice mode:
          - Detects tables with visible borders/gridlines
          - Higher precision, works best with ruled tables
          - Most academic result sheets have visible borders

        Pass 2 — Stream mode (fallback):
          - Detects tables based on whitespace alignment
          - Works with borderless tables
          - Lower precision but broader coverage

        Each table gets a confidence score based on camelot's accuracy metric.
        """
        import camelot
        import tempfile
        import os

        tables: list[ExtractedTable] = []

        # camelot requires a file path, so write to temp file
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name

        try:
            # Pass 1: Lattice mode
            pages = f"1-{min(page_count, self.settings.document.max_pages_per_document)}"
            try:
                camelot_tables = camelot.read_pdf(
                    tmp_path,
                    pages=pages,
                    flavor="lattice",
                    line_scale=40,
                    process_background=True,
                )

                for i, ct in enumerate(camelot_tables):
                    if ct.accuracy >= self.settings.document.table_detection_confidence * 100:
                        table = self._camelot_to_extracted_table(ct, i)
                        tables.append(table)

            except Exception as e:
                logger.warning("camelot_lattice_failed", error=str(e))

            # Pass 2: Stream mode for pages without detected tables
            if not tables:
                try:
                    camelot_tables = camelot.read_pdf(
                        tmp_path,
                        pages=pages,
                        flavor="stream",
                        edge_tol=50,
                        row_tol=10,
                    )

                    for i, ct in enumerate(camelot_tables):
                        if ct.accuracy >= self.settings.document.table_detection_confidence * 100:
                            table = self._camelot_to_extracted_table(ct, len(tables) + i)
                            tables.append(table)

                except Exception as e:
                    logger.warning("camelot_stream_failed", error=str(e))

        finally:
            os.unlink(tmp_path)

        return tables

    def _extract_tables_tabula(self, file_bytes: bytes) -> list[ExtractedTable]:
        """
        Fallback table extraction using tabula-py.

        tabula-py uses Java's tabula library under the hood.
        Lower accuracy than camelot but handles some edge cases better.
        """
        try:
            import tabula
            import tempfile
            import os

            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name

            try:
                dfs = tabula.read_pdf(tmp_path, pages="all", multiple_tables=True)
                tables = []

                for i, df in enumerate(dfs):
                    if df.empty or len(df) < 2:
                        continue

                    headers = [str(col) for col in df.columns]
                    rows = [[str(val) for val in row] for row in df.values.tolist()]

                    table = ExtractedTable(
                        page_number=1,  # tabula doesn't always report page numbers
                        table_index=i,
                        headers=headers,
                        rows=rows,
                        confidence=0.6,  # Lower confidence for tabula fallback
                        num_rows=len(rows),
                        num_cols=len(headers),
                    )
                    tables.append(table)

                return tables

            finally:
                os.unlink(tmp_path)

        except Exception as e:
            logger.warning("tabula_fallback_failed", error=str(e))
            return []

    def _camelot_to_extracted_table(self, camelot_table: Any, index: int) -> ExtractedTable:
        """Convert a camelot table object to our ExtractedTable model."""
        df = camelot_table.df

        # First row is often headers
        headers = [str(val).strip() for val in df.iloc[0].values]
        rows = [
            [str(val).strip() for val in row]
            for row in df.iloc[1:].values.tolist()
        ]

        # Map camelot accuracy (0-100) to our confidence (0-1)
        confidence = camelot_table.accuracy / 100.0

        return ExtractedTable(
            page_number=camelot_table.page if hasattr(camelot_table, "page") else 1,
            table_index=index,
            headers=headers,
            rows=rows,
            confidence=confidence,
            num_rows=len(rows),
            num_cols=len(headers),
        )
