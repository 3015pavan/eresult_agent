"""
Document Router.

Central dispatcher that routes documents to the appropriate parsing pipeline
based on file type detection and content analysis.

Routing logic:
  1. Detect file type (magic bytes + extension)
  2. For PDFs: determine native vs scanned
  3. Route to parser: NativePDFParser, OCRPipeline, ExcelParser
  4. Handle fallbacks when primary parser fails
"""

from __future__ import annotations

import io
import time
from typing import Any
from uuid import UUID

from src.common.config import get_settings
from src.common.models import (
    AttachmentInfo,
    DocumentType,
    DocumentParseResult,
    ExtractedTable,
)
from src.common.observability import (
    get_logger,
    DOCUMENTS_PARSED,
    DOCUMENT_PARSE_LATENCY,
)

logger = get_logger(__name__)


class DocumentRouter:
    """
    Routes documents to appropriate parsing pipelines.

    Decision tree:
      file_type_detect(bytes) →
        PDF →
          is_native_pdf(text_density) →
            YES → NativePDFParser (pdfplumber + camelot)
            NO  → OCRPipeline (PaddleOCR/Tesseract + LayoutLMv3)
        XLSX/XLS →
          ExcelParser (openpyxl + pandas)
        CSV →
          CSVParser (pandas)
        UNKNOWN →
          Quarantine

    Fallback chain:
      NativePDFParser fails → OCRPipeline
      PaddleOCR fails → Tesseract
      Tesseract fails → Donut (end-to-end VLM)
      All fail → Quarantine for human review
    """

    # Minimum text density (chars per page) to classify PDF as native
    MIN_TEXT_DENSITY = 100

    def __init__(self) -> None:
        self.settings = get_settings()
        # Lazy imports to avoid loading heavy ML models at startup
        self._native_parser = None
        self._ocr_pipeline = None
        self._excel_parser = None

    @property
    def native_parser(self):
        if self._native_parser is None:
            from .pdf_parser import NativePDFParser
            self._native_parser = NativePDFParser()
        return self._native_parser

    @property
    def ocr_pipeline(self):
        if self._ocr_pipeline is None:
            from .ocr_pipeline import OCRPipeline
            self._ocr_pipeline = OCRPipeline()
        return self._ocr_pipeline

    @property
    def excel_parser(self):
        if self._excel_parser is None:
            from .excel_parser import ExcelParser
            self._excel_parser = ExcelParser()
        return self._excel_parser

    async def parse_document(
        self,
        attachment: AttachmentInfo,
        file_bytes: bytes,
    ) -> DocumentParseResult:
        """
        Route and parse a document through the appropriate pipeline.

        Returns DocumentParseResult with extracted tables, text, and metadata.
        """
        start = time.perf_counter()

        try:
            doc_type = attachment.document_type
            if doc_type == DocumentType.UNKNOWN:
                doc_type = self._detect_type(file_bytes, attachment.filename)

            if doc_type in (DocumentType.PDF_NATIVE, DocumentType.PDF_SCANNED):
                result = await self._parse_pdf(attachment, file_bytes, doc_type)
            elif doc_type == DocumentType.EXCEL:
                result = await self._parse_excel(attachment, file_bytes)
            elif doc_type == DocumentType.CSV:
                result = await self._parse_csv(attachment, file_bytes)
            else:
                result = DocumentParseResult(
                    attachment_id=attachment.id,
                    document_type=doc_type,
                    errors=[f"Unsupported document type: {doc_type.value}"],
                )

            elapsed_ms = int((time.perf_counter() - start) * 1000)
            result.parse_time_ms = elapsed_ms

            DOCUMENTS_PARSED.labels(
                document_type=doc_type.value,
                parse_method=result.parse_method,
            ).inc()
            DOCUMENT_PARSE_LATENCY.labels(document_type=doc_type.value).observe(
                elapsed_ms / 1000
            )

            logger.info(
                "document_parsed",
                attachment_id=str(attachment.id),
                doc_type=doc_type.value,
                tables=len(result.tables),
                pages=result.page_count,
                parse_method=result.parse_method,
                elapsed_ms=elapsed_ms,
                errors=result.errors,
            )

            return result

        except Exception as e:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            logger.error(
                "document_parse_failed",
                attachment_id=str(attachment.id),
                filename=attachment.filename,
                error=str(e),
                elapsed_ms=elapsed_ms,
            )
            return DocumentParseResult(
                attachment_id=attachment.id,
                document_type=attachment.document_type,
                parse_time_ms=elapsed_ms,
                errors=[str(e)],
            )

    async def _parse_pdf(
        self,
        attachment: AttachmentInfo,
        file_bytes: bytes,
        doc_type: DocumentType,
    ) -> DocumentParseResult:
        """
        Parse a PDF with native→OCR fallback chain.

        Strategy:
          1. Try native text extraction (pdfplumber)
          2. Check text density: chars_per_page >= 100 → native PDF
          3. If native: extract tables with camelot (lattice→stream)
          4. If scanned: route to OCR pipeline
          5. If native extraction produces low-confidence tables: try OCR as fallback
        """
        # First, attempt native extraction to determine PDF type
        native_result = await self.native_parser.parse(attachment, file_bytes)

        if native_result.page_count > 0:
            avg_density = len(native_result.raw_text) / max(native_result.page_count, 1)
        else:
            avg_density = 0

        if avg_density >= self.MIN_TEXT_DENSITY:
            # Native PDF with text layer
            if native_result.tables and all(
                t.confidence >= self.settings.document.table_detection_confidence
                for t in native_result.tables
            ):
                return native_result

            # Tables not confident enough, try OCR as supplement
            logger.info(
                "pdf_native_tables_low_confidence",
                attachment_id=str(attachment.id),
                avg_confidence=sum(t.confidence for t in native_result.tables) / max(len(native_result.tables), 1),
            )

        # Scanned PDF or native with poor tables → OCR pipeline
        ocr_result = await self.ocr_pipeline.parse(attachment, file_bytes)

        # If OCR produced better results, use it; otherwise merge
        if ocr_result.tables and (
            not native_result.tables
            or ocr_result.tables[0].confidence > native_result.tables[0].confidence
            if native_result.tables else True
        ):
            return ocr_result
        elif native_result.tables:
            return native_result
        else:
            # Both failed
            return DocumentParseResult(
                attachment_id=attachment.id,
                document_type=DocumentType.PDF_SCANNED,
                raw_text=native_result.raw_text or ocr_result.raw_text,
                page_count=native_result.page_count or ocr_result.page_count,
                errors=native_result.errors + ocr_result.errors + ["No tables extracted from PDF"],
            )

    async def _parse_excel(
        self,
        attachment: AttachmentInfo,
        file_bytes: bytes,
    ) -> DocumentParseResult:
        """Parse an Excel file."""
        return await self.excel_parser.parse(attachment, file_bytes)

    async def _parse_csv(
        self,
        attachment: AttachmentInfo,
        file_bytes: bytes,
    ) -> DocumentParseResult:
        """Parse a CSV file using pandas."""
        import pandas as pd

        try:
            df = pd.read_csv(io.BytesIO(file_bytes))

            headers = [str(col) for col in df.columns]
            rows = [[str(val) for val in row] for row in df.values.tolist()]

            table = ExtractedTable(
                page_number=1,
                headers=headers,
                rows=rows,
                confidence=0.95,  # CSV parsing is high confidence
                num_rows=len(rows),
                num_cols=len(headers),
            )

            return DocumentParseResult(
                attachment_id=attachment.id,
                document_type=DocumentType.CSV,
                tables=[table],
                page_count=1,
                parse_method="pandas_csv",
            )
        except Exception as e:
            return DocumentParseResult(
                attachment_id=attachment.id,
                document_type=DocumentType.CSV,
                errors=[f"CSV parse error: {e}"],
            )

    def _detect_type(self, file_bytes: bytes, filename: str) -> DocumentType:
        """Detect document type from magic bytes and filename."""
        if file_bytes[:5] == b"%PDF-":
            return DocumentType.PDF_NATIVE

        if file_bytes[:4] == b"PK\x03\x04":
            ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
            if ext in ("xlsx", "xlsm"):
                return DocumentType.EXCEL

        if file_bytes[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
            return DocumentType.EXCEL

        ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
        if ext == "csv":
            return DocumentType.CSV

        return DocumentType.UNKNOWN
