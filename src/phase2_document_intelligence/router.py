"""
Document Router — Phase 2 entry point.

Inspects a document (file path + mime-type) and dispatches to the
correct parser, returning a normalised ParsedDocument.

Routing strategy:
  1. Native PDF (text layer present) → pdf_parser
  2. Scanned PDF / image → ocr_pipeline
  3. Excel / CSV                     → excel_parser
  4. Email body text                 → inline (passed through as-is)
"""

from __future__ import annotations

import logging
import mimetypes
import os
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ParsedDocument:
    """Normalised output from any parser in Phase 2."""
    source_path: str = ""
    mime_type: str = ""
    text: str = ""                       # raw extracted text
    tables: list[list[list[str]]] = field(default_factory=list)  # [ [[cell,...],...]  ]
    metadata: dict = field(default_factory=dict)
    parse_strategy: str = "unknown"
    confidence: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def has_tables(self) -> bool:
        return bool(self.tables)

    def flat_text(self) -> str:
        """Return text + flattened tables as a single string for extraction."""
        parts = [self.text] if self.text else []
        for tbl in self.tables:
            for row in tbl:
                parts.append(", ".join(str(c) for c in row))
        return "\n".join(parts)


def _sniff_mime(path: str) -> str:
    """Detect mime-type from file extension."""
    mime, _ = mimetypes.guess_type(path)
    ext = os.path.splitext(path)[1].lower()
    if ext in (".pdf",):
        return "application/pdf"
    if ext in (".xlsx", ".xls"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if ext in (".csv",):
        return "text/csv"
    if ext in (".png", ".jpg", ".jpeg", ".tiff", ".bmp"):
        return f"image/{ext.lstrip('.')}"
    return mime or "application/octet-stream"


def _pdf_has_text(path: str) -> bool:
    """Return True if the PDF has an extractable text layer."""
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages[:3]:  # check first 3 pages
                if page.extract_text():
                    return True
    except Exception:
        pass
    return False


def route_to_parser(
    path: str,
    mime_type: Optional[str] = None,
    email_body: Optional[str] = None,
) -> ParsedDocument:
    """
    Central dispatcher — choose and run the right parser.

    Args:
        path:       Filesystem path to the document (empty string for body-only).
        mime_type:  Optional MIME type hint. Auto-detected if None.
        email_body: Email body text (used when path is empty or as supplement).

    Returns:
        ParsedDocument with extracted text and tables.
    """
    # ── Body-only (no attachment) ─────────────────────────────────────────────
    if not path or not os.path.exists(path):
        if email_body:
            return ParsedDocument(
                text=email_body,
                parse_strategy="email_body",
                confidence=0.9,
            )
        return ParsedDocument(errors=["no_document_and_no_body"])

    mime = mime_type or _sniff_mime(path)
    logger.info("document_router", path=os.path.basename(path), mime=mime)

    # ── PDF ───────────────────────────────────────────────────────────────────
    if "pdf" in mime:
        if _pdf_has_text(path):
            from .pdf_parser import parse_pdf_native
            return parse_pdf_native(path)
        else:
            from .ocr_pipeline import parse_pdf_scanned
            return parse_pdf_scanned(path)

    # ── Excel / CSV ───────────────────────────────────────────────────────────
    if "spreadsheet" in mime or "excel" in mime or path.endswith((".xlsx", ".xls", ".csv")):
        from .excel_parser import parse_spreadsheet
        return parse_spreadsheet(path, mime)

    # ── HTML ──────────────────────────────────────────────────────────────────
    if "html" in mime or path.endswith((".html", ".htm")):
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                html = f.read()
            from .html_parser import parse_html
            return parse_html(html, source_path=path)
        except Exception as exc:
            return ParsedDocument(errors=[f"html_parse:{exc}"])

    # ── DOCX / ODT / RTF ──────────────────────────────────────────────────────
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    if ext in ("docx", "doc", "odt", "ods", "odf", "rtf", "md") or \
       "wordprocessingml" in mime or "opendocument" in mime or "msword" in mime:
        from .docx_odf_parser import parse_document_file
        return parse_document_file(path, mime)

    # ── Images (standalone scanned page) ─────────────────────────────────────
    if mime.startswith("image/"):
        from .universal_converter import ocr_image_path
        return ocr_image_path(path)

    # ── Plain text / other ────────────────────────────────────────────────────
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
        return ParsedDocument(text=text, parse_strategy="plain_text", confidence=0.85)
    except Exception as exc:
        return ParsedDocument(errors=[f"unhandled_mime:{mime}:{exc}"])
