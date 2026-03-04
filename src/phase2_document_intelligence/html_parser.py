"""
HTML Parser — Phase 2.

Extracts clean text and structured tables from HTML content.
Used for:  HTML email bodies, .html/.htm attachments.

Strategy:
  1. BeautifulSoup (lxml) — table extraction + text cleaning
  2. regex fallback    — strip tags when bs4 unavailable
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from .router import ParsedDocument

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bs4_available() -> bool:
    try:
        import bs4  # noqa: F401
        return True
    except ImportError:
        return False


def _strip_html_regex(html: str) -> str:
    """Naive regex HTML stripper when bs4 is unavailable."""
    html = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</(?:p|div|tr|li|h[1-6]|ul|ol)>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"<[^>]+>", " ", html)
    html = re.sub(r"&nbsp;", " ", html)
    html = re.sub(r"&amp;", "&", html)
    html = re.sub(r"&lt;", "<", html)
    html = re.sub(r"&gt;", ">", html)
    html = re.sub(r"[ \t]+", " ", html)
    return re.sub(r"\n{3,}", "\n\n", html).strip()


def _parse_tables_bs4(soup) -> list[list[list[str]]]:
    """Extract all <table> elements as list[rows[cells]]."""
    tables: list[list[list[str]]] = []
    for tbl in soup.find_all("table"):
        rows: list[list[str]] = []
        for tr in tbl.find_all("tr"):
            cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all(["td", "th"])]
            if any(cells):
                rows.append(cells)
        if rows:
            tables.append(rows)
    return tables


def _extract_text_bs4(soup) -> str:
    """Get readable text from BeautifulSoup tree."""
    # Remove scripts and styles
    for tag in soup(["script", "style", "head", "meta", "link"]):
        tag.decompose()

    # Replace table cells with pipe-separated CSV-like lines for text extractor
    for tbl in soup.find_all("table"):
        rows = []
        for tr in tbl.find_all("tr"):
            cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all(["td", "th"])]
            if cells:
                rows.append(" | ".join(cells))
        if rows:
            tbl.replace_with(soup.new_string("\n".join(rows) + "\n"))

    text = soup.get_text(separator="\n")
    # Collapse excessive whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ── Public API ────────────────────────────────────────────────────────────────

def parse_html(
    html_content: str,
    source_path: str = "",
) -> ParsedDocument:
    """
    Parse HTML content and return a ParsedDocument.

    Args:
        html_content: Raw HTML string.
        source_path:  Original file path (for metadata).

    Returns:
        ParsedDocument with text (tables converted to pipe rows) and
        tables (raw cell data per table).
    """
    if not html_content or not html_content.strip():
        return ParsedDocument(
            source_path=source_path,
            mime_type="text/html",
            text="",
            parse_strategy="html_empty",
            confidence=0.0,
        )

    if _bs4_available():
        try:
            from bs4 import BeautifulSoup
            try:
                soup = BeautifulSoup(html_content, "lxml")
            except Exception:
                soup = BeautifulSoup(html_content, "html.parser")

            text   = _extract_text_bs4(soup)
            tables = _parse_tables_bs4(BeautifulSoup(html_content, "html.parser"))
            # Re-parse without modifications for table extraction
            return ParsedDocument(
                source_path=source_path,
                mime_type="text/html",
                text=text,
                tables=tables,
                parse_strategy="beautifulsoup",
                confidence=0.85 if text else 0.3,
            )
        except Exception as exc:
            logger.warning("html_parser bs4 failed: %s", exc)

    # Fallback: regex strip
    text = _strip_html_regex(html_content)
    return ParsedDocument(
        source_path=source_path,
        mime_type="text/html",
        text=text,
        tables=[],
        parse_strategy="html_regex_strip",
        confidence=0.60 if text else 0.0,
    )
