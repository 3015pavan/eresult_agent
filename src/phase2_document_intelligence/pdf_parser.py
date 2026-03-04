"""
PDF Parser — Phase 2.

Strategy:
  1. pdfplumber  (always available, good for dense text)
  2. camelot     (lattice mode for ruled tables, optional)
  3. tabula-py   (stream mode fallback, optional)
  4. table_stitcher for cross-page rows
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from .router import ParsedDocument
from .table_stitcher import stitch_tables

logger = logging.getLogger(__name__)


def parse_pdf_native(path: str) -> ParsedDocument:
    """
    Extract text and tables from a PDF with a text layer.
    Tries camelot (lattice) → tabula (stream) → pdfplumber fallback.
    """
    text_parts: list[str] = []
    all_tables: list[list[list[str]]] = []
    strategy = "pdfplumber"
    confidence = 0.75
    errors: list[str] = []

    # ── 1. pdfplumber for raw text ────────────────────────────────────────────
    try:
        import pdfplumber  # already installed
        with pdfplumber.open(path) as pdf:
            page_tables_plumber: list[list[list[str]]] = []
            for page in pdf.pages:
                pt = page.extract_text() or ""
                if pt:
                    text_parts.append(pt)
                # pdfplumber table extraction
                for tbl in page.extract_tables() or []:
                    cleaned = [[str(c or "").strip() for c in row] for row in tbl if tbl]
                    if cleaned:
                        page_tables_plumber.append(cleaned)
            if page_tables_plumber:
                all_tables.extend(page_tables_plumber)
                strategy = "pdfplumber_tables"
                confidence = 0.80
    except Exception as exc:
        errors.append(f"pdfplumber:{exc}")

    # ── 2. camelot (lattice) for bordered tables ──────────────────────────────
    try:
        import camelot  # type: ignore
        tables = camelot.read_pdf(path, pages="all", flavor="lattice")
        for tbl in tables:
            df = tbl.df
            rows = [list(df.columns)] + df.values.tolist()
            all_tables.append([[str(c).strip() for c in row] for row in rows])
        if tables:
            strategy = "camelot_lattice"
            confidence = 0.90
    except ImportError:
        pass  # camelot not installed — ok
    except Exception as exc:
        logger.debug("camelot_lattice_failed: %s", exc)
        errors.append(f"camelot:{exc}")
        # Try stream mode as fallback
        try:
            import camelot  # type: ignore
            tables = camelot.read_pdf(path, pages="all", flavor="stream")
            for tbl in tables:
                df = tbl.df
                rows = df.values.tolist()
                all_tables.append([[str(c).strip() for c in row] for row in rows])
            if tables:
                strategy = "camelot_stream"
                confidence = 0.82
        except Exception as exc2:
            errors.append(f"camelot_stream:{exc2}")

    # ── 3. tabula-py as additional fallback ───────────────────────────────────
    if not all_tables:
        try:
            import tabula  # type: ignore
            dfs = tabula.read_pdf(path, pages="all", multiple_tables=True, silent=True)
            for df in (dfs or []):
                rows = [list(df.columns)] + df.values.tolist()
                all_tables.append([[str(c).strip() for c in row] for row in rows])
            if dfs:
                strategy = "tabula"
                confidence = 0.78
        except ImportError:
            pass
        except Exception as exc:
            errors.append(f"tabula:{exc}")

    # ── 4. Stitch multi-page tables ───────────────────────────────────────────
    if len(all_tables) > 1:
        all_tables = stitch_tables(all_tables)

    return ParsedDocument(
        source_path=path,
        mime_type="application/pdf",
        text="\n\n".join(text_parts),
        tables=all_tables,
        parse_strategy=strategy,
        confidence=confidence,
        errors=errors,
    )
