"""
Optional LlamaParse adapter for digital PDFs.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .router import ParsedDocument

logger = logging.getLogger(__name__)


def parse_pdf_with_llamaparse(path: str) -> ParsedDocument | None:
    """
    Parse digital PDFs with LlamaParse when configured and installed.
    Returns None when unavailable so callers can gracefully fall back.
    """
    try:
        from src.common.config import get_settings
        cfg = get_settings().document_ai
        if not cfg.llamaparse_enabled or not cfg.llamaparse_api_key:
            logger.info("LlamaParse disabled or API key missing")
            return None
        from llama_parse import LlamaParse  # type: ignore
        parser = LlamaParse(
            api_key=cfg.llamaparse_api_key,
            result_type=cfg.llamaparse_result_type,
        )
        docs = parser.load_data(str(Path(path)))
        text = "\n\n".join(getattr(doc, "text", "") for doc in docs if getattr(doc, "text", ""))
        if not text.strip():
            logger.warning(f"LlamaParse returned empty text for {path}")
            return None
        logger.info(f"LlamaParse parsed PDF: {path}")
        return ParsedDocument(
            source_path=path,
            mime_type="application/pdf",
            text=text,
            tables=[],
            parse_strategy="llamaparse",
            confidence=0.93,
            metadata={"provider": "llamaparse"},
            errors=[],
        )
    except ImportError:
        logger.error("LlamaParse not installed. Please install llama_parse.")
        return None
    except Exception as exc:
        logger.error(f"LlamaParse failed for {path}: {exc}")
        return None
