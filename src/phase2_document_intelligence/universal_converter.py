"""
Universal Document Converter — Phase 2.

Master entry point that accepts ANY format (path, bytes, or attachment dict)
and returns a ParsedDocument ready for extraction.

Supported formats:
  Text     : plain text, markdown, log files
  HTML     : email bodies, .html files → BeautifulSoup
  PDF      : native text layer → pdfplumber; scanned → OCR
  Images   : JPG/PNG/TIFF/BMP → Vision LLM (Groq) + PIL pre-processing
  Excel    : .xlsx / .xls / .csv → openpyxl / pandas
  DOCX     : .docx → python-docx
  ODT/ODF  : .odt / .ods → odfpy
  RTF      : .rtf → striprtf / regex
  Gmail    : attachment dict with attachmentId → fetched via Gmail API

Vision LLM strategy (images):
  1. Groq llama-3.2-11b-vision-preview (fast, free tier)
  2. PIL→text heuristics (fallback when no API key)
  3. pytesseract (if installed)
"""

from __future__ import annotations

import base64
import logging
import os
import tempfile
from typing import Optional, Union

from .router import ParsedDocument

logger = logging.getLogger(__name__)

_VISION_PROMPT = """You are an academic result OCR engine.
Extract ALL text visible in this image exactly as it appears.
If it is a mark sheet / result card, preserve:
- Student name, USN/Registration number
- Semester number, academic year
- Subject codes, subject names, marks, grades, PASS/FAIL status
- SGPA, CGPA values

Return only the extracted text, no commentary."""


def _groq_key() -> str:
    """Read GROQ key lazily (allows dotenv to load first)."""
    return os.getenv("GROQ_API_KEY", "")


# ── Image OCR ─────────────────────────────────────────────────────────────────

def _ocr_with_vision_llm(image_bytes: bytes, mime_type: str = "image/jpeg") -> str:
    """
    Use Groq Vision LLM to OCR an image.
    Returns extracted text string.
    """
    import httpx
    b64 = base64.b64encode(image_bytes).decode()
    # Use llama-4-scout for vision (llama-3.2-vision was decommissioned)
    vision_model = os.getenv("GROQ_VISION_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
    payload = {
        "model": vision_model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:{mime_type};base64,{b64}",
                        },
                    },
                    {"type": "text", "text": _VISION_PROMPT},
                ],
            }
        ],
        "temperature": 0.0,
        "max_tokens": 2048,
    }
    resp = httpx.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {_groq_key()}", "Content-Type": "application/json"},
        json=payload,
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _ocr_with_pil_preprocess_tesseract(image_bytes: bytes) -> str:
    """Enhance image with PIL then run Tesseract."""
    from PIL import Image, ImageFilter, ImageEnhance  # type: ignore
    import io
    img = Image.open(io.BytesIO(image_bytes))
    # Convert to greyscale + sharpen for better OCR
    img = img.convert("L")
    img = img.filter(ImageFilter.SHARPEN)
    img = ImageEnhance.Contrast(img).enhance(2.0)

    try:
        import pytesseract  # type: ignore
        return pytesseract.image_to_string(img, config="--oem 3 --psm 6")
    except ImportError:
        pass

    # Last resort: save to temp, use ocr_pipeline
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        img.save(tmp.name)
        tmp_path = tmp.name
    try:
        from .ocr_pipeline import parse_image
        doc = parse_image(tmp_path)
        return doc.text
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def ocr_image_bytes(image_bytes: bytes, mime_type: str = "image/jpeg") -> tuple[str, str]:
    """
    OCR image bytes to text. Returns (text, strategy).
    Tries Vision LLM → PIL+Tesseract.
    """
    if _groq_key():
        try:
            text = _ocr_with_vision_llm(image_bytes, mime_type)
            if text and len(text) > 20:
                return text, "groq_vision"
        except Exception as exc:
            logger.warning("vision_llm_ocr failed: %s", exc)

    try:
        text = _ocr_with_pil_preprocess_tesseract(image_bytes)
        return text, "pil_tesseract"
    except Exception as exc:
        logger.warning("pil_tesseract_ocr failed: %s", exc)

    return "", "ocr_failed"


def ocr_image_path(path: str) -> ParsedDocument:
    """OCR a local image file. Returns ParsedDocument."""
    mime = f"image/{os.path.splitext(path)[1].lstrip('.').lower()}"
    try:
        with open(path, "rb") as f:
            image_bytes = f.read()
        text, strategy = ocr_image_bytes(image_bytes, mime)
    except Exception as exc:
        return ParsedDocument(
            source_path=path,
            mime_type=mime,
            parse_strategy="ocr_failed",
            confidence=0.0,
            errors=[str(exc)],
        )
    return ParsedDocument(
        source_path=path,
        mime_type=mime,
        text=text,
        parse_strategy=f"image_{strategy}",
        confidence=0.80 if strategy == "groq_vision" and text else 0.60 if text else 0.0,
        errors=["no_text_extracted"] if not text else [],
    )


# ── Gmail attachment fetcher ──────────────────────────────────────────────────

def fetch_gmail_attachment(
    message_id: str,
    attachment_id: str,
    filename: str = "",
    mime_type: str = "",
) -> Optional[bytes]:
    """
    Fetch attachment bytes from Gmail API.
    Returns None if unavailable (no credentials, not found).
    """
    try:
        from src.common.config import load_config
        from googleapiclient.discovery import build  # type: ignore
        from google.oauth2.credentials import Credentials  # type: ignore
        import json
        from pathlib import Path

        token_path = Path(__file__).resolve().parents[3] / "config" / "secrets" / "token.json"
        creds_path = Path(__file__).resolve().parents[3] / "config" / "secrets" / "credentials.json"

        if not token_path.exists():
            logger.warning("fetch_gmail_attachment: token.json not found")
            return None

        creds = Credentials.from_authorized_user_file(str(token_path))
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        att = service.users().messages().attachments().get(
            userId="me", messageId=message_id, id=attachment_id
        ).execute()
        data = att.get("data", "")
        # Gmail API uses URL-safe base64
        return base64.urlsafe_b64decode(data + "==")
    except Exception as exc:
        logger.warning("fetch_gmail_attachment failed for %s: %s", attachment_id[:20], exc)
        return None


# ── Bytes → ParsedDocument ────────────────────────────────────────────────────

def convert_bytes(
    data: bytes,
    mime_type: str,
    filename: str = "",
    source_hint: str = "",
) -> ParsedDocument:
    """
    Convert raw bytes of any supported format to ParsedDocument.

    Args:
        data:        Raw file bytes.
        mime_type:   MIME type string.
        filename:    Original filename (for extension detection).
        source_hint: Descriptive hint for logging.
    """
    if not data:
        return ParsedDocument(
            source_path=source_hint,
            mime_type=mime_type,
            parse_strategy="empty",
            confidence=0.0,
            errors=["no_data"],
        )

    ext = os.path.splitext(filename)[1].lower().lstrip(".") if filename else ""

    # ── Images: use Vision LLM OCR ────────────────────────────────────────────
    if mime_type.startswith("image/") or ext in ("jpg", "jpeg", "png", "tiff", "bmp", "webp", "gif"):
        text, strategy = ocr_image_bytes(data, mime_type or f"image/{ext or 'jpeg'}")
        return ParsedDocument(
            source_path=source_hint or filename,
            mime_type=mime_type,
            text=text,
            parse_strategy=f"image_{strategy}",
            confidence=0.80 if strategy == "groq_vision" and text else 0.60 if text else 0.0,
            errors=["no_text_extracted"] if not text else [],
        )

    # ── Write to temp file and use path-based router ──────────────────────────
    suffix = f".{ext}" if ext else _mime_to_ext(mime_type)
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name

    try:
        return convert_path(tmp_path, mime_type)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _mime_to_ext(mime_type: str) -> str:
    """Map MIME type to file extension."""
    _map = {
        "application/pdf": ".pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/msword": ".doc",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/vnd.ms-excel": ".xls",
        "text/csv": ".csv",
        "text/html": ".html",
        "text/plain": ".txt",
        "application/vnd.oasis.opendocument.text": ".odt",
        "application/vnd.oasis.opendocument.spreadsheet": ".ods",
        "application/rtf": ".rtf",
        "text/rtf": ".rtf",
    }
    return _map.get(mime_type, ".bin")


# ── Path → ParsedDocument ─────────────────────────────────────────────────────

def convert_path(path: str, mime_type: Optional[str] = None) -> ParsedDocument:
    """
    Convert a local file (any format) to ParsedDocument.
    Uses the Phase 2 router as the primary dispatcher, with new parsers for
    DOCX/ODT/HTML/image types.
    """
    from .router import route_to_parser, _sniff_mime

    detected_mime = mime_type or _sniff_mime(path)
    ext = os.path.splitext(path)[1].lower().lstrip(".")

    # ── HTML ──────────────────────────────────────────────────────────────────
    if detected_mime == "text/html" or ext in ("html", "htm"):
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                html = f.read()
            from .html_parser import parse_html
            return parse_html(html, source_path=path)
        except Exception as exc:
            logger.warning("html_parser path failed: %s", exc)

    # ── DOCX / ODT / RTF ──────────────────────────────────────────────────────
    if ext in ("docx", "doc", "odt", "ods", "odf", "rtf", "txt", "md", "text", "log") or \
       "wordprocessingml" in detected_mime or "opendocument" in detected_mime or "msword" in detected_mime:
        from .docx_odf_parser import parse_document_file
        return parse_document_file(path, detected_mime)

    # ── Images → OCR ──────────────────────────────────────────────────────────
    if detected_mime.startswith("image/") or ext in ("jpg", "jpeg", "png", "tiff", "bmp", "webp"):
        return ocr_image_path(path)

    # ── Delegate to existing router (PDF, Excel/CSV) ──────────────────────────
    return route_to_parser(path, detected_mime)


# ── Gmail attachment → ParsedDocument ────────────────────────────────────────

def convert_gmail_attachment(
    attachment_meta: dict,
    message_id: str,
) -> Optional[ParsedDocument]:
    """
    Fetch a Gmail attachment and convert it to ParsedDocument.

    Args:
        attachment_meta: Dict from emails_cache with keys:
                         filename, mimeType, size, attachmentId
        message_id:      The Gmail message ID (required for API fetch).

    Returns:
        ParsedDocument or None if fetch failed.
    """
    att_id  = attachment_meta.get("attachmentId", "")
    fname   = attachment_meta.get("filename", "")
    mime    = attachment_meta.get("mimeType", "")
    size    = attachment_meta.get("size", 0)

    if not att_id:
        logger.debug("convert_gmail_attachment: no attachmentId for %s", fname)
        return None

    if size > 20 * 1024 * 1024:  # Skip files >20MB
        logger.warning("convert_gmail_attachment: file too large (%dMB) %s", size // 1_048_576, fname)
        return None

    logger.info("Fetching Gmail attachment: %s (%s, %d bytes)", fname, mime, size)
    data = fetch_gmail_attachment(message_id, att_id, fname, mime)
    if data is None:
        return None

    return convert_bytes(data, mime, filename=fname, source_hint=f"gmail://{message_id}/{fname}")


# ── HTML body → ParsedDocument ────────────────────────────────────────────────

def convert_html_body(html: str, subject: str = "") -> ParsedDocument:
    """
    Convert an HTML email body string to ParsedDocument.
    Useful when Gmail returns HTML instead of plain text.
    """
    from .html_parser import parse_html
    doc = parse_html(html, source_path=f"email_body:{subject[:40]}")
    return doc


# ── Universal entry point ────────────────────────────────────────────────────

def convert_any(
    source: Union[str, bytes, dict],
    mime_type: str = "",
    filename: str = "",
    message_id: str = "",
) -> Optional[ParsedDocument]:
    """
    Convert ANYTHING to a ParsedDocument.

    Args:
        source:     str  → file path OR raw HTML/text content
                    bytes → raw file bytes
                    dict  → Gmail attachment metadata dict
        mime_type:  MIME type hint (optional for path/bytes)
        filename:   Original filename hint
        message_id: Gmail message ID (required when source is attachment dict)

    Returns:
        ParsedDocument or None on unrecoverable error.
    """
    try:
        if isinstance(source, bytes):
            return convert_bytes(source, mime_type, filename, source_hint=filename)

        if isinstance(source, dict):
            # Gmail attachment metadata dict
            return convert_gmail_attachment(source, message_id)

        if isinstance(source, str):
            # Is it an HTML string?
            if mime_type == "text/html" or (source.lstrip().startswith("<") and "</" in source):
                from .html_parser import parse_html
                return parse_html(source, source_path=filename or "html_body")

            # Is it a file path?
            if os.path.exists(source):
                return convert_path(source, mime_type or None)

            # Treat as raw plain text
            return ParsedDocument(
                source_path=filename or "inline_text",
                mime_type=mime_type or "text/plain",
                text=source,
                parse_strategy="inline_text",
                confidence=0.70,
            )

    except Exception as exc:
        logger.error("convert_any failed: %s", exc, exc_info=True)
        return None

    return None
