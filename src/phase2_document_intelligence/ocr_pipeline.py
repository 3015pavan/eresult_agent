"""
OCR Pipeline — Phase 2.

Handles scanned PDFs and standalone images.

Strategy:
  Primary:  PaddleOCR  (high accuracy, GPU-optional)
  Fallback: Tesseract  (pytesseract — always available if installed)
  Fallback: pdfplumber forced mode (sometimes works on clean scans)

The OCR result is returned as text + a table (rows of detected lines).
"""

from __future__ import annotations

import logging
import os
import tempfile

from .router import ParsedDocument
from .table_detector import detect_table_regions

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pdf_to_images(path: str) -> list[str]:
    """
    Convert PDF pages to PNG images using pdf2image.
    Returns a list of temp file paths.
    """
    try:
        from pdf2image import convert_from_path  # type: ignore
        images = convert_from_path(path, dpi=200)
        paths: list[str] = []
        for i, img in enumerate(images):
            tmp = tempfile.NamedTemporaryFile(suffix=f"_p{i}.png", delete=False)
            img.save(tmp.name, "PNG")
            paths.append(tmp.name)
        return paths
    except ImportError:
        logger.warning("pdf2image not installed — cannot convert PDF pages to images")
        return []
    except Exception as exc:
        logger.warning("pdf_to_images failed: %s", exc)
        return []


def _ocr_with_paddle(image_path: str) -> str:
    """Run PaddleOCR on a single image. Returns extracted text."""
    try:
        from paddleocr import PaddleOCR  # type: ignore
        # Use lightweight det+rec, no angle classifier for speed
        ocr = PaddleOCR(use_angle_cls=False, lang="en", show_log=False)
        result = ocr.ocr(image_path, cls=False)
        lines = []
        for line_result in (result or [[]])[0] or []:
            if line_result and len(line_result) >= 2:
                text, confidence = line_result[1]
                lines.append(str(text))
        return "\n".join(lines)
    except ImportError:
        raise ImportError("paddleocr not installed")
    except Exception as exc:
        raise RuntimeError(f"PaddleOCR failed: {exc}") from exc


def _ocr_with_tesseract(image_path: str) -> str:
    """Run Tesseract OCR on a single image. Returns extracted text."""
    try:
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore
        img = Image.open(image_path)
        # --oem 3: LSTM engine, --psm 6: uniform block of text
        config = r"--oem 3 --psm 6 -c tessedit_char_blacklist=|"
        return pytesseract.image_to_string(img, config=config)
    except ImportError:
        raise ImportError("pytesseract / Pillow not installed")
    except Exception as exc:
        raise RuntimeError(f"Tesseract failed: {exc}") from exc


def _ocr_image(image_path: str) -> tuple[str, str]:
    """
    Try PaddleOCR then Tesseract.
    Returns (text, strategy_name).
    """
    try:
        text = _ocr_with_paddle(image_path)
        return text, "paddleocr"
    except Exception as pe:
        logger.error(f"PaddleOCR failed: {pe}. Trying Tesseract.")

    try:
        text = _ocr_with_tesseract(image_path)
        return text, "tesseract"
    except Exception as te:
        logger.error(f"Tesseract OCR failed: {te}")

    logger.error(f"OCR failed for image: {image_path}")
    return "", "ocr_failed"


def _ocr_detected_regions(image_path: str) -> tuple[str, str]:
    """
    Use YOLOv8-detected table regions when available, falling back to the whole
    page if detection is unavailable.
    """
    try:
        from PIL import Image  # type: ignore

        regions = detect_table_regions(image_path)
        if not regions:
            return _ocr_image(image_path)

        img = Image.open(image_path)
        texts: list[str] = []
        strategies: set[str] = set()
        for i, region in enumerate(regions):
            crop = img.crop((region["x1"], region["y1"], region["x2"], region["y2"]))
            with tempfile.NamedTemporaryFile(suffix=f"_table_{i}.png", delete=False) as tmp:
                crop.save(tmp.name)
                crop_path = tmp.name
            try:
                text, strat = _ocr_image(crop_path)
                if text:
                    texts.append(text)
                strategies.add(strat)
            finally:
                try:
                    os.unlink(crop_path)
                except OSError:
                    pass
        if texts:
            label = "+".join(sorted(strategies)) if strategies else "ocr"
            return "\n\n".join(texts), f"yolo_regions_{label}"
    except Exception as exc:
        logger.debug("yolo_region_ocr_failed: %s", exc)
    return _ocr_image(image_path)


# ── Public API ────────────────────────────────────────────────────────────────

def parse_pdf_scanned(path: str) -> ParsedDocument:
    """
    Extract text from a scanned (image-based) PDF.
    Converts pages to images, then runs OCR on each.
    """
    image_paths = _pdf_to_images(path)
    if not image_paths:
        return ParsedDocument(
            source_path=path,
            mime_type="application/pdf",
            parse_strategy="ocr_no_images",
            confidence=0.0,
            errors=["pdf2image_unavailable"],
        )

    all_text: list[str] = []
    strategy = "ocr_failed"

    try:
        for img_path in image_paths:
            text, strat = _ocr_detected_regions(img_path)
            strategy = strat
            if text:
                all_text.append(text)
    finally:
        # Clean up temp files
        for p in image_paths:
            try:
                os.unlink(p)
            except OSError:
                pass

    combined = "\n\n".join(all_text)
    confidence = 0.70 if combined else 0.0

    return ParsedDocument(
        source_path=path,
        mime_type="application/pdf",
        text=combined,
        tables=[],
        parse_strategy=f"scanned_{strategy}",
        confidence=confidence,
        errors=["no_text_extracted"] if not combined else [],
    )


def parse_image(path: str) -> ParsedDocument:
    """Extract text from a standalone image file (PNG, JPG, TIFF)."""
    mime = f"image/{os.path.splitext(path)[1].lstrip('.').lower()}"
    text, strategy = _ocr_detected_regions(path)
    return ParsedDocument(
        source_path=path,
        mime_type=mime,
        text=text,
        parse_strategy=f"image_{strategy}",
        confidence=0.65 if text else 0.0,
        errors=["no_text_extracted"] if not text else [],
    )
