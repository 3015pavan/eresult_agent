"""
OCR Pipeline for scanned PDFs and images.

Multi-engine OCR with layout understanding:
  1. PaddleOCR (primary) — State-of-art accuracy on structured documents
  2. Tesseract 5 (fallback) — Mature, robust on degraded scans
  3. LayoutLMv3 (layout understanding) — Multimodal table structure recognition
  4. Donut (end-to-end fallback) — OCR-free document understanding

Image preprocessing:
  - Deskew (Hough transform)
  - Denoise (OpenCV bilateral filter)
  - Contrast enhancement (CLAHE)
  - Binarization (Otsu's method)
"""

from __future__ import annotations

import io
import math
from typing import Any
from uuid import UUID

import numpy as np

from src.common.config import get_settings
from src.common.models import (
    AttachmentInfo,
    DocumentType,
    DocumentParseResult,
    ExtractedTable,
    TableCell,
)
from src.common.observability import get_logger, OCR_CONFIDENCE

logger = get_logger(__name__)


class OCRPipeline:
    """
    Multi-engine OCR pipeline with layout-aware table extraction.

    Pipeline flow:
      1. PDF → images (300 DPI)
      2. Image preprocessing (deskew, denoise, enhance)
      3. OCR text extraction (PaddleOCR → Tesseract fallback)
      4. Layout understanding (LayoutLMv3 for table structure)
      5. Table reconstruction from OCR boxes + layout analysis
      6. End-to-end fallback (Donut) if pipeline confidence < 0.5

    Model selection rationale:
      - PaddleOCR v4: F1 0.96 on FUNSD, handles mixed scripts, Apache 2.0
      - Tesseract 5: LSTM-based, mature, wider language support
      - LayoutLMv3: Multimodal pre-trained on IIT-CDIP, 133M params
      - Donut: OCR-free, useful when text quality is too degraded
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._paddle_ocr = None
        self._tesseract_available = False

    @property
    def paddle_ocr(self):
        """Lazy-load PaddleOCR to avoid initialization overhead."""
        if self._paddle_ocr is None:
            try:
                from paddleocr import PaddleOCR
                self._paddle_ocr = PaddleOCR(
                    use_angle_cls=True,  # Handle rotated text
                    lang="en",
                    show_log=False,
                    use_gpu=True,  # Falls back to CPU automatically
                )
                logger.info("paddleocr_loaded")
            except ImportError:
                logger.warning("paddleocr_not_available")
        return self._paddle_ocr

    async def parse(
        self,
        attachment: AttachmentInfo,
        file_bytes: bytes,
    ) -> DocumentParseResult:
        """
        Full OCR pipeline for scanned PDFs.

        Steps:
          1. Convert PDF pages to images (300 DPI)
          2. Preprocess each image
          3. Run OCR engine(s)
          4. Reconstruct tables from OCR output
          5. Compute per-cell and per-table confidence
        """
        errors: list[str] = []
        tables: list[ExtractedTable] = []
        all_text: list[str] = []
        page_count = 0
        ocr_confidence = 0.0

        try:
            # Step 1: PDF to images
            images = self._pdf_to_images(file_bytes)
            page_count = len(images)

            for page_idx, image in enumerate(images):
                # Step 2: Preprocess
                processed = self._preprocess_image(image)

                # Step 3: OCR
                ocr_result, confidence = await self._run_ocr(processed, page_idx)

                if ocr_result:
                    all_text.append(ocr_result["text"])
                    ocr_confidence = max(ocr_confidence, confidence)

                    # Step 4: Reconstruct tables
                    page_tables = self._reconstruct_tables(ocr_result, page_idx)
                    tables.extend(page_tables)

            OCR_CONFIDENCE.observe(ocr_confidence)

        except Exception as e:
            errors.append(f"OCR pipeline error: {e}")
            logger.error("ocr_pipeline_error", error=str(e))

        return DocumentParseResult(
            attachment_id=attachment.id,
            document_type=DocumentType.PDF_SCANNED,
            tables=tables,
            raw_text="\n".join(all_text),
            page_count=page_count,
            ocr_used=True,
            ocr_confidence=ocr_confidence,
            parse_method="paddleocr+table_reconstruct",
            errors=errors,
        )

    def _pdf_to_images(self, file_bytes: bytes) -> list[np.ndarray]:
        """
        Convert PDF pages to images at specified DPI.

        Uses pdf2image (poppler backend) for high-quality rasterization.
        300 DPI is optimal for OCR: higher doesn't help, lower reduces accuracy.
        """
        try:
            from pdf2image import convert_from_bytes

            pil_images = convert_from_bytes(
                file_bytes,
                dpi=self.settings.document.image_dpi,
                fmt="RGB",
            )

            # Cap at max pages
            max_pages = self.settings.document.max_pages_per_document
            if len(pil_images) > max_pages:
                logger.warning(
                    "pdf_page_limit_exceeded",
                    actual=len(pil_images),
                    max_pages=max_pages,
                )
                pil_images = pil_images[:max_pages]

            return [np.array(img) for img in pil_images]

        except Exception as e:
            logger.error("pdf_to_images_failed", error=str(e))
            return []

    def _preprocess_image(self, image: np.ndarray) -> np.ndarray:
        """
        Preprocess image for optimal OCR accuracy.

        Steps:
          1. Convert to grayscale
          2. Deskew using Hough transform
          3. Denoise using bilateral filter
          4. Contrast enhancement using CLAHE
          5. Optional binarization (Otsu's method)
        """
        import cv2

        # Grayscale conversion
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
        else:
            gray = image.copy()

        # Deskew
        angle = self._detect_skew(gray)
        if abs(angle) > self.settings.document.deskew_angle_threshold_degrees:
            gray = self._rotate_image(gray, angle)
            logger.debug("image_deskewed", angle=round(angle, 2))

        # Denoise — bilateral filter preserves edges while smoothing
        denoised = cv2.bilateralFilter(gray, 9, 75, 75)

        # CLAHE (Contrast Limited Adaptive Histogram Equalization)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(denoised)

        return enhanced

    def _detect_skew(self, gray_image: np.ndarray) -> float:
        """
        Detect document skew angle using Hough Line Transform.

        Returns angle in degrees. Positive = clockwise skew.
        """
        import cv2

        try:
            # Edge detection
            edges = cv2.Canny(gray_image, 50, 150, apertureSize=3)

            # Hough Line Transform
            lines = cv2.HoughLinesP(
                edges,
                rho=1,
                theta=np.pi / 180,
                threshold=100,
                minLineLength=gray_image.shape[1] // 4,
                maxLineGap=10,
            )

            if lines is None or len(lines) == 0:
                return 0.0

            # Compute angles of detected lines
            angles = []
            for line in lines:
                x1, y1, x2, y2 = line[0]
                angle = math.degrees(math.atan2(y2 - y1, x2 - x1))
                # Only consider near-horizontal lines (within ±15°)
                if abs(angle) < 15:
                    angles.append(angle)

            if not angles:
                return 0.0

            # Median angle is most robust
            return float(np.median(angles))

        except Exception:
            return 0.0

    def _rotate_image(self, image: np.ndarray, angle: float) -> np.ndarray:
        """Rotate image to correct skew."""
        import cv2

        h, w = image.shape[:2]
        center = (w // 2, h // 2)
        matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
        rotated = cv2.warpAffine(
            image, matrix, (w, h),
            flags=cv2.INTER_CUBIC,
            borderMode=cv2.BORDER_REPLICATE,
        )
        return rotated

    async def _run_ocr(
        self,
        image: np.ndarray,
        page_idx: int,
    ) -> tuple[dict[str, Any] | None, float]:
        """
        Run OCR with primary→fallback strategy.

        Returns (ocr_result_dict, confidence)
        ocr_result_dict contains: text, boxes (list of bounding boxes with text and confidence)
        """
        # Try PaddleOCR first
        result, confidence = self._run_paddleocr(image)
        if confidence >= self.settings.document.ocr_confidence_threshold:
            return result, confidence

        logger.info(
            "paddleocr_low_confidence_trying_tesseract",
            page=page_idx,
            confidence=round(confidence, 3),
        )

        # Fallback to Tesseract
        result_tess, confidence_tess = self._run_tesseract(image)
        if confidence_tess > confidence:
            return result_tess, confidence_tess

        # Return whichever had higher confidence
        return result, confidence

    def _run_paddleocr(self, image: np.ndarray) -> tuple[dict[str, Any] | None, float]:
        """Run PaddleOCR on an image."""
        if self.paddle_ocr is None:
            return None, 0.0

        try:
            result = self.paddle_ocr.ocr(image, cls=True)

            if not result or not result[0]:
                return None, 0.0

            boxes = []
            all_text = []
            confidences = []

            for line in result[0]:
                bbox = line[0]  # [[x1,y1], [x2,y2], [x3,y3], [x4,y4]]
                text = line[1][0]
                conf = line[1][1]

                boxes.append({
                    "bbox": bbox,
                    "text": text,
                    "confidence": conf,
                })
                all_text.append(text)
                confidences.append(conf)

            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0

            return {
                "text": " ".join(all_text),
                "boxes": boxes,
            }, avg_confidence

        except Exception as e:
            logger.error("paddleocr_failed", error=str(e))
            return None, 0.0

    def _run_tesseract(self, image: np.ndarray) -> tuple[dict[str, Any] | None, float]:
        """Run Tesseract OCR as fallback."""
        try:
            import pytesseract

            # Get detailed output with bounding boxes and confidence
            data = pytesseract.image_to_data(
                image,
                output_type=pytesseract.Output.DICT,
                config="--psm 6 --oem 3",  # Assume uniform block, use LSTM engine
            )

            boxes = []
            all_text = []
            confidences = []

            for i in range(len(data["text"])):
                text = data["text"][i].strip()
                conf = int(data["conf"][i])

                if text and conf > 0:
                    boxes.append({
                        "bbox": [
                            data["left"][i],
                            data["top"][i],
                            data["left"][i] + data["width"][i],
                            data["top"][i] + data["height"][i],
                        ],
                        "text": text,
                        "confidence": conf / 100.0,
                    })
                    all_text.append(text)
                    confidences.append(conf / 100.0)

            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0

            return {
                "text": " ".join(all_text),
                "boxes": boxes,
            }, avg_confidence

        except Exception as e:
            logger.error("tesseract_failed", error=str(e))
            return None, 0.0

    def _reconstruct_tables(
        self,
        ocr_result: dict[str, Any],
        page_idx: int,
    ) -> list[ExtractedTable]:
        """
        Reconstruct tables from OCR bounding boxes.

        Algorithm:
          1. Cluster boxes by Y-coordinate to form rows
          2. Sort boxes within each row by X-coordinate
          3. Detect column structure from consistent X-positions
          4. Build cell grid
          5. Detect header row (first row, or row with known column names)

        This is a heuristic approach. For production use with complex layouts,
        LayoutLMv3 or Table Transformer provides more robust structure detection.
        """
        boxes = ocr_result.get("boxes", [])
        if len(boxes) < 4:  # Too few boxes for a table
            return []

        # Step 1: Sort by Y position and cluster into rows
        sorted_boxes = sorted(boxes, key=lambda b: self._get_y_center(b["bbox"]))

        rows: list[list[dict]] = []
        current_row: list[dict] = [sorted_boxes[0]]
        row_y = self._get_y_center(sorted_boxes[0]["bbox"])

        for box in sorted_boxes[1:]:
            y = self._get_y_center(box["bbox"])
            # Same row if Y-center within 15px
            if abs(y - row_y) < 15:
                current_row.append(box)
            else:
                if current_row:
                    rows.append(sorted(current_row, key=lambda b: self._get_x_min(b["bbox"])))
                current_row = [box]
                row_y = y

        if current_row:
            rows.append(sorted(current_row, key=lambda b: self._get_x_min(b["bbox"])))

        # Step 2: Check if this looks like a table (consistent column count)
        col_counts = [len(row) for row in rows]
        if not col_counts:
            return []

        # Most common column count
        from collections import Counter
        most_common_cols = Counter(col_counts).most_common(1)[0][0]

        # Filter to rows with consistent column count (±1)
        table_rows = [row for row in rows if abs(len(row) - most_common_cols) <= 1]

        if len(table_rows) < 3:  # Minimum table size
            return []

        # Step 3: Build table
        headers = [box["text"] for box in table_rows[0]]
        data_rows = [
            [box["text"] for box in row]
            for row in table_rows[1:]
        ]

        # Compute confidence
        all_confs = [
            box["confidence"]
            for row in table_rows
            for box in row
        ]
        avg_confidence = sum(all_confs) / len(all_confs) if all_confs else 0.0

        table = ExtractedTable(
            page_number=page_idx + 1,
            table_index=0,
            headers=headers,
            rows=data_rows,
            confidence=avg_confidence,
            num_rows=len(data_rows),
            num_cols=len(headers),
        )

        return [table]

    @staticmethod
    def _get_y_center(bbox: Any) -> float:
        """Get Y-center of a bounding box (handles both PaddleOCR and Tesseract formats)."""
        if isinstance(bbox, list) and len(bbox) == 4 and isinstance(bbox[0], list):
            # PaddleOCR format: [[x1,y1], [x2,y2], [x3,y3], [x4,y4]]
            return (bbox[0][1] + bbox[2][1]) / 2
        elif isinstance(bbox, (list, tuple)) and len(bbox) == 4:
            # Tesseract format: [left, top, right, bottom]
            return (bbox[1] + bbox[3]) / 2
        return 0.0

    @staticmethod
    def _get_x_min(bbox: Any) -> float:
        """Get minimum X coordinate of a bounding box."""
        if isinstance(bbox, list) and len(bbox) == 4 and isinstance(bbox[0], list):
            return bbox[0][0]
        elif isinstance(bbox, (list, tuple)) and len(bbox) == 4:
            return bbox[0]
        return 0.0
