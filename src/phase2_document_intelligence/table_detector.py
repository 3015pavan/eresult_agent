"""
Optional YOLOv8 table detection helpers.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def detect_table_regions(image_path: str) -> list[dict[str, Any]]:
    """
    Detect table regions using YOLOv8 when configured.
    Returns a list of bounding-box dicts, or [] when unavailable.
    """
    try:
        from src.common.config import get_settings
        cfg = get_settings().document_ai
        if not cfg.yolo_enabled or not cfg.yolo_model_path:
            logger.info("YOLOv8 disabled or model path missing")
            return []
        from ultralytics import YOLO  # type: ignore
        model = YOLO(cfg.yolo_model_path)
        results = model.predict(image_path, conf=cfg.yolo_confidence, verbose=False)
        boxes: list[dict[str, Any]] = []
        for result in results:
            for box in getattr(result, "boxes", []):
                coords = box.xyxy.tolist()[0]
                boxes.append({
                    "x1": int(coords[0]),
                    "y1": int(coords[1]),
                    "x2": int(coords[2]),
                    "y2": int(coords[3]),
                    "confidence": float(box.conf.tolist()[0]),
                })
        logger.info(f"YOLOv8 detected {len(boxes)} table regions in {image_path}")
        return boxes
    except ImportError:
        logger.error("YOLOv8 not installed. Please install ultralytics.")
        return []
    except Exception as exc:
        logger.error(f"YOLOv8 table detection failed: {exc}")
        return []
