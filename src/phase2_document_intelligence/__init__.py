"""
Phase 2 — Document Intelligence.

Exports the central entry points.
"""

from .router import route_to_parser, ParsedDocument
from .universal_converter import (
    convert_any,
    convert_bytes,
    convert_path,
    convert_html_body,
    convert_gmail_attachment,
    ocr_image_bytes,
    ocr_image_path,
)

__all__ = [
    "route_to_parser",
    "ParsedDocument",
    "convert_any",
    "convert_bytes",
    "convert_path",
    "convert_html_body",
    "convert_gmail_attachment",
    "ocr_image_bytes",
    "ocr_image_path",
]
