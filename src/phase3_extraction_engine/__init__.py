"""
Phase 3 — Information Extraction Engine.

Exports the multi-strategy extractor with voting.
"""

from .strategy_merger import extract_with_voting
from .llm_extractor import llm_extract
from .validator import validate_and_correct
from .review_queue import enqueue_for_review, get_review_queue

__all__ = [
    "extract_with_voting",
    "llm_extract",
    "validate_and_correct",
    "enqueue_for_review",
    "get_review_queue",
]
