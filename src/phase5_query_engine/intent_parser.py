"""Simple intent parser used by legacy tests and structured query helpers."""

from __future__ import annotations

import re

from src.common.models import QueryIntent


class IntentParser:
    """Pattern-first parser for common academic query intents."""

    def _quick_pattern_match(self, text: str) -> QueryIntent:
        message = (text or "").strip().upper()
        if re.search(r"\b[1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3}\b", message):
            return QueryIntent.STUDENT_LOOKUP
        if any(token in message for token in ("TOP ", "TOP-", "RANK", "HIGHEST", "BEST")):
            return QueryIntent.TOP_N
        if "BACKLOG" in message or "FAILED" in message:
            return QueryIntent.BACKLOGS
        if any(token in message for token in ("AVERAGE", "MEAN", "AGGREGATE", "CGPA", "SGPA")):
            return QueryIntent.AGGREGATION
        if any(token in message for token in ("HOW MANY", "COUNT", "TOTAL NUMBER")):
            return QueryIntent.COUNT
        return QueryIntent.UNKNOWN
