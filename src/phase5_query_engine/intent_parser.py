"""
Intent Parser.

Parses natural language teacher queries into structured intents
using LLM JSON mode.

Supported query intents:
  - STUDENT_LOOKUP: "Show results for 1BM21CS001"
  - SUBJECT_PERFORMANCE: "How did students do in Data Structures?"
  - COMPARISON: "Compare semester GPA of CS vs IS"
  - AGGREGATION: "What is the average CGPA of 2021 batch?"
  - BACKLOGS: "List students with more than 3 backlogs"
  - TOP_N: "Top 10 students by SGPA in 5th semester"
  - TREND: "Show SGPA trend for student across semesters"
  - COUNT: "How many students passed in Mathematics?"
"""

from __future__ import annotations

import json
from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import get_settings
from src.common.models import ParsedQuery, QueryIntent
from src.common.observability import get_logger

logger = get_logger(__name__)


INTENT_SYSTEM_PROMPT = """You are a query intent parser for an academic results database.

Parse the user's natural language query into a structured intent.

Available intents:
1. student_lookup - Query about a specific student (by USN or name)
2. subject_performance - Query about subject-level performance
3. comparison - Compare groups (departments, batches, semesters)
4. aggregation - Aggregate statistics (average, median, percentile)
5. backlogs - Queries about failed subjects / backlogs
6. top_n - Ranking queries (top/bottom N students)
7. trend - Temporal trend analysis
8. count - Counting queries

Output JSON:
{
  "intent": "one of the intents above",
  "entities": {
    "usn": "extracted USN if any",
    "student_name": "student name if mentioned",
    "department": "department code if mentioned",
    "semester": "semester number if mentioned",
    "subject": "subject name/code if mentioned",
    "batch_year": "batch year if mentioned",
    "limit": "number for top-N queries",
    "comparison_groups": ["group1", "group2"],
    "metric": "what to measure (sgpa, cgpa, marks, pass_rate)"
  },
  "filters": {
    "min_value": null,
    "max_value": null,
    "status": null,
    "exam_type": null
  },
  "sort": {
    "field": "field to sort by",
    "direction": "asc or desc"
  },
  "confidence": 0.0 to 1.0
}

Rules:
- Set fields to null if not mentioned
- Extract ALL entities from the query
- Infer reasonable defaults (e.g., latest semester, current year)"""


class IntentParser:
    """
    Parse natural language queries into structured intents.

    Two-stage parsing:
      1. Rule-based pattern matching (fast, for common patterns)
      2. LLM parsing (for complex or ambiguous queries)
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._client = None

        # Common query patterns for fast matching
        import re
        self._patterns = {
            QueryIntent.STUDENT_LOOKUP: [
                re.compile(r"(?:show|get|find|display)\s+(?:results?|marks?|grades?)\s+(?:for|of)\s+(\w+)", re.I),
                re.compile(r"(\b[1-4][A-Z]{2}\d{2}[A-Z]{2,3}\d{3}\b)", re.I),
            ],
            QueryIntent.TOP_N: [
                re.compile(r"(?:top|best|highest)\s+(\d+)", re.I),
                re.compile(r"(?:bottom|worst|lowest)\s+(\d+)", re.I),
            ],
            QueryIntent.BACKLOGS: [
                re.compile(r"backlog|fail|arrear", re.I),
            ],
            QueryIntent.COUNT: [
                re.compile(r"how\s+many|count|number\s+of", re.I),
            ],
            QueryIntent.AGGREGATION: [
                re.compile(r"average|mean|median|percentile", re.I),
            ],
            QueryIntent.COMPARISON: [
                re.compile(r"compare|versus|vs|difference\s+between", re.I),
            ],
            QueryIntent.TREND: [
                re.compile(r"trend|progress|over\s+time|across\s+semesters", re.I),
            ],
        }

    @property
    def client(self):
        """Lazy-init OpenAI client."""
        if self._client is None:
            import openai
            self._client = openai.AsyncOpenAI(
                api_key=self.settings.llm.providers["openai"]["api_key"],
            )
        return self._client

    async def parse(self, query: str) -> ParsedQuery:
        """
        Parse a natural language query into structured intent.

        Args:
            query: Natural language query from teacher/admin

        Returns:
            ParsedQuery with intent, entities, filters
        """
        # Stage 1: Pattern matching
        quick_intent = self._quick_pattern_match(query)

        # Stage 2: LLM parsing for full structure
        try:
            parsed = await self._llm_parse(query)

            # Override intent if pattern match is very confident
            if quick_intent and parsed.confidence < 0.8:
                parsed.intent = quick_intent

            logger.info(
                "intent_parsed",
                query=query[:100],
                intent=parsed.intent.value,
                confidence=parsed.confidence,
            )

            return parsed

        except Exception as e:
            logger.error("intent_parse_failed", error=str(e))
            # Fallback: use pattern match
            return ParsedQuery(
                raw_query=query,
                intent=quick_intent or QueryIntent.STUDENT_LOOKUP,
                entities={},
                filters={},
                confidence=0.3,
            )

    def _quick_pattern_match(self, query: str) -> QueryIntent | None:
        """Fast pattern-based intent detection."""
        for intent, patterns in self._patterns.items():
            for pattern in patterns:
                if pattern.search(query):
                    return intent
        return None

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def _llm_parse(self, query: str) -> ParsedQuery:
        """Parse query using LLM JSON mode."""
        response = await self.client.chat.completions.create(
            model=self.settings.query.intent_model,
            messages=[
                {"role": "system", "content": INTENT_SYSTEM_PROMPT},
                {"role": "user", "content": query},
            ],
            temperature=0,
            response_format={"type": "json_object"},
            max_tokens=512,
        )

        raw = response.choices[0].message.content
        data = json.loads(raw)

        # Map intent string to enum
        intent_str = data.get("intent", "student_lookup").lower()
        intent_map = {i.value: i for i in QueryIntent}
        intent = intent_map.get(intent_str, QueryIntent.STUDENT_LOOKUP)

        return ParsedQuery(
            raw_query=query,
            intent=intent,
            entities=data.get("entities", {}),
            filters=data.get("filters", {}),
            sort=data.get("sort"),
            confidence=float(data.get("confidence", 0.7)),
        )
