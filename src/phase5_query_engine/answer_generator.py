"""
Answer Generator.

Generates human-readable answers from query results using
RAG (Retrieval-Augmented Generation).

Supports:
  - Tabular answers (formatted tables)
  - Narrative answers (natural language summaries)
  - Chart specifications (for frontend rendering)
  - Confidence-qualified answers
"""

from __future__ import annotations

import json
from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import get_settings
from src.common.models import ParsedQuery, QueryResult, QueryIntent
from src.common.observability import get_logger

logger = get_logger(__name__)


ANSWER_SYSTEM_PROMPT = """You are an academic results assistant that generates clear, professional answers.

Given a query and database results, generate a human-readable answer.

Rules:
1. Be precise with numbers — never round unless asked
2. Use tables for multi-row results
3. Include relevant context (e.g., "out of 120 students")
4. Flag anomalies or interesting patterns
5. For trend data, describe the trajectory
6. Keep answers concise but complete
7. Use markdown formatting

Output JSON:
{
  "text_answer": "markdown-formatted answer text",
  "summary": "one-line summary",
  "data_table": [{"col1": "val1", ...}] or null,
  "chart_spec": {"type": "bar|line|pie", "data": {...}} or null,
  "confidence": 0.0-1.0,
  "caveats": ["list of data quality notes"]
}"""


class AnswerGenerator:
    """
    Generate human-readable answers from query results.

    Three answer modes:
      1. Deterministic: Counted answers (exact from SQL)
      2. Formatted: Table/chart formatting (template-based)
      3. Narrative: Natural language summary (LLM-generated)
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._client = None

    @property
    def client(self):
        """Lazy-init OpenAI client."""
        if self._client is None:
            import openai
            self._client = openai.AsyncOpenAI(
                api_key=self.settings.llm.providers["openai"]["api_key"],
            )
        return self._client

    async def generate(
        self,
        parsed: ParsedQuery,
        rows: list[dict[str, Any]],
    ) -> QueryResult:
        """
        Generate answer from parsed query and database results.

        Strategy selection:
          - COUNT intent → deterministic
          - TOP_N, BACKLOGS → formatted table
          - AGGREGATION, COMPARISON → formatted + narrative
          - STUDENT_LOOKUP → formatted + narrative
          - TREND → chart + narrative
        """
        if not rows:
            return QueryResult(
                query=parsed.raw_query,
                intent=parsed.intent,
                text_answer="No results found matching your query.",
                data=[],
                confidence=1.0,
            )

        # Deterministic answers (no LLM needed)
        if parsed.intent == QueryIntent.COUNT:
            return self._deterministic_count(parsed, rows)

        # Try formatted answer first
        formatted = self._format_table(rows)

        # Generate narrative via LLM
        try:
            narrative = await self._generate_narrative(parsed, rows)
        except Exception as e:
            logger.warning("narrative_generation_failed", error=str(e))
            narrative = {
                "text_answer": formatted,
                "summary": f"Found {len(rows)} results",
                "confidence": 0.7,
                "caveats": [],
            }

        return QueryResult(
            query=parsed.raw_query,
            intent=parsed.intent,
            text_answer=narrative.get("text_answer", formatted),
            summary=narrative.get("summary"),
            data=rows,
            chart_spec=narrative.get("chart_spec"),
            confidence=narrative.get("confidence", 0.8),
            caveats=narrative.get("caveats", []),
        )

    def _deterministic_count(
        self,
        parsed: ParsedQuery,
        rows: list[dict],
    ) -> QueryResult:
        """Generate deterministic count answer."""
        count = rows[0].get("student_count", rows[0].get("count", 0))

        return QueryResult(
            query=parsed.raw_query,
            intent=parsed.intent,
            text_answer=f"**{count}** students match your query.",
            summary=f"{count} students",
            data=rows,
            confidence=1.0,
        )

    def _format_table(self, rows: list[dict]) -> str:
        """Format rows as a markdown table."""
        if not rows:
            return "No results."

        headers = list(rows[0].keys())

        # Build markdown table
        lines = []
        lines.append("| " + " | ".join(str(h) for h in headers) + " |")
        lines.append("| " + " | ".join("---" for _ in headers) + " |")

        for row in rows[:50]:  # Cap at 50 rows
            values = [str(row.get(h, "")) for h in headers]
            lines.append("| " + " | ".join(values) + " |")

        if len(rows) > 50:
            lines.append(f"\n*...and {len(rows) - 50} more rows*")

        return "\n".join(lines)

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def _generate_narrative(
        self,
        parsed: ParsedQuery,
        rows: list[dict],
    ) -> dict[str, Any]:
        """Generate narrative answer using LLM."""
        # Limit data sent to LLM
        sample_rows = rows[:20]
        total_rows = len(rows)

        user_message = (
            f"Query: {parsed.raw_query}\n"
            f"Intent: {parsed.intent.value}\n"
            f"Total results: {total_rows}\n\n"
            f"Data (first {len(sample_rows)} rows):\n"
            f"{json.dumps(sample_rows, indent=2, default=str)}"
        )

        response = await self.client.chat.completions.create(
            model=self.settings.query.answer_model,
            messages=[
                {"role": "system", "content": ANSWER_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
            max_tokens=1024,
        )

        raw = response.choices[0].message.content
        return json.loads(raw)
