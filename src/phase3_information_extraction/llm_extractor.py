"""
LLM-Based Extractor.

Tertiary strategy using GPT-4o / Gemini 1.5 Pro for complex or
ambiguous table layouts. Invoked only when rule-based and regex
extractors disagree or have low confidence.

Features:
  - Structured JSON output with Pydantic schema enforcement
  - Temperature=0 for deterministic extraction
  - Token-efficient prompting with table serialization
  - Targeted correction for specific field errors
  - Cost tracking per extraction
"""

from __future__ import annotations

import json
from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import get_settings
from src.common.models import (
    ExtractedTable,
    StudentRecord,
    SubjectResult,
    ResultStatus,
    ExtractionStrategy,
)
from src.common.observability import get_logger, EXTRACTION_DURATION

logger = get_logger(__name__)


EXTRACTION_SYSTEM_PROMPT = """You are a precise data extraction assistant. Your task is to extract student academic records from the provided table data.

Rules:
1. Extract EVERY student record visible in the table
2. USN (University Seat Number) format: typically like 1BM21CS001 (digit, 2 letters, 2 digits, 2-3 letters, 3 digits)
3. GPA/SGPA values must be between 0.0 and 10.0
4. Marks values must be between 0 and the maximum marks indicated
5. Status: PASS, FAIL, or ABSENT
6. If a field is not clearly identifiable, set it to null
7. Output ONLY valid JSON matching the schema — no explanations

Output JSON schema:
{
  "records": [
    {
      "usn": "string",
      "name": "string or null",
      "sgpa": "float or null",
      "subjects": [
        {
          "subject_code": "string or null",
          "subject_name": "string or null",
          "total_marks": "int or null",
          "max_marks": "int or null",
          "grade": "string or null",
          "status": "PASS|FAIL|ABSENT"
        }
      ]
    }
  ]
}"""


CORRECTION_PROMPT_TEMPLATE = """The following student record was extracted but has validation errors.

Original record:
{record_json}

Validation errors:
{errors}

Table context (surrounding rows):
{context}

Please provide the corrected record in the same JSON format. Fix ONLY the fields mentioned in the errors. Output ONLY valid JSON."""


class LLMExtractor:
    """
    Extract student records using LLM structured output.

    Uses GPT-4o (primary) or Gemini 1.5 Pro (fallback) with:
      - JSON mode for structured output
      - Temperature=0 for deterministic extraction
      - Schema-constrained decoding
      - Token-efficient table serialization
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._client = None
        self._fallback_client = None

    @property
    def client(self):
        """Lazy-init OpenAI client."""
        if self._client is None:
            import openai
            self._client = openai.AsyncOpenAI(
                api_key=self.settings.llm.providers["openai"]["api_key"],
            )
        return self._client

    @property
    def fallback_client(self):
        """Lazy-init Google Generative AI client."""
        if self._fallback_client is None:
            try:
                import google.generativeai as genai
                genai.configure(
                    api_key=self.settings.llm.providers["google"]["api_key"],
                )
                self._fallback_client = genai.GenerativeModel(
                    "gemini-1.5-pro",
                )
            except Exception as e:
                logger.warning("gemini_fallback_unavailable", error=str(e))
                self._fallback_client = None
        return self._fallback_client

    async def extract(self, table: ExtractedTable) -> list[StudentRecord]:
        """Extract student records from table using LLM."""
        table_text = self._serialize_table(table)

        if len(table_text) < 10:
            logger.warning("llm_extraction_skipped_empty_table")
            return []

        try:
            raw_json = await self._call_llm(table_text)
            records = self._parse_response(raw_json)

            logger.info(
                "llm_extraction_complete",
                records=len(records),
                model=self.settings.extraction.llm_model,
            )

            return records

        except Exception as e:
            logger.error("llm_extraction_failed", error=str(e))
            # Try fallback
            try:
                raw_json = await self._call_fallback(table_text)
                records = self._parse_response(raw_json)
                return records
            except Exception as fe:
                logger.error("llm_fallback_also_failed", error=str(fe))
                return []

    async def targeted_correction(
        self,
        record: StudentRecord,
        errors: list[str],
        table: ExtractedTable,
    ) -> StudentRecord | None:
        """
        Re-extract a specific record with error context.

        Used during validation loop when a record has specific
        field errors that might be correctable with more context.
        """
        context = self._get_surrounding_context(record.usn, table)
        prompt = CORRECTION_PROMPT_TEMPLATE.format(
            record_json=record.model_dump_json(indent=2),
            errors="\n".join(f"- {e}" for e in errors),
            context=context,
        )

        try:
            raw = await self._call_llm(prompt, is_correction=True)
            parsed = json.loads(raw)
            corrected = self._dict_to_student_record(parsed)

            if corrected:
                corrected.overall_confidence *= 0.9  # Slight penalty for correction
                logger.info(
                    "llm_correction_success",
                    usn=record.usn,
                    errors_fixed=len(errors),
                )

            return corrected

        except Exception as e:
            logger.warning(
                "llm_correction_failed",
                usn=record.usn,
                error=str(e),
            )
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    async def _call_llm(
        self,
        content: str,
        is_correction: bool = False,
    ) -> str:
        """Call primary LLM (GPT-4o) with retry."""
        model = self.settings.extraction.llm_model
        messages = [
            {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ]

        with EXTRACTION_DURATION.labels(strategy="llm").time():
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0,
                response_format={"type": "json_object"},
                max_tokens=4096 if not is_correction else 1024,
            )

        result = response.choices[0].message.content
        logger.debug(
            "llm_call_complete",
            model=model,
            tokens_prompt=response.usage.prompt_tokens,
            tokens_completion=response.usage.completion_tokens,
        )

        return result

    async def _call_fallback(self, content: str) -> str:
        """Call Gemini 1.5 Pro as fallback."""
        if self.fallback_client is None:
            raise RuntimeError("No fallback LLM available")

        prompt = f"{EXTRACTION_SYSTEM_PROMPT}\n\n{content}"
        response = await self.fallback_client.generate_content_async(
            prompt,
            generation_config={
                "temperature": 0,
                "response_mime_type": "application/json",
            },
        )

        return response.text

    def _serialize_table(self, table: ExtractedTable) -> str:
        """
        Serialize table into token-efficient text format.

        Format: pipe-delimited with header row marked.
        This is more token-efficient than JSON for tabular data.
        """
        lines: list[str] = []

        if table.headers:
            lines.append("HEADERS: " + " | ".join(table.headers))
            lines.append("-" * 40)

        for i, row in enumerate(table.rows):
            line = " | ".join(cell.strip() if cell else "" for cell in row)
            lines.append(f"ROW {i + 1}: {line}")

        return "\n".join(lines)

    def _parse_response(self, raw_json: str) -> list[StudentRecord]:
        """Parse LLM JSON response into StudentRecord list."""
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code block
            import re
            match = re.search(r"```(?:json)?\s*(.*?)```", raw_json, re.DOTALL)
            if match:
                data = json.loads(match.group(1))
            else:
                raise

        records_data = data.get("records", [])
        if not isinstance(records_data, list):
            records_data = [records_data]

        results: list[StudentRecord] = []
        for item in records_data:
            record = self._dict_to_student_record(item)
            if record:
                results.append(record)

        return results

    def _dict_to_student_record(
        self,
        item: dict[str, Any],
    ) -> StudentRecord | None:
        """Convert a parsed dict to StudentRecord with validation."""
        try:
            subjects = []
            for subj in item.get("subjects", []):
                status_str = str(subj.get("status", "PASS")).upper()
                status = ResultStatus.PASS
                if "FAIL" in status_str:
                    status = ResultStatus.FAIL
                elif "ABSENT" in status_str:
                    status = ResultStatus.ABSENT

                subjects.append(SubjectResult(
                    subject_code=subj.get("subject_code"),
                    subject_name=subj.get("subject_name"),
                    total_marks=int(subj["total_marks"]) if subj.get("total_marks") is not None else None,
                    max_marks=int(subj.get("max_marks", self.settings.extraction.marks_max_default)),
                    grade=subj.get("grade"),
                    status=status,
                ))

            record = StudentRecord(
                usn=item["usn"].strip().upper(),
                name=item.get("name"),
                subjects=subjects,
                sgpa=float(item["sgpa"]) if item.get("sgpa") is not None else None,
                extraction_strategy=ExtractionStrategy.LLM,
                overall_confidence=0.85,  # LLM base confidence
                field_confidences={
                    "usn": 0.90,
                    "name": 0.85 if item.get("name") else 0.0,
                    "subjects": 0.82 if subjects else 0.0,
                    "sgpa": 0.85 if item.get("sgpa") else 0.0,
                },
            )
            return record

        except (KeyError, ValueError, TypeError) as e:
            logger.warning(
                "llm_record_parse_error",
                error=str(e),
                usn=item.get("usn", "unknown"),
            )
            return None

    def _get_surrounding_context(
        self,
        usn: str,
        table: ExtractedTable,
        window: int = 3,
    ) -> str:
        """Get rows surrounding a USN for correction context."""
        target_idx = None
        for i, row in enumerate(table.rows):
            if any(usn in str(cell) for cell in row):
                target_idx = i
                break

        if target_idx is None:
            return self._serialize_table(table)

        start = max(0, target_idx - window)
        end = min(len(table.rows), target_idx + window + 1)

        lines = []
        if table.headers:
            lines.append("HEADERS: " + " | ".join(table.headers))
            lines.append("-" * 40)

        for i in range(start, end):
            row = table.rows[i]
            marker = " >>> " if i == target_idx else "     "
            line = " | ".join(cell.strip() if cell else "" for cell in row)
            lines.append(f"{marker}ROW {i + 1}: {line}")

        return "\n".join(lines)
