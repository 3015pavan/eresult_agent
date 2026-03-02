"""
Extraction Pipeline — Orchestrates three extraction strategies.

Architecture:
  This pipeline implements a hybrid neuro-symbolic extraction system that
  combines rule-based, regex, and LLM approaches with a voting merger
  and constraint-based validation loop.

  ┌──────────────────────────────────────────────────────┐
  │                   Extracted Table                     │
  └──────────┬──────────────┬──────────────┬─────────────┘
             │              │              │
             ▼              ▼              ▼
  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
  │ Rule-Based   │ │   Regex      │ │  LLM JSON    │
  │ (structured) │ │ (semi-struct)│ │  (ambiguous)  │
  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
         │                │                │
         ▼                ▼                ▼
  ┌────────────────────────────────────────────────┐
  │              Extraction Merger                  │
  │  (voting, conflict resolution, confidence)     │
  └──────────────────┬─────────────────────────────┘
                     │
                     ▼
  ┌────────────────────────────────────────────────┐
  │           Validation & Verification             │
  │  (constraints, cross-field, correction loop)   │
  └──────────────────┬─────────────────────────────┘
                     │
                     ▼
              ExtractionResult
"""

from __future__ import annotations

import time
from typing import Any
from uuid import UUID, uuid4

from src.common.config import get_settings
from src.common.models import (
    DocumentParseResult,
    ExtractionResult,
    ExtractionStrategy,
    ExtractedTable,
    StudentRecord,
)
from src.common.observability import (
    get_logger,
    RECORDS_EXTRACTED,
    EXTRACTION_CONFIDENCE,
    RECORDS_QUARANTINED,
    timer,
)

logger = get_logger(__name__)


class ExtractionPipeline:
    """
    Orchestrates the three-strategy extraction pipeline.

    Strategy execution order:
      1. Rule-based (fastest, highest precision for known formats)
      2. Regex (fast, medium precision for semi-structured data)
      3. LLM (slowest, used for ambiguous/unknown layouts)

    The merger combines outputs using a voting mechanism:
      - All 3 agree → confidence = 0.98
      - 2 agree → majority wins, confidence = 0.85
      - All disagree → rule-based preferred for numbers, flag for review

    Validation loop (max 3 iterations):
      - Check domain constraints (GPA ≤ 10, marks ≤ max, etc.)
      - Cross-field consistency (marks vs status)
      - Cross-record consistency (same USN → same name)
      - If violations: re-extract with targeted context
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._rule_extractor = None
        self._regex_extractor = None
        self._llm_extractor = None
        self._merger = None
        self._validator = None

    @property
    def rule_extractor(self):
        if self._rule_extractor is None:
            from .rule_engine import RuleBasedExtractor
            self._rule_extractor = RuleBasedExtractor()
        return self._rule_extractor

    @property
    def regex_extractor(self):
        if self._regex_extractor is None:
            from .regex_engine import RegexExtractor
            self._regex_extractor = RegexExtractor()
        return self._regex_extractor

    @property
    def llm_extractor(self):
        if self._llm_extractor is None:
            from .llm_extractor import LLMExtractor
            self._llm_extractor = LLMExtractor()
        return self._llm_extractor

    @property
    def merger(self):
        if self._merger is None:
            from .merger import ExtractionMerger
            self._merger = ExtractionMerger()
        return self._merger

    @property
    def validator(self):
        if self._validator is None:
            from .validator import ExtractionValidator
            self._validator = ExtractionValidator()
        return self._validator

    async def extract(
        self,
        parse_result: DocumentParseResult,
    ) -> ExtractionResult:
        """
        Run the full extraction pipeline on parsed document output.

        Flow:
          1. For each table in the document:
             a. Run rule-based extraction
             b. Run regex extraction
             c. Run LLM extraction (if needed)
          2. Merge results from all strategies
          3. Validate with domain constraints
          4. Re-extract on validation failure (max 3 retries)
          5. Return final extraction result with confidence scores
        """
        start = time.perf_counter()
        extraction_id = uuid4()
        all_records: list[StudentRecord] = []
        all_errors: list[str] = []
        total_llm_tokens = 0

        for table in parse_result.tables:
            if table.num_rows < 1:
                continue

            # Run three strategies
            rule_records = self.rule_extractor.extract(table)
            regex_records = self.regex_extractor.extract(table)

            # Only invoke LLM if rule + regex disagree or produce low confidence
            llm_records: list[StudentRecord] = []
            needs_llm = self._needs_llm_extraction(rule_records, regex_records)

            if needs_llm:
                llm_result = await self.llm_extractor.extract(table)
                llm_records = llm_result.records
                total_llm_tokens += llm_result.llm_tokens_used

            # Merge
            merged = self.merger.merge(rule_records, regex_records, llm_records)

            # Validate with retry loop
            max_retries = self.settings.extraction.max_validation_retries
            for attempt in range(max_retries + 1):
                validation_result = self.validator.validate_batch(merged)

                if validation_result.is_valid:
                    break

                if attempt < max_retries:
                    logger.info(
                        "validation_failed_retrying",
                        attempt=attempt + 1,
                        errors=validation_result.errors[:5],
                        table_page=table.page_number,
                    )
                    # Re-extract with targeted context
                    merged = await self._retry_extraction(
                        table, merged, validation_result.errors
                    )
                else:
                    all_errors.extend(validation_result.errors)
                    RECORDS_QUARANTINED.inc(len(validation_result.quarantined))

            all_records.extend(merged)

        elapsed_ms = int((time.perf_counter() - start) * 1000)

        # Compute overall confidence
        overall_confidence = (
            sum(r.overall_confidence for r in all_records) / len(all_records)
            if all_records else 0.0
        )

        EXTRACTION_CONFIDENCE.observe(overall_confidence)
        for record in all_records:
            RECORDS_EXTRACTED.labels(strategy=record.extraction_strategy.value).inc()

        result = ExtractionResult(
            extraction_id=extraction_id,
            attachment_id=parse_result.attachment_id,
            strategy=ExtractionStrategy.HYBRID,
            records=all_records,
            overall_confidence=overall_confidence,
            validation_errors=all_errors,
            extraction_time_ms=elapsed_ms,
            llm_tokens_used=total_llm_tokens,
        )

        logger.info(
            "extraction_completed",
            extraction_id=str(extraction_id),
            records=len(all_records),
            confidence=round(overall_confidence, 3),
            elapsed_ms=elapsed_ms,
            llm_tokens=total_llm_tokens,
            errors=len(all_errors),
        )

        return result

    def _needs_llm_extraction(
        self,
        rule_records: list[StudentRecord],
        regex_records: list[StudentRecord],
    ) -> bool:
        """
        Determine if LLM extraction is needed.

        LLM extraction is expensive (~100ms + API cost), so only used when:
          1. Rule-based produced no results
          2. Regex produced no results
          3. Rule and regex significantly disagree
          4. Both produced low confidence results (< 0.7)
        """
        if not rule_records and not regex_records:
            return True

        if not rule_records or not regex_records:
            return True

        # Check if they significantly disagree on record count
        if abs(len(rule_records) - len(regex_records)) > len(rule_records) * 0.2:
            return True

        # Check average confidence
        rule_conf = sum(r.overall_confidence for r in rule_records) / len(rule_records)
        regex_conf = sum(r.overall_confidence for r in regex_records) / len(regex_records)

        if max(rule_conf, regex_conf) < 0.7:
            return True

        return False

    async def _retry_extraction(
        self,
        table: ExtractedTable,
        current_records: list[StudentRecord],
        errors: list[str],
    ) -> list[StudentRecord]:
        """
        Re-extract with targeted context from validation errors.

        Constructs a specific prompt for the LLM including:
          - The original table data
          - Which fields failed validation
          - What the constraints are
          - Request to fix specific issues

        This targeted approach has higher success rate than blind re-extraction.
        """
        error_context = "; ".join(errors[:5])
        logger.info(
            "targeted_re_extraction",
            table_page=table.page_number,
            error_context=error_context,
        )

        # Use LLM with error context for targeted correction
        retry_result = await self.llm_extractor.extract_with_corrections(
            table, current_records, errors
        )

        if retry_result.records:
            return retry_result.records

        return current_records
