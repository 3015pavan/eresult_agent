"""
Structured logging and observability infrastructure.

Uses structlog for structured JSON logging and OpenTelemetry for distributed tracing.
All pipeline stages emit structured events for complete audit trails.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Generator
from uuid import UUID

import structlog
from prometheus_client import Counter, Histogram, Gauge

# =============================================================================
# STRUCTURED LOGGER SETUP
# =============================================================================

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger bound to a module name."""
    return structlog.get_logger(module=name)


# =============================================================================
# PROMETHEUS METRICS
# =============================================================================

# Phase 1 — Email Intelligence
EMAILS_INGESTED = Counter(
    "acadextract_emails_ingested_total",
    "Total emails ingested",
    ["account_id", "institution_id"],
)
EMAILS_CLASSIFIED = Counter(
    "acadextract_emails_classified_total",
    "Total emails classified",
    ["classification", "institution_id"],
)
EMAIL_CLASSIFICATION_CONFIDENCE = Histogram(
    "acadextract_email_classification_confidence",
    "Distribution of email classification confidence scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0],
)
EMAILS_DEDUPLICATED = Counter(
    "acadextract_emails_deduplicated_total",
    "Total duplicate emails detected and skipped",
    ["dedup_method"],  # exact_hash, simhash, attachment_hash
)
EMAIL_INGESTION_LATENCY = Histogram(
    "acadextract_email_ingestion_latency_seconds",
    "Email ingestion latency",
)

# Phase 2 — Document Intelligence
DOCUMENTS_PARSED = Counter(
    "acadextract_documents_parsed_total",
    "Total documents parsed",
    ["document_type", "parse_method"],
)
DOCUMENT_PARSE_LATENCY = Histogram(
    "acadextract_document_parse_latency_seconds",
    "Document parsing latency",
    ["document_type"],
)
OCR_CONFIDENCE = Histogram(
    "acadextract_ocr_confidence",
    "OCR confidence scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0],
)
TABLES_EXTRACTED = Counter(
    "acadextract_tables_extracted_total",
    "Total tables extracted from documents",
)

# Phase 3 — Information Extraction
RECORDS_EXTRACTED = Counter(
    "acadextract_records_extracted_total",
    "Total student records extracted",
    ["strategy"],
)
EXTRACTION_CONFIDENCE = Histogram(
    "acadextract_extraction_confidence",
    "Extraction confidence scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0],
)
VALIDATION_FAILURES = Counter(
    "acadextract_validation_failures_total",
    "Extraction validation failures by type",
    ["failure_type"],
)
RECORDS_QUARANTINED = Counter(
    "acadextract_records_quarantined_total",
    "Records quarantined for human review",
)
LLM_TOKENS_USED = Counter(
    "acadextract_llm_tokens_used_total",
    "Total LLM tokens consumed",
    ["provider", "model", "phase"],
)

# Phase 4 — Agent
AGENT_RUNS = Counter(
    "acadextract_agent_runs_total",
    "Total agent execution runs",
    ["final_state"],
)
AGENT_STEPS = Counter(
    "acadextract_agent_steps_total",
    "Total agent steps by type",
    ["step_type"],
)
AGENT_TOOL_CALLS = Counter(
    "acadextract_agent_tool_calls_total",
    "Agent tool invocations",
    ["tool_name", "success"],
)
AGENT_ACTIVE = Gauge(
    "acadextract_agent_active",
    "Currently active agent runs",
)

# Phase 5 — Query Engine
QUERIES_PROCESSED = Counter(
    "acadextract_queries_processed_total",
    "Total teacher queries processed",
    ["intent"],
)
QUERY_LATENCY = Histogram(
    "acadextract_query_latency_seconds",
    "Query processing latency",
    ["intent"],
)
QUERY_DURATION = Histogram(
    "acadextract_query_duration_seconds",
    "End-to-end query duration",
    ["intent"],
)

# Phase 3 — Extraction (additional)
EXTRACTION_DURATION = Histogram(
    "acadextract_extraction_duration_seconds",
    "Extraction processing duration",
    ["strategy"],
)

# System-level
PIPELINE_ERRORS = Counter(
    "acadextract_pipeline_errors_total",
    "Total pipeline errors",
    ["phase", "error_type"],
)
ACTIVE_WORKERS = Gauge(
    "acadextract_active_workers",
    "Currently active worker count",
    ["queue"],
)


# =============================================================================
# TIMING UTILITIES
# =============================================================================

@contextmanager
def timer() -> Generator[dict[str, float], None, None]:
    """Context manager that measures elapsed time in milliseconds."""
    result: dict[str, float] = {}
    start = time.perf_counter()
    try:
        yield result
    finally:
        result["elapsed_ms"] = (time.perf_counter() - start) * 1000


def timed(metric: Histogram | None = None):
    """Decorator that logs and optionally records execution time."""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                elapsed = (time.perf_counter() - start) * 1000
                logger.info(
                    "function_completed",
                    function=func.__name__,
                    elapsed_ms=round(elapsed, 2),
                )
                if metric:
                    metric.observe(elapsed / 1000)
                return result
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                logger.error(
                    "function_failed",
                    function=func.__name__,
                    elapsed_ms=round(elapsed, 2),
                    error=str(e),
                )
                raise

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                elapsed = (time.perf_counter() - start) * 1000
                logger.info(
                    "function_completed",
                    function=func.__name__,
                    elapsed_ms=round(elapsed, 2),
                )
                if metric:
                    metric.observe(elapsed / 1000)
                return result
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                logger.error(
                    "function_failed",
                    function=func.__name__,
                    elapsed_ms=round(elapsed, 2),
                    error=str(e),
                )
                raise

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    return decorator
