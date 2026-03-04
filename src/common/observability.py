"""
Structured logging and observability infrastructure.

Structured JSON logging via structlog.
Prometheus metrics for query engine and pipeline.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Generator

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

# Pipeline
EMAILS_INGESTED = Counter(
    "acadextract_emails_ingested_total",
    "Total emails ingested",
    ["classification"],
)
EMAILS_DEDUPLICATED = Counter(
    "acadextract_emails_deduplicated_total",
    "Total duplicate emails skipped",
    ["dedup_method"],
)
RECORDS_EXTRACTED = Counter(
    "acadextract_records_extracted_total",
    "Total student records extracted",
    ["strategy"],
)
PIPELINE_ERRORS = Counter(
    "acadextract_pipeline_errors_total",
    "Total pipeline errors",
    ["phase", "error_type"],
)

# Query Engine
QUERIES_PROCESSED = Counter(
    "acadextract_queries_processed_total",
    "Total teacher queries processed",
    ["intent"],
)
QUERY_DURATION = Histogram(
    "acadextract_query_duration_seconds",
    "End-to-end query duration",
    ["intent"],
)

# System
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
            log = get_logger(func.__module__)
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                elapsed = (time.perf_counter() - start) * 1000
                log.info("function_completed", function=func.__name__, elapsed_ms=round(elapsed, 2))
                if metric:
                    metric.observe(elapsed / 1000)
                return result
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                log.error("function_failed", function=func.__name__, elapsed_ms=round(elapsed, 2), error=str(e))
                raise

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            log = get_logger(func.__module__)
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                elapsed = (time.perf_counter() - start) * 1000
                log.info("function_completed", function=func.__name__, elapsed_ms=round(elapsed, 2))
                if metric:
                    metric.observe(elapsed / 1000)
                return result
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                log.error("function_failed", function=func.__name__, elapsed_ms=round(elapsed, 2), error=str(e))
                raise

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    return decorator


# =============================================================================
# OPENTELEMETRY DISTRIBUTED TRACING
# =============================================================================

import os as _os
import logging as _logging

_otel_log = _logging.getLogger(__name__)
_OTEL_ENDPOINT = _os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
_SERVICE_NAME  = _os.getenv("OTEL_SERVICE_NAME", "acadextract")

# Try to initialise the OTel SDK; degrade gracefully when not installed.
try:
    from opentelemetry import trace as _otel_trace
    from opentelemetry.sdk.trace import TracerProvider as _TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor as _BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource as _Resource

    _resource = _Resource.create({"service.name": _SERVICE_NAME})
    _provider = _TracerProvider(resource=_resource)

    if _OTEL_ENDPOINT:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter as _OTLPExporter,
            )
            _exporter = _OTLPExporter(endpoint=_OTEL_ENDPOINT)
            _provider.add_span_processor(_BatchSpanProcessor(_exporter))
            _otel_log.info("OpenTelemetry OTLP exporter configured: %s", _OTEL_ENDPOINT)
        except Exception as _exc:
            _otel_log.warning("OTLP exporter init failed: %s — traces will not be exported", _exc)

    _otel_trace.set_tracer_provider(_provider)
    _tracer = _otel_trace.get_tracer(_SERVICE_NAME)
    _OTEL_AVAILABLE = True

except ImportError:
    _OTEL_AVAILABLE = False
    _otel_log.info("opentelemetry-sdk not installed — distributed tracing disabled")

    class _NullSpan:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def set_attribute(self, *_): pass
        def record_exception(self, *_): pass

    class _NullTracer:
        def start_as_current_span(self, name, **_):
            return _NullSpan()

    _tracer = _NullTracer()


def get_tracer():
    """Return the configured OTel tracer (or a null tracer when SDK is absent)."""
    return _tracer


def instrument_fastapi(app) -> None:
    """
    Auto-instrument a FastAPI app with OpenTelemetry.
    Call this once after `create_app()` when the SDK is available.
    """
    if not _OTEL_AVAILABLE:
        return
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app)
        _otel_log.info("FastAPI OTel instrumentation enabled")
    except Exception as exc:
        _otel_log.warning("FastAPI OTel instrumentation failed: %s", exc)


def instrument_psycopg2() -> None:
    """Auto-instrument psycopg2 DB calls with OTel spans."""
    if not _OTEL_AVAILABLE:
        return
    try:
        from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
        Psycopg2Instrumentor().instrument()
        _otel_log.info("psycopg2 OTel instrumentation enabled")
    except Exception as exc:
        _otel_log.warning("psycopg2 OTel instrumentation failed: %s", exc)
