"""Lightweight Phase 5 query engine helpers.

These modules provide a small compatibility surface for older tests and
utility code that still imports ``src.phase5_query_engine``.
"""

from .aggregation_engine import AggregationEngine, SGPAResult, BatchStatistics, CGPAResult
from .intent_parser import IntentParser
from .sql_generator import SQLGenerator, SecurityError

__all__ = [
    "AggregationEngine",
    "BatchStatistics",
    "CGPAResult",
    "IntentParser",
    "SGPAResult",
    "SQLGenerator",
    "SecurityError",
]