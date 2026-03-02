"""
Phase 5 — Student Retrieval & Teacher Query Engine.

Natural language query interface for teachers and administrators
to query student results, compute aggregations, and generate reports.
"""

from src.phase5_query_engine.intent_parser import IntentParser
from src.phase5_query_engine.entity_resolver import EntityResolver
from src.phase5_query_engine.sql_generator import SQLGenerator
from src.phase5_query_engine.aggregation_engine import AggregationEngine
from src.phase5_query_engine.answer_generator import AnswerGenerator

__all__ = [
    "IntentParser",
    "EntityResolver",
    "SQLGenerator",
    "AggregationEngine",
    "AnswerGenerator",
]
