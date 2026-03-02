"""
Query Endpoints.

Natural language query interface for teachers and administrators.

Endpoints:
  - POST /query — Submit a natural language query
  - GET /query/{query_id} — Get query result by ID
  - GET /student/{usn} — Direct student lookup
  - GET /student/{usn}/trend — Student SGPA trend
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from src.common.observability import get_logger, QUERY_DURATION

logger = get_logger(__name__)
router = APIRouter()


# ── Request/Response Models ─────────────────────────────────────────


class QueryRequest(BaseModel):
    """Natural language query request."""

    query: str = Field(
        ...,
        min_length=3,
        max_length=500,
        description="Natural language query",
        examples=["Show results for 1BM21CS001"],
    )
    context: dict[str, Any] | None = Field(
        default=None,
        description="Optional context (department, semester, etc.)",
    )


class QueryResponse(BaseModel):
    """Query response."""

    query: str
    intent: str
    text_answer: str
    summary: str | None = None
    data: list[dict[str, Any]] = []
    chart_spec: dict[str, Any] | None = None
    confidence: float
    caveats: list[str] = []


class StudentSummary(BaseModel):
    """Student summary response."""

    usn: str
    name: str
    department: str
    batch_year: int
    current_cgpa: float
    active_backlogs: int
    semesters: list[dict[str, Any]]


class TrendResponse(BaseModel):
    """Student SGPA trend response."""

    usn: str
    name: str
    trend: list[dict[str, Any]]


# ── Endpoints ───────────────────────────────────────────────────────


@router.post("/query", response_model=QueryResponse)
async def submit_query(request: QueryRequest) -> QueryResponse:
    """
    Submit a natural language query about student results.

    The query goes through:
      1. Intent parsing
      2. Entity resolution
      3. SQL generation
      4. Query execution
      5. Answer generation
    """
    from src.phase5_query_engine.intent_parser import IntentParser
    from src.phase5_query_engine.entity_resolver import EntityResolver
    from src.phase5_query_engine.sql_generator import SQLGenerator
    from src.phase5_query_engine.answer_generator import AnswerGenerator

    with QUERY_DURATION.labels(intent="unknown").time():
        # 1. Parse intent
        parser = IntentParser()
        parsed = await parser.parse(request.query)

        # 2. Resolve entities
        resolver = EntityResolver()
        parsed = await resolver.resolve(parsed)

        # 3. Generate SQL
        generator = SQLGenerator()
        sql, params = generator.generate(parsed)

        # 4. Execute query
        try:
            import asyncpg
            from src.common.config import get_settings
            pool = await asyncpg.create_pool(
                dsn=get_settings().database.url,
                min_size=1,
                max_size=3,
            )
            async with pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
            data = [dict(r) for r in rows]
            await pool.close()
        except Exception as e:
            logger.error("query_execution_failed", error=str(e))
            raise HTTPException(
                status_code=500,
                detail=f"Query execution failed: {e}",
            )

        # 5. Generate answer
        answerer = AnswerGenerator()
        result = await answerer.generate(parsed, data)

    return QueryResponse(
        query=request.query,
        intent=result.intent.value,
        text_answer=result.text_answer,
        summary=result.summary,
        data=result.data,
        chart_spec=result.chart_spec,
        confidence=result.confidence,
        caveats=result.caveats or [],
    )


@router.get("/student/{usn}", response_model=StudentSummary)
async def get_student(usn: str) -> StudentSummary:
    """Get student summary by USN."""
    try:
        import asyncpg
        from src.common.config import get_settings
        pool = await asyncpg.create_pool(
            dsn=get_settings().database.url,
            min_size=1,
            max_size=3,
        )
        async with pool.acquire() as conn:
            student = await conn.fetchrow(
                """
                SELECT s.usn, s.name, d.code AS department,
                       s.batch_year, s.current_cgpa, s.active_backlogs
                FROM students s
                JOIN departments d ON d.id = s.department_id
                WHERE s.usn = $1
                """,
                usn.upper(),
            )

            if not student:
                await pool.close()
                raise HTTPException(status_code=404, detail="Student not found")

            semesters = await conn.fetch(
                """
                SELECT semester, sgpa, credits_earned,
                       credits_attempted, subjects_passed, subjects_failed
                FROM semester_aggregates
                WHERE student_id = (SELECT id FROM students WHERE usn = $1)
                ORDER BY semester
                """,
                usn.upper(),
            )

        await pool.close()

        return StudentSummary(
            usn=student["usn"],
            name=student["name"],
            department=student["department"],
            batch_year=student["batch_year"],
            current_cgpa=float(student["current_cgpa"] or 0),
            active_backlogs=student["active_backlogs"] or 0,
            semesters=[dict(s) for s in semesters],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("student_lookup_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/student/{usn}/trend", response_model=TrendResponse)
async def get_student_trend(usn: str) -> TrendResponse:
    """Get student SGPA trend across semesters."""
    try:
        import asyncpg
        from src.common.config import get_settings
        pool = await asyncpg.create_pool(
            dsn=get_settings().database.url,
            min_size=1,
            max_size=3,
        )
        async with pool.acquire() as conn:
            student = await conn.fetchrow(
                "SELECT usn, name FROM students WHERE usn = $1",
                usn.upper(),
            )
            if not student:
                await pool.close()
                raise HTTPException(status_code=404, detail="Student not found")

            trend = await conn.fetch(
                """
                SELECT semester, sgpa, credits_earned, subjects_passed, subjects_failed
                FROM semester_aggregates
                WHERE student_id = (SELECT id FROM students WHERE usn = $1)
                ORDER BY semester
                """,
                usn.upper(),
            )

        await pool.close()

        return TrendResponse(
            usn=student["usn"],
            name=student["name"],
            trend=[dict(t) for t in trend],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("trend_lookup_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
