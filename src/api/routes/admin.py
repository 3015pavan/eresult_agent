"""
Admin Endpoints.

Pipeline management endpoints for administrators:
  - Trigger email ingestion
  - View processing status
  - Reprocess failed documents
  - View agent traces
  - System statistics
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.common.observability import get_logger

logger = get_logger(__name__)
router = APIRouter()


# ── Request/Response Models ─────────────────────────────────────────


class IngestionTrigger(BaseModel):
    """Trigger email ingestion."""
    max_emails: int = Field(default=50, ge=1, le=500)
    since_hours: int = Field(default=24, ge=1, le=168)


class ReprocessRequest(BaseModel):
    """Reprocess a failed document."""
    attachment_id: str
    force: bool = False


class PipelineStatus(BaseModel):
    """Pipeline status response."""
    emails_processed_24h: int
    documents_parsed_24h: int
    records_extracted_24h: int
    pending_emails: int
    failed_documents: int
    agent_runs_24h: int
    avg_processing_time_ms: float


class AgentTraceResponse(BaseModel):
    """Agent trace response."""
    run_id: str
    final_state: str
    total_steps: int
    duration_ms: int
    steps: list[dict[str, Any]]


# ── Endpoints ───────────────────────────────────────────────────────


@router.post("/ingest")
async def trigger_ingestion(
    request: IngestionTrigger,
) -> dict:
    """Trigger email ingestion manually."""
    from src.phase4_agentic_layer.agent import AgentOrchestrator

    agent = AgentOrchestrator()
    run = await agent.run(
        context={
            "task": "manual_ingestion",
            "max_emails": request.max_emails,
            "since_hours": request.since_hours,
        },
    )

    return {
        "run_id": run.run_id,
        "state": run.final_state.value,
        "steps": run.total_steps,
        "duration_ms": run.total_duration_ms,
    }


@router.post("/reprocess")
async def reprocess_document(request: ReprocessRequest) -> dict:
    """Reprocess a failed document."""
    from src.phase4_agentic_layer.agent import AgentOrchestrator

    agent = AgentOrchestrator()
    run = await agent.process_single_document(
        attachment_id=request.attachment_id,
    )

    return {
        "run_id": run.run_id,
        "state": run.final_state.value,
        "steps": run.total_steps,
    }


@router.get("/status", response_model=PipelineStatus)
async def pipeline_status() -> PipelineStatus:
    """Get current pipeline status."""
    # In production, these would query actual metrics
    return PipelineStatus(
        emails_processed_24h=0,
        documents_parsed_24h=0,
        records_extracted_24h=0,
        pending_emails=0,
        failed_documents=0,
        agent_runs_24h=0,
        avg_processing_time_ms=0.0,
    )


@router.get("/traces/{run_id}", response_model=AgentTraceResponse)
async def get_agent_trace(run_id: str) -> AgentTraceResponse:
    """Get agent trace by run ID."""
    try:
        import asyncpg
        import json
        from src.common.config import get_settings

        pool = await asyncpg.create_pool(
            dsn=get_settings().database.url,
            min_size=1,
            max_size=2,
        )
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT run_id, final_state, total_steps,
                       steps_json,
                       EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000 AS duration_ms
                FROM agent_traces
                WHERE run_id = $1
                """,
                run_id,
            )

        await pool.close()

        if not row:
            raise HTTPException(status_code=404, detail="Trace not found")

        return AgentTraceResponse(
            run_id=row["run_id"],
            final_state=row["final_state"],
            total_steps=row["total_steps"],
            duration_ms=int(row["duration_ms"] or 0),
            steps=json.loads(row["steps_json"]),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("trace_lookup_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def system_statistics() -> dict:
    """Get system-wide statistics."""
    try:
        import asyncpg
        from src.common.config import get_settings

        pool = await asyncpg.create_pool(
            dsn=get_settings().database.url,
            min_size=1,
            max_size=2,
        )
        async with pool.acquire() as conn:
            stats = await conn.fetchrow(
                """
                SELECT
                    (SELECT COUNT(*) FROM students) AS total_students,
                    (SELECT COUNT(*) FROM student_results) AS total_results,
                    (SELECT COUNT(*) FROM email_metadata) AS total_emails,
                    (SELECT COUNT(*) FROM extractions) AS total_extractions,
                    (SELECT ROUND(AVG(current_cgpa)::numeric, 2) FROM students WHERE current_cgpa > 0) AS avg_cgpa,
                    (SELECT COUNT(*) FROM students WHERE active_backlogs > 0) AS students_with_backlogs
                """,
            )

        await pool.close()

        return dict(stats) if stats else {}

    except Exception as e:
        logger.error("stats_failed", error=str(e))
        return {"error": str(e)}
