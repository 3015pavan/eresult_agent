"""
Agent Endpoint — POST /api/v1/agent/run

Exposes the Phase 4 AcadExtract agent via a simple REST interface.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.common.observability import get_logger

logger = get_logger(__name__)
router = APIRouter()


class AgentRunRequest(BaseModel):
    goal: str = Field(
        ...,
        min_length=5,
        max_length=500,
        description="Natural language goal for the agent to accomplish.",
        examples=["Process all unprocessed result emails and save records"],
    )
    context: dict[str, Any] | None = Field(
        default=None,
        description="Optional key-value context to guide the agent (e.g. {'usn': '1MS23CS001'}).",
    )


class AgentRunResponse(BaseModel):
    run_id: str
    goal: str
    state: str
    steps_executed: int
    final_output: Any | None
    critic_score: float | None
    critic_reason: str | None
    error: str | None


@router.post("/run", response_model=AgentRunResponse, summary="Run agentic pipeline")
async def run_agent_endpoint(request: AgentRunRequest) -> AgentRunResponse:
    """
    Execute the AcadExtract agent with a goal string.

    The agent will:
      1. Plan a sequence of tool calls to achieve the goal.
      2. Execute each step, resolving inter-step dependencies.
      3. Validate the final output with a critic.
      4. Persist the run trace to the `agent_traces` table.
    """
    try:
        from src.phase4_agentic_layer.agent import run_agent

        agent_run = run_agent(goal=request.goal, context=request.context or {})

        return AgentRunResponse(
            run_id=agent_run.run_id,
            goal=agent_run.goal,
            state=agent_run.state.value if hasattr(agent_run.state, "value") else str(agent_run.state),
            steps_executed=len(agent_run.steps or []),
            final_output=agent_run.result,
            critic_score=getattr(agent_run, "critic_score", None),
            critic_reason=getattr(agent_run, "critic_reason", None),
            error=agent_run.error,
        )

    except Exception as exc:
        logger.error("agent_run_failed", goal=request.goal, error=str(exc))
        raise HTTPException(status_code=500, detail=f"Agent run failed: {exc}")


@router.get("/tools", summary="List available agent tools")
async def list_agent_tools() -> dict:
    """Return the registry of tools available to the agent."""
    try:
        from src.phase4_agentic_layer.tools import list_tools
        return {"tools": list_tools()}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
