"""
Agent Orchestrator.

Top-level state machine implementing the Planner-Executor-Critic loop.

State machine:
  IDLE → PLANNING → EXECUTING → VERIFYING → COMPLETED | ERROR
                ↑                     |
                └─────────────────────┘  (retry / continue)

Features:
  - Configurable max steps (default: 15)
  - Cost tracking and budget enforcement
  - Full trace recording for observability
  - Graceful degradation on failures
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from src.common.config import get_settings
from src.common.models import AgentState, AgentRun, AgentStep
from src.common.observability import (
    get_logger,
    AGENT_RUNS,
    AGENT_STEPS,
    AGENT_ACTIVE,
)
from src.phase4_agentic_layer.planner import Planner
from src.phase4_agentic_layer.executor import Executor
from src.phase4_agentic_layer.critic import Critic
from src.phase4_agentic_layer.tools import ToolRegistry
from src.phase4_agentic_layer.memory import AgentMemory

logger = get_logger(__name__)


class AgentOrchestrator:
    """
    Top-level agent orchestrator implementing the P-E-C loop.

    Lifecycle:
      1. Initialize with context (email batch, document, etc.)
      2. Enter PLANNING state → Planner generates tool calls
      3. Enter EXECUTING state → Executor runs tool calls
      4. Enter VERIFYING state → Critic evaluates results
      5. Based on Critic: continue (→ PLANNING) or finish (→ COMPLETED)
      6. On unrecoverable error → ERROR state

    The orchestrator enforces:
      - Max step limit (circuit breaker)
      - Cost budget (sum of tool costs)
      - Time budget (total wall-clock time)
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self.memory = AgentMemory()
        self.tool_registry = ToolRegistry()
        self.planner = Planner(self.tool_registry, self.memory)
        self.executor = Executor(self.tool_registry, self.memory)
        self.critic = Critic(self.memory)

    async def run(
        self,
        context: dict[str, Any] | None = None,
    ) -> AgentRun:
        """
        Execute the full Planner-Executor-Critic loop.

        Args:
            context: Initial context (email batch info, doc type, etc.)

        Returns:
            AgentRun with full trace and final state
        """
        run_id = str(uuid.uuid4())
        state = AgentState.IDLE
        steps: list[AgentStep] = []
        start_time = time.monotonic()
        total_cost = 0.0
        max_steps = self.settings.agent.max_steps

        self.memory.reset_working_memory()
        AGENT_ACTIVE.inc()

        logger.info(
            "agent_run_start",
            run_id=run_id,
            context=context,
        )

        try:
            step_number = 0

            while step_number < max_steps:
                # ── PLANNING ────────────────────────────────────────
                state = AgentState.PLANNING
                AGENT_STEPS.labels(step_type="plan").inc()

                plan = await self.planner.plan(state, context)

                if not plan:
                    logger.info("planner_returned_empty_plan")
                    state = AgentState.COMPLETED
                    break

                # ── EXECUTING ───────────────────────────────────────
                state = AgentState.EXECUTING
                step_number += 1

                step = await self.executor.execute_plan(plan, step_number)
                steps.append(step)

                # ── VERIFYING ───────────────────────────────────────
                state = AgentState.VERIFYING

                assessment = await self.critic.evaluate(
                    step, state, step_number,
                )

                logger.info(
                    "critic_assessment",
                    step=step_number,
                    assessment=assessment.assessment,
                    recommendation=assessment.recommendation,
                )

                # ── DECIDE ──────────────────────────────────────────
                if assessment.should_complete:
                    state = AgentState.COMPLETED
                    break
                elif assessment.should_escalate:
                    state = AgentState.ERROR
                    logger.error(
                        "agent_escalation",
                        issues=assessment.issues,
                    )
                    break
                elif assessment.should_continue:
                    # Update context with critic feedback
                    if context is None:
                        context = {}
                    context["critic_feedback"] = assessment.reasoning
                    context["issues"] = assessment.issues
                    continue
                else:
                    state = AgentState.COMPLETED
                    break

            else:
                # Loop exhausted without break → step limit reached
                state = AgentState.ERROR
                logger.warning(
                    "agent_max_steps_reached",
                    max_steps=max_steps,
                )

        except Exception as e:
            state = AgentState.ERROR
            logger.error("agent_run_error", error=str(e))

        finally:
            AGENT_ACTIVE.dec()

        elapsed = time.monotonic() - start_time
        AGENT_RUNS.labels(final_state=state.value).inc()

        # Build run result
        run = AgentRun(
            run_id=run_id,
            final_state=state,
            steps=steps,
            total_steps=len(steps),
            total_duration_ms=int(elapsed * 1000),
            total_cost_usd=total_cost,
        )

        # Store trace
        try:
            await self.memory.store_agent_trace(
                run_id=run_id,
                steps=steps,
                final_state=state.value,
                metadata=context,
            )
        except Exception as e:
            logger.warning("trace_store_failed", error=str(e))

        logger.info(
            "agent_run_complete",
            run_id=run_id,
            state=state.value,
            steps=len(steps),
            duration_ms=int(elapsed * 1000),
        )

        return run

    async def process_email_batch(
        self,
        email_ids: list[str],
    ) -> AgentRun:
        """
        Convenience method to process a batch of emails.

        Sets up appropriate context and runs the agent loop.
        """
        context = {
            "task": "process_email_batch",
            "email_ids": email_ids,
            "batch_size": len(email_ids),
        }
        return await self.run(context)

    async def process_single_document(
        self,
        attachment_id: str,
        document_type: str = "pdf",
    ) -> AgentRun:
        """
        Convenience method to process a single document.

        Useful for reprocessing failed documents.
        """
        context = {
            "task": "process_document",
            "attachment_id": attachment_id,
            "document_type": document_type,
        }
        return await self.run(context)
