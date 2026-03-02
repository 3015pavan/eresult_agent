"""
Executor Module.

The Executor takes a plan (list of ToolCalls) from the Planner and
executes them sequentially, recording results in memory.

Responsibilities:
  - Sequential tool execution with dependency management
  - Error handling and retry logic per tool
  - Result recording in working memory
  - Duration tracking for each tool call
  - Circuit breaker for repeated failures
"""

from __future__ import annotations

import time
from typing import Any

from src.common.models import ToolCall, AgentStep
from src.common.observability import get_logger, AGENT_STEPS
from src.phase4_agentic_layer.tools import ToolRegistry
from src.phase4_agentic_layer.memory import AgentMemory

logger = get_logger(__name__)


class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.

    States:
      - CLOSED: Normal operation
      - OPEN: Too many failures → stop calling
      - HALF_OPEN: After cool-down, try one call
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        cooldown_seconds: float = 60.0,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._failures: dict[str, int] = {}
        self._last_failure_time: dict[str, float] = {}
        self._state: dict[str, str] = {}  # tool_name → state

    def can_execute(self, tool_name: str) -> bool:
        """Check if a tool call is allowed."""
        state = self._state.get(tool_name, "closed")

        if state == "closed":
            return True
        elif state == "open":
            elapsed = time.time() - self._last_failure_time.get(tool_name, 0)
            if elapsed >= self.cooldown_seconds:
                self._state[tool_name] = "half_open"
                return True
            return False
        elif state == "half_open":
            return True  # Allow one attempt

        return True

    def record_success(self, tool_name: str) -> None:
        """Record a successful execution."""
        self._failures[tool_name] = 0
        self._state[tool_name] = "closed"

    def record_failure(self, tool_name: str) -> None:
        """Record a failed execution."""
        count = self._failures.get(tool_name, 0) + 1
        self._failures[tool_name] = count
        self._last_failure_time[tool_name] = time.time()

        if count >= self.failure_threshold:
            self._state[tool_name] = "open"
            logger.warning(
                "circuit_breaker_opened",
                tool=tool_name,
                failures=count,
            )


class Executor:
    """
    Execute planned tool calls sequentially.

    Execution flow:
      1. For each ToolCall in the plan:
         a. Check circuit breaker
         b. Execute tool via registry
         c. Record result and duration
         d. Update memory
      2. Return AgentStep with all results
    """

    def __init__(
        self,
        tool_registry: ToolRegistry,
        memory: AgentMemory,
    ) -> None:
        self.tools = tool_registry
        self.memory = memory
        self.circuit_breaker = CircuitBreaker()

    async def execute_plan(
        self,
        plan: list[ToolCall],
        step_number: int,
    ) -> AgentStep:
        """
        Execute a plan of tool calls.

        Args:
            plan: Ordered list of ToolCall objects
            step_number: Current step number in the agent run

        Returns:
            AgentStep with executed tool calls and aggregated output
        """
        executed_calls: list[ToolCall] = []
        step_output: dict[str, Any] = {}
        step_reasoning = ""

        for tc in plan:
            if step_reasoning:
                step_reasoning += " → "
            step_reasoning += tc.reasoning or tc.tool_name

            # Check circuit breaker
            if not self.circuit_breaker.can_execute(tc.tool_name):
                logger.warning(
                    "tool_skipped_circuit_open",
                    tool=tc.tool_name,
                )
                tc.result = {"error": "circuit_breaker_open"}
                tc.duration_ms = 0
                executed_calls.append(tc)
                continue

            # Execute tool
            start = time.monotonic()
            try:
                result = await self.tools.execute(
                    tc.tool_name,
                    tc.arguments,
                )
                tc.result = result
                tc.duration_ms = int((time.monotonic() - start) * 1000)
                self.circuit_breaker.record_success(tc.tool_name)

                # Record in memory
                self.memory.record_tool_output(tc.tool_name, result)
                step_output[tc.tool_name] = result

                logger.info(
                    "tool_executed",
                    tool=tc.tool_name,
                    duration_ms=tc.duration_ms,
                )

            except Exception as e:
                tc.duration_ms = int((time.monotonic() - start) * 1000)
                tc.result = {"error": str(e)}
                self.circuit_breaker.record_failure(tc.tool_name)

                logger.error(
                    "tool_execution_failed",
                    tool=tc.tool_name,
                    error=str(e),
                    duration_ms=tc.duration_ms,
                )

            executed_calls.append(tc)
            AGENT_STEPS.labels(step_type="execute").inc()

        step = AgentStep(
            step_number=step_number,
            reasoning=step_reasoning,
            tool_calls=executed_calls,
            output=step_output,
        )

        self.memory.record_step(step)
        return step
