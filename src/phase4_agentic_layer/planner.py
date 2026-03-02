"""
Planner Module.

The Planner is the first component of the Planner-Executor-Critic loop.
It takes the current state and produces a plan of tool calls to execute.

Uses GPT-4o with function-calling to generate structured plans.
Includes context from:
  - Current working memory
  - Recent agent steps
  - Tool specifications
  - Past successful traces (few-shot)
"""

from __future__ import annotations

import json
from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import get_settings
from src.common.models import ToolCall, AgentState
from src.common.observability import get_logger
from src.phase4_agentic_layer.tools import ToolRegistry
from src.phase4_agentic_layer.memory import AgentMemory

logger = get_logger(__name__)


PLANNER_SYSTEM_PROMPT = """You are the planning component of an autonomous academic result extraction system.

Your job: Given the current state and available tools, produce a PLAN — an ordered list of tool calls to execute next.

Context:
- You process academic result emails with PDF/Excel/image attachments
- The pipeline: Fetch emails → Classify → Dedup → Parse documents → Extract records → Validate → Store
- Each tool call should advance the pipeline toward completion

Rules:
1. Output a JSON array of tool calls, each with "tool", "arguments", and "reasoning"
2. Plan 1-5 tool calls at a time (not the entire pipeline)
3. Consider dependencies: some tools need outputs from previous tools
4. If validation fails, plan correction steps
5. If all emails are processed, plan a completion step
6. Consider cost: use cheaper tools first, LLM tools only when needed

Current state will be provided in the user message."""


class Planner:
    """
    Generate execution plans from current agent state.

    The planner uses LLM function-calling to decide which tools
    to invoke next, based on:
      1. Current pipeline state
      2. Working memory (recent steps, tool outputs)
      3. Available tools
      4. Past traces for similar documents
    """

    def __init__(
        self,
        tool_registry: ToolRegistry,
        memory: AgentMemory,
    ) -> None:
        self.tools = tool_registry
        self.memory = memory
        self.settings = get_settings()
        self._client = None

    @property
    def client(self):
        """Lazy-init OpenAI client."""
        if self._client is None:
            import openai
            self._client = openai.AsyncOpenAI(
                api_key=self.settings.llm.providers["openai"]["api_key"],
            )
        return self._client

    async def plan(
        self,
        state: AgentState,
        context: dict[str, Any] | None = None,
    ) -> list[ToolCall]:
        """
        Generate the next set of tool calls.

        Args:
            state: Current agent state
            context: Additional context (email batch info, etc.)

        Returns:
            Ordered list of ToolCall objects to execute
        """
        # Build context message
        user_message = self._build_context_message(state, context)

        # Get tool schemas for function calling
        tool_schemas = self.tools.get_schemas()

        # Call LLM for plan generation
        plan_data = await self._generate_plan(user_message, tool_schemas)

        # Convert to ToolCall objects
        tool_calls = []
        for item in plan_data:
            tc = ToolCall(
                tool_name=item["tool"],
                arguments=item.get("arguments", {}),
                reasoning=item.get("reasoning", ""),
            )
            tool_calls.append(tc)

        logger.info(
            "plan_generated",
            state=state.value,
            num_calls=len(tool_calls),
            tools=[tc.tool_name for tc in tool_calls],
        )

        return tool_calls

    def _build_context_message(
        self,
        state: AgentState,
        context: dict[str, Any] | None,
    ) -> str:
        """Build the context message for the planner LLM."""
        parts = [
            f"CURRENT STATE: {state.value}",
            "",
        ]

        # Recent steps
        recent = self.memory.get_context_window(max_steps=5)
        if recent:
            parts.append("RECENT STEPS:")
            for step in recent:
                parts.append(
                    f"  Step {step['step']}: {step['reasoning']}"
                )
                for tc in step["tool_calls"]:
                    parts.append(
                        f"    → {tc['tool']}: {tc['result_summary']}"
                    )
            parts.append("")

        # Reflections
        reflections = self.memory.get_reflections()
        if reflections:
            parts.append("REFLECTIONS:")
            for r in reflections[-3:]:
                parts.append(f"  - {r}")
            parts.append("")

        # Additional context
        if context:
            parts.append("CONTEXT:")
            for k, v in context.items():
                parts.append(f"  {k}: {v}")
            parts.append("")

        # Available tools summary
        tools = self.tools.list_tools()
        parts.append("AVAILABLE TOOLS:")
        for tool in tools:
            parts.append(f"  - {tool.name}: {tool.description[:80]}")

        return "\n".join(parts)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=15),
    )
    async def _generate_plan(
        self,
        user_message: str,
        tool_schemas: list[dict],
    ) -> list[dict[str, Any]]:
        """Call LLM to generate a plan."""
        response = await self.client.chat.completions.create(
            model=self.settings.agent.planner_model,
            messages=[
                {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
            max_tokens=2048,
        )

        raw = response.choices[0].message.content
        data = json.loads(raw)

        # Handle both {"plan": [...]} and [...] formats
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "plan" in data:
            return data["plan"]
        elif isinstance(data, dict) and "tool_calls" in data:
            return data["tool_calls"]
        else:
            logger.warning("unexpected_plan_format", data=str(data)[:200])
            return []
