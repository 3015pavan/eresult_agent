"""
Critic Module.

The Critic evaluates the results of executed steps and provides
feedback to guide the next planning iteration.

Responsibilities:
  - Evaluate step outcomes against expected results
  - Detect anomalies (too few records, all failures, etc.)
  - Generate reflections for the planner
  - Decide: continue, retry, escalate, or complete
"""

from __future__ import annotations

import json
from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import get_settings
from src.common.models import AgentStep, AgentState
from src.common.observability import get_logger
from src.phase4_agentic_layer.memory import AgentMemory

logger = get_logger(__name__)


CRITIC_SYSTEM_PROMPT = """You are the critic component of an autonomous academic result extraction system.

Your job: Evaluate the results of the last execution step and decide what to do next.

Evaluate based on:
1. Did the tools execute successfully?
2. Were the results reasonable? (e.g., expected number of records)
3. Are there errors that need correction?
4. Is the overall pipeline making progress?

Output JSON with:
{
  "assessment": "success|partial_success|failure",
  "issues": ["list of identified issues"],
  "reflections": ["insights for future planning"],
  "recommendation": "continue|retry|escalate|complete",
  "reasoning": "explanation of your assessment"
}"""


class CriticAssessment:
    """Result of critic evaluation."""

    def __init__(
        self,
        assessment: str,
        issues: list[str],
        reflections: list[str],
        recommendation: str,
        reasoning: str,
    ) -> None:
        self.assessment = assessment
        self.issues = issues
        self.reflections = reflections
        self.recommendation = recommendation
        self.reasoning = reasoning

    @property
    def should_continue(self) -> bool:
        return self.recommendation in ("continue", "retry")

    @property
    def should_complete(self) -> bool:
        return self.recommendation == "complete"

    @property
    def should_escalate(self) -> bool:
        return self.recommendation == "escalate"


class Critic:
    """
    Evaluate execution results and guide next steps.

    Evaluation criteria:
      1. Tool success rate: % of tools that executed without error
      2. Data quality: Are extracted records valid?
      3. Pipeline progress: Are we moving forward?
      4. Cost efficiency: Are we using expensive tools unnecessarily?
      5. Anomaly detection: Unexpected patterns in results
    """

    def __init__(self, memory: AgentMemory) -> None:
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

    async def evaluate(
        self,
        step: AgentStep,
        state: AgentState,
        total_steps: int,
    ) -> CriticAssessment:
        """
        Evaluate the last execution step.

        Uses a combination of:
          1. Heuristic checks (fast, deterministic)
          2. LLM evaluation (when heuristics are inconclusive)
        """
        # First: fast heuristic checks
        heuristic = self._heuristic_evaluation(step, total_steps)
        if heuristic:
            for r in heuristic.reflections:
                self.memory.add_reflection(r)
            return heuristic

        # Fall back to LLM evaluation
        try:
            llm_result = await self._llm_evaluation(step, state, total_steps)
            for r in llm_result.reflections:
                self.memory.add_reflection(r)
            return llm_result
        except Exception as e:
            logger.error("critic_llm_failed", error=str(e))
            return CriticAssessment(
                assessment="partial_success",
                issues=[f"Critic LLM failed: {e}"],
                reflections=["Critic evaluation was degraded"],
                recommendation="continue",
                reasoning="LLM evaluation failed, proceeding with caution",
            )

    def _heuristic_evaluation(
        self,
        step: AgentStep,
        total_steps: int,
    ) -> CriticAssessment | None:
        """
        Fast rule-based evaluation.

        Returns None if heuristics are inconclusive.
        """
        if not step.tool_calls:
            return CriticAssessment(
                assessment="failure",
                issues=["No tool calls in step"],
                reflections=["Empty step detected — planner may be stuck"],
                recommendation="retry",
                reasoning="Step had no tool calls to execute",
            )

        # Check for all-failure
        errors = [
            tc for tc in step.tool_calls
            if isinstance(tc.result, dict) and "error" in tc.result
        ]

        if len(errors) == len(step.tool_calls):
            return CriticAssessment(
                assessment="failure",
                issues=[
                    f"{tc.tool_name}: {tc.result.get('error', 'unknown')}"
                    for tc in errors
                ],
                reflections=[
                    "All tools failed — possible infrastructure issue",
                ],
                recommendation="retry" if total_steps < self.settings.agent.max_steps - 2 else "escalate",
                reasoning="All tool calls in this step failed",
            )

        # Check step limit
        if total_steps >= self.settings.agent.max_steps:
            return CriticAssessment(
                assessment="partial_success",
                issues=["Maximum step limit reached"],
                reflections=["Step limit reached — saving progress"],
                recommendation="complete",
                reasoning=f"Reached max steps ({self.settings.agent.max_steps})",
            )

        # Check for completion signals
        for tc in step.tool_calls:
            if tc.tool_name == "store_records" and isinstance(tc.result, dict):
                if tc.result.get("status") == "stored":
                    return CriticAssessment(
                        assessment="success",
                        issues=[],
                        reflections=["Records stored successfully"],
                        recommendation="complete",
                        reasoning="Records have been validated and stored",
                    )

        # Inconclusive — return None to trigger LLM evaluation
        return None

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def _llm_evaluation(
        self,
        step: AgentStep,
        state: AgentState,
        total_steps: int,
    ) -> CriticAssessment:
        """LLM-based evaluation for complex cases."""
        # Build context
        step_summary = {
            "step_number": step.step_number,
            "reasoning": step.reasoning,
            "tool_calls": [
                {
                    "tool": tc.tool_name,
                    "arguments": tc.arguments,
                    "result": str(tc.result)[:500],
                    "duration_ms": tc.duration_ms,
                    "has_error": isinstance(tc.result, dict) and "error" in tc.result,
                }
                for tc in step.tool_calls
            ],
            "output": str(step.output)[:1000] if step.output else None,
        }

        user_message = (
            f"State: {state.value}\n"
            f"Total steps so far: {total_steps}\n"
            f"Max steps allowed: {self.settings.agent.max_steps}\n\n"
            f"Last step:\n{json.dumps(step_summary, indent=2, default=str)}"
        )

        response = await self.client.chat.completions.create(
            model=self.settings.agent.planner_model,
            messages=[
                {"role": "system", "content": CRITIC_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0,
            response_format={"type": "json_object"},
            max_tokens=1024,
        )

        raw = response.choices[0].message.content
        data = json.loads(raw)

        return CriticAssessment(
            assessment=data.get("assessment", "partial_success"),
            issues=data.get("issues", []),
            reflections=data.get("reflections", []),
            recommendation=data.get("recommendation", "continue"),
            reasoning=data.get("reasoning", ""),
        )
