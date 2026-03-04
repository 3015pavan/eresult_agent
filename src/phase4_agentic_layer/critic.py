"""
Critic — Phase 4.

Evaluates completed agent runs and individual step outputs.
Uses rule-based checks and optional LLM evaluation.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

_GROQ_KEY = os.getenv("GROQ_API_KEY", "")


@dataclass
class CriticResult:
    passed: bool
    score: float          # 0.0 – 1.0
    reason: str
    suggestions: list[str]


class CriticAgent:
    """
    Evaluates an agent run or individual step result.

    Rule-based checks are always applied.  When a Groq key is available,
    an LLM critique is solicited as well and the scores are averaged.
    """

    def evaluate_step(
        self,
        tool: str,
        args: dict,
        output: Any,
        error: str | None,
    ) -> CriticResult:
        """Evaluate a single tool invocation."""
        if error:
            return CriticResult(
                passed=False,
                score=0.0,
                reason=f"Tool {tool!r} raised error: {error}",
                suggestions=[f"Retry {tool} with adjusted arguments or skip this step."],
            )

        score, reasons, suggestions = self._rule_check_step(tool, output)

        return CriticResult(
            passed=score >= 0.5,
            score=score,
            reason="; ".join(reasons) or "OK",
            suggestions=suggestions,
        )

    def evaluate(self, run: Any) -> CriticResult:
        """
        Evaluate a completed AgentRun.

        `run` is expected to have attributes:
            .goal  str
            .steps list[dict]   each dict has keys: tool, output, error
            .final_output Any
        """
        score, reasons, suggestions = 1.0, [], []

        steps = getattr(run, "steps", []) or []
        # Steps can be Step dataclass objects or plain dicts
        def _step_get(s: Any, key: str, default: Any = None) -> Any:
            if isinstance(s, dict):
                return s.get(key, default)
            return getattr(s, key, default)

        failed = [s for s in steps if _step_get(s, "error")]
        succeeded = [s for s in steps if not _step_get(s, "error")]

        if not steps:
            return CriticResult(False, 0.0, "No steps were executed.", ["Check planning logic."])

        # Penalise failed steps
        if failed:
            penalty = min(len(failed) * 0.2, 0.6)
            score -= penalty
            reasons.append(f"{len(failed)} step(s) failed")
            for s in failed:
                suggestions.append(f"Step {_step_get(s, 'tool')} failed: {_step_get(s, 'error')}")

        # Check that extraction/save steps ran
        tool_names = {_step_get(s, "tool") for s in succeeded}
        goal_lower = getattr(run, "goal", "").lower()

        if any(w in goal_lower for w in ("email", "extract", "process")):
            if "extract_records" not in tool_names:
                score -= 0.15
                reasons.append("extraction step did not run")
                suggestions.append("Ensure extract_records is in the plan.")
            if "save_results" not in tool_names and "save" in goal_lower:
                score -= 0.10
                reasons.append("save_results step did not run")

        # LLM critique (optional)
        if _GROQ_KEY and steps:
            llm_score, llm_reason = self._llm_critique(run)
            score = (score + llm_score) / 2
            if llm_reason:
                reasons.append(f"LLM: {llm_reason}")

        score = max(0.0, min(1.0, score))
        return CriticResult(
            passed=score >= 0.5,
            score=round(score, 3),
            reason="; ".join(reasons) or "Run completed successfully",
            suggestions=suggestions,
        )

    # ── Internals ─────────────────────────────────────────────────────────────

    def _rule_check_step(
        self, tool: str, output: Any
    ) -> tuple[float, list[str], list[str]]:
        """Per-tool heuristic checks."""
        reasons: list[str] = []
        suggestions: list[str] = []
        score = 1.0

        if output is None:
            return 0.3, ["Output is None"], [f"{tool} returned no data — check tool impl."]

        if tool == "extract_records":
            records = output if isinstance(output, list) else output.get("records", []) if isinstance(output, dict) else []
            if not records:
                score = 0.4
                reasons.append("No records extracted")
                suggestions.append("Email body may be too sparse; try LLM extractor.")
            else:
                for r in records:
                    if not r.get("usn"):
                        score -= 0.1
                        reasons.append("Record missing USN")

        if tool == "validate":
            if isinstance(output, dict):
                invalid = output.get("invalid_count", 0)
                if invalid:
                    score -= invalid * 0.05
                    reasons.append(f"{invalid} invalid record(s) after validation")

        if tool == "save_results":
            saved = output.get("saved", 0) if isinstance(output, dict) else 0
            if saved == 0:
                score = 0.4
                reasons.append("No records were saved")

        return max(0.0, score), reasons, suggestions

    def _llm_critique(self, run: Any) -> tuple[float, str]:
        """Ask Groq to score the run quality."""
        import json
        import httpx

        def _sg(s: Any, key: str, default: Any = None) -> Any:
            if isinstance(s, dict):
                return s.get(key, default)
            return getattr(s, key, default)

        steps_summary = [
            {"tool": _sg(s, "tool"), "ok": not _sg(s, "error"),
             "output_len": len(str(_sg(s, "output", "")))}
            for s in (getattr(run, "steps", []) or [])
        ]
        prompt = (
            f"Goal: {getattr(run, 'goal', '')}\n"
            f"Steps: {json.dumps(steps_summary)}\n\n"
            "Rate this agent run from 0.0 to 1.0. Respond with just a JSON object: "
            '{"score": <float>, "reason": "<1-sentence reason>"}'
        )
        try:
            resp = httpx.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {_GROQ_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0,
                    "max_tokens": 80,
                },
                timeout=10.0,
            )
            resp.raise_for_status()
            raw = resp.json()["choices"][0]["message"]["content"]
            data = json.loads(raw)
            return float(data.get("score", 0.5)), str(data.get("reason", ""))
        except Exception as exc:
            logger.debug("critic llm_critique failed: %s", exc)
            return 0.5, ""
