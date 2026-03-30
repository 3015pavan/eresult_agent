"""
Agent State Machine — Phase 4.

Implements the AcadExtract autonomous agent with states:
  IDLE → PLANNING → EXECUTING → VERIFYING → COMPLETED | FAILED

The agent receives a high-level goal (e.g. "process new emails and extract results"),
breaks it into steps using the planner, executes tools, verifies outputs via the
critic, and stores episodic memory for future context.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class AgentState(str, Enum):
    IDLE        = "IDLE"
    PLANNING    = "PLANNING"
    EXECUTING   = "EXECUTING"
    VERIFYING   = "VERIFYING"
    COMPLETED   = "COMPLETED"
    FAILED      = "FAILED"


@dataclass
class Step:
    step_id:   str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    tool:      str = ""
    args:      dict = field(default_factory=dict)
    result:    Any  = None
    error:     Optional[str] = None
    started_at: float = 0.0
    elapsed_ms: float = 0.0


@dataclass
class AgentRun:
    run_id:    str = field(default_factory=lambda: str(uuid.uuid4()))
    goal:      str = ""
    state:     AgentState = AgentState.IDLE
    plan:      list[dict] = field(default_factory=list)
    steps:     list[Step] = field(default_factory=list)
    result:    Any = None
    error:     Optional[str] = None
    critic_score:  Optional[float] = None
    critic_reason: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    context:   dict = field(default_factory=dict)

    @property
    def elapsed_seconds(self) -> float:
        end = self.completed_at or time.time()
        return round(end - self.created_at, 2)

    def to_dict(self) -> dict:
        return {
            "run_id":       self.run_id,
            "goal":         self.goal,
            "state":        self.state.value,
            "steps":        len(self.steps),
            "result":       self.result,
            "error":        self.error,
            "elapsed_s":    self.elapsed_seconds,
        }


class AcadExtractAgent:
    """
    Autonomous agent that processes emails, extracts academic results,
    and answers queries using a tool registry + planner + critic.
    """

    MAX_STEPS    = 20   # safety limit to prevent infinite loops
    MAX_RETRIES  = 2    # retries per step on tool error

    def __init__(self):
        self._current_run: Optional[AgentRun] = None
        self._history: list[AgentRun] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self, goal: str, context: Optional[dict] = None) -> AgentRun:
        """
        Synchronously execute a goal end-to-end.
        Returns the completed AgentRun.
        """
        run = AgentRun(goal=goal, context=context or {})
        self._current_run = run

        try:
            self._transition(run, AgentState.PLANNING)
            run.plan = self._plan(run)
            logger.info("agent_plan run_id=%s steps=%d", run.run_id, len(run.plan))

            self._transition(run, AgentState.EXECUTING)
            self._execute_plan(run)

            self._transition(run, AgentState.VERIFYING)
            verdict = self._verify(run)
            run.critic_score  = verdict.get("score")
            run.critic_reason = verdict.get("reason")

            if verdict["pass"]:
                self._transition(run, AgentState.COMPLETED)
                run.result = self._collect_results(run)
            else:
                run.error = verdict.get("reason", "verification_failed")
                self._transition(run, AgentState.FAILED)

        except Exception as exc:
            logger.error("agent_run_error run_id=%s error=%s", run.run_id, str(exc))
            run.error = str(exc)
            self._transition(run, AgentState.FAILED)
        finally:
            run.completed_at = time.time()
            self._history.append(run)
            self._persist_trace(run)

        return run

    # ── State machine ─────────────────────────────────────────────────────────

    def _transition(self, run: AgentRun, new_state: AgentState) -> None:
        logger.info(
            "agent_state_transition run_id=%s %s -> %s",
            run.run_id, run.state.value, new_state.value,
        )
        run.state = new_state

    # ── Planning ──────────────────────────────────────────────────────────────

    def _plan(self, run: AgentRun) -> list[dict]:
        from .planner import create_plan
        return create_plan(run.goal, run.context)

    # ── Execution ─────────────────────────────────────────────────────────────

    def _execute_plan(self, run: AgentRun) -> None:
        from .executor import execute_step
        from .memory import MemoryStore

        memory = MemoryStore(run_id=run.run_id)
        step_results: dict[str, Any] = {}

        for i, plan_step in enumerate(run.plan):
            if len(run.steps) >= self.MAX_STEPS:
                logger.warning("agent_step_limit_reached: run_id=%s", run.run_id)
                break

            tool   = plan_step.get("tool", "")
            args   = plan_step.get("args", {})

            # Resolve dynamic args that reference previous step results
            resolved_args = _resolve_args(args, step_results)

            step = Step(tool=tool, args=resolved_args, started_at=time.time())
            run.steps.append(step)

            for attempt in range(self.MAX_RETRIES + 1):
                try:
                    execution = execute_step(tool, resolved_args, memory=step_results)
                    step.result = execution.output
                    step.error = execution.error
                    step.elapsed_ms = execution.duration_ms
                    step_results[f"step_{i}"] = step.result
                    break
                except Exception as exc:
                    step.error = str(exc)
                    logger.warning(
                        "agent_step_error step=%s attempt=%d error=%s",
                        tool, attempt + 1, str(exc),
                    )
                    if attempt == self.MAX_RETRIES:
                        # Non-fatal: continue to next step
                        break

            if not step.elapsed_ms:
                step.elapsed_ms = (time.time() - step.started_at) * 1000

            # Store in episodic memory
            memory.store(
                event_type=f"tool_call:{tool}",
                content={"args": resolved_args, "result_summary": str(step.result)[:200]},
            )

    # ── Verification ─────────────────────────────────────────────────────────

    def _verify(self, run: AgentRun) -> dict:
        from .critic import CriticAgent
        critic = CriticAgent()
        result = critic.evaluate(run)
        # CriticResult is a dataclass — convert to dict for the caller
        return {"pass": result.passed, "score": result.score, "reason": result.reason, "suggestions": result.suggestions}

    # ── Result collection ─────────────────────────────────────────────────────

    def _collect_results(self, run: AgentRun) -> dict:
        save_steps = [s for s in run.steps if s.tool == "save_results" and s.result]
        records_saved = sum(
            int(s.result.get("saved", 0)) for s in save_steps if isinstance(s.result, dict)
        )
        return {
            "goal": run.goal,
            "steps_executed": len(run.steps),
            "records_saved": records_saved,
            "elapsed_s": run.elapsed_seconds,
        }

    # ── Trace persistence ─────────────────────────────────────────────────────

    def _persist_trace(self, run: AgentRun) -> None:
        """Write agent run to agent_traces table."""
        import json
        try:
            from src.common.database import get_connection
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS agent_traces (
                            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            run_id      TEXT UNIQUE NOT NULL,
                            goal        TEXT,
                            state       TEXT,
                            steps_json  JSONB,
                            result_json JSONB,
                            error       TEXT,
                            elapsed_s   DECIMAL(8,2),
                            created_at  TIMESTAMPTZ DEFAULT NOW()
                        )
                    """)
                    # Ensure run_id column exists if table was created in a previous schema
                    cur.execute("""
                        DO $$ BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_name='agent_traces' AND column_name='run_id'
                            ) THEN
                                ALTER TABLE agent_traces ADD COLUMN run_id TEXT UNIQUE NOT NULL DEFAULT gen_random_uuid()::text;
                            END IF;
                        END $$;
                    """)
                    steps_data = [
                        {
                            "step_id": s.step_id, "tool": s.tool,
                            "args": s.args,
                            "result": (str(s.result)[:300] if s.result else None),
                            "error": s.error,
                            "elapsed_ms": round(s.elapsed_ms, 1),
                        }
                        for s in run.steps
                    ]
                    cur.execute("""
                        INSERT INTO agent_traces (run_id, goal, state, steps_json, result_json, error, elapsed_s)
                        VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
                        ON CONFLICT (run_id) DO UPDATE SET
                            state       = EXCLUDED.state,
                            steps_json  = EXCLUDED.steps_json,
                            result_json = EXCLUDED.result_json,
                            error       = EXCLUDED.error
                    """, (
                        run.run_id, run.goal, run.state.value,
                        json.dumps(steps_data),
                        json.dumps(run.result),
                        run.error,
                        run.elapsed_seconds,
                    ))
        except Exception as exc:
            logger.warning("agent_trace_persist_failed: %s", exc)

    def get_history(self, limit: int = 10) -> list[dict]:
        return [r.to_dict() for r in self._history[-limit:]]


# ── Helper ─────────────────────────────────────────────────────────────────

def _resolve_args(args: dict, step_results: dict) -> dict:
    """
    Replace template references like "{step_0.records}" with actual values.
    """
    resolved = {}
    for k, v in args.items():
        if isinstance(v, str) and v.startswith("{") and v.endswith("}"):
            ref = v[1:-1]  # e.g. "step_0.records"
            parts = ref.split(".", 1)
            step_key = parts[0]
            sub_key  = parts[1] if len(parts) > 1 else None
            val = step_results.get(step_key)
            if val is not None and sub_key:
                val = val.get(sub_key) if isinstance(val, dict) else val
            resolved[k] = val if val is not None else v
        else:
            resolved[k] = v
    return resolved


def run_agent(goal: str, context: Optional[dict] = None) -> "AgentRun":
    """Convenience function — create agent, run goal, return AgentRun object."""
    agent = AcadExtractAgent()
    run   = agent.run(goal, context)
    return run
