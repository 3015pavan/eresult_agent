"""
Agent Memory System.

Two-tier memory for the agentic layer:
  1. Short-term (Redis): Current session context, recent tool outputs,
     working set of records being processed
  2. Long-term (PostgreSQL): Historical agent traces, learned patterns,
     extraction corrections, performance baselines

Reflection storage: The agent can store and retrieve insights about
past extraction runs to improve future behavior.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from src.common.config import get_settings
from src.common.models import AgentStep, ToolCall
from src.common.observability import get_logger

logger = get_logger(__name__)


@dataclass
class MemoryEntry:
    """A single memory entry."""

    key: str
    value: Any
    timestamp: float = field(default_factory=time.time)
    ttl: int | None = None  # seconds
    source: str = "agent"  # agent | tool | user | system
    importance: float = 0.5  # 0.0-1.0


class AgentMemory:
    """
    Two-tier memory system for the agent.

    Short-term memory (Redis):
      - Current email batch being processed
      - Tool call results from current run
      - In-progress extraction state
      - Conversation context

    Long-term memory (PostgreSQL):
      - Agent trace history
      - Extraction pattern library
      - Correction history
      - Performance baselines per document type
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._short_term: dict[str, MemoryEntry] = {}
        self._redis_client = None
        self._db_pool = None

        # Working memory for current run
        self._current_steps: list[AgentStep] = []
        self._tool_outputs: dict[str, Any] = {}
        self._reflections: list[str] = []

    @property
    def redis(self):
        """Lazy-init Redis client."""
        if self._redis_client is None:
            import redis.asyncio as redis
            self._redis_client = redis.Redis(
                host=self.settings.redis.host,
                port=self.settings.redis.port,
                db=self.settings.redis.db,
                decode_responses=True,
            )
        return self._redis_client

    # ── Short-term Memory ───────────────────────────────────────────

    async def store_short(
        self,
        key: str,
        value: Any,
        ttl: int = 3600,
    ) -> None:
        """Store value in short-term (Redis) memory."""
        serialized = json.dumps(value, default=str)

        try:
            await self.redis.setex(
                f"agent:memory:{key}",
                ttl,
                serialized,
            )
        except Exception:
            # Fallback to in-memory
            self._short_term[key] = MemoryEntry(
                key=key,
                value=value,
                ttl=ttl,
            )

    async def recall_short(self, key: str) -> Any | None:
        """Recall value from short-term memory."""
        try:
            raw = await self.redis.get(f"agent:memory:{key}")
            if raw:
                return json.loads(raw)
        except Exception:
            entry = self._short_term.get(key)
            if entry:
                if entry.ttl and (time.time() - entry.timestamp) > entry.ttl:
                    del self._short_term[key]
                    return None
                return entry.value

        return None

    async def clear_short(self, pattern: str = "*") -> None:
        """Clear short-term memory entries matching pattern."""
        try:
            keys = []
            async for key in self.redis.scan_iter(
                f"agent:memory:{pattern}",
            ):
                keys.append(key)
            if keys:
                await self.redis.delete(*keys)
        except Exception:
            self._short_term.clear()

    # ── Working Memory (in-process) ─────────────────────────────────

    def record_step(self, step: AgentStep) -> None:
        """Record an agent step in working memory."""
        self._current_steps.append(step)

    def record_tool_output(self, tool_name: str, output: Any) -> None:
        """Record tool output for context."""
        self._tool_outputs[tool_name] = output

    def add_reflection(self, reflection: str) -> None:
        """Add an agent reflection."""
        self._reflections.append(reflection)
        logger.info("agent_reflection", reflection=reflection)

    def get_context_window(self, max_steps: int = 10) -> list[dict]:
        """
        Get recent steps as context for the planner.

        Returns the last N steps with their tool calls and reasoning.
        """
        recent = self._current_steps[-max_steps:]
        return [
            {
                "step": s.step_number,
                "reasoning": s.reasoning,
                "tool_calls": [
                    {"tool": tc.tool_name, "result_summary": str(tc.result)[:200]}
                    for tc in s.tool_calls
                ],
                "output": str(s.output)[:500] if s.output else None,
            }
            for s in recent
        ]

    def get_reflections(self) -> list[str]:
        """Get all reflections from current run."""
        return list(self._reflections)

    # ── Long-term Memory (PostgreSQL) ───────────────────────────────

    async def store_agent_trace(
        self,
        run_id: str,
        steps: list[AgentStep],
        final_state: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Store complete agent trace to PostgreSQL."""
        if self._db_pool is None:
            await self._init_db()

        if self._db_pool is None:
            logger.warning("db_unavailable_skipping_trace_store")
            return

        trace_json = json.dumps(
            [
                {
                    "step": s.step_number,
                    "reasoning": s.reasoning,
                    "tool_calls": [
                        {
                            "tool": tc.tool_name,
                            "args": tc.arguments,
                            "result": str(tc.result)[:1000],
                            "duration": tc.duration_ms,
                        }
                        for tc in s.tool_calls
                    ],
                }
                for s in steps
            ],
            default=str,
        )

        async with self._db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_traces (
                    run_id, steps_json, final_state,
                    total_steps, metadata
                ) VALUES ($1, $2::jsonb, $3, $4, $5::jsonb)
                """,
                run_id,
                trace_json,
                final_state,
                len(steps),
                json.dumps(metadata or {}),
            )

        logger.info(
            "agent_trace_stored",
            run_id=run_id,
            steps=len(steps),
        )

    async def recall_similar_traces(
        self,
        document_type: str,
        limit: int = 5,
    ) -> list[dict]:
        """
        Recall similar past traces for few-shot learning.

        Finds traces that processed similar document types and
        completed successfully — useful for the planner.
        """
        if self._db_pool is None:
            await self._init_db()

        if self._db_pool is None:
            return []

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT run_id, steps_json, final_state, total_steps
                FROM agent_traces
                WHERE final_state = 'completed'
                  AND metadata->>'document_type' = $1
                ORDER BY created_at DESC
                LIMIT $2
                """,
                document_type,
                limit,
            )

        return [
            {
                "run_id": r["run_id"],
                "steps": json.loads(r["steps_json"]),
                "total_steps": r["total_steps"],
            }
            for r in rows
        ]

    async def store_correction_pattern(
        self,
        error_type: str,
        original_value: str,
        corrected_value: str,
        context: dict[str, Any],
    ) -> None:
        """Store a correction pattern for future reference."""
        key = f"correction:{error_type}:{hash(original_value)}"
        await self.store_short(
            key,
            {
                "error_type": error_type,
                "original": original_value,
                "corrected": corrected_value,
                "context": context,
            },
            ttl=86400 * 30,  # 30 days
        )

    async def get_performance_baseline(
        self,
        document_type: str,
    ) -> dict[str, float] | None:
        """Get performance baseline for a document type."""
        key = f"baseline:{document_type}"
        return await self.recall_short(key)

    async def update_performance_baseline(
        self,
        document_type: str,
        metrics: dict[str, float],
    ) -> None:
        """Update performance baseline with exponential moving average."""
        existing = await self.get_performance_baseline(document_type)

        if existing:
            alpha = 0.3  # EMA smoothing factor
            updated = {
                k: alpha * metrics.get(k, 0) + (1 - alpha) * existing.get(k, 0)
                for k in set(list(metrics.keys()) + list(existing.keys()))
            }
        else:
            updated = metrics

        await self.store_short(
            f"baseline:{document_type}",
            updated,
            ttl=86400 * 90,  # 90 days
        )

    # ── Reset ───────────────────────────────────────────────────────

    def reset_working_memory(self) -> None:
        """Reset working memory for a new run."""
        self._current_steps.clear()
        self._tool_outputs.clear()
        self._reflections.clear()

    # ── Private ─────────────────────────────────────────────────────

    async def _init_db(self) -> None:
        """Initialize database connection pool."""
        try:
            import asyncpg
            self._db_pool = await asyncpg.create_pool(
                dsn=self.settings.database.url,
                min_size=2,
                max_size=5,
            )
        except Exception as e:
            logger.warning("db_pool_init_failed", error=str(e))
            self._db_pool = None
