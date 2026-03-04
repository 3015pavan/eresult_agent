"""
Executor — Phase 4.

Executes a single plan step with memory context injection
and standardised error handling.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any

from .tools import call_tool

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_nested(obj: Any, path: str) -> Any:
    """
    Resolve a dotted path like "emails[0].id" or "records" from obj.
    Supports list indexing: field[N].subfield
    """
    parts = path.replace("]", "").replace("[", ".").split(".")
    cur: Any = obj
    for p in parts:
        if cur is None:
            return None
        if p.isdigit():
            try:
                cur = cur[int(p)]
            except (IndexError, TypeError, KeyError):
                return None
        else:
            try:
                cur = cur[p]
            except (KeyError, TypeError):
                try:
                    cur = getattr(cur, p)
                except AttributeError:
                    return None
    return cur


def _resolve_args(args: dict, memory: dict) -> dict:
    """
    Substitute {step_N.field} and {step_N.field.sub} placeholders
    in string arg values using memory of previous step outputs.
    """
    resolved: dict = {}
    for key, value in args.items():
        if not isinstance(value, str):
            resolved[key] = value
            continue

        def _sub(m: re.Match) -> str:
            step_key = m.group(1)   # e.g. "step_0"
            field    = m.group(2)   # e.g. "emails[0].id"
            step_out = memory.get(step_key)
            if step_out is None:
                return m.group(0)  # leave as-is when step not yet run
            result = _get_nested(step_out, field)
            if result is None:
                return ""
            if isinstance(result, (dict, list)):
                import json as _json
                return _json.dumps(result)
            return str(result)

        replaced = re.sub(r"\{(step_\d+)\.([^}]+)\}", _sub, value)
        resolved[key] = replaced

    return resolved


# ── Public API ────────────────────────────────────────────────────────────────

class StepResult:
    """Return value from execute_step."""
    __slots__ = ("tool", "output", "error", "duration_ms", "success")

    def __init__(self, tool: str, output: Any, error: str | None, duration_ms: float):
        self.tool        = tool
        self.output      = output
        self.error       = error
        self.duration_ms = duration_ms
        self.success     = error is None


def execute_step(
    tool: str,
    args: dict,
    memory: dict,
    *,
    retries: int = 1,
) -> StepResult:
    """
    Execute a single tool call.

    Args:
        tool:    Name of the tool to call.
        args:    Raw args dict (may contain {step_N.field} templates).
        memory:  Dict of {step_0: output_0, step_1: output_1, …} for template resolution.
        retries: How many times to retry on transient failure.

    Returns:
        StepResult with output and timing.
    """
    resolved_args = _resolve_args(args, memory)
    logger.debug("executor: %s(%s)", tool, resolved_args)

    last_error: str | None = None
    t0 = time.perf_counter()

    for attempt in range(retries + 1):
        try:
            output = call_tool(tool, **resolved_args)
            duration_ms = (time.perf_counter() - t0) * 1000
            logger.info("executor: %s OK in %.0fms", tool, duration_ms)
            return StepResult(tool, output, None, duration_ms)
        except Exception as exc:
            last_error = str(exc)
            logger.warning(
                "executor: %s attempt=%d failed: %s", tool, attempt + 1, exc
            )

    duration_ms = (time.perf_counter() - t0) * 1000
    logger.error("executor: %s FAILED after %d attempts: %s", tool, retries + 1, last_error)
    return StepResult(tool, None, last_error, duration_ms)
