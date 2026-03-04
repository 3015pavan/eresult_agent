"""
Phase 4 — Agentic Layer.

Re-exports the main agent interface.
"""

from .agent import AcadExtractAgent, AgentState, run_agent

__all__ = ["AcadExtractAgent", "AgentState", "run_agent"]
