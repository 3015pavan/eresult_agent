"""
Memory — Phase 4.

Episodic memory store for the AcadExtract agent.
Persists events to an `episodic_memory` DB table and
provides recency/similarity-based retrieval.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS episodic_memory (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      TEXT,
    event_type  TEXT        NOT NULL,
    content     JSONB       NOT NULL,
    summary     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_episodic_run    ON episodic_memory(run_id);
CREATE INDEX IF NOT EXISTS idx_episodic_type   ON episodic_memory(event_type);
CREATE INDEX IF NOT EXISTS idx_episodic_time   ON episodic_memory(created_at DESC);
"""


def _db():
    """Lazy import to avoid circular deps."""
    from src.common.database import get_connection
    return get_connection


def _ensure_table() -> None:
    get_connection = _db()
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_TABLE_DDL)
    except Exception as exc:
        logger.warning("memory: could not create episodic_memory table: %s", exc)


_table_ready = False


def _ready():
    global _table_ready
    if not _table_ready:
        _ensure_table()
        _table_ready = True


class MemoryStore:
    """
    Per-agent-run episodic memory.

    Usage::

        mem = MemoryStore(run_id="abc-123")
        mem.store("extraction", {"usn": "1XX22CS001", "subjects": 6})
        recent = mem.retrieve(limit=5)
    """

    def __init__(self, run_id: str | None = None) -> None:
        self.run_id = run_id or str(uuid.uuid4())
        self._local: list[dict] = []   # in-memory fallback when DB not available

    # ── Write ─────────────────────────────────────────────────────────────────

    def store(
        self,
        event_type: str,
        content: Any,
        summary: str | None = None,
    ) -> str:
        """
        Persist an event to episodic memory.
        Returns the event id.
        """
        _ready()
        event_id = str(uuid.uuid4())
        payload = content if isinstance(content, dict) else {"data": content}
        now = datetime.utcnow().isoformat()

        # Always keep a local copy (fast retrieval within same run)
        self._local.append(
            {
                "id": event_id,
                "run_id": self.run_id,
                "event_type": event_type,
                "content": payload,
                "summary": summary,
                "created_at": now,
            }
        )

        # Persist to DB
        get_connection = _db()
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                    INSERT INTO episodic_memory (id, run_id, event_type, content, summary)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                        (event_id, self.run_id, event_type, json.dumps(payload), summary),
                    )
        except Exception as exc:
            logger.warning("memory.store failed: %s", exc)

        return event_id

    # ── Read ──────────────────────────────────────────────────────────────────

    def retrieve(
        self,
        event_type: str | None = None,
        limit: int = 10,
        run_id: str | None = None,
    ) -> list[dict]:
        """
        Return the most recent `limit` events, optionally filtered by type.

        Uses local cache first; falls back to DB for cross-run queries.
        """
        target_run = run_id or self.run_id

        # Local first (fast)
        local = [e for e in reversed(self._local) if run_id is None or e["run_id"] == target_run]
        if event_type:
            local = [e for e in local if e["event_type"] == event_type]
        if len(local) >= limit:
            return local[:limit]

        # DB fallback
        _ready()
        get_connection = _db()
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    if event_type:
                        cur.execute(
                            """
                            SELECT id, run_id, event_type, content, summary, created_at
                            FROM episodic_memory
                            WHERE run_id = %s AND event_type = %s
                            ORDER BY created_at DESC LIMIT %s
                            """,
                            (target_run, event_type, limit),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT id, run_id, event_type, content, summary, created_at
                            FROM episodic_memory
                            WHERE run_id = %s
                            ORDER BY created_at DESC LIMIT %s
                            """,
                            (target_run, limit),
                        )
                    rows = cur.fetchall()
                    return [
                        {
                            "id":         str(r[0]),
                            "run_id":     r[1],
                            "event_type": r[2],
                            "content":    r[3],
                            "summary":    r[4],
                            "created_at": r[5].isoformat() if r[5] else None,
                        }
                        for r in rows
                    ]
        except Exception as exc:
            logger.warning("memory.retrieve failed: %s", exc)
            return local

    def retrieve_all_runs(self, event_type: str | None = None, limit: int = 50) -> list[dict]:
        """Retrieve events across all runs (for context window enrichment)."""
        _ready()
        get_connection = _db()
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    if event_type:
                        cur.execute(
                            """
                            SELECT id, run_id, event_type, content, summary, created_at
                            FROM episodic_memory WHERE event_type = %s
                            ORDER BY created_at DESC LIMIT %s
                            """,
                            (event_type, limit),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT id, run_id, event_type, content, summary, created_at
                            FROM episodic_memory
                            ORDER BY created_at DESC LIMIT %s
                            """,
                            (limit,),
                        )
                    rows = cur.fetchall()
                    return [
                        {
                            "id":         str(r[0]),
                            "run_id":     r[1],
                            "event_type": r[2],
                            "content":    r[3],
                            "summary":    r[4],
                            "created_at": r[5].isoformat() if r[5] else None,
                        }
                        for r in rows
                    ]
        except Exception as exc:
            logger.warning("memory.retrieve_all_runs failed: %s", exc)
            return []

    def summarise(self) -> str:
        """Return a brief text summary of this run's memory for context injection."""
        events = self.retrieve(limit=20)
        if not events:
            return "No prior memory for this run."
        lines = []
        for e in reversed(events):
            content = e.get("content", {})
            summary = e.get("summary") or repr(content)[:100]
            lines.append(f"[{e['event_type']}] {summary}")
        return "\n".join(lines)
