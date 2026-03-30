"""
Memory — Phase 4.

Episodic memory store for the AcadExtract agent.

Two tiers:
  - Short-term (Redis): current-run context, fast key-value, TTL-bounded
  - Long-term (PostgreSQL + pgvector): persistent episodic events with
    semantic vector search for past extraction patterns
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
    embedding   vector(1536),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_episodic_run    ON episodic_memory(run_id);
CREATE INDEX IF NOT EXISTS idx_episodic_type   ON episodic_memory(event_type);
CREATE INDEX IF NOT EXISTS idx_episodic_time   ON episodic_memory(created_at DESC);
"""

_VECTOR_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_episodic_embedding
ON episodic_memory
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 50);
"""

# Redis key config
_REDIS_KEY_PREFIX = "agent:memory:"
_REDIS_TTL_SECONDS = 3600  # 1 hour per run


def _db():
    from src.common.database import get_connection
    return get_connection


def _ensure_table() -> None:
    get_connection = _db()
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_TABLE_DDL)
                # Migrate old schema: add embedding column if missing
                cur.execute("""
                    ALTER TABLE episodic_memory
                    ADD COLUMN IF NOT EXISTS embedding vector(1536)
                """)
                # Create vector index (requires pgvector + ivfflat)
                try:
                    cur.execute(_VECTOR_INDEX_DDL)
                except Exception:
                    pass
    except Exception as exc:
        logger.warning("memory: could not create episodic_memory table: %s", exc)


_table_ready = False


def _ready():
    global _table_ready
    if not _table_ready:
        _ensure_table()
        _table_ready = True


def _get_redis():
    """Return Redis client or None when unavailable."""
    try:
        from src.common.cache import get_cache
        return get_cache().r
    except Exception:
        return None


def _redis_key(run_id: str) -> str:
    return f"{_REDIS_KEY_PREFIX}{run_id}"


class MemoryStore:
    """
    Per-agent-run episodic memory with two tiers:

    Short-term (Redis hash, TTL=1h):
      - set_context(key, value) / get_context(key) / get_all_context()
      - Fast in-run key-value store; automatically expires after 1 hour
      - Used for: current goal, step outputs, intermediate results

    Long-term (PostgreSQL + pgvector):
      - store(event_type, content, summary) → persists with embedding
      - retrieve() → recency-ordered
      - semantic_search(query) → pgvector cosine similarity

    Usage::

        mem = MemoryStore(run_id="abc-123")
        mem.set_context("current_goal", "find backlogs for 4th sem CS")
        mem.store("extraction", {"usn": "1XX22CS001", "subjects": 6})
        recent = mem.retrieve(limit=5)
        similar = mem.semantic_search("VTU result extraction AIML dept", limit=3)
    """

    def __init__(self, run_id: str | None = None) -> None:
        self.run_id = run_id or str(uuid.uuid4())
        self._local: list[dict] = []   # in-memory fallback when DB not available

    # ── Short-term Redis ──────────────────────────────────────────────────────

    def set_context(self, key: str, value: Any) -> None:
        """Store a key-value in Redis for this run (TTL-bounded)."""
        r = _get_redis()
        if r is None:
            return
        try:
            r.hset(_redis_key(self.run_id), key, json.dumps(value, default=str))
            r.expire(_redis_key(self.run_id), _REDIS_TTL_SECONDS)
        except Exception as exc:
            logger.debug("memory.set_context failed: %s", exc)

    def get_context(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from Redis short-term memory."""
        r = _get_redis()
        if r is None:
            return default
        try:
            raw = r.hget(_redis_key(self.run_id), key)
            return json.loads(raw) if raw else default
        except Exception:
            return default

    def get_all_context(self) -> dict:
        """Return all short-term context keys for this run."""
        r = _get_redis()
        if r is None:
            return {}
        try:
            raw = r.hgetall(_redis_key(self.run_id))
            return {k: json.loads(v) for k, v in (raw or {}).items()}
        except Exception:
            return {}

    def clear_context(self) -> None:
        """Flush Redis short-term context for this run."""
        r = _get_redis()
        if r is None:
            return
        try:
            r.delete(_redis_key(self.run_id))
        except Exception:
            pass

    # ── Long-term DB + vector ─────────────────────────────────────────────────

    def store(
        self,
        event_type: str,
        content: Any,
        summary: str | None = None,
    ) -> str:
        """
        Persist an event to PostgreSQL episodic memory with pgvector embedding.
        Also caches summary in Redis for fast within-run retrieval.
        Returns the event id.
        """
        _ready()
        event_id = str(uuid.uuid4())
        payload = content if isinstance(content, dict) else {"data": content}
        now = datetime.utcnow().isoformat()

        self._local.append({
            "id":         event_id,
            "run_id":     self.run_id,
            "event_type": event_type,
            "content":    payload,
            "summary":    summary,
            "created_at": now,
        })

        # Mirror latest summary to Redis short-term
        if summary:
            try:
                self.set_context(f"event:{event_type}:latest", {"id": event_id, "summary": summary})
            except Exception:
                pass

        # Generate embedding for semantic retrieval
        embedding_literal: str | None = None
        _text_for_embed = (summary or json.dumps(payload, default=str))[:300]
        try:
            from src.common.embeddings import embed_text, _vec_to_pg_literal
            embedding_literal = _vec_to_pg_literal(embed_text(_text_for_embed))
        except Exception:
            pass

        get_connection = _db()
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    if embedding_literal:
                        cur.execute(
                            """
                            INSERT INTO episodic_memory
                                (id, run_id, event_type, content, summary, embedding)
                            VALUES (%s, %s, %s, %s, %s, %s::vector)
                            """,
                            (event_id, self.run_id, event_type,
                             json.dumps(payload), summary, embedding_literal),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO episodic_memory
                                (id, run_id, event_type, content, summary)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (event_id, self.run_id, event_type, json.dumps(payload), summary),
                        )
        except Exception as exc:
            logger.warning("memory.store failed: %s", exc)

        return event_id

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
        local = [e for e in reversed(self._local)
                 if run_id is None or e["run_id"] == target_run]
        if event_type:
            local = [e for e in local if e["event_type"] == event_type]
        if len(local) >= limit:
            return local[:limit]

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
        """Retrieve events across all runs (for context enrichment)."""
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

    def semantic_search(
        self,
        query: str,
        limit: int = 5,
        event_type: str | None = None,
    ) -> list[dict]:
        """
        Retrieve episodic memories semantically similar to *query* via pgvector.
        Falls back to recency-ordered retrieve() if pgvector is unavailable.
        """
        try:
            from src.common.embeddings import embed_text, _vec_to_pg_literal
            pg_literal = _vec_to_pg_literal(embed_text(query))

            from psycopg2.extras import RealDictCursor
            get_connection = _db()
            with get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
                    if not cur.fetchone():
                        return self.retrieve(event_type=event_type, limit=limit)

                    if event_type:
                        cur.execute(
                            """
                            SELECT id, run_id, event_type, content, summary, created_at,
                                   1 - (embedding <=> %s::vector) AS similarity
                            FROM episodic_memory
                            WHERE embedding IS NOT NULL AND event_type = %s
                            ORDER BY embedding <=> %s::vector
                            LIMIT %s
                            """,
                            (pg_literal, event_type, pg_literal, limit),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT id, run_id, event_type, content, summary, created_at,
                                   1 - (embedding <=> %s::vector) AS similarity
                            FROM episodic_memory
                            WHERE embedding IS NOT NULL
                            ORDER BY embedding <=> %s::vector
                            LIMIT %s
                            """,
                            (pg_literal, pg_literal, limit),
                        )
                    rows = cur.fetchall()
                    return [
                        {
                            "id":         str(r["id"]),
                            "run_id":     r["run_id"],
                            "event_type": r["event_type"],
                            "content":    r["content"],
                            "summary":    r["summary"],
                            "created_at": str(r["created_at"]) if r["created_at"] else None,
                            "similarity": float(r.get("similarity") or 0),
                        }
                        for r in rows
                    ]
        except Exception as exc:
            logger.debug("memory.semantic_search failed: %s", exc)
            return self.retrieve(event_type=event_type, limit=limit)

    def summarise(self) -> str:
        """Return a brief text summary of this run's memory for context injection."""
        # Fast path: Redis short-term context
        ctx = self.get_all_context()
        if ctx:
            lines = [f"{k}: {json.dumps(v, default=str)[:80]}" for k, v in list(ctx.items())[:10]]
            return "\n".join(lines)

        events = self.retrieve(limit=20)
        if not events:
            return "No prior memory for this run."
        lines = []
        for e in reversed(events):
            content = e.get("content", {})
            summary = e.get("summary") or repr(content)[:100]
            lines.append(f"[{e['event_type']}] {summary}")
        return "\n".join(lines)
