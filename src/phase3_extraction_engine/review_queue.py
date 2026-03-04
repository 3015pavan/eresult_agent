"""
Human Review Queue — Phase 3.

Low-confidence extractions are routed to a review queue stored in PostgreSQL.
Teachers/admins can review, correct, and approve records via the API.

Table: review_queue (created on-demand if not present)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Confidence threshold below which records go to review
REVIEW_THRESHOLD = 0.65


def _ensure_table() -> None:
    """Create the review_queue table if it does not exist."""
    from src.common.database import get_connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS review_queue (
                    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    email_id        UUID,
                    email_subject   TEXT,
                    email_from      TEXT,
                    raw_text        TEXT,
                    extracted_data  JSONB,
                    confidence      DECIMAL(4,3),
                    validation_errors TEXT[],
                    status          TEXT NOT NULL DEFAULT 'pending'
                                    CHECK (status IN ('pending','approved','rejected','corrected')),
                    reviewer_id     UUID,
                    corrected_data  JSONB,
                    notes           TEXT,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    reviewed_at     TIMESTAMPTZ
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_review_queue_status
                ON review_queue(status, created_at DESC)
            """)


def enqueue_for_review(
    email_id: Optional[str],
    email_subject: str,
    email_from: str,
    raw_text: str,
    extracted_records: list[dict],
    confidence: float,
    validation_errors: Optional[list[str]] = None,
) -> str:
    """
    Add a low-confidence extraction to the human review queue.

    Returns the queue item UUID.
    """
    try:
        _ensure_table()
        from src.common.database import get_connection
        from psycopg2.extras import RealDictCursor

        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO review_queue
                        (email_id, email_subject, email_from, raw_text,
                         extracted_data, confidence, validation_errors)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                    RETURNING id
                """, (
                    email_id, email_subject[:500], email_from[:200],
                    raw_text[:5000],
                    json.dumps(extracted_records),
                    round(confidence, 3),
                    validation_errors or [],
                ))
                row = cur.fetchone()
                queue_id = str(row["id"])
                logger.info(
                    "review_queue: enqueued email_id=%s confidence=%.2f queue_id=%s",
                    email_id, confidence, queue_id,
                )
                return queue_id
    except Exception as exc:
        logger.warning("review_queue.enqueue failed: %s", exc)
        return ""


def get_review_queue(
    status: str = "pending",
    limit: int = 50,
) -> list[dict]:
    """Return review queue items filtered by status."""
    try:
        _ensure_table()
        from src.common.database import get_connection
        from psycopg2.extras import RealDictCursor

        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, email_subject, email_from, confidence,
                           validation_errors, status, created_at, reviewed_at,
                           extracted_data, corrected_data, notes
                    FROM review_queue
                    WHERE status = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (status, limit))
                return [
                    {k: str(v) if not isinstance(v, (str, int, float, bool, type(None), list, dict)) else v
                     for k, v in dict(r).items()}
                    for r in cur.fetchall()
                ]
    except Exception as exc:
        logger.warning("review_queue.get failed: %s", exc)
        return []


def approve_review_item(
    item_id: str,
    corrected_data: Optional[list[dict]] = None,
    notes: str = "",
) -> bool:
    """Approve (and optionally correct) a review queue item."""
    try:
        from src.common.database import get_connection
        new_status = "corrected" if corrected_data else "approved"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE review_queue
                    SET status        = %s,
                        corrected_data = %s::jsonb,
                        notes         = %s,
                        reviewed_at   = NOW()
                    WHERE id = %s
                """, (
                    new_status,
                    json.dumps(corrected_data) if corrected_data else None,
                    notes,
                    item_id,
                ))
        return True
    except Exception as exc:
        logger.warning("review_queue.approve failed: %s", exc)
        return False


def reject_review_item(item_id: str, notes: str = "") -> bool:
    """Reject a review queue item (mark as irrelevant/incorrect)."""
    try:
        from src.common.database import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE review_queue
                    SET status = 'rejected', notes = %s, reviewed_at = NOW()
                    WHERE id = %s
                """, (notes, item_id))
        return True
    except Exception as exc:
        logger.warning("review_queue.reject failed: %s", exc)
        return False
