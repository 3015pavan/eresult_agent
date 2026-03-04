"""
Celery tasks — Gmail ingestion.
Queue: email_ingestion
"""

from __future__ import annotations

import logging
from typing import Any

from src.common.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="tasks.sync_gmail_inbox",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    queue="email_ingestion",
)
def sync_gmail_inbox(self, institution_id: str | None = None) -> dict:
    """
    Periodic task: pull new emails from Gmail and push them through the
    classification/extraction pipeline.

    Beat schedule: every 15 minutes (configured in celery_app.py).
    """
    try:
        from src.common.cache import is_duplicate_sha256, mark_seen_sha256
        import hashlib, json, os

        cache_path = "data/emails_cache.json"
        if not os.path.exists(cache_path):
            return {"status": "no_cache", "processed": 0}

        with open(cache_path) as f:
            emails: list[dict] = json.load(f)

        enqueued = 0
        for email in emails:
            body = email.get("body", "")
            h = hashlib.sha256(body.encode()).hexdigest()
            if is_duplicate_sha256(h):
                continue
            mark_seen_sha256(h)
            extract_email.apply_async(
                kwargs={"email": email, "institution_id": institution_id},
                queue="extraction",
            )
            enqueued += 1

        logger.info("sync_gmail_inbox: enqueued %d emails for extraction", enqueued)
        return {"status": "ok", "enqueued": enqueued, "total": len(emails)}

    except Exception as exc:
        logger.error("sync_gmail_inbox failed: %s", exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="tasks.ingest_single_email",
    bind=True,
    max_retries=2,
    queue="email_ingestion",
)
def ingest_single_email(self, message_id: str, institution_id: str | None = None) -> dict:
    """
    On-demand task: fetch a single Gmail message and enqueue for extraction.
    Called from the webhook handler when Gmail push notification arrives.
    """
    try:
        from src.common.cache import is_duplicate_sha256

        if is_duplicate_sha256(message_id):
            return {"status": "duplicate", "message_id": message_id}

        # Delegate to extraction immediately
        extract_email.apply_async(
            kwargs={"email": {"id": message_id}, "institution_id": institution_id},
            queue="extraction",
        )
        return {"status": "enqueued", "message_id": message_id}

    except Exception as exc:
        logger.error("ingest_single_email failed: %s", exc)
        raise self.retry(exc=exc)
