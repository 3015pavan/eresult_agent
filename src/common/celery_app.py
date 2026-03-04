"""
Celery task queue configuration.

Queues:
  - email_ingestion   : Gmail sync tasks
  - extraction        : PDF/email extraction tasks
  - indexing          : Elasticsearch indexing tasks
  - notifications     : Teacher alert tasks

Worker: celery -A src.common.celery_app worker --loglevel=info -Q email_ingestion,extraction
Beat:   celery -A src.common.celery_app beat   --loglevel=info
"""

from __future__ import annotations

import os

from celery import Celery
from celery.schedules import crontab
from kombu import Exchange, Queue


def _get_broker_url() -> str:
    host     = os.getenv("REDIS_HOST", "localhost")
    port     = os.getenv("REDIS_PORT", "6379")
    password = os.getenv("REDIS_PASSWORD", "")
    if password:
        return f"redis://:{password}@{host}:{port}/1"
    return f"redis://{host}:{port}/1"


def _get_result_backend() -> str:
    return _get_broker_url().replace("/1", "/2")


# ── App ───────────────────────────────────────────────────────────────────────
app = Celery(
    "acadextract",
    broker=_get_broker_url(),
    backend=_get_result_backend(),
    include=[
        "src.tasks.ingestion",
        "src.tasks.extraction",
        "src.tasks.indexing",
    ],
)

# ── Configuration ─────────────────────────────────────────────────────────────
app.conf.update(
    # Serialisation
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    # Timezone
    timezone="UTC",
    enable_utc=True,
    # Queues
    task_queues=(
        Queue("email_ingestion", Exchange("email_ingestion"), routing_key="email_ingestion"),
        Queue("extraction",      Exchange("extraction"),      routing_key="extraction"),
        Queue("indexing",        Exchange("indexing"),        routing_key="indexing"),
        Queue("notifications",   Exchange("notifications"),   routing_key="notifications"),
    ),
    task_default_queue="extraction",
    task_default_exchange="extraction",
    task_default_routing_key="extraction",
    # Route tasks to correct queues
    task_routes={
        "src.tasks.ingestion.*":  {"queue": "email_ingestion"},
        "src.tasks.extraction.*": {"queue": "extraction"},
        "src.tasks.indexing.*":   {"queue": "indexing"},
    },
    # Reliability
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    # Result TTL: 24 hours
    result_expires=86400,
    # Retry policy
    task_max_retries=3,
    task_default_retry_delay=30,
)

# ── Periodic tasks (Beat scheduler) ──────────────────────────────────────────
app.conf.beat_schedule = {
    # Auto-sync Gmail every 15 minutes
    "sync-gmail-every-15m": {
        "task":     "src.tasks.ingestion.sync_gmail",
        "schedule": crontab(minute="*/15"),
        "options":  {"queue": "email_ingestion"},
    },
    # Rebuild pgvector embeddings nightly
    "rebuild-embeddings-nightly": {
        "task":     "src.tasks.indexing.rebuild_embeddings",
        "schedule": crontab(hour=2, minute=0),
        "options":  {"queue": "indexing"},
    },
    # Refresh Elasticsearch index hourly
    "refresh-es-index-hourly": {
        "task":     "src.tasks.indexing.refresh_elasticsearch",
        "schedule": crontab(minute=0),
        "options":  {"queue": "indexing"},
    },
}
