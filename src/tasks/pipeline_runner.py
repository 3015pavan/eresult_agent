"""
Celery task for queue-first pipeline orchestration.
Queue: email_ingestion
"""

from __future__ import annotations

import logging

from src.common.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="tasks.run_pipeline_batch",
    bind=True,
    max_retries=1,
    default_retry_delay=15,
    queue="email_ingestion",
)
def run_pipeline_batch(self, force: bool = False) -> dict:
    """Run the full pipeline in a worker process."""
    try:
        from src.api.routes.pipeline import _run_pipeline_sync

        return _run_pipeline_sync(force=force, task_id=self.request.id)
    except Exception as exc:
        logger.error("run_pipeline_batch failed: %s", exc)
        raise self.retry(exc=exc)
