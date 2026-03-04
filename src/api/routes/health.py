"""
Health Check Endpoints.

Provides liveness and readiness probes for Kubernetes,
plus a Prometheus metrics endpoint.
"""

from __future__ import annotations

from fastapi import APIRouter, Response

from src.common.observability import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/healthz")
async def liveness() -> dict:
    """Kubernetes liveness probe."""
    return {"status": "alive"}


@router.get("/readyz")
async def readiness() -> dict:
    """
    Kubernetes readiness probe.

    Checks:
      - Database connectivity
      - Redis connectivity
    """
    checks = {}

    # Database check (uses sync psycopg2 pool)
    try:
        from src.common.database import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {e}"

    # Redis check (uses sync redis client)
    try:
        from src.common.cache import get_cache
        ok = get_cache().ping()
        checks["redis"] = "ok" if ok else "error: ping failed"
    except Exception as e:
        checks["redis"] = f"error: {e}"

    all_ok = all(v == "ok" for v in checks.values())

    return {
        "status": "ready" if all_ok else "degraded",
        "checks": checks,
    }


@router.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
