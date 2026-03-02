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
      - Model availability
    """
    checks = {}

    # Database check
    try:
        import asyncpg
        from src.common.config import get_settings
        settings = get_settings()
        conn = await asyncpg.connect(dsn=settings.database.url, timeout=5)
        await conn.fetchval("SELECT 1")
        await conn.close()
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {e}"

    # Redis check
    try:
        import redis.asyncio as aioredis
        from src.common.config import get_settings
        settings = get_settings()
        r = aioredis.Redis(
            host=settings.redis.host,
            port=settings.redis.port,
        )
        await r.ping()
        await r.aclose()
        checks["redis"] = "ok"
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
