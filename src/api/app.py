"""
FastAPI Application Factory.

Creates and configures the production FastAPI application with:
  - CORS, request validation
  - Structured logging middleware
  - Prometheus metrics endpoint
  - Health check endpoints
  - API versioning (v1)
"""

from __future__ import annotations

import os
import pathlib
from contextlib import asynccontextmanager
from typing import AsyncIterator

# ── Load .env early so os.getenv() picks up all keys ─────────────────────────
try:
    from dotenv import load_dotenv  # type: ignore
    _env_path = pathlib.Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(dotenv_path=_env_path, override=False)
except ImportError:
    pass  # python-dotenv not installed — rely on shell environment

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from src.common.config import get_settings
from src.common.observability import get_logger

FRONTEND_DIR = pathlib.Path(__file__).resolve().parent.parent / "frontend"

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifecycle: startup and shutdown."""
    settings = get_settings()
    logger.info(
        "application_starting",
        environment=settings.environment,
    )

    # ── PostgreSQL ───────────────────────────────────────────────────────────
    try:
        from src.common.database import init_db
        init_db()
        logger.info("postgresql_ready")
    except Exception as exc:
        logger.error("postgresql_init_failed", error=str(exc))

    # ── MinIO object storage ─────────────────────────────────────────────────
    try:
        from src.common.storage import get_storage
        get_storage().ensure_buckets()
        logger.info("minio_ready")
    except Exception as exc:
        logger.warning("minio_init_failed", error=str(exc))

    # ── Redis cache / dedup ──────────────────────────────────────────────────
    try:
        from src.common.cache import get_cache
        ok = get_cache().ping()
        if ok:
            logger.info("redis_ready")
        else:
            logger.warning("redis_unreachable")
    except Exception as exc:
        logger.warning("redis_init_failed", error=str(exc))

    # ── OpenTelemetry ────────────────────────────────────────────────────────
    try:
        from src.common.observability import instrument_psycopg2
        instrument_psycopg2()
    except Exception as exc:
        logger.warning("otel_init_failed", error=str(exc))

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────
    logger.info("application_shutting_down")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="Academic Result Extraction System",
        description=(
            "Autonomous AI system for extracting student academic results "
            "from email streams and enabling natural language queries."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url="/redoc" if settings.environment != "production" else None,
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.security.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register routers
    from src.api.routes import query, admin, health, auth, webhook, accounts, sync, pipeline
    from src.api.routes.agent import router as agent_router
    app.include_router(health.router, tags=["Health"])
    app.include_router(auth.router, prefix="/api/v1", tags=["Auth"])
    app.include_router(query.router, prefix="/api/v1", tags=["Query"])
    app.include_router(admin.router, prefix="/api/v1/admin", tags=["Admin"])
    app.include_router(webhook.router, prefix="/webhooks", tags=["Webhooks"])
    app.include_router(accounts.router, prefix="/api/v1/accounts", tags=["Accounts"])
    app.include_router(sync.router, prefix="/api/v1", tags=["Sync"])
    app.include_router(pipeline.router, prefix="/api/v1", tags=["Pipeline"])
    app.include_router(agent_router, prefix="/api/v1/agent", tags=["Agent"])

    # Serve frontend static assets (CSS, JS)
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR / "static")), name="static")

    # Serve frontend index.html at root
    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def serve_frontend():
        index = FRONTEND_DIR / "index.html"
        return HTMLResponse(content=index.read_text(encoding="utf-8"))

    # OpenTelemetry FastAPI instrumentation (best-effort)
    try:
        from src.common.observability import instrument_fastapi
        instrument_fastapi(app)
    except Exception:
        pass

    return app


app = create_app()
