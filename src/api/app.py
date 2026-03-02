"""
FastAPI Application Factory.

Creates and configures the production FastAPI application with:
  - CORS, rate limiting, request validation
  - Structured logging middleware
  - Prometheus metrics endpoint
  - Health check endpoints
  - API versioning (v1)
"""

from __future__ import annotations

import pathlib
from contextlib import asynccontextmanager
from typing import AsyncIterator

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

    # Startup: initialize connections, models, etc.
    # In production, warm up ML models and DB pools here
    yield

    # Shutdown: cleanup
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
    from src.api.routes import query, admin, health, auth
    app.include_router(health.router, tags=["Health"])
    app.include_router(auth.router, prefix="/api/v1", tags=["Auth"])
    app.include_router(query.router, prefix="/api/v1", tags=["Query"])
    app.include_router(admin.router, prefix="/api/v1/admin", tags=["Admin"])

    # Serve frontend static assets (CSS, JS)
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR / "static")), name="static")

    # Serve frontend index.html at root
    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def serve_frontend():
        index = FRONTEND_DIR / "index.html"
        return HTMLResponse(content=index.read_text(encoding="utf-8"))

    return app
