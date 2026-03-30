"""
API Layer — FastAPI Application.

Provides REST endpoints for:
  - Teacher query interface
  - Admin pipeline management
  - Health checks & metrics
  - Webhook receivers for email notifications
"""


def create_app():
    """Lazy import to avoid module-level side-effects."""
    from src.api.app import create_app as _create
    return _create()


__all__ = ["create_app"]
