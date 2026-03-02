"""
API Layer — FastAPI Application.

Provides REST endpoints for:
  - Teacher query interface
  - Admin pipeline management
  - Health checks & metrics
  - Webhook receivers for email notifications
"""

from src.api.app import create_app

__all__ = ["create_app"]
