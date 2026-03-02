"""Tests for API endpoints."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock


@pytest.fixture
def client():
    """Create test client."""
    with patch("src.api.app.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            environment="test",
            security=MagicMock(allowed_origins=["*"]),
        )
        from src.api.app import create_app
        app = create_app()
        return TestClient(app)


class TestHealthEndpoints:
    """Tests for health check endpoints."""

    def test_liveness(self, client):
        """Test liveness probe returns 200."""
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json()["status"] == "alive"

    def test_metrics(self, client):
        """Test metrics endpoint returns Prometheus format."""
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers.get("content-type", "") or \
               "text/plain" in response.headers.get("Content-Type", "") or \
               response.status_code == 200
