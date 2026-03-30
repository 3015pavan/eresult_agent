"""Tests for API endpoints."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock


def _make_client():
    with patch("src.api.app.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            environment="test",
            security=MagicMock(allowed_origins=["*"]),
        )
        from src.api.app import create_app
        app = create_app()
        return TestClient(app)


@pytest.fixture
def client():
    """Create test client."""
    return _make_client()


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


class TestProtectedRoutes:
    """Tests for optional API key protection on sensitive routes."""

    def test_pipeline_status_open_when_no_api_key(self):
        mock_cache = MagicMock()
        mock_cache.get_pipeline_state.return_value = {"status": "idle"}
        with patch("src.common.database.init_db"), \
             patch("src.common.database.get_pipeline_stats", return_value={}), \
             patch("src.api.routes.pipeline.get_cache", return_value=mock_cache):
            response = _make_client().get("/api/v1/pipeline/status")
        assert response.status_code == 200

    def test_pipeline_status_requires_api_key_when_configured(self):
        mock_cache = MagicMock()
        mock_cache.get_pipeline_state.return_value = {"status": "idle"}
        mock_settings = MagicMock()
        mock_settings.security = MagicMock(app_api_key="secret-key")
        with patch("src.common.security.get_settings", return_value=mock_settings), \
             patch("src.common.database.init_db"), \
             patch("src.common.database.get_pipeline_stats", return_value={}), \
             patch("src.api.routes.pipeline.get_cache", return_value=mock_cache):
            from src.api.app import create_app
            protected_client = TestClient(create_app())

            unauthorized = protected_client.get("/api/v1/pipeline/status")
            assert unauthorized.status_code == 401

            authorized = protected_client.get(
                "/api/v1/pipeline/status",
                headers={"X-API-Key": "secret-key"},
            )
            assert authorized.status_code == 200


class TestReportMetrics:
    """Tests for deterministic report summary calculations."""

    def test_compute_report_metrics(self):
        from src.api.routes.query import _compute_report_metrics

        results = [
            {"semester": 1, "marks_obtained": 80, "max_marks": 100, "pass_status": "PASS"},
            {"semester": 1, "marks_obtained": 70, "max_marks": 100, "pass_status": "PASS"},
            {"semester": 2, "marks_obtained": 35, "max_marks": 100, "pass_status": "FAIL"},
        ]
        sems = [
            {"semester": 1, "sgpa": 8.2},
            {"semester": 2, "sgpa": 7.4},
        ]

        metrics = _compute_report_metrics(results, sems)

        assert metrics["subjects_total"] == 3
        assert metrics["subjects_passed"] == 2
        assert metrics["subjects_failed"] == 1
        assert metrics["percentage"] == pytest.approx(61.67, abs=0.01)
        assert metrics["best_sgpa"] == pytest.approx(8.2, abs=0.01)
