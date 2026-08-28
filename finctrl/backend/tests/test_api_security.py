"""
Tests for API security: X-API-Key authentication and RBAC.
"""
import pytest
from fastapi.testclient import TestClient
from fastapi import FastAPI
from unittest.mock import AsyncMock, patch
import json

from finctrl.backend.api.main import app
from finctrl.backend.api.security import verify_api_key, require_admin, require_read_only
from fastapi import Depends, Security
from fastapi.testclient import TestClient
import asyncio


@pytest.fixture
def client():
    """Create test client with mocked database."""
    return TestClient(app)


def test_health_endpoint_public():
    """Health endpoint should be accessible without authentication."""
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_ready_endpoint_public():
    """Ready endpoint should be accessible without authentication."""
    with TestClient(app) as client:
        response = client.get("/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}


def test_missing_api_key():
    """Endpoints should return 401 when X-API-Key header is missing."""
    with TestClient(app) as client:
        response = client.get("/metrics")
        assert response.status_code == 401
        assert "Missing X-API-Key header" in response.json()["detail"]


def test_invalid_api_key():
    """Endpoints should return 403 when X-API-Key is invalid."""
    with TestClient(app) as client:
        response = client.get("/metrics", headers={"X-API-Key": "invalid_key"})
        assert response.status_code == 403
        assert "Invalid X-API-Key" in response.json()["detail"]


@pytest.mark.asyncio
async def test_verify_api_key_admin():
    """verify_api_key should return ADMIN role for admin key."""
    # Mock settings
    with patch('finctrl.backend.api.security.settings') as mock_settings:
        mock_settings.ADMIN_API_KEY = "admin_key"
        mock_settings.READ_ONLY_API_KEY = "readonly_key"

        # Test admin key
        role = await verify_api_key("admin_key")
        assert role == "ADMIN"


@pytest.mark.asyncio
async def test_verify_api_key_read_only():
    """verify_api_key should return READ_ONLY role for read-only key."""
    with patch('finctrl.backend.api.security.settings') as mock_settings:
        mock_settings.ADMIN_API_KEY = "admin_key"
        mock_settings.READ_ONLY_API_KEY = "readonly_key"

        # Test read-only key
        role = await verify_api_key("readonly_key")
        assert role == "READ_ONLY"


@pytest.mark.asyncio
async def test_require_admin_allows_admin():
    """require_admin should allow ADMIN role."""
    with patch('finctrl.backend.api.security.settings') as mock_settings:
        mock_settings.ADMIN_API_KEY = "admin_key"
        mock_settings.READ_ONLY_API_KEY = "readonly_key"

        # Admin should pass
        role = await require_admin("ADMIN")
        assert role == "ADMIN"


@pytest.mark.asyncio
async def test_require_admin_rejects_read_only():
    """require_admin should reject READ_ONLY role."""
    with patch('finctrl.backend.api.security.settings') as mock_settings:
        mock_settings.ADMIN_API_KEY = "admin_key"
        mock_settings.READ_ONLY_API_KEY = "readonly_key"

        # Read-only should be rejected
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await require_admin("READ_ONLY")

        assert exc_info.value.status_code == 403
        assert "ADMIN role required" in exc_info.value.detail


@pytest.mark.asyncio
async def test_require_read_only_allows_both():
    """require_read_only should allow both ADMIN and READ_ONLY roles."""
    with patch('finctrl.backend.api.security.settings') as mock_settings:
        mock_settings.ADMIN_API_KEY = "admin_key"
        mock_settings.READ_ONLY_API_KEY = "readonly_key"

        # Admin should pass
        role = await require_read_only("ADMIN")
        assert role == "ADMIN"

        # Read-only should also pass
        role = await require_read_only("READ_ONLY")
        assert role == "READ_ONLY"


def test_read_only_endpoints_with_read_only_key():
    """Read-only endpoints should work with READ_ONLY API key."""
    with TestClient(app) as client:
        # Mock settings
        with patch('finctrl.backend.api.security.settings') as mock_settings:
            mock_settings.ADMIN_API_KEY = "admin_key"
            mock_settings.READ_ONLY_API_KEY = "readonly_key"

            # Mock database response to avoid actual DB dependency
            with patch('finctrl.backend.api.routes.get_db_session'):
                response = client.get("/metrics", headers={"X-API-Key": "readonly_key"})

                # Should not get authentication error
                assert response.status_code != 401
                assert response.status_code != 403
                # Actual endpoint logic would need proper DB mocking


def test_admin_endpoints_reject_read_only_key():
    """Admin endpoints should reject READ_ONLY API key."""
    with TestClient(app) as client:
        # Mock settings
        with patch('finctrl.backend.api.security.settings') as mock_settings:
            mock_settings.ADMIN_API_KEY = "admin_key"
            mock_settings.READ_ONLY_API_KEY = "readonly_key"

            response = client.post("/reconciliation/run", headers={"X-API-Key": "readonly_key"})

            # Should get permission denied
            assert response.status_code == 403
            assert "ADMIN role required" in response.json()["detail"]


def test_admin_endpoints_allow_admin_key():
    """Admin endpoints should allow ADMIN API key."""
    with TestClient(app) as client:
        # Mock settings
        with patch('finctrl.backend.api.security.settings') as mock_settings:
            mock_settings.ADMIN_API_KEY = "admin_key"
            mock_settings.READ_ONLY_API_KEY = "readonly_key"

            # Mock database response
            with patch('finctrl.backend.api.routes.get_db_session'):
                response = client.post("/reconciliation/run", headers={"X-API-Key": "admin_key"})

                # Should not get permission error
                assert response.status_code != 401
                assert response.status_code != 403
                # Actual endpoint logic would need proper DB mocking


def test_correlation_id_header():
    """Middleware should add X-Correlation-ID header to responses."""
    with TestClient(app) as client:
        # Test without providing correlation ID
        response = client.get("/health")
        assert "X-Correlation-ID" in response.headers
        correlation_id = response.headers["X-Correlation-ID"]
        assert correlation_id  # Should not be empty

        # Test with provided correlation ID
        custom_correlation_id = "custom-12345"
        response = client.get("/health", headers={"X-Correlation-ID": custom_correlation_id})
        assert response.headers["X-Correlation-ID"] == custom_correlation_id


def test_webhook_endpoint_no_api_key():
    """Webhook endpoint should not require X-API-Key header."""
    with TestClient(app) as client:
        # Mock webhook verification to succeed
        with patch('finctrl.backend.api.routes.verify_signature') as mock_verify:
            mock_verify.return_value = True

            # Mock database to avoid actual processing
            with patch('finctrl.backend.api.routes.get_db_session'):
                # Try without API key
                response = client.post(
                    "/webhooks/razorpay",
                    headers={
                        "x-razorpay-signature": "test_sig",
                        "x-razorpay-event-id": "test_event_id"
                    },
                    content=b'{"event": "test"}'
                )

                # Should not get authentication error
                assert response.status_code != 401
                assert response.status_code != 403
                # Will get other errors due to mocked dependencies, but not auth errors
