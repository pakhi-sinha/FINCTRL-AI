import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from finctrl.backend.api.main import app
from finctrl.backend.config import settings

@pytest.mark.asyncio
async def test_auth_enforcement():
    # Setup test secrets
    settings.APP_MODE = "production"
    settings.ADMIN_API_KEY = "admin_key"
    settings.READ_ONLY_API_KEY = "read_key"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # No key -> 401
        resp = await ac.post("/reconciliation/run")
        assert resp.status_code == 401

        # Invalid key -> 401
        resp = await ac.post("/reconciliation/run", headers={"X-API-Key": "wrong_key"})
        assert resp.status_code == 401

        # Read only key on Admin endpoint -> 403
        resp = await ac.post("/reconciliation/run", headers={"X-API-Key": "read_key"})
        assert resp.status_code == 403

        # Read only key on Read endpoint -> 200 OK (Assuming E2E DB is mocked/empty, returns empty list)
        resp = await ac.get("/matches", headers={"X-API-Key": "read_key"})
        assert resp.status_code == 200

        # Admin key on Admin endpoint -> OK (reconciliation run will return stats)
        # Note: Depending on the database setup, it might just return 0.
        resp = await ac.post("/reconciliation/run", headers={"X-API-Key": "admin_key"})
        assert resp.status_code == 200

    # Teardown
    settings.APP_MODE = "test"
    settings.ADMIN_API_KEY = None
    settings.READ_ONLY_API_KEY = None
