import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

import pytest
import pytest_asyncio
import hmac
import hashlib
import json
from httpx import AsyncClient, ASGITransport
from finctrl.backend.api.main import app
from finctrl.backend.config import settings
from finctrl.backend.database.database import get_db_session, init_db

@pytest_asyncio.fixture(autouse=True)
async def clear_db():
    await init_db()
    yield

@pytest.mark.asyncio
async def test_webhook_signature_validation():
    # Setup test secret
    settings.RAZORPAY_KEY_SECRET = "test_secret"

    payload = {"event": "payment.captured", "payload": {}}
    body = json.dumps(payload).encode()

    # Generate valid signature
    valid_sig = hmac.new(b"test_secret", body, hashlib.sha256).hexdigest()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Test 1: Missing signature -> 400
        resp1 = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-event-id": "ev_123"})
        assert resp1.status_code == 400

        # Test 2: Invalid signature -> 400
        resp2 = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-event-id": "ev_123", "x-razorpay-signature": "invalid"})
        assert resp2.status_code == 400

        # Test 3: Missing event ID -> 400
        resp3 = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-signature": valid_sig})
        assert resp3.status_code == 400

        # Test 4: Valid signature and event ID -> 200
        resp4 = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-event-id": "ev_123", "x-razorpay-signature": valid_sig})
        assert resp4.status_code == 200

        # Test 5: Idempotency (duplicate event ID) -> 200 but already_processed
        resp5 = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-event-id": "ev_123", "x-razorpay-signature": valid_sig})
        assert resp5.status_code == 200
        assert resp5.json() == {"status": "already_processed"}

        # Test 6: Missing configuration secret -> fail closed
        settings.RAZORPAY_KEY_SECRET = ""
        resp6 = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-event-id": "ev_124", "x-razorpay-signature": valid_sig})
        assert resp6.status_code == 500
        assert "Webhook secret not configured" in resp6.json()["detail"]
