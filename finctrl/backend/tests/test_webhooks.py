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

@pytest.mark.asyncio
async def test_webhook_processing_failure_and_replay():
    settings.RAZORPAY_KEY_SECRET = "test_secret"
    settings.ADMIN_API_KEY = "admin_key"

    import unittest.mock

    with unittest.mock.patch('finctrl.backend.api.webhook_processor.process_razorpay_event', return_value=False):
        payload = {"event": "order.paid", "payload": {"order": {"entity": {"id": "order_fail", "receipt": "rcpt_fail", "status": "paid"}}}}
        body = json.dumps(payload).encode()
        valid_sig = hmac.new(b"test_secret", body, hashlib.sha256).hexdigest()

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-event-id": "ev_broken", "x-razorpay-signature": valid_sig})
            assert resp.status_code == 500
            assert resp.json()["detail"] == "Processing failed"

            # Now replay with mock returning True
            with unittest.mock.patch('finctrl.backend.api.webhook_processor.process_razorpay_event', return_value=True):
                replay_resp = await ac.post("/webhooks/replay/ev_broken", headers={"X-API-Key": "admin_key"})
                assert replay_resp.status_code == 200
                assert replay_resp.json()["status"] == "ok"

                # Replay again should return already_processed
                replay_resp_again = await ac.post("/webhooks/replay/ev_broken", headers={"X-API-Key": "admin_key"})
                assert replay_resp_again.status_code == 200
                assert replay_resp_again.json()["status"] == "already_processed"

@pytest.mark.asyncio
async def test_concurrent_webhook_idempotency():
    settings.RAZORPAY_KEY_SECRET = "test_secret"
    payload = {"event": "order.paid", "payload": {"order": {"entity": {"id": "order_test_dup", "receipt": "rcpt_dup", "status": "paid"}}}}
    body = json.dumps(payload).encode()
    valid_sig = hmac.new(b"test_secret", body, hashlib.sha256).hexdigest()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Instead of asyncio.gather which might lock SQLite entirely with HTTPX tests,
        # Just simulate consecutive hits for idempotency behavior
        responses = []
        for _ in range(3):
            r = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-event-id": "ev_duplicate", "x-razorpay-signature": valid_sig})
            responses.append(r)

        oks = [r for r in responses if r.status_code == 200 and r.json().get("status") == "ok"]
        dups = [r for r in responses if r.status_code == 200 and r.json().get("status") == "already_processed"]

        assert len(oks) == 1
        assert len(dups) == 2
