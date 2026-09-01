import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

import pytest
import pytest_asyncio
import asyncio
import hmac
import hashlib
import json
from copy import deepcopy
from httpx import AsyncClient, ASGITransport
from sqlalchemy import select
from finctrl.backend.api.main import app
from finctrl.backend.config import settings
from finctrl.backend.database.database import async_session_maker, init_db
from finctrl.backend.database.models import FinancialEventModel
from finctrl.backend.integrations.webhook_processor import WebhookProcessor

@pytest_asyncio.fixture(autouse=True)
async def clear_db():
    await init_db()
    yield

@pytest.mark.asyncio
async def test_webhook_signature_validation():
    # Setup test secret
    settings.RAZORPAY_WEBHOOK_SECRET = "test_secret"

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
        settings.RAZORPAY_WEBHOOK_SECRET = ""
        resp6 = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-event-id": "ev_124", "x-razorpay-signature": valid_sig})
        assert resp6.status_code == 500
        assert "Webhook secret not configured" in resp6.json()["detail"]

@pytest.mark.asyncio
async def test_webhook_processing_failure():
    settings.RAZORPAY_WEBHOOK_SECRET = "test_secret"

    # Intentionally broken payload format to trigger parsing error
    payload = {"event": "payment.captured", "payload": "NOT_A_DICT"}
    body = json.dumps(payload).encode()

    valid_sig = hmac.new(b"test_secret", body, hashlib.sha256).hexdigest()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post("/webhooks/razorpay", content=body, headers={"x-razorpay-event-id": "ev_broken", "x-razorpay-signature": valid_sig})
        assert resp.status_code == 500
        assert resp.json()["detail"] == "Processing failed"


@pytest.mark.asyncio
async def test_webhook_processing_failure_and_replay():
    payload = {"event": "payment.captured", "payload": {}}
    body = json.dumps(payload).encode()
    payload_hash = hashlib.sha256(body).hexdigest()

    async with async_session_maker() as db:
        event = FinancialEventModel(
            provider="razorpay",
            provider_event_id="ev_replay",
            event_type=payload["event"],
            payload_hash=payload_hash,
            raw_payload=payload,
            processing_status="FAILED",
            attempt_count=1,
            error_message="temporary processing failure",
        )
        db.add(event)
        await db.commit()
        event_id = str(event.id)
        original_payload = deepcopy(event.raw_payload)

        success, replayed_event_id, error = await WebhookProcessor(db).replay_event(event_id)

        assert success is True
        assert replayed_event_id == event_id
        assert error is None

        db.expire_all()
        replayed = await db.scalar(
            select(FinancialEventModel).where(FinancialEventModel.id == event_id)
        )
        assert replayed.processing_status == "PROCESSED"
        assert replayed.raw_payload == original_payload
        assert replayed.payload_hash == payload_hash


@pytest.mark.asyncio
async def test_concurrent_webhook_idempotency():
    settings.RAZORPAY_WEBHOOK_SECRET = "test_secret"
    payload = {"event": "payment.captured", "payload": {}}
    body = json.dumps(payload).encode()
    signature = hmac.new(b"test_secret", body, hashlib.sha256).hexdigest()
    headers = {
        "x-razorpay-event-id": "ev_concurrent",
        "x-razorpay-signature": signature,
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        responses = await asyncio.gather(
            *(ac.post("/webhooks/razorpay", content=body, headers=headers) for _ in range(10))
        )

    successes = [response for response in responses if response.json().get("status") == "ok"]
    duplicates = [
        response
        for response in responses
        if response.json() == {"status": "already_processed"}
    ]
    assert len(successes) == 1
    assert successes[0].status_code == 200
    assert len(duplicates) == len(responses) - 1
    assert all(response.status_code == 200 for response in duplicates)
