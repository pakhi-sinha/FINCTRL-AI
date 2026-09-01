import hashlib
import hmac
import json
import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from finctrl.backend.api.main import app
from finctrl.backend.api.routes import MAX_WEBHOOK_BODY_BYTES
from finctrl.backend.config import Settings, settings
from finctrl.backend.database.database import DATABASE_URL, async_session_maker
from finctrl.backend.database.models import (
    BankRecordModel, ERPRecordModel, FinancialEventModel, RazorpayOrderModel,
    RazorpayPaymentModel, ReconciliationMatchModel, ReconciliationPeriodModel,
    ReconciliationRunModel,
)
from finctrl.backend.reconciliation.engine import ReconciliationScope, stage_a_exact_match
from finctrl.backend.reconciliation.run_control import ReconciliationRunService
from finctrl.backend.reconciliation.reporting import (
    ClosedPeriodViolation, reconciliation_period_id, reconciliation_period_key,
)
from finctrl.backend.integrations.razorpay.sync import RazorpaySyncService, SyncStatistics
from finctrl.backend.reconciliation.forecasting import CashForecastService


def _headers():
    return {"X-API-Key": settings.ADMIN_API_KEY}


def test_dotenv_database_url_is_authoritative(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text("DATABASE_URL=sqlite+aiosqlite:///configured.db\n", encoding="utf-8")
    monkeypatch.delenv("DATABASE_URL")
    loaded = Settings(_env_file=env_path)
    assert loaded.DATABASE_URL == "sqlite+aiosqlite:///configured.db"
    assert DATABASE_URL == settings.DATABASE_URL


@pytest.mark.asyncio
async def test_forecast_rejects_excessive_historical_window():
    async with async_session_maker() as db:
        with pytest.raises(ValueError, match="historical window"):
            await CashForecastService(db).forecast(0, 367 * 86400, 14)


@pytest.mark.asyncio
async def test_webhook_uses_only_dedicated_secret_and_rejects_oversize():
    old_api, old_webhook = settings.RAZORPAY_KEY_SECRET, settings.RAZORPAY_WEBHOOK_SECRET
    settings.RAZORPAY_KEY_SECRET = "api-only-secret"
    settings.RAZORPAY_WEBHOOK_SECRET = "dedicated-webhook-secret"
    body = json.dumps({"event": "payment.captured", "payload": {}}).encode()
    base = {"x-razorpay-event-id": f"evt_{uuid4()}"}
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            api_signature = hmac.new(b"api-only-secret", body, hashlib.sha256).hexdigest()
            denied = await client.post("/webhooks/razorpay", content=body,
                headers={**base, "x-razorpay-signature": api_signature})
            assert denied.status_code == 400
            webhook_signature = hmac.new(b"dedicated-webhook-secret", body, hashlib.sha256).hexdigest()
            accepted = await client.post("/webhooks/razorpay", content=body,
                headers={**base, "x-razorpay-event-id": f"evt_{uuid4()}",
                         "x-razorpay-signature": webhook_signature})
            assert accepted.status_code == 200
            oversized = await client.post("/webhooks/razorpay", content=b"x" * (MAX_WEBHOOK_BODY_BYTES + 1),
                headers={"x-razorpay-event-id": f"evt_{uuid4()}",
                         "x-razorpay-signature": "invalid"})
            assert oversized.status_code == 413
    finally:
        settings.RAZORPAY_KEY_SECRET, settings.RAZORPAY_WEBHOOK_SECRET = old_api, old_webhook


@pytest.mark.asyncio
async def test_webhook_rejects_conflicting_provider_identity_payload():
    provider_id = f"pay_conflict_{uuid4().hex}"
    old_secret = settings.RAZORPAY_WEBHOOK_SECRET
    settings.RAZORPAY_WEBHOOK_SECRET = "conflict-webhook-secret"
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            responses = []
            for amount in (100, 999):
                payload = {"event": "payment.captured", "payload": {"payment": {"entity": {
                    "id": provider_id, "amount": amount, "currency": "INR",
                    "status": "captured", "created_at": 2_100_000_000}}}}
                body = json.dumps(payload).encode()
                signature = hmac.new(settings.RAZORPAY_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
                responses.append(await client.post("/webhooks/razorpay", content=body, headers={
                    "x-razorpay-event-id": f"evt_{uuid4()}", "x-razorpay-signature": signature}))
            assert responses[0].status_code == 200
            assert responses[1].status_code == 409
    finally:
        settings.RAZORPAY_WEBHOOK_SECRET = old_secret


@pytest.mark.asyncio
async def test_http_replay_reprocesses_provider_fact():
    payment_id = f"pay_replay_{uuid4().hex}"
    payload = {"event": "payment.captured", "payload": {"payment": {"entity": {
        "id": payment_id, "order_id": f"order_{uuid4().hex}", "amount": 500,
        "currency": "INR", "status": "captured", "created_at": 2_000_000_000}}}}
    async with async_session_maker() as db:
        event = FinancialEventModel(provider="razorpay", provider_event_id=f"payment:{payment_id}",
            event_type="payment.captured", payload_hash=hashlib.sha256(json.dumps(payload).encode()).hexdigest(),
            raw_payload=payload, processing_status="FAILED", attempt_count=1, error_message="sanitized failure")
        db.add(event); await db.commit(); event_id = str(event.id)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(f"/webhooks/replay/{event_id}", headers=_headers())
    assert response.status_code == 200
    async with async_session_maker() as db:
        assert await db.scalar(select(RazorpayPaymentModel).where(
            RazorpayPaymentModel.rzp_payment_id == payment_id)) is not None
        assert (await db.get(FinancialEventModel, event.id)).processing_status == "PROCESSED"


@pytest.mark.asyncio
async def test_reconciliation_scope_leaves_out_of_window_records_untouched():
    marker = uuid4().hex
    inside = datetime.fromtimestamp(1_700_000_000, timezone.utc)
    outside = datetime.fromtimestamp(1_600_000_000, timezone.utc)
    async with async_session_maker() as db:
        for label, when, epoch in (("in", inside, 1_700_000_000), ("out", outside, 1_600_000_000)):
            order_id, payment_id = f"order_{label}_{marker}", f"pay_{label}_{marker}"
            db.add_all([
                ERPRecordModel(reference_id=f"ref_{label}_{marker}", amount=1000, currency="INR",
                    timestamp=when, type="SALE", status="PENDING"),
                RazorpayOrderModel(rzp_order_id=order_id, receipt=f"ref_{label}_{marker}", amount=1000,
                    amount_due=0, status="paid", created_at_ts=epoch),
                RazorpayPaymentModel(rzp_payment_id=payment_id, rzp_order_id=order_id,
                    amount=1000, currency="INR", status="captured", fee=0, tax=0, created_at_ts=epoch),
                BankRecordModel(transaction_ref=f"bank_{label}_{marker}", description=payment_id,
                    amount=1000, type="CREDIT", timestamp=when, status="CLEARED"),
            ])
        await db.commit()
        assert await stage_a_exact_match(db, ReconciliationScope(1_699_999_999, 1_700_000_001)) == 1
        await db.commit()
        out_payment = await db.scalar(select(RazorpayPaymentModel).where(
            RazorpayPaymentModel.rzp_payment_id == f"pay_out_{marker}"))
        assert out_payment.reconciliation_status == "UNRECONCILED"


@pytest.mark.asyncio
async def test_concurrent_runs_receive_distinct_immutable_windows():
    observed = []

    async def capture_scope(db, scope):
        await asyncio.sleep(0)
        observed.append((scope.from_ts, scope.to_ts))
        return 0

    service = ReconciliationRunService(stage_functions=(("SCOPE_TEST", capture_scope),))
    first, second = await asyncio.gather(
        service.request_and_run(from_ts=100, to_ts=199, request_key=f"scope-{uuid4()}"),
        service.request_and_run(from_ts=200, to_ts=299, request_key=f"scope-{uuid4()}"),
    )
    assert first.status == second.status == "SUCCEEDED"
    assert sorted(observed) == [(100, 199), (200, 299)]


@pytest.mark.asyncio
async def test_retry_cannot_bypass_closed_period():
    start, end = 1_910_000_000, 1_910_000_100
    async with async_session_maker() as db:
        run = ReconciliationRunModel(run_key=f"failed-{uuid4()}", status="FAILED",
            from_ts=start, to_ts=end, attempt=1)
        period_key = reconciliation_period_key(start, end)
        period = ReconciliationPeriodModel(id=reconciliation_period_id(period_key), period_key=period_key,
            from_ts=start, to_ts=end, status="CLOSED", closed_at=datetime.now(timezone.utc), closed_by="ADMIN")
        db.add_all([run, period]); await db.commit(); run_id = run.id
    with pytest.raises(ClosedPeriodViolation):
        await ReconciliationRunService().retry(run_id)


@pytest.mark.asyncio
async def test_closed_period_blocks_ingest_sync_and_allows_audited_reopen():
    timestamp = 1_800_000_000
    key = reconciliation_period_key(timestamp - 5, timestamp + 5)
    async with async_session_maker() as db:
        period = ReconciliationPeriodModel(id=reconciliation_period_id(key), period_key=key,
            from_ts=timestamp - 5, to_ts=timestamp + 5, status="CLOSED",
            closed_at=datetime.now(timezone.utc), closed_by="ADMIN")
        db.add(period); await db.commit(); period_id = str(period.id)
        with pytest.raises(ClosedPeriodViolation):
            await RazorpaySyncService(db)._persist("orders", {"id": f"order_{uuid4().hex}",
                "amount": 1, "amount_paid": 1, "amount_due": 0, "currency": "INR",
                "status": "paid", "created_at": timestamp}, SyncStatistics("orders"))
    payload = {"records": [{"id": str(uuid4()), "reference_id": f"ref_{uuid4().hex}",
        "amount": 100, "currency": "INR", "timestamp": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
        "type": "SALE", "status": "PENDING"}]}
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        blocked = await client.post("/ingest/erp", json=payload, headers=_headers())
        assert blocked.status_code == 409
        webhook_id = f"pay_closed_{uuid4().hex}"
        webhook_payload = {"event": "payment.captured", "payload": {"payment": {"entity": {
            "id": webhook_id, "amount": 1, "currency": "INR", "status": "captured",
            "created_at": timestamp}}}}
        webhook_body = json.dumps(webhook_payload).encode()
        signature = hmac.new(settings.RAZORPAY_WEBHOOK_SECRET.encode(), webhook_body, hashlib.sha256).hexdigest()
        webhook = await client.post("/webhooks/razorpay", content=webhook_body, headers={
            "x-razorpay-event-id": f"evt_{uuid4()}", "x-razorpay-signature": signature})
        assert webhook.status_code == 500
        async with async_session_maker() as db:
            assert await db.scalar(select(RazorpayPaymentModel).where(
                RazorpayPaymentModel.rzp_payment_id == webhook_id)) is None
        reopened = await client.post(f"/reconciliation/periods/{period_id}/reopen?reason=late+arrival",
                                     headers=_headers())
        assert reopened.status_code == 200 and reopened.json()["status"] == "OPEN"
        accepted = await client.post("/ingest/erp", json=payload, headers=_headers())
        assert accepted.status_code == 200


@pytest.mark.asyncio
async def test_provider_identity_unique_constraint_is_authoritative():
    provider_id = f"pay_unique_{uuid4().hex}"
    async with async_session_maker() as db:
        db.add(RazorpayPaymentModel(rzp_payment_id=provider_id, amount=1, currency="INR",
            status="captured", created_at_ts=1))
        await db.commit()
        db.add(RazorpayPaymentModel(rzp_payment_id=provider_id, amount=1, currency="INR",
            status="captured", created_at_ts=1))
        with pytest.raises(IntegrityError):
            await db.commit()
        await db.rollback()


@pytest.mark.asyncio
async def test_production_legacy_ai_endpoint_is_controlled_and_non_mutating():
    old_mode = settings.APP_MODE
    settings.APP_MODE = "production"
    async with async_session_maker() as db:
        before = await db.scalar(select(func.count(ReconciliationMatchModel.id)))
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(f"/ai/process/{uuid4()}", headers=_headers())
        assert response.status_code == 409
        async with async_session_maker() as db:
            assert await db.scalar(select(func.count(ReconciliationMatchModel.id))) == before
    finally:
        settings.APP_MODE = old_mode
