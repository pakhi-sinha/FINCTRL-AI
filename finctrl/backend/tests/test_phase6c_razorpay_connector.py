import asyncio
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from finctrl.backend.api.main import app
from finctrl.backend.config import settings
from finctrl.backend.database.models import (
    Base, FinancialEventModel, RazorpayOrderModel, RazorpayPaymentModel,
    RazorpayRefundModel, RazorpaySettlementModel, RazorpaySyncStateModel,
)
from finctrl.backend.integrations.razorpay.client import (
    RazorpayClient, RazorpayConnectorError, RazorpayMalformedResponse,
)
from finctrl.backend.integrations.razorpay.sync import RazorpayIdentityConflict, RazorpaySyncService
from finctrl.backend.integrations.webhook_processor import WebhookProcessor


class Resource:
    def __init__(self, pages=None, error=None):
        self.pages, self.error, self.calls = pages or [[]], error, []

    def all(self, params):
        self.calls.append(params.copy())
        if self.error: raise self.error
        index = params["skip"] // params["count"]
        return {"items": self.pages[index] if index < len(self.pages) else []}

    def fetch(self, object_id):
        return {"id": object_id, "amount": 100, "currency": "INR", "status": "created", "created_at": 1}


class SDK:
    def __init__(self, pages=None):
        self.order = Resource(pages); self.payment = Resource(pages)
        self.refund = Resource(pages); self.settlement = Resource(pages)
        self.order.payment_calls = []
        def order_payments(order_id):
            self.order.payment_calls.append((order_id,))
            return {"items": self.order.pages[0]}
        self.order.payments = order_payments


def item(object_id, **values):
    return {"id": object_id, "amount": 100, "currency": "INR", "status": "created", "created_at": 1, **values}


@pytest_asyncio.fixture
async def sync_db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'phase6c.db').as_posix()}")
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as connection: await connection.run_sync(Base.metadata.create_all)
    async with sessions() as session: yield session
    await engine.dispose()


def test_test_mode_configuration_and_injected_connector():
    connector = RazorpayClient(SDK(), page_size=25)
    assert settings.RAZORPAY_MODE == "test"
    assert connector.mode == "test" and connector.page_size == 25


def test_page_size_is_bounded():
    with pytest.raises(ValueError): RazorpayClient(SDK(), page_size=101)


@pytest.mark.parametrize("method", ["fetch_orders", "fetch_payments", "fetch_refunds", "fetch_settlements"])
def test_collection_resources_are_read_only(method):
    sdk = SDK([[item("obj_1")]])
    assert [x["id"] for x in getattr(RazorpayClient(sdk), method)()] == ["obj_1"]
    assert not any(callable(getattr(RazorpayClient(sdk), name, None)) for name in ("create_order", "capture_payment", "create_refund", "create_payout"))


def test_payments_for_order_uses_read_endpoint():
    sdk = SDK([[item("pay_1", order_id="order_1")]])
    assert RazorpayClient(sdk).fetch_order_payments("order_1")[0]["order_id"] == "order_1"
    assert sdk.order.payment_calls == [("order_1",)]


def test_order_payments_rejects_unsupported_collection_parameters():
    sdk = SDK([[item("pay_1")]])
    with pytest.raises(ValueError, match="does not support"):
        RazorpayClient(sdk).fetch_order_payments("order_1", from_ts=1, to_ts=2)
    assert sdk.order.payment_calls == []


def test_missing_client_fails_immediately_with_authentication_error():
    connector = RazorpayClient(sdk_client=None)
    connector.client = None
    with pytest.raises(RazorpayConnectorError) as raised:
        connector.fetch_orders()
    assert raised.value.status_code == 401
    assert raised.value.category == "authentication"
    assert str(raised.value) == "Razorpay client is not configured"


def test_all_collection_sdk_argument_shapes():
    sdk = SDK([[]]); connector = RazorpayClient(sdk, page_size=25)
    connector.fetch_orders(from_ts=1, to_ts=2)
    connector.fetch_payments(from_ts=1, to_ts=2)
    connector.fetch_refunds(from_ts=1, to_ts=2)
    connector.fetch_settlements(from_ts=1, to_ts=2)
    connector.fetch_order_payments("order_shape")
    expected = {"count": 25, "skip": 0, "from": 1, "to": 2}
    assert sdk.order.calls == [expected]
    assert sdk.payment.calls == [expected]
    assert sdk.refund.calls == [expected]
    assert sdk.settlement.calls == [expected]
    assert sdk.order.payment_calls == [("order_shape",)]


def test_pagination_window_overlap_and_final_page():
    sdk = SDK([[item("a"), item("b")], [item("b"), item("c")], [item("d")]])
    result = RazorpayClient(sdk, page_size=2).fetch_orders(from_ts=10, to_ts=20)
    assert [x["id"] for x in result] == ["a", "b", "c", "d"]
    assert [c["skip"] for c in sdk.order.calls] == [0, 2, 4]
    assert all(c["from"] == 10 and c["to"] == 20 for c in sdk.order.calls)


def test_empty_page_terminates():
    sdk = SDK([[]]); assert RazorpayClient(sdk).fetch_orders() == []; assert len(sdk.order.calls) == 1


def test_repeated_page_without_progress_is_rejected():
    sdk = SDK([[item("a")], [item("a")]])
    with pytest.raises(RazorpayMalformedResponse): RazorpayClient(sdk, page_size=1).fetch_orders()


def test_malformed_response_is_rejected():
    sdk = SDK(); sdk.order.all = lambda params: {"items": "invalid"}
    with pytest.raises(RazorpayMalformedResponse): RazorpayClient(sdk).fetch_orders()


@pytest.mark.parametrize("error", [TimeoutError(), ConnectionError()])
def test_transient_failures_retry_with_exponential_backoff(error):
    sleeps, calls = [], 0
    sdk = SDK()
    def flaky(params):
        nonlocal calls; calls += 1
        if calls < 3: raise error
        return {"items": []}
    sdk.order.all = flaky
    assert RazorpayClient(sdk, max_retries=2, backoff_seconds=1, sleep=sleeps.append).fetch_orders() == []
    assert sleeps == [1, 2]


def test_permanent_4xx_is_not_retried():
    error = RuntimeError(); error.status_code = 400
    sdk = SDK(); sdk.order.error = error
    with pytest.raises(RazorpayConnectorError) as raised: RazorpayClient(sdk, sleep=Mock()).fetch_orders()
    assert raised.value.status_code == 400 and len(sdk.order.calls) == 1


def test_terminal_provider_error_is_sanitized_and_categorized():
    error = RuntimeError("Authorization: Basic key_id:super-secret request_body=private")
    error.status_code = 503
    sdk = SDK(); sdk.order.error = error
    with pytest.raises(RazorpayConnectorError) as raised:
        RazorpayClient(sdk, max_retries=0).fetch_orders()
    assert raised.value.status_code == 503
    assert raised.value.category == "provider_server"
    assert str(raised.value) == "Razorpay read failed: provider_server (HTTP 503)"
    assert "secret" not in str(raised.value) and "Authorization" not in str(raised.value)


def test_rate_limit_is_bounded_and_backed_off():
    error = RuntimeError(); error.status_code = 429
    sdk, sleep = SDK(), Mock(); sdk.order.error = error
    with pytest.raises(RazorpayConnectorError): RazorpayClient(sdk, max_retries=2, sleep=sleep).fetch_orders()
    assert len(sdk.order.calls) == 3 and sleep.call_count == 2


def test_provider_5xx_retries_then_succeeds():
    error = RuntimeError(); error.status_code = 503
    sdk, calls = SDK(), 0
    def flaky(params):
        nonlocal calls; calls += 1
        if calls == 1: raise error
        return {"items": []}
    sdk.order.all = flaky
    assert RazorpayClient(sdk, max_retries=1, backoff_seconds=0, sleep=lambda _: None).fetch_orders() == []
    assert calls == 2


@pytest.mark.asyncio
async def test_incremental_sync_persists_provenance_and_state(sync_db):
    sdk = SDK([[item("order_1", receipt="r1", amount_due=100, amount_paid=0)]])
    stats = await RazorpaySyncService(sync_db, RazorpayClient(sdk)).sync_resource("orders", from_ts=1, to_ts=2)
    order = await sync_db.scalar(select(RazorpayOrderModel))
    event = await sync_db.get(FinancialEventModel, order.source_event_id)
    state = await sync_db.scalar(select(RazorpaySyncStateModel))
    assert stats["created"] == 1 and event.provider_event_id == "order:order_1"
    assert state.last_from_ts == 1 and state.last_to_ts == 2 and state.last_status == "SUCCESS"


@pytest.mark.asyncio
@pytest.mark.parametrize("resource,model,payload", [
    ("payments", RazorpayPaymentModel, item("pay_1", order_id="o1", captured=True)),
    ("refunds", RazorpayRefundModel, item("rfnd_1", payment_id="pay_1")),
    ("settlements", RazorpaySettlementModel, item("setl_1", fees=0, tax=0)),
])
async def test_entity_normalization(sync_db, resource, model, payload):
    sdk = SDK([[payload]])
    await RazorpaySyncService(sync_db, RazorpayClient(sdk)).sync_resource(resource)
    assert await sync_db.scalar(select(func.count(model.id))) == 1


@pytest.mark.asyncio
async def test_duplicate_sync_is_idempotent(sync_db):
    sdk = SDK([[item("order_dup", receipt="r", amount_due=100, amount_paid=0)]])
    service = RazorpaySyncService(sync_db, RazorpayClient(sdk))
    await service.sync_resource("orders"); sdk.order.calls.clear()
    second = await service.sync_resource("orders")
    assert second["duplicates_ignored"] == 1
    assert await sync_db.scalar(select(func.count(RazorpayOrderModel.id))) == 1
    assert await sync_db.scalar(select(func.count(FinancialEventModel.id))) == 1


@pytest.mark.asyncio
async def test_webhook_created_record_is_deduplicated_by_api_sync(sync_db):
    sync_db.add(RazorpayPaymentModel(rzp_payment_id="pay_webhook", amount=100, currency="INR", status="captured", created_at_ts=1))
    await sync_db.commit()
    sdk = SDK([[item("pay_webhook", captured=True)]])
    result = await RazorpaySyncService(sync_db, RazorpayClient(sdk)).sync_resource("payments")
    assert result["updated"] == 1
    assert await sync_db.scalar(select(func.count(RazorpayPaymentModel.id))) == 1


@pytest.mark.asyncio
async def test_api_sync_then_webhook_delivery_deduplicates(sync_db):
    sdk = SDK([[item("pay_api_first", captured=True)]])
    await RazorpaySyncService(sync_db, RazorpayClient(sdk)).sync_resource("payments")
    payload = {"event": "payment.captured", "payload": {
        "payment": {"entity": item("pay_api_first", captured=True)}}}
    body = json.dumps(payload).encode()
    result = await WebhookProcessor(sync_db)._create_and_process_event("delivery_api_first", body, payload)
    payment = await sync_db.scalar(select(RazorpayPaymentModel))
    assert await sync_db.scalar(select(func.count(RazorpayPaymentModel.id))) == 1
    assert await sync_db.scalar(select(func.count(FinancialEventModel.id))) == 1
    assert result[0] is True
    assert str(payment.source_event_id) == result[1]


@pytest.mark.asyncio
async def test_webhook_first_api_sync_converges_financial_event(sync_db):
    payload = {"event": "payment.captured", "payload": {
        "payment": {"entity": item("pay_webhook_first", captured=True)}}}
    body = json.dumps(payload).encode()
    webhook = await WebhookProcessor(sync_db)._create_and_process_event("delivery_webhook_first", body, payload)
    sdk = SDK([[item("pay_webhook_first", captured=True)]])
    await RazorpaySyncService(sync_db, RazorpayClient(sdk)).sync_resource("payments")
    payment = await sync_db.scalar(select(RazorpayPaymentModel))
    event = await sync_db.scalar(select(FinancialEventModel))
    assert await sync_db.scalar(select(func.count(FinancialEventModel.id))) == 1
    assert event.provider_event_id == "payment:pay_webhook_first"
    assert str(payment.source_event_id) == webhook[1] == str(event.id)


@pytest.mark.asyncio
async def test_concurrent_api_webhook_insert_has_one_event_and_provider(tmp_path):
    database_path = (tmp_path / "api-webhook-race.db").as_posix()
    engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as connection: await connection.run_sync(Base.metadata.create_all)
    payload = {"event": "payment.captured", "payload": {
        "payment": {"entity": item("pay_race", captured=True)}}}
    body = json.dumps(payload).encode()

    async def api_worker():
        async with sessions() as db:
            return await RazorpaySyncService(db, RazorpayClient(SDK([[item("pay_race", captured=True)]]))).sync_resource("payments")

    async def webhook_worker():
        async with sessions() as db:
            return await WebhookProcessor(db)._create_and_process_event("delivery_race", body, payload)

    await asyncio.gather(api_worker(), webhook_worker())
    async with sessions() as db:
        payment = await db.scalar(select(RazorpayPaymentModel))
        event = await db.scalar(select(FinancialEventModel))
        assert await db.scalar(select(func.count(RazorpayPaymentModel.id))) == 1
        assert await db.scalar(select(func.count(FinancialEventModel.id))) == 1
        assert payment.source_event_id == event.id
    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("field,incoming", [("amount", 101), ("currency", "USD")])
async def test_authoritative_financial_field_conflict_is_audited(sync_db, field, incoming):
    sync_db.add(RazorpayPaymentModel(rzp_payment_id="pay_financial_conflict", amount=100,
        currency="INR", status="captured", created_at_ts=1))
    await sync_db.commit()
    payload = item("pay_financial_conflict", captured=True, **{field: incoming})
    with pytest.raises(RazorpayIdentityConflict):
        await RazorpaySyncService(sync_db, RazorpayClient(SDK([[payload]]))).sync_resource("payments")
    payment = await sync_db.scalar(select(RazorpayPaymentModel))
    from finctrl.backend.database.models import AuditLogModel
    audit = await sync_db.scalar(select(AuditLogModel).where(
        AuditLogModel.action == "IMMUTABLE_IDENTITY_CONFLICT"))
    assert payment.amount == 100 and payment.currency == "INR"
    assert audit.changes["provider_id"] == "pay_financial_conflict"


@pytest.mark.asyncio
async def test_sync_emits_structured_statistics(sync_db, caplog):
    sdk = SDK([[]])
    with caplog.at_level("INFO"):
        result = await RazorpaySyncService(sync_db, RazorpayClient(sdk)).sync_resource("orders")
    assert result["fetched"] == 0
    record = next(item for item in caplog.records if item.message == "Razorpay sync completed")
    assert record.razorpay_sync["resource_type"] == "orders"


@pytest.mark.asyncio
async def test_immutable_identity_conflict_is_detected(sync_db):
    sync_db.add(RazorpayOrderModel(rzp_order_id="order_conflict", receipt="r", amount=100,
        amount_paid=0, amount_due=100, currency="INR", status="created", created_at_ts=1))
    await sync_db.commit()
    sdk = SDK([[item("order_conflict", receipt="r", amount_due=100, amount_paid=0, created_at=2)]])
    with pytest.raises(RazorpayIdentityConflict):
        await RazorpaySyncService(sync_db, RazorpayClient(sdk)).sync_resource("orders")
    state = await sync_db.scalar(select(RazorpaySyncStateModel))
    assert state.last_status == "FAILED"


@pytest.mark.asyncio
async def test_partial_sync_failure_rolls_back_resource_records(sync_db):
    sdk = SDK([[item("good", receipt="r", amount_due=100, amount_paid=0), {"amount": 1}]])
    with pytest.raises(RazorpayMalformedResponse):
        await RazorpaySyncService(sync_db, RazorpayClient(sdk)).sync_resource("orders")
    assert await sync_db.scalar(select(func.count(RazorpayOrderModel.id))) == 0
    state = await sync_db.scalar(select(RazorpaySyncStateModel))
    assert state.last_status == "FAILED"
    assert state.last_error == "RazorpayMalformedResponse: Razorpay synchronization failed"


@pytest.mark.asyncio
async def test_sync_endpoint_requires_admin():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        denied = await client.post("/razorpay/sync/orders", headers={"X-API-Key": settings.READ_ONLY_API_KEY})
        missing = await client.post("/razorpay/sync/orders")
    assert denied.status_code == 403 and missing.status_code == 401
