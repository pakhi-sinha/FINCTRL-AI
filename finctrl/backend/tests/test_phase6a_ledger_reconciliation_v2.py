import hashlib
import json
from datetime import datetime

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from finctrl.backend.database.database import async_session_maker
from finctrl.backend.database.models import (
    BankRecordModel,
    ERPRecordModel,
    FinancialEventModel,
    MatchEvidenceModel,
    RazorpayPaymentModel,
    RazorpaySettlementModel,
    ReconciliationMatchModel,
    financial_event_id,
)
from finctrl.backend.integrations.webhook_processor import WebhookProcessor
from finctrl.backend.reconciliation.engine import run_reconciliation


@pytest_asyncio.fixture(autouse=True)
async def clean_phase6a_records():
    async with async_session_maker() as db:
        for model in (
            MatchEvidenceModel,
            ReconciliationMatchModel,
            RazorpaySettlementModel,
            RazorpayPaymentModel,
            BankRecordModel,
            ERPRecordModel,
            FinancialEventModel,
        ):
            await db.execute(delete(model))
        await db.commit()


@pytest.mark.asyncio
async def test_ledger_identity_and_duplicate_ingestion_are_deterministic():
    payload = {"event": "payment.captured", "payload": {}}
    body = json.dumps(payload, sort_keys=True).encode()

    async with async_session_maker() as db:
        first = await WebhookProcessor(db)._create_and_process_event("evt_phase6", body, payload)
        second = await WebhookProcessor(db).process_razorpay_webhook(body, "unused", "evt_phase6")

        expected_id = str(financial_event_id("razorpay", "evt_phase6"))
        assert first == (False, expected_id, None)
        assert second == (True, expected_id, None)
        events = (await db.scalars(select(FinancialEventModel))).all()
        assert len(events) == 1
        assert events[0].attempt_count == 1


@pytest.mark.asyncio
async def test_failed_event_retry_preserves_identity_and_source_payload():
    payload = {"event": "payment.captured", "payload": {}}
    body = json.dumps(payload, sort_keys=True).encode()
    event_id = financial_event_id("razorpay", "evt_retry_phase6")

    async with async_session_maker() as db:
        event = FinancialEventModel(
            id=event_id,
            provider="razorpay",
            provider_event_id="evt_retry_phase6",
            event_type="payment.captured",
            payload_hash=hashlib.sha256(body).hexdigest(),
            raw_payload=payload,
            processing_status="FAILED",
            attempt_count=1,
            error_message="transient failure",
        )
        db.add(event)
        await db.commit()

        success, replayed_id, error = await WebhookProcessor(db).replay_event(str(event_id))
        await db.refresh(event)

        assert (success, replayed_id, error) == (True, str(event_id), None)
        assert event.processing_status == "PROCESSED"
        assert event.attempt_count == 2
        assert event.raw_payload == payload
        assert event.payload_hash == hashlib.sha256(body).hexdigest()
        assert event.processed_at is not None


@pytest.mark.asyncio
async def test_exact_match_has_stable_tri_party_evidence_and_is_not_duplicated():
    async with async_session_maker() as db:
        db.add_all([
            ERPRecordModel(reference_id="ERP-6A", amount=10000, timestamp=datetime.utcnow(), type="SALE", status="PENDING"),
            RazorpayPaymentModel(rzp_payment_id="pay_6a", rzp_order_id="ERP-6A", amount=10000, fee=200, tax=36, currency="INR", status="captured", created_at_ts=1),
            BankRecordModel(transaction_ref="bank_6a", description="RAZORPAY pay_6a", amount=9764, type="CREDIT", timestamp=datetime.utcnow(), status="CLEARED"),
        ])
        await db.commit()

        first = await run_reconciliation(db)
        second = await run_reconciliation(db)
        match = await db.scalar(select(ReconciliationMatchModel).where(ReconciliationMatchModel.match_type == "EXACT_1_1"))
        evidence = (await db.scalars(select(MatchEvidenceModel).where(MatchEvidenceModel.match_id == match.id))).all()

        assert first.matches_created == 1
        assert second.matches_created == 0
        assert match.match_key
        assert {(item.record_type, item.source_id) for item in evidence} == {
            ("ERP", "ERP-6A"), ("RZP", "pay_6a"), ("BANK", "bank_6a")
        }


@pytest.mark.asyncio
async def test_consolidated_match_retains_all_tri_party_source_evidence():
    async with async_session_maker() as db:
        db.add_all([
            ERPRecordModel(reference_id="ERP-C1", amount=1000, timestamp=datetime.utcnow(), type="SALE", status="PENDING"),
            ERPRecordModel(reference_id="ERP-C2", amount=2000, timestamp=datetime.utcnow(), type="SALE", status="PENDING"),
            RazorpayPaymentModel(rzp_payment_id="pay_c1", rzp_order_id="ERP-C1", rzp_settlement_id="set_6a", amount=1000, fee=10, tax=1, currency="INR", status="captured", created_at_ts=1),
            RazorpayPaymentModel(rzp_payment_id="pay_c2", rzp_order_id="ERP-C2", rzp_settlement_id="set_6a", amount=2000, fee=20, tax=2, currency="INR", status="captured", created_at_ts=1),
            RazorpaySettlementModel(rzp_settlement_id="set_6a", amount=2967, fees=30, tax=3, status="processed", created_at_ts=2),
            BankRecordModel(transaction_ref="bank_set_6a", description="SETTLEMENT set_6a", amount=2967, type="CREDIT", timestamp=datetime.utcnow(), status="CLEARED"),
        ])
        await db.commit()

        result = await run_reconciliation(db)
        match = await db.scalar(select(ReconciliationMatchModel).where(ReconciliationMatchModel.match_type == "CONSOLIDATED"))
        evidence = (await db.scalars(select(MatchEvidenceModel).where(MatchEvidenceModel.match_id == match.id))).all()
        sources = {(item.record_type, item.source_id) for item in evidence}

        assert result.matches_created == 1
        assert {"ERP-C1", "ERP-C2"} <= {source for kind, source in sources if kind == "ERP"}
        assert {"set_6a", "pay_c1", "pay_c2"} <= {source for kind, source in sources if kind == "RZP"}
        assert ("BANK", "bank_set_6a") in sources


@pytest.mark.asyncio
async def test_amount_only_signal_never_creates_reconciliation():
    async with async_session_maker() as db:
        db.add_all([
            ERPRecordModel(reference_id="ERP-NO-REF", amount=5000, timestamp=datetime.utcnow(), type="SALE", status="PENDING"),
            RazorpayPaymentModel(rzp_payment_id="pay_no_ref", rzp_order_id="OTHER", amount=5000, fee=0, tax=0, currency="INR", status="captured", created_at_ts=1),
            BankRecordModel(transaction_ref="bank_no_ref", description="UNRELATED", amount=5000, type="CREDIT", timestamp=datetime.utcnow(), status="CLEARED"),
        ])
        await db.commit()

        result = await run_reconciliation(db)
        assert result.matches_created == 0
        assert await db.scalar(select(ReconciliationMatchModel)) is None
