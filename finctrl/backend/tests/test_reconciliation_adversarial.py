import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

import pytest
import pytest_asyncio
from datetime import datetime

from finctrl.backend.database.database import get_db_session, init_db
from finctrl.backend.database.models import ERPRecordModel, RazorpayPaymentModel, RazorpayRefundModel, RazorpaySettlementModel, BankRecordModel, ReconciliationMatchModel, ReconciliationCandidateModel, ExceptionModel
from finctrl.backend.reconciliation.engine import run_reconciliation
from sqlalchemy import select

@pytest_asyncio.fixture(autouse=True)
async def clear_db():
    await init_db()
    async for db in get_db_session():
        for model in [ERPRecordModel, RazorpayPaymentModel, RazorpayRefundModel, RazorpaySettlementModel, BankRecordModel, ReconciliationMatchModel, ReconciliationCandidateModel, ExceptionModel]:
            await db.execute(model.__table__.delete())
        await db.commit()
    yield

@pytest.mark.asyncio
async def test_fee_tax_mismatch_exception():
    async for db in get_db_session():
        # amount = 100, fee = 1000, tax = 200 (exceeds amount)
        rzp = RazorpayPaymentModel(rzp_payment_id="pay_fee_err", rzp_order_id="ORD-001", amount=100, fee=1000, tax=200, currency="INR", status="CAPTURED", created_at_ts=0)
        db.add(rzp)
        await db.commit()
        response = await run_reconciliation(db)
        assert response.exceptions_created >= 1

        result = await db.execute(select(ExceptionModel).filter_by(record_id=rzp.id, anomaly_type="FEE_TAX_EXCEEDS_GROSS"))
        exc = result.scalar_one_or_none()
        assert exc is not None

@pytest.mark.asyncio
async def test_negative_gross_amount_exception():
    async for db in get_db_session():
        rzp = RazorpayPaymentModel(rzp_payment_id="pay_neg", rzp_order_id="ORD-002", amount=-100, fee=0, tax=0, currency="INR", status="CAPTURED", created_at_ts=0)
        db.add(rzp)
        await db.commit()
        response = await run_reconciliation(db)
        assert response.exceptions_created >= 1

        result = await db.execute(select(ExceptionModel).filter_by(record_id=rzp.id, anomaly_type="NEGATIVE_GROSS_AMOUNT"))
        exc = result.scalar_one_or_none()
        assert exc is not None

@pytest.mark.asyncio
async def test_settlement_shortfall_exception():
    from unittest.mock import patch

    async for db in get_db_session():
        # We need a settlement matched to a bank record, but with linked payments that do not sum to expected net.
        pm1 = RazorpayPaymentModel(rzp_payment_id="p1", rzp_order_id="O1", amount=1000, fee=10, tax=1, currency="INR", status="C", created_at_ts=0, reconciliation_status="PENDING")

        # Expected net from payment: 1000 - 10 - 1 = 989.
        # But settlement says 900.
        set1 = RazorpaySettlementModel(rzp_settlement_id="set_123", amount=900, fees=100, tax=0, status="C", created_at_ts=0)
        bank = BankRecordModel(transaction_ref="tx_1", description="SETTLEMENT set_123", amount=900, type="CR", timestamp=datetime.utcnow(), status="C")

        db.add_all([pm1, set1, bank])
        await db.commit()

        # Fetch it back and manually set the volatile attribute so the engine sees it
        await db.refresh(pm1)
        setattr(pm1, "_rzp_settlement_id", "set_123")

        # Mock get_unresolved_rzp_payments to return our patched instance
        with patch("finctrl.backend.reconciliation.engine.get_unresolved_rzp_payments", return_value=[pm1]):
            response = await run_reconciliation(db)

        assert response.exceptions_created >= 1

        result = await db.execute(select(ExceptionModel).filter_by(anomaly_type="SETTLEMENT_SHORTFALL"))
        exc = result.scalar_one_or_none()
        assert exc is not None

@pytest.mark.asyncio
async def test_refund_aware_match():
    async for db in get_db_session():
        pm = RazorpayPaymentModel(rzp_payment_id="pay_ref", rzp_order_id="O1", amount=1000, fee=0, tax=0, currency="INR", status="CAPTURED", created_at_ts=0)
        rm = RazorpayRefundModel(rzp_refund_id="rf_1", rzp_payment_id="pay_ref", amount=1000, currency="INR", status="processed", created_at_ts=0)

        db.add_all([pm, rm])
        await db.commit()
        response = await run_reconciliation(db)
        assert response.matches_created >= 1

        await db.refresh(rm)
        assert rm.reconciliation_status == "RECONCILED"
