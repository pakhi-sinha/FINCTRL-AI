import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

import pytest
import pytest_asyncio
import asyncio
from datetime import datetime
from uuid import uuid4

from finctrl.backend.database.database import get_db_session, init_db
from finctrl.backend.database.models import ERPRecordModel, RazorpayPaymentModel, RazorpaySettlementModel, BankRecordModel, ReconciliationMatchModel, ReconciliationCandidateModel
from finctrl.backend.reconciliation.engine import run_reconciliation
from sqlalchemy import select

@pytest_asyncio.fixture(autouse=True)
async def clear_db():
    await init_db()
    async for db in get_db_session():
        for model in [ERPRecordModel, RazorpayPaymentModel, RazorpaySettlementModel, BankRecordModel, ReconciliationMatchModel, ReconciliationCandidateModel]:
            await db.execute(model.__table__.delete())
        await db.commit()
    yield

@pytest.mark.asyncio
async def test_exact_1_1_match():
    async for db in get_db_session():
        erp = ERPRecordModel(reference_id="ORD-001", amount=10000, timestamp=datetime.utcnow(), type="SALE", status="PENDING")
        rzp = RazorpayPaymentModel(rzp_payment_id="pay_abc123", rzp_order_id="ORD-001", amount=10000, fee=200, tax=36, currency="INR", status="CAPTURED", created_at_ts=0)
        bank = BankRecordModel(transaction_ref="tx_999", description="RAZORPAY SETTLEMENT pay_abc123", amount=9764, type="CREDIT", timestamp=datetime.utcnow(), status="CLEARED")
        db.add_all([erp, rzp, bank])
        await db.commit()
        response = await run_reconciliation(db)
        assert response.matches_created == 1
        await db.refresh(erp)
        assert erp.status == "RECONCILED"

@pytest.mark.asyncio
async def test_amount_only_match_rejected():
    async for db in get_db_session():
        erp = ERPRecordModel(reference_id="ORD-002", amount=5000, timestamp=datetime.utcnow(), type="SALE", status="PENDING")
        rzp = RazorpayPaymentModel(rzp_payment_id="pay_def456", rzp_order_id="ORD-WRONG", amount=5000, fee=100, tax=18, currency="INR", status="CAPTURED", created_at_ts=0)
        bank = BankRecordModel(transaction_ref="tx_888", description="UNKNOWN SOURCE", amount=4882, type="CREDIT", timestamp=datetime.utcnow(), status="CLEARED")
        db.add_all([erp, rzp, bank])
        await db.commit()
        response = await run_reconciliation(db)
        assert response.matches_created == 0
        assert response.candidates_created > 0
        await db.refresh(erp)
        assert erp.status != "RECONCILED"

@pytest.mark.asyncio
async def test_consolidated_settlement():
    async for db in get_db_session():
        erp1 = ERPRecordModel(reference_id="O1", amount=1000, timestamp=datetime.utcnow(), type="SALE", status="P")
        erp2 = ERPRecordModel(reference_id="O2", amount=2000, timestamp=datetime.utcnow(), type="SALE", status="P")
        rzp1 = RazorpayPaymentModel(rzp_payment_id="p1", rzp_order_id="O1", amount=1000, fee=10, tax=1, currency="INR", status="C", created_at_ts=0)
        rzp2 = RazorpayPaymentModel(rzp_payment_id="p2", rzp_order_id="O2", amount=2000, fee=20, tax=2, currency="INR", status="C", created_at_ts=0)
        set1 = RazorpaySettlementModel(rzp_settlement_id="set_123", amount=2967, fees=30, tax=3, status="C", created_at_ts=0)
        bank = BankRecordModel(transaction_ref="tx_1", description="SETTLEMENT set_123", amount=2967, type="CR", timestamp=datetime.utcnow(), status="C")
        db.add_all([erp1, erp2, rzp1, rzp2, set1, bank])
        await db.commit()
        response = await run_reconciliation(db)
        assert response.matches_created >= 1
        await db.refresh(erp1)
        assert erp1.status == "RECONCILED"
