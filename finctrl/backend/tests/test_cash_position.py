import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

import pytest
import pytest_asyncio
from datetime import datetime

from finctrl.backend.database.database import get_db_session, init_db
from finctrl.backend.database.models import ERPRecordModel, RazorpayPaymentModel, RazorpayRefundModel, RazorpaySettlementModel, BankRecordModel, ReconciliationMatchModel, ReconciliationCandidateModel, ExceptionModel, MatchEvidenceModel
from finctrl.backend.reconciliation.engine import run_reconciliation

@pytest_asyncio.fixture(autouse=True)
async def clear_db():
    await init_db()
    async for db in get_db_session():
        for model in [ERPRecordModel, RazorpayPaymentModel, RazorpayRefundModel, RazorpaySettlementModel, BankRecordModel, ReconciliationMatchModel, ReconciliationCandidateModel, ExceptionModel, MatchEvidenceModel]:
            await db.execute(model.__table__.delete())
        await db.commit()
    yield

@pytest.mark.asyncio
async def test_cash_position_ignores_unrelated_credits():
    from httpx import AsyncClient, ASGITransport
    from finctrl.backend.api.main import app

    async for db in get_db_session():
        # Setup an unrelated fully reconciled bank credit
        unrelated_bank = BankRecordModel(transaction_ref="tx_unrelated", description="PAYROLL REVERSAL", amount=50000, type="CREDIT", timestamp=datetime.utcnow(), status="RECONCILED")

        # Setup a linked reconciled bank credit
        pm1 = RazorpayPaymentModel(rzp_payment_id="p_cash_1", rzp_order_id="O1", rzp_settlement_id="set_cash_1", amount=1000, fee=0, tax=0, currency="INR", status="C", created_at_ts=0, reconciliation_status="PENDING")
        set1 = RazorpaySettlementModel(rzp_settlement_id="set_cash_1", amount=1000, fees=0, tax=0, status="C", created_at_ts=0)
        bank = BankRecordModel(transaction_ref="tx_cash_1", description="SETTLEMENT set_cash_1", amount=1000, type="CREDIT", timestamp=datetime.utcnow(), status="C")

        db.add_all([unrelated_bank, pm1, set1, bank])
        await db.commit()

        await run_reconciliation(db)

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            # Use read-only API key for GET request
            headers = {"X-API-Key": "test_readonly_key"}
            resp = await ac.get("/cash-position", headers=headers)
            assert resp.status_code == 200
            data = resp.json()

            # The realized cash should ONLY include the 1000 from the settlement, NOT the 50000 unrelated credit.
            assert data["current_realized_cash"] == 1000
