from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select

from finctrl.backend.api.routes import ingest_rzp
from finctrl.backend.api.schemas import RZPBatchPayload
from finctrl.backend.database.database import get_db_session
from finctrl.backend.database.models import (
    BankRecordModel,
    ERPRecordModel,
    MatchEvidenceModel,
    RazorpayOrderModel,
    RazorpayPaymentModel,
    RazorpayRefundModel,
    RazorpaySettlementModel,
    ReconciliationMatchModel,
)
from finctrl.backend.reconciliation.engine import run_reconciliation
from finctrl.backend.schemas.models import RazorpayRecord


def legacy_record(*, payment_id, settlement_id, receipt, amount, kind="payment", currency="INR"):
    return RazorpayRecord(
        id=uuid4(), rzp_payment_id=payment_id, rzp_settlement_id=settlement_id,
        order_receipt=receipt, gross_amount=amount, fee=0, tax=0,
        net_amount=amount, type=kind, timestamp=datetime.now(timezone.utc),
        status="processed" if kind == "refund" else "captured",
    )


@pytest.mark.asyncio
async def test_legacy_batch_materializes_one_aggregate_settlement_and_real_refunds():
    async for db in get_db_session():
        payments = [
            legacy_record(payment_id="pay_h1", settlement_id="set_h", receipt="R1", amount=400),
            legacy_record(payment_id="pay_h2", settlement_id="set_h", receipt="R2", amount=600),
        ]
        refunds = [
            legacy_record(payment_id="rfnd_h1", settlement_id="set_rf_h", receipt="RR1", amount=100, kind="refund"),
            legacy_record(payment_id="rfnd_h2", settlement_id="set_rf_h", receipt="RR2", amount=200, kind="refund"),
        ]
        payload = RZPBatchPayload(records=[*payments, *refunds])
        await ingest_rzp(payload, db)
        await ingest_rzp(payload, db)

        settlements = (await db.scalars(select(RazorpaySettlementModel).where(
            RazorpaySettlementModel.rzp_settlement_id == "set_h"))).all()
        refund_rows = (await db.scalars(select(RazorpayRefundModel).where(
            RazorpayRefundModel.rzp_refund_id.in_(("rfnd_h1", "rfnd_h2"))))).all()
        payment_ids = set((await db.scalars(select(RazorpayPaymentModel.rzp_payment_id))).all())
        assert len(settlements) == 1 and settlements[0].amount == 1000
        assert {row.rzp_refund_id for row in refund_rows} == {"rfnd_h1", "rfnd_h2"}
        assert not ({"rfnd_h1", "rfnd_h2"} & payment_ids)


@pytest.mark.asyncio
async def test_missing_required_erp_cannot_become_consolidated_match():
    async for db in get_db_session():
        order = RazorpayOrderModel(rzp_order_id="order_missing", receipt="ERP_MISSING", amount=1000,
                                   amount_due=0, status="paid", created_at_ts=1)
        payment = RazorpayPaymentModel(rzp_payment_id="pay_missing", rzp_order_id="order_missing",
            rzp_settlement_id="set_missing", amount=1000, fee=0, tax=0, currency="INR",
            status="captured", created_at_ts=1)
        settlement = RazorpaySettlementModel(rzp_settlement_id="set_missing", amount=1000,
            fees=0, tax=0, status="processed", created_at_ts=2)
        bank = BankRecordModel(transaction_ref="set_missing", description="SETTLEMENT set_missing",
            amount=1000, type="CREDIT", timestamp=datetime.now(timezone.utc), status="CLEARED")
        db.add_all([order, payment, settlement, bank]); await db.commit()
        result = await run_reconciliation(db)
        assert result.matches_created == 0
        assert await db.scalar(select(MatchEvidenceModel).where(
            MatchEvidenceModel.record_id == settlement.id)) is None


@pytest.mark.asyncio
async def test_consolidated_refund_match_has_complete_evidence_and_is_idempotent():
    async for db in get_db_session():
        records = [
            legacy_record(payment_id="rfnd_e1", settlement_id="set_rf_e", receipt="REF_E1", amount=125, kind="refund"),
            legacy_record(payment_id="rfnd_e2", settlement_id="set_rf_e", receipt="REF_E2", amount=275, kind="refund"),
        ]
        await ingest_rzp(RZPBatchPayload(records=records), db)
        db.add_all([
            ERPRecordModel(reference_id="REF_E1", amount=125, currency="INR",
                           timestamp=datetime.now(timezone.utc), type="refund", status="completed"),
            ERPRecordModel(reference_id="REF_E2", amount=275, currency="INR",
                           timestamp=datetime.now(timezone.utc), type="refund", status="completed"),
            BankRecordModel(transaction_ref="set_rf_e", description="Razorpay Refund set_rf_e",
                            amount=400, type="debit", timestamp=datetime.now(timezone.utc), status="processed"),
        ])
        await db.commit()

        first = await run_reconciliation(db)
        second = await run_reconciliation(db)
        match = await db.scalar(select(ReconciliationMatchModel).join(MatchEvidenceModel).where(
            ReconciliationMatchModel.match_type == "REFUND_MATCH",
            MatchEvidenceModel.source_id == "rfnd_e1"))
        evidence = (await db.scalars(select(MatchEvidenceModel).where(
            MatchEvidenceModel.match_id == match.id))).all()
        assert first.matches_created == 1 and second.matches_created == 0
        assert {item.source_id for item in evidence} == {
            "rfnd_e1", "rfnd_e2", "REF_E1", "REF_E2", "set_rf_e"
        }
