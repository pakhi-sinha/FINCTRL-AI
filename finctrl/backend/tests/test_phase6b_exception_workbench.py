from datetime import datetime

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

from finctrl.backend.api.main import app
from finctrl.backend.database.database import async_session_maker
from finctrl.backend.database.models import (
    BankRecordModel,
    ERPRecordModel,
    ExceptionModel,
    ExceptionAuditModel,
    ExceptionEvidenceModel,
    RazorpayPaymentModel,
    RazorpayRefundModel,
    RazorpaySettlementModel,
    ReconciliationCandidateModel,
    ReconciliationExceptionModel,
    ReconciliationMatchModel,
)
from finctrl.backend.reconciliation.engine import run_reconciliation
from finctrl.backend.reconciliation.workbench import run_exception_workbench


@pytest_asyncio.fixture(autouse=True)
async def clean_workbench():
    async with async_session_maker() as db:
        for model in (
            ExceptionAuditModel, ExceptionEvidenceModel, ReconciliationExceptionModel,
            ReconciliationCandidateModel, ReconciliationMatchModel, ExceptionModel,
            RazorpayRefundModel, RazorpaySettlementModel, BankRecordModel,
            RazorpayPaymentModel, ERPRecordModel,
        ):
            await db.execute(delete(model))
        await db.commit()


@pytest.mark.asyncio
async def test_unresolved_exception_is_deterministic_idempotent_and_authoritative():
    async with async_session_maker() as db:
        erp = ERPRecordModel(
            reference_id="ERP-MISSING-RZP", amount=1200, currency="INR",
            timestamp=datetime.utcnow(), type="SALE", status="PENDING",
        )
        db.add(erp)
        await db.commit()

        first = await run_exception_workbench(db)
        second = await run_exception_workbench(db)
        exceptions = (await db.scalars(select(ReconciliationExceptionModel))).all()
        evidence = (await db.scalars(select(ExceptionEvidenceModel))).all()

        assert first == (0, 1)
        assert second == (0, 0)
        assert len(exceptions) == 1
        assert exceptions[0].exception_type == "MISSING_RAZORPAY"
        assert len(exceptions[0].exception_key) == 64
        assert [(item.record_type, str(item.record_id), item.source_id) for item in evidence] == [
            ("ERP", str(erp.id), "ERP-MISSING-RZP")
        ]


@pytest.mark.asyncio
async def test_candidate_score_and_signals_are_deterministic_without_auto_match():
    async with async_session_maker() as db:
        erp = ERPRecordModel(reference_id="ERP-A", amount=5000, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING")
        payment = RazorpayPaymentModel(rzp_payment_id="pay_a", rzp_order_id="DIFFERENT", amount=5000, currency="INR", status="captured", created_at_ts=0)
        db.add_all([erp, payment])
        await db.commit()

        await run_exception_workbench(db)
        candidate = await db.scalar(select(ReconciliationCandidateModel))
        first_key, first_score, first_signals = candidate.candidate_key, candidate.score, candidate.evidence_payload["signals"]
        await run_exception_workbench(db)

        assert (first_score, first_signals) == (60, ["amount_exact", "currency_exact"])
        assert await db.scalar(select(func.count(ReconciliationCandidateModel.id))) == 1
        assert (await db.scalar(select(ReconciliationCandidateModel))).candidate_key == first_key
        assert await db.scalar(select(func.count(ReconciliationMatchModel.id))) == 0


@pytest.mark.asyncio
async def test_phase6a_amount_candidate_path_and_legacy_signal_are_preserved():
    async with async_session_maker() as db:
        db.add_all([
            ERPRecordModel(reference_id="ERP-LEGACY-CANDIDATE", amount=5100, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING"),
            RazorpayPaymentModel(rzp_payment_id="pay_legacy_candidate", rzp_order_id="OTHER-REF", amount=5100, currency="INR", status="captured", created_at_ts=0),
        ])
        await db.commit()

        result = await run_reconciliation(db)
        candidate = await db.scalar(select(ReconciliationCandidateModel))

        assert result.candidates_created == 1
        assert candidate.evidence_payload["signal"] == "AMOUNT_MATCH_REF_MISMATCH"
        assert candidate.evidence_payload["erp_id"]
        assert candidate.evidence_payload["rzp_id"]
        assert candidate.candidate_key
        assert candidate.score == 60
        assert await db.scalar(select(func.count(ReconciliationMatchModel.id))) == 0


@pytest.mark.asyncio
async def test_multiple_candidates_create_ambiguous_exception_with_candidate_evidence():
    async with async_session_maker() as db:
        erp = ERPRecordModel(reference_id="ERP-AMB", amount=7000, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING")
        payments = [
            RazorpayPaymentModel(rzp_payment_id=f"pay_amb_{index}", rzp_order_id=f"OTHER-{index}", amount=7000, currency="INR", status="captured", created_at_ts=0)
            for index in (1, 2)
        ]
        db.add_all([erp, *payments])
        await db.commit()

        await run_exception_workbench(db)
        exception = await db.scalar(select(ReconciliationExceptionModel).where(
            ReconciliationExceptionModel.exception_type == "AMBIGUOUS_MATCH"
        ))
        evidence = (await db.scalars(select(ExceptionEvidenceModel).where(
            ExceptionEvidenceModel.exception_id == exception.id
        ))).all()

        assert exception is not None
        assert sum(item.record_type == "RECONCILIATION_CANDIDATE" for item in evidence) == 2
        assert await db.scalar(select(func.count(ReconciliationMatchModel.id))) == 0


@pytest.mark.asyncio
async def test_phase6a_exact_match_does_not_become_exception():
    async with async_session_maker() as db:
        db.add_all([
            ERPRecordModel(reference_id="ERP-OK", amount=1000, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING"),
            RazorpayPaymentModel(rzp_payment_id="pay_ok", rzp_order_id="ERP-OK", amount=1000, fee=0, tax=0, currency="INR", status="captured", created_at_ts=0),
            BankRecordModel(transaction_ref="bank_ok", description="RAZORPAY pay_ok", amount=1000, type="CREDIT", timestamp=datetime.utcnow(), status="CLEARED"),
        ])
        await db.commit()

        result = await run_reconciliation(db)
        assert result.matches_created == 1
        assert await db.scalar(select(func.count(ReconciliationExceptionModel.id))) == 0


@pytest.mark.asyncio
async def test_lifecycle_api_evidence_candidates_and_audit():
    async with async_session_maker() as db:
        db.add(ERPRecordModel(reference_id="ERP-API", amount=900, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING"))
        await db.commit()
        await run_exception_workbench(db)
        await db.commit()
        exception = await db.scalar(select(ReconciliationExceptionModel))
        exception_id = str(exception.id)

    transport = ASGITransport(app=app)
    admin = {"X-API-Key": "test_admin_key"}
    readonly = {"X-API-Key": "test_readonly_key"}
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        listed = await client.get("/exceptions", headers=readonly)
        evidence = await client.get(f"/exceptions/{exception_id}/evidence", headers=readonly)
        candidates = await client.get(f"/exceptions/{exception_id}/candidates", headers=readonly)
        investigating = await client.post(f"/exceptions/{exception_id}/investigate", headers=admin)
        resolved = await client.post(
            f"/exceptions/{exception_id}/resolve", headers=admin,
            json={"resolution_type": "MANUAL_REVIEW", "resolution_note": "Verified externally"},
        )
        invalid = await client.post(f"/exceptions/{exception_id}/dismiss", headers=admin, json={})

    assert listed.status_code == 200 and len(listed.json()) == 1
    assert evidence.json()["missing_sources"] == ["BANK", "RZP"]
    assert evidence.json()["facts"][0]["reference_id"] == "ERP-API"
    assert evidence.json()["facts"][0]["amount"] == 900
    assert evidence.json()["facts"][0]["currency"] == "INR"
    assert candidates.json() == []
    assert investigating.json()["status"] == "INVESTIGATING"
    assert resolved.json()["status"] == "RESOLVED"
    assert resolved.json()["resolved_at"] is not None
    assert invalid.status_code == 409
    async with async_session_maker() as db:
        audits = (await db.scalars(select(ExceptionAuditModel).order_by(ExceptionAuditModel.timestamp))).all()
        assert [(item.previous_status, item.new_status, item.actor) for item in audits] == [
            ("OPEN", "INVESTIGATING", "ADMIN"),
            ("INVESTIGATING", "RESOLVED", "ADMIN"),
        ]


@pytest.mark.asyncio
async def test_duplicate_exception_evidence_is_rejected_by_database():
    async with async_session_maker() as db:
        erp = ERPRecordModel(reference_id="ERP-EVIDENCE", amount=1, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING")
        db.add(erp)
        await db.commit()
        await run_exception_workbench(db)
        exception = await db.scalar(select(ReconciliationExceptionModel))
        db.add(ExceptionEvidenceModel(
            exception_id=exception.id, record_type="ERP", record_id=erp.id, source_id=erp.reference_id
        ))
        with pytest.raises(IntegrityError):
            await db.commit()


@pytest.mark.asyncio
async def test_open_exception_can_be_dismissed_and_unknown_id_is_not_found():
    async with async_session_maker() as db:
        db.add(ERPRecordModel(reference_id="ERP-DISMISS", amount=2, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING"))
        await db.commit()
        await run_exception_workbench(db)
        await db.commit()
        exception_id = str((await db.scalar(select(ReconciliationExceptionModel))).id)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        dismissed = await client.post(
            f"/exceptions/{exception_id}/dismiss",
            headers={"X-API-Key": "test_admin_key"},
            json={"resolution_type": "NOT_ACTIONABLE", "resolution_note": "Reviewed"},
        )
        missing = await client.get(
            "/exceptions/00000000-0000-0000-0000-000000000000",
            headers={"X-API-Key": "test_readonly_key"},
        )

    assert dismissed.status_code == 200
    assert dismissed.json()["status"] == "DISMISSED"
    assert dismissed.json()["resolved_at"] is not None
    assert missing.status_code == 404
