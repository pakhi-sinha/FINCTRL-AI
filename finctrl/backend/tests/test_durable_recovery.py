import asyncio
from datetime import datetime

import pytest
import pytest_asyncio
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from finctrl.backend.database.models import (
    AIInvestigationApprovalModel, AIInvestigationModel, Base, ERPRecordModel,
    ExceptionEvidenceModel, FinancialEventModel, RazorpayPaymentModel,
    ReconciliationExceptionModel, ReconciliationRunModel,
)
from finctrl.backend.integrations.webhook_processor import WebhookProcessor
from finctrl.backend.reconciliation.investigation import (
    InvestigationProvider, InvestigationResult, InvestigationService,
)
from finctrl.backend.reconciliation.run_control import ReconciliationRunService, reconciliation_run_key
from finctrl.backend.recovery.leases import Lease, claim, heartbeat, owned
from finctrl.backend.recovery.worker import RecoveryWorker


@pytest_asyncio.fixture
async def lease_env(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'leases.db').as_posix()}")
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield sessions
    await engine.dispose()


async def expire(db, model, entity_id):
    await db.execute(update(model).where(model.id == entity_id).values(
        lease_expires_at=func.datetime(func.current_timestamp(), "-1 second")))
    await db.commit()


@pytest.mark.asyncio
async def test_database_time_claim_heartbeat_takeover_and_stale_fencing(lease_env):
    async with lease_env() as db:
        run = ReconciliationRunModel(run_key="lease-cas", status="REQUESTED")
        db.add(run); await db.commit(); run_id = run.id
        first, second = Lease.new("worker-a"), Lease.new("worker-b")
        assert await claim(db, ReconciliationRunModel, run_id, first, 600,
                           eligible_statuses={"REQUESTED"}, active_status="RUNNING")
        await db.commit()
    async with lease_env() as db:
        assert not await claim(db, ReconciliationRunModel, run_id, second, 600,
                               eligible_statuses={"REQUESTED"}, active_status="RUNNING")
        await db.rollback()
        assert await heartbeat(db, ReconciliationRunModel, run_id, first, 600,
                               active_status="RUNNING")
        await db.commit()
        await expire(db, ReconciliationRunModel, run_id)
        assert await claim(db, ReconciliationRunModel, run_id, second, 600,
                           eligible_statuses={"REQUESTED"}, active_status="RUNNING")
        await db.commit()
        stale = await db.execute(update(ReconciliationRunModel).where(
            owned(ReconciliationRunModel, run_id, first, active_status="RUNNING")
        ).values(status="SUCCEEDED"))
        assert stale.rowcount == 0


@pytest.mark.asyncio
async def test_reconciliation_expired_run_recovers_same_identity_and_window(lease_env):
    service = ReconciliationRunService(lease_env)
    async with lease_env() as db:
        run = ReconciliationRunModel(run_key=reconciliation_run_key(10, 20, "crash"),
            status="RUNNING", from_ts=10, to_ts=20, lease_owner="dead",
            execution_attempt_id="dead-attempt",
            lease_expires_at=func.datetime(func.current_timestamp(), "-1 second"))
        db.add(run); await db.commit(); run_id = run.id
    assert await service.recover_eligible("worker-recovery") == 1
    recovered = await service.get_run(run_id)
    assert recovered.status == "SUCCEEDED"
    assert (recovered.from_ts, recovered.to_ts, str(recovered.id)) == (10, 20, str(run_id))


@pytest.mark.asyncio
async def test_concurrent_replay_has_one_owner_and_one_provider_fact(lease_env):
    payload = {"event": "payment.captured", "payload": {"payment": {"entity": {
        "id": "pay_recovery", "order_id": "order_recovery", "amount": 100,
        "currency": "INR", "status": "captured", "created_at": 1}}}}
    async with lease_env() as db:
        event = FinancialEventModel(provider="razorpay", provider_event_id="payment:pay_recovery",
            event_type="payment.captured", payload_hash="hash", raw_payload=payload,
            processing_status="FAILED", attempt_count=1)
        db.add(event); await db.commit(); event_id = event.id

    async def replay(worker):
        async with lease_env() as db:
            return await WebhookProcessor(db, lease_env, worker).replay_event(str(event_id))

    results = await asyncio.gather(replay("worker-a"), replay("worker-b"))
    assert sum(result[0] for result in results) == 1
    async with lease_env() as db:
        assert await db.scalar(select(func.count(RazorpayPaymentModel.id))) == 1
        event = await db.get(FinancialEventModel, event_id)
        assert event.processing_status == "PROCESSED"


class RecoveryProvider(InvestigationProvider):
    name, model = "test", "recovery"

    async def investigate(self, case_payload):
        reference = case_payload["evidence"][0]["reference"]
        return InvestigationResult(classification="MISSING_RECORD", root_cause="Missing provider fact",
            summary="Manual review required", recommended_action="MANUAL_REVIEW",
            confidence=0.9, evidence_references=[reference], requires_human_approval=True)


@pytest.mark.asyncio
async def test_ai_expired_takeover_stays_approval_gated(lease_env):
    async with lease_env() as db:
        erp = ERPRecordModel(reference_id="ERP-LEASE", amount=100, currency="INR",
            timestamp=datetime.utcnow(), type="SALE", status="PENDING")
        db.add(erp); await db.flush()
        exception = ReconciliationExceptionModel(exception_key="ai-lease", exception_type="MISSING_RAZORPAY",
            severity="HIGH", description="Missing", status="OPEN")
        db.add(exception); await db.flush()
        db.add(ExceptionEvidenceModel(exception_id=exception.id, record_type="ERP",
            record_id=erp.id, source_id=erp.reference_id))
        item = AIInvestigationModel(exception_id=exception.id, request_key="a" * 64,
            provider="test", model="recovery", status="RUNNING", input_hash="b" * 64,
            requested_by="ADMIN", lease_owner="dead", execution_attempt_id="dead-attempt",
            lease_expires_at=func.datetime(func.current_timestamp(), "-1 second"))
        db.add(item); await db.commit(); item_id = item.id
    async with lease_env() as db:
        service = InvestigationService(db, RecoveryProvider(), lease_env, "worker-ai")
        assert await service.recover_eligible("worker-ai") == 1
    async with lease_env() as db:
        item = await db.get(AIInvestigationModel, item_id)
        approval = await db.scalar(select(AIInvestigationApprovalModel).where(
            AIInvestigationApprovalModel.investigation_id == item_id))
        assert item.status == "COMPLETED" and approval.status == "PENDING"


@pytest.mark.asyncio
async def test_competing_recovery_workers_database_selects_one(lease_env):
    async with lease_env() as db:
        db.add(ReconciliationRunModel(run_key="worker-race", status="REQUESTED"))
        await db.commit()
    first, second = RecoveryWorker("worker-1", lease_env), RecoveryWorker("worker-2", lease_env)
    results = await asyncio.gather(first.scan_once(), second.scan_once())
    assert sum(result["reconciliation"] for result in results) == 1
    async with lease_env() as db:
        run = await db.scalar(select(ReconciliationRunModel))
        assert run.status == "SUCCEEDED"
