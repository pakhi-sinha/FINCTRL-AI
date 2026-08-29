import asyncio
from datetime import datetime

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from finctrl.backend.api.main import app
from finctrl.backend.config import settings
from finctrl.backend.database.models import (
    AuditLogModel, BankRecordModel, Base, ERPRecordModel, RazorpayPaymentModel,
    ReconciliationCandidateModel, ReconciliationExceptionModel,
    ReconciliationMatchModel, ReconciliationRunModel, ReconciliationStageRunModel,
)
from finctrl.backend.reconciliation.run_control import (
    ReconciliationRunService, reconciliation_run_key,
)


@pytest_asyncio.fixture
async def run_env(tmp_path):
    path = (tmp_path / "phase6d.db").as_posix()
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield sessions
    await engine.dispose()


@pytest.mark.asyncio
async def test_run_creation_lifecycle_stage_history_and_audit(run_env):
    run = await ReconciliationRunService(run_env).request_and_run(
        actor="ADMIN", correlation_id="corr-run-1", request_key="run-1")
    assert run.status == "SUCCEEDED"
    assert run.requested_at and run.started_at and run.completed_at
    assert run.initiated_by == "ADMIN" and run.correlation_id == "corr-run-1"
    assert len(run.stages) == 5
    assert [stage.sequence for stage in sorted(run.stages, key=lambda x: x.sequence)] == [1, 2, 3, 4, 5]
    assert all(stage.status == "SUCCEEDED" for stage in run.stages)
    assert all(stage.started_at and stage.completed_at and stage.duration_ms >= 0 for stage in run.stages)
    async with run_env() as db:
        actions = set((await db.scalars(select(AuditLogModel.action).where(
            AuditLogModel.entity_type == "RECONCILIATION_RUN"))).all())
    assert {"RECONCILIATION_RUN_REQUESTED", "RECONCILIATION_RUN_STARTED",
            "RECONCILIATION_RUN_SUCCEEDED", "RECONCILIATION_STAGE_SUCCEEDED"} <= actions


@pytest.mark.asyncio
async def test_successful_run_records_financial_counts(run_env):
    async with run_env() as db:
        db.add_all([
            ERPRecordModel(reference_id="RUN-ERP", amount=1000, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING"),
            RazorpayPaymentModel(rzp_payment_id="pay_run", rzp_order_id="RUN-ERP", amount=1000,
                currency="INR", status="captured", fee=10, tax=1, created_at_ts=1),
            BankRecordModel(transaction_ref="bank_run", description="RAZORPAY pay_run", amount=989,
                type="CREDIT", timestamp=datetime.utcnow(), status="CLEARED"),
        ])
        await db.commit()
    run = await ReconciliationRunService(run_env).request_and_run(request_key="financial-counts")
    assert run.matches_created == 1 and run.records_examined > 0
    assert sum(stage.matches_created for stage in run.stages) == 1
    async with run_env() as db:
        assert await db.scalar(select(func.count(ReconciliationMatchModel.id))) == 1


@pytest.mark.asyncio
async def test_deterministic_run_idempotency_reuses_run(run_env):
    service = ReconciliationRunService(run_env)
    first = await service.request_and_run(from_ts=1, to_ts=2, request_key="same")
    second = await service.request_and_run(from_ts=1, to_ts=2, request_key="same")
    assert first.id == second.id
    assert first.run_key == reconciliation_run_key(1, 2, "same")
    async with run_env() as db:
        assert await db.scalar(select(func.count(ReconciliationRunModel.id))) == 1
        assert await db.scalar(select(func.count(ReconciliationStageRunModel.id))) == 5


def test_run_key_distinguishes_none_zero_and_empty_request_key():
    assert reconciliation_run_key(None, 1, "key") != reconciliation_run_key(0, 1, "key")
    assert reconciliation_run_key(1, None, "key") != reconciliation_run_key(1, 0, "key")
    assert reconciliation_run_key(1, 2, None) != reconciliation_run_key(1, 2, "")
    assert reconciliation_run_key(0, 0, "") == reconciliation_run_key(0, 0, "")


def test_stage_count_contract_matches_phase6a_return_shapes():
    counts = ReconciliationRunService._stage_counts
    assert counts(1, 2) == {"matches_created": 2, "candidates_created": 0, "exceptions_created": 0}
    assert counts(2, 3) == {"matches_created": 0, "candidates_created": 0, "exceptions_created": 3}
    assert counts(3, (4, 5)) == {"matches_created": 4, "candidates_created": 0, "exceptions_created": 5}
    assert counts(4, (6, 7)) == {"matches_created": 6, "candidates_created": 0, "exceptions_created": 7}
    assert counts(5, (8, 9)) == {"matches_created": 0, "candidates_created": 8, "exceptions_created": 9}


@pytest.mark.asyncio
async def test_stage_audit_timeline_brackets_execution(run_env):
    run = await ReconciliationRunService(run_env).request_and_run(request_key="audit-order")
    async with run_env() as db:
        entries = (await db.scalars(select(AuditLogModel).where(
            AuditLogModel.entity_id == run.id,
            AuditLogModel.action.like("RECONCILIATION_STAGE_%"),
        ).order_by(AuditLogModel.timestamp, AuditLogModel.id))).all()
    for stage in run.stages:
        relevant = [entry for entry in entries if entry.changes.get("stage") == stage.stage_name]
        assert [entry.action for entry in relevant] == [
            "RECONCILIATION_STAGE_STARTED", "RECONCILIATION_STAGE_SUCCEEDED"]
        assert relevant[0].timestamp <= relevant[1].timestamp
        assert relevant[0].timestamp == stage.started_at
        assert relevant[1].timestamp == stage.completed_at


@pytest.mark.asyncio
async def test_concurrent_identical_requests_execute_once(run_env):
    first, second = await asyncio.gather(*(
        ReconciliationRunService(run_env).request_and_run(request_key="concurrent") for _ in range(2)))
    assert first.id == second.id
    async with run_env() as db:
        assert await db.scalar(select(func.count(ReconciliationRunModel.id))) == 1
        assert await db.scalar(select(func.count(ReconciliationStageRunModel.id))) == 5
        assert await db.scalar(select(func.count(ReconciliationMatchModel.id))) == 0
        assert await db.scalar(select(func.count(ReconciliationCandidateModel.id))) == 0
        assert await db.scalar(select(func.count(ReconciliationExceptionModel.id))) == 0


async def _ok_stage(_db): return 0
async def _tuple_stage(_db): return 0, 0
async def _failed_stage(_db): raise RuntimeError("sensitive internal details")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_index,expected", [(0, "FAILED"), (1, "FAILED")])
async def test_failed_run_history_is_durable_and_sanitized(run_env, failure_index, expected):
    functions = [
        ("STAGE_A_EXACT_MATCH", _ok_stage),
        ("STAGE_B_PAYMENT_ARITHMETIC", _ok_stage),
        ("STAGE_C_SETTLEMENT", _tuple_stage),
        ("STAGE_D_REFUND", _tuple_stage),
        ("STAGE_E_CANDIDATES_EXCEPTIONS", _tuple_stage),
    ]
    functions[failure_index] = (functions[failure_index][0], _failed_stage)
    run = await ReconciliationRunService(run_env, tuple(functions)).request_and_run(
        request_key=f"failure-{failure_index}")
    assert run.status == expected and run.errors_count == 1
    assert run.error_message == "RuntimeError: reconciliation stage failed"
    assert "sensitive" not in run.error_message
    assert any(stage.status == "FAILED" for stage in run.stages)
    assert len(run.stages) == 5


async def _write_match_stage(db):
    db.add(ReconciliationMatchModel(match_key="rolled-back-match", match_type="EXACT_1_1"))
    await db.flush()
    return 1


@pytest.mark.asyncio
async def test_later_stage_failure_rolls_back_financial_work_and_reports_failed(run_env):
    functions = (
        ("STAGE_A_EXACT_MATCH", _write_match_stage),
        ("STAGE_B_PAYMENT_ARITHMETIC", _failed_stage),
        ("STAGE_C_SETTLEMENT", _tuple_stage),
        ("STAGE_D_REFUND", _tuple_stage),
        ("STAGE_E_CANDIDATES_EXCEPTIONS", _tuple_stage),
    )
    run = await ReconciliationRunService(run_env, functions).request_and_run(
        request_key="rollback-semantics")
    stages = sorted(run.stages, key=lambda stage: stage.sequence)
    async with run_env() as db:
        committed_matches = await db.scalar(select(func.count(ReconciliationMatchModel.id)))

    assert committed_matches == 0
    assert run.status == "FAILED"
    assert run.matches_created == 0
    assert run.candidates_created == 0
    assert run.exceptions_created == 0
    assert stages[0].status == "SUCCEEDED" and stages[0].matches_created == 1
    assert stages[1].status == "FAILED"
    assert [stage.status for stage in stages[2:]] == ["SKIPPED", "SKIPPED", "SKIPPED"]


class CommitFailSession(AsyncSession):
    async def commit(self):
        raise RuntimeError("simulated final commit failure")


@pytest.mark.asyncio
async def test_final_financial_commit_failure_is_durably_terminalized(run_env):
    failing_execution_sessions = async_sessionmaker(
        run_env.kw["bind"], class_=CommitFailSession, expire_on_commit=False)
    service = ReconciliationRunService(
        run_env, (("STAGE_A_EXACT_MATCH", _write_match_stage),),
        execution_session_factory=failing_execution_sessions)
    run = await service.request_and_run(request_key="commit-failure")
    async with run_env() as db:
        matches = await db.scalar(select(func.count(ReconciliationMatchModel.id)))
        failed_audit = await db.scalar(select(AuditLogModel).where(
            AuditLogModel.entity_id == run.id,
            AuditLogModel.action == "RECONCILIATION_RUN_FAILED"))

    assert run.status == "FAILED" and run.completed_at is not None
    assert run.errors_count == 1
    assert run.error_message == "RuntimeError: reconciliation transaction failed"
    assert (run.matches_created, run.candidates_created, run.exceptions_created) == (0, 0, 0)
    assert matches == 0 and failed_audit is not None
    assert len(run.stages) == 1 and run.stages[0].status == "SUCCEEDED"


class UnexpectedExecutionFailureService(ReconciliationRunService):
    async def _records_examined(self, db, sequence):
        raise OSError("simulated execution-level failure")


@pytest.mark.asyncio
async def test_unexpected_execution_failure_never_leaves_run_running(run_env):
    run = await UnexpectedExecutionFailureService(run_env).request_and_run(
        request_key="unexpected-execution-failure")
    stages = sorted(run.stages, key=lambda stage: stage.sequence)
    async with run_env() as db:
        assert await db.scalar(select(func.count(ReconciliationMatchModel.id))) == 0
        assert await db.scalar(select(func.count(ReconciliationCandidateModel.id))) == 0
        assert await db.scalar(select(func.count(ReconciliationExceptionModel.id))) == 0

    assert run.status == "FAILED" and run.status != "RUNNING"
    assert run.completed_at is not None and run.errors_count == 1
    assert (run.matches_created, run.candidates_created, run.exceptions_created) == (0, 0, 0)
    assert stages[0].status == "FAILED"
    assert all(stage.status == "SKIPPED" for stage in stages[1:])


@pytest.mark.asyncio
async def test_failed_run_retry_is_linked_and_bounded(run_env):
    functions = (("STAGE_A_EXACT_MATCH", _failed_stage),)
    service = ReconciliationRunService(run_env, functions)
    original = await service.request_and_run(request_key="retry-source")
    retry = await service.retry(original.id, actor="ADMIN", correlation_id="corr-retry")
    assert original.status == retry.status == "FAILED"
    assert retry.retry_of_id == original.id and retry.attempt == 2 and retry.id != original.id
    second_retry = await service.retry(retry.id)
    assert second_retry.attempt == 3
    with pytest.raises(ValueError, match="Maximum"):
        await service.retry(second_retry.id)


@pytest.mark.asyncio
async def test_successful_run_cannot_be_retried(run_env):
    service = ReconciliationRunService(run_env)
    run = await service.request_and_run(request_key="no-retry")
    with pytest.raises(ValueError, match="cannot be retried"):
        await service.retry(run.id)


@pytest.mark.asyncio
async def test_invalid_window_rejected(run_env):
    with pytest.raises(ValueError, match="from_ts"):
        await ReconciliationRunService(run_env).request_and_run(from_ts=2, to_ts=1)


@pytest.mark.asyncio
async def test_run_api_rbac_correlation_and_inspection():
    transport = ASGITransport(app=app)
    headers = {"X-API-Key": settings.ADMIN_API_KEY, "X-Correlation-ID": "corr-api-6d"}
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        denied = await client.post("/reconciliation/runs", headers={"X-API-Key": settings.READ_ONLY_API_KEY})
        created = await client.post("/reconciliation/runs?idempotency_key=api-6d", headers=headers)
        run_id = created.json()["id"]
        inspected = await client.get(f"/reconciliation/runs/{run_id}", headers={"X-API-Key": settings.READ_ONLY_API_KEY})
        stages = await client.get(f"/reconciliation/runs/{run_id}/stages", headers={"X-API-Key": settings.READ_ONLY_API_KEY})
        unauthenticated = await client.get("/reconciliation/runs")
    assert denied.status_code == 403 and unauthenticated.status_code == 401
    assert created.status_code == inspected.status_code == stages.status_code == 200
    assert created.json()["correlation_id"] == "corr-api-6d"
    assert created.headers["X-Correlation-ID"] == "corr-api-6d"
    assert len(stages.json()) == 5
