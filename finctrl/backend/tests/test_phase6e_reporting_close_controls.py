import asyncio
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from finctrl.backend.api.main import app
from finctrl.backend.config import settings
from finctrl.backend.database.models import (
    AuditLogModel, BankRecordModel, Base, ERPRecordModel, ExceptionEvidenceModel,
    RazorpayPaymentModel, ReconciliationCandidateModel, ReconciliationExceptionModel,
    ReconciliationMatchModel, ReconciliationPeriodModel, ReconciliationRunModel,
)
from finctrl.backend.reconciliation.reporting import (
    ReconciliationReportingService, reconciliation_period_key,
)


@pytest_asyncio.fixture
async def report_env(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'phase6e.db').as_posix()}")
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as connection: await connection.run_sync(Base.metadata.create_all)
    yield sessions
    await engine.dispose()


async def add_run(db, key, from_ts, to_ts, status="SUCCEEDED", requested_at=None):
    run = ReconciliationRunModel(run_key=key, from_ts=from_ts, to_ts=to_ts, status=status,
        requested_at=requested_at or datetime.now(timezone.utc),
        started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc))
    db.add(run); await db.flush(); return run


@pytest.mark.asyncio
async def test_period_creation_identity_idempotency_and_window_filter(report_env):
    async with report_env() as db:
        service = ReconciliationReportingService(db)
        first, created = await service.create_period(0, 100, actor="ADMIN", correlation_id="corr-period")
        second, duplicate = await service.create_period(0, 100, actor="ADMIN")
        await service.create_period(200, 300, actor="ADMIN")
        filtered = await service.list_periods(from_ts=50, to_ts=150)
        assert created is True and duplicate is False and first.id == second.id
        assert first.period_key == reconciliation_period_key(0, 100)
        assert [period.id for period in filtered] == [first.id]
        assert first.correlation_id == "corr-period"


@pytest.mark.asyncio
async def test_concurrent_period_creation_has_one_record(report_env):
    async def worker():
        async with report_env() as db:
            return await ReconciliationReportingService(db).create_period(10, 20, actor="ADMIN")
    results = await asyncio.gather(worker(), worker())
    async with report_env() as db:
        assert await db.scalar(select(func.count(ReconciliationPeriodModel.id))) == 1
    assert sorted(created for _, created in results) == [False, True]


@pytest.mark.asyncio
async def test_report_aggregation_distributions_and_currency_boundaries(report_env):
    async with report_env() as db:
        service = ReconciliationReportingService(db)
        period, _ = await service.create_period(100, 200, actor="ADMIN")
        erp_inr = ERPRecordModel(reference_id="ERP-INR", amount=100, currency="INR",
            timestamp=datetime.fromtimestamp(150, timezone.utc), type="SALE", status="PENDING")
        erp_usd = ERPRecordModel(reference_id="ERP-USD", amount=250, currency="USD",
            timestamp=datetime.fromtimestamp(160, timezone.utc), type="SALE", status="PENDING")
        payment = RazorpayPaymentModel(rzp_payment_id="pay_report", amount=100, currency="INR",
            status="captured", created_at_ts=150)
        bank = BankRecordModel(transaction_ref="bank_report", description="REPORT", amount=100,
            type="CREDIT", timestamp=datetime.fromtimestamp(150, timezone.utc), status="CLEARED")
        db.add_all([erp_inr, erp_usd, payment, bank]); await db.flush()
        match = ReconciliationMatchModel(match_key="report-match", match_type="EXACT_1_1")
        db.add(match); await db.flush()
        from finctrl.backend.database.models import MatchEvidenceModel
        db.add(MatchEvidenceModel(match_id=match.id, record_type="ERP", record_id=erp_inr.id, source_id="ERP-INR"))
        exception = ReconciliationExceptionModel(exception_key="report-exception", exception_type="MISSING_BANK",
            status="OPEN", severity="CRITICAL", description="Missing evidence")
        db.add(exception); await db.flush()
        db.add(ExceptionEvidenceModel(exception_id=exception.id, record_type="ERP", record_id=erp_inr.id, source_id="ERP-INR"))
        candidate = ReconciliationCandidateModel(candidate_key="report-candidate", candidate_type="POTENTIAL_1_1",
            score=80, status="PENDING_INVESTIGATION",
            evidence_payload={"erp_id": str(erp_inr.id), "rzp_id": str(payment.id)})
        db.add(candidate)
        await add_run(db, "report-run", 100, 200)
        await db.commit()

        report = await service.report(period)
        assert report["counts"]["erp_records"] == 2
        assert report["counts"]["razorpay_payments"] == 1
        assert report["counts"]["reconciled_matches"] == 1
        assert report["counts"]["unresolved_candidates"] == 1
        assert report["exception_types"] == {"MISSING_BANK": 1}
        assert report["exception_severities"] == {"CRITICAL": 1}
        assert report["match_types"] == {"EXACT_1_1": 1}
        assert report["amounts_by_population_and_currency"]["erp"] == {"INR": 100, "USD": 250}
        assert all(isinstance(amount, int) for amount in report["amounts_by_population_and_currency"]["erp"].values())


@pytest.mark.asyncio
async def test_latest_successful_exact_window_run_selection_ignores_overlap(report_env):
    async with report_env() as db:
        service = ReconciliationReportingService(db)
        period, _ = await service.create_period(100, 200)
        older = await add_run(db, "older-success", 100, 200, requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
        newer = await add_run(db, "newer-success", 100, 200, requested_at=datetime(2026, 1, 2, tzinfo=timezone.utc))
        await add_run(db, "overlap-success", 50, 250, requested_at=datetime(2026, 1, 3, tzinfo=timezone.utc))
        await db.commit()
        report = await service.report(period)
        runs = await service._runs(period)
        assert report["latest_successful_run_id"] == str(newer.id)
        assert {str(run.id) for run in runs} == {older.id, newer.id}


@pytest.mark.asyncio
async def test_older_exact_window_success_remains_source_when_only_newer_run_overlaps(report_env):
    async with report_env() as db:
        service = ReconciliationReportingService(db)
        period, _ = await service.create_period(100, 200)
        exact = await add_run(db, "only-exact-success", 100, 200,
                              requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
        await add_run(db, "newer-overlapping-failure", 99, 201, "FAILED",
                      datetime(2026, 1, 2, tzinfo=timezone.utc))
        await db.commit()

        readiness = await service.readiness(period)
        assert readiness["ready"] is True
        assert readiness["blocking_reasons"] == []
        assert readiness["source_run_id"] == exact.id
        assert [str(run.id) for run in await service._runs(period)] == [exact.id]


@pytest.mark.asyncio
async def test_failed_latest_run_blocks_even_with_older_success(report_env):
    async with report_env() as db:
        service = ReconciliationReportingService(db)
        period, _ = await service.create_period(100, 200)
        older = await add_run(db, "success-first", 100, 200, requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
        await add_run(db, "failed-latest", 100, 200, "FAILED", datetime(2026, 1, 2, tzinfo=timezone.utc))
        await db.commit()
        readiness = await service.readiness(period)
        assert readiness["ready"] is False
        assert "LATEST_RECONCILIATION_RUN_NOT_SUCCESSFUL" in readiness["blocking_reasons"]
        assert readiness["source_run_id"] == older.id


@pytest.mark.asyncio
async def test_readiness_no_run_critical_candidate_and_missing_evidence_block(report_env):
    now = int(datetime.now(timezone.utc).timestamp())
    async with report_env() as db:
        service = ReconciliationReportingService(db)
        period, _ = await service.create_period(now - 10, now + 10)
        exception = ReconciliationExceptionModel(exception_key="critical-no-evidence", exception_type="MISSING_BANK",
            status="OPEN", severity="CRITICAL", description="No evidence")
        erp = ERPRecordModel(reference_id="ERP-BLOCKED", amount=1, currency="INR",
            timestamp=datetime.fromtimestamp(now, timezone.utc), type="SALE", status="PENDING")
        db.add_all([exception, erp]); await db.flush()
        candidate = ReconciliationCandidateModel(candidate_key="unresolved-period-candidate",
            candidate_type="POTENTIAL_1_1", score=1, status="PENDING_INVESTIGATION",
            evidence_payload={"erp_id": str(erp.id), "rzp_id": str(exception.id)})
        db.add(candidate); await db.commit()
        readiness = await service.readiness(period)
        assert set(readiness["blocking_reasons"]) == {
            "NO_SUCCESSFUL_RECONCILIATION_RUN", "OPEN_CRITICAL_EXCEPTION",
            "UNRESOLVED_CANDIDATES", "OPEN_EXCEPTION_MISSING_EVIDENCE"}


@pytest.mark.asyncio
async def test_exception_reporting_filters_and_aging(report_env):
    now = datetime.now(timezone.utc)
    async with report_env() as db:
        service = ReconciliationReportingService(db)
        period, _ = await service.create_period(int(now.timestamp()) - 100, int(now.timestamp()) + 100)
        exception = ReconciliationExceptionModel(exception_key="aging-exception", exception_type="MISSING_ERP",
            status="OPEN", severity="HIGH", description="Aging", created_at=now)
        db.add(exception); await db.commit()
        rows = await service.exception_report(period, status="OPEN", severity="HIGH",
                                              exception_type="MISSING_ERP",
                                              evaluated_at=now.replace(microsecond=now.microsecond) )
        assert len(rows) == 1 and rows[0]["age_seconds"] == 0 and rows[0]["age_days"] == 0
        assert rows[0]["evidence_available"] is False
        assert rows[0]["latest_successful_run_id"] is None
        assert await service.exception_report(period, severity="LOW") == []


@pytest.mark.asyncio
async def test_exception_aging_normalizes_naive_evaluated_at_as_utc(report_env):
    created = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    naive_evaluated_at = datetime(2026, 1, 3, 1, 2, 3)
    async with report_env() as db:
        service = ReconciliationReportingService(db)
        period, _ = await service.create_period(
            int(created.timestamp()) - 1, int(naive_evaluated_at.replace(tzinfo=timezone.utc).timestamp()) + 1)
        db.add(ReconciliationExceptionModel(exception_key="naive-aging", exception_type="MISSING_ERP",
            status="OPEN", severity="HIGH", description="Aging", created_at=created))
        await db.commit()

        rows = await service.exception_report(period, evaluated_at=naive_evaluated_at)
        assert rows[0]["age_seconds"] == 2 * 86400 + 3723
        assert rows[0]["age_days"] == 2


@pytest.mark.asyncio
async def test_close_denied_with_blockers_and_succeeds_when_ready(report_env):
    async with report_env() as db:
        service = ReconciliationReportingService(db)
        blocked, _ = await service.create_period(1, 2)
        with pytest.raises(ValueError, match="not close-ready"):
            await service.close_period(blocked, actor="ADMIN")

        ready, _ = await service.create_period(10, 20, correlation_id="corr-close")
        run = await add_run(db, "ready-run", 10, 20)
        await db.commit()
        closed = await service.close_period(ready, actor="ADMIN", correlation_id="corr-close")
        assert closed.status == "CLOSED" and closed.closed_by == "ADMIN"
        assert closed.closed_at is not None and str(closed.latest_run_id) == run.id
        with pytest.raises(ValueError, match="already closed"):
            await service.close_period(closed, actor="ADMIN")
        actions = set((await db.scalars(select(AuditLogModel.action).where(
            AuditLogModel.entity_id == closed.id))).all())
        assert {"RECONCILIATION_PERIOD_CREATED", "RECONCILIATION_PERIOD_CLOSE_READINESS",
                "RECONCILIATION_PERIOD_CLOSED"} <= actions


@pytest.mark.asyncio
async def test_period_and_report_api_rbac_and_correlation():
    now = int(datetime.now(timezone.utc).timestamp())
    transport = ASGITransport(app=app)
    admin = {"X-API-Key": settings.ADMIN_API_KEY, "X-Correlation-ID": "corr-api-6e"}
    readonly = {"X-API-Key": settings.READ_ONLY_API_KEY}
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        denied = await client.post(f"/reconciliation/periods?from_ts={now}&to_ts={now+1}", headers=readonly)
        created = await client.post(f"/reconciliation/periods?from_ts={now}&to_ts={now+1}", headers=admin)
        period_id = created.json()["id"]
        report = await client.get(f"/reconciliation/reports/{period_id}", headers=readonly)
        runs = await client.get(f"/reconciliation/reports/{period_id}/runs", headers=readonly)
        readiness = await client.get(f"/reconciliation/reports/{period_id}/close-readiness", headers=readonly)
        close_denied = await client.post(f"/reconciliation/periods/{period_id}/close", headers=readonly)
        unauthenticated = await client.get("/reconciliation/periods")
    assert denied.status_code == close_denied.status_code == 403
    assert unauthenticated.status_code == 401
    assert created.status_code == report.status_code == runs.status_code == readiness.status_code == 200
    assert runs.json() == []
    assert created.json()["correlation_id"] == "corr-api-6e"
    assert created.headers["X-Correlation-ID"] == "corr-api-6e"
