import asyncio
import json
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from finctrl.backend.api.main import app
from finctrl.backend.config import settings
from finctrl.backend.database.database import async_session_maker
from finctrl.backend.database.models import (
    AIInvestigationApprovalModel, AIInvestigationModel, AuditLogModel,
    Base, ERPRecordModel, ExceptionEvidenceModel, ReconciliationExceptionModel,
)
from finctrl.backend.reconciliation import investigation as module
from finctrl.backend.reconciliation.investigation import (
    GeminiInvestigationProvider, InvestigationProvider, InvestigationProviderError, InvestigationValidationError,
    InvestigationResult, OpenRouterInvestigationProvider, get_investigation_provider,
)

REAL_GEMINI_INVESTIGATE = GeminiInvestigationProvider.investigate
REAL_OPENROUTER_INVESTIGATE = OpenRouterInvestigationProvider.investigate


def result(reference):
    return InvestigationResult(
        classification="MISSING_RECORD", root_cause="Provider record is absent",
        summary="The authoritative ERP entry has no linked provider entry.",
        recommended_action="REQUEST_EVIDENCE", confidence=.91,
        evidence_references=[reference], requires_human_approval=True,
    )


class StubProvider(InvestigationProvider):
    name, model = "stub", "deterministic-v1"
    def __init__(self, invented=False, failure=False):
        self.invented, self.failure = invented, failure
    async def investigate(self, payload):
        if self.failure:
            raise RuntimeError("secret=provider-key raw failure")
        ref = "ERP:00000000-0000-0000-0000-000000000000" if self.invented else payload["evidence"][0]["reference"]
        return result(ref)


@pytest_asyncio.fixture(autouse=True)
async def clean_phase6d(monkeypatch):
    monkeypatch.setattr(settings, "AI_PROVIDER", "gemini")
    monkeypatch.setattr(settings, "GEMINI_API_KEY", "test-provider-secret")
    monkeypatch.setattr(GeminiInvestigationProvider, "investigate", StubProvider().investigate)
    async with async_session_maker() as db:
        for model in (AIInvestigationApprovalModel, AIInvestigationModel, AuditLogModel,
                      ExceptionEvidenceModel, ReconciliationExceptionModel, ERPRecordModel):
            await db.execute(delete(model))
        await db.commit()
    yield


async def seed_case():
    async with async_session_maker() as db:
        erp = ERPRecordModel(reference_id="ERP-AI-1", amount=12345, currency="INR",
                             timestamp=datetime.utcnow(), type="SALE", status="PENDING")
        exception = ReconciliationExceptionModel(
            exception_key="a" * 64, exception_type="MISSING_RAZORPAY", severity="HIGH",
            description="Authoritative provider record missing.")
        db.add_all([erp, exception]); await db.flush()
        db.add(ExceptionEvidenceModel(exception_id=exception.id, record_type="ERP",
                                      record_id=erp.id, source_id=erp.reference_id))
        await db.commit()
        return str(exception.id), str(erp.id)


@pytest.mark.asyncio
async def test_gemini_and_openrouter_structured_success(monkeypatch):
    payload = result("ERP:id").model_dump()
    monkeypatch.setattr(module, "_post_json", lambda *args: {
        "candidates": [{"content": {"parts": [{"text": json.dumps(payload)}]}}]})
    gemini = GeminiInvestigationProvider()
    assert (await REAL_GEMINI_INVESTIGATE(gemini, {"evidence": []})).confidence == .91
    monkeypatch.setattr(settings, "OPENROUTER_API_KEY", "router-secret")
    monkeypatch.setattr(module, "_post_json", lambda *args: {
        "choices": [{"message": {"content": json.dumps(payload)}}]})
    router = OpenRouterInvestigationProvider()
    assert (await REAL_OPENROUTER_INVESTIGATE(router, {})).classification == "MISSING_RECORD"


def test_provider_selection_and_malformed_output(monkeypatch):
    assert isinstance(get_investigation_provider(), GeminiInvestigationProvider)
    monkeypatch.setattr(settings, "AI_PROVIDER", "openrouter")
    assert isinstance(get_investigation_provider(), OpenRouterInvestigationProvider)
    with pytest.raises(InvestigationValidationError):
        module._parse_result({"confidence": 4})


@pytest.mark.asyncio
async def test_creation_idempotency_audit_and_financial_immutability():
    exception_id, erp_id = await seed_case()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.post(f"/reconciliation/exceptions/{exception_id}/investigations",
                                  headers={"X-API-Key": "test_admin_key", "X-Correlation-ID": "corr-1"})
        second = await client.post(f"/reconciliation/exceptions/{exception_id}/investigations",
                                   headers={"X-API-Key": "test_admin_key"})
    assert first.status_code == 200 and first.json()["status"] == "COMPLETED"
    assert first.json()["investigation_id"] == second.json()["investigation_id"]
    assert first.json()["approval"]["status"] == "PENDING"
    async with async_session_maker() as db:
        assert await db.scalar(select(func.count(AIInvestigationModel.id))) == 1
        assert (await db.get(ERPRecordModel, erp_id)).amount == 12345
        actions = set((await db.scalars(select(AuditLogModel.action))).all())
        assert {"INVESTIGATION_REQUESTED", "INVESTIGATION_STARTED", "INVESTIGATION_COMPLETED", "APPROVAL_REQUESTED"} <= actions


@pytest.mark.asyncio
async def test_invented_evidence_and_failure_are_sanitized(monkeypatch):
    exception_id, _ = await seed_case()
    monkeypatch.setattr(GeminiInvestigationProvider, "investigate", StubProvider(invented=True).investigate)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(f"/reconciliation/exceptions/{exception_id}/investigations",
                                     headers={"X-API-Key": "test_admin_key"})
    assert response.json()["status"] == "FAILED"
    body = json.dumps(response.json())
    assert "provider-key" not in body and "test-provider-secret" not in body
    async with async_session_maker() as db:
        failed = await db.scalar(select(AuditLogModel).where(AuditLogModel.action == "INVESTIGATION_FAILED"))
        assert failed.changes["failure_code"] == "INVESTIGATION_VALIDATION_FAILURE"


@pytest.mark.asyncio
async def test_provider_failure_is_distinct_and_unexpected_errors_propagate(monkeypatch):
    exception_id, _ = await seed_case()

    async def provider_failure(self, payload):
        raise InvestigationProviderError("sanitized")

    monkeypatch.setattr(GeminiInvestigationProvider, "investigate", provider_failure)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(f"/reconciliation/exceptions/{exception_id}/investigations",
                                     headers={"X-API-Key": "test_admin_key"})
    assert response.json()["failure_code"] == "PROVIDER_FAILURE"

    async with async_session_maker() as db:
        item = await db.scalar(select(AIInvestigationModel))
        assert item.failure_code == "PROVIDER_FAILURE"

    async with async_session_maker() as db:
        await db.execute(delete(AIInvestigationApprovalModel))
        await db.execute(delete(AuditLogModel))
        await db.execute(delete(AIInvestigationModel))
        await db.commit()
        exception = await db.scalar(select(ReconciliationExceptionModel).where(
            ReconciliationExceptionModel.id == exception_id).options(selectinload(ReconciliationExceptionModel.evidence)))
        with pytest.raises(RuntimeError):
            await module.InvestigationService(
                db, StubProvider(failure=True)
            ).create(exception, "ADMIN", None)
        assert await db.scalar(select(func.count(AIInvestigationModel.id))) == 1
        item = await db.scalar(select(AIInvestigationModel))
        assert item.status == "RUNNING" and item.failure_code is None


@pytest.mark.asyncio
async def test_stored_investigation_lifecycle_timestamps_are_utc_aware():
    exception_id, _ = await seed_case()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(f"/reconciliation/exceptions/{exception_id}/investigations",
                                    headers={"X-API-Key": "test_admin_key"})
    investigation_id = created.json()["investigation_id"]
    async with async_session_maker() as db:
        item = await db.get(AIInvestigationModel, investigation_id)
        assert item.started_at.tzinfo is not None
        assert item.completed_at.tzinfo is not None
        assert item.started_at.utcoffset() == item.completed_at.utcoffset() == timezone.utc.utcoffset(None)


@pytest.mark.asyncio
async def test_admin_approval_readonly_denial_terminal_conflict_and_no_financial_change():
    exception_id, erp_id = await seed_case()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(f"/reconciliation/exceptions/{exception_id}/investigations",
                                    headers={"X-API-Key": "test_admin_key"})
        inv = created.json()["investigation_id"]
        denied = await client.post(f"/reconciliation/investigations/{inv}/approve", json={},
                                   headers={"X-API-Key": "test_readonly_key"})
        approved = await client.post(f"/reconciliation/investigations/{inv}/approve",
                                     json={"reason": "Evidence checked", "correlation_id": "decision-1"},
                                     headers={"X-API-Key": "test_admin_key"})
        repeated = await client.post(f"/reconciliation/investigations/{inv}/approve", json={},
                                     headers={"X-API-Key": "test_admin_key"})
        conflict = await client.post(f"/reconciliation/investigations/{inv}/reject", json={},
                                     headers={"X-API-Key": "test_admin_key"})
    assert denied.status_code == 403
    assert approved.json()["approval"]["status"] == repeated.json()["approval"]["status"] == "APPROVED"
    assert conflict.status_code == 409
    async with async_session_maker() as db:
        assert (await db.get(ERPRecordModel, erp_id)).amount == 12345


@pytest.mark.asyncio
async def test_successful_rejection_and_read_access():
    exception_id, _ = await seed_case()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(f"/reconciliation/exceptions/{exception_id}/investigations",
                                    headers={"X-API-Key": "test_admin_key"})
        inv = created.json()["investigation_id"]
        rejected = await client.post(f"/reconciliation/investigations/{inv}/reject", json={},
                                     headers={"X-API-Key": "test_admin_key"})
        fetched = await client.get(f"/reconciliation/investigations/{inv}",
                                   headers={"X-API-Key": "test_readonly_key"})
    assert rejected.json()["approval"]["status"] == "REJECTED"
    assert fetched.status_code == 200 and fetched.json()["approval"]["status"] == "REJECTED"


@pytest.mark.asyncio
async def test_concurrent_creation_has_one_database_identity(monkeypatch, tmp_path):
    race_engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'create-race.db').as_posix()}")
    sessions = async_sessionmaker(race_engine, expire_on_commit=False)
    async with race_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with sessions() as db:
        erp = ERPRecordModel(reference_id="ERP-RACE", amount=1, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING")
        exception = ReconciliationExceptionModel(exception_key="b" * 64, exception_type="MISSING_RAZORPAY", severity="HIGH", description="Missing")
        db.add_all([erp, exception]); await db.flush()
        db.add(ExceptionEvidenceModel(exception_id=exception.id, record_type="ERP", record_id=erp.id, source_id=erp.reference_id))
        await db.commit(); exception_id = exception.id

    async def slow_investigate(self, payload):
        await asyncio.sleep(.05)
        return result(payload["evidence"][0]["reference"])

    monkeypatch.setattr(GeminiInvestigationProvider, "investigate", slow_investigate)
    async def worker():
        async with sessions() as db:
            exception = await db.scalar(select(ReconciliationExceptionModel).where(
                ReconciliationExceptionModel.id == exception_id).options(selectinload(ReconciliationExceptionModel.evidence)))
            return await module.InvestigationService(db, GeminiInvestigationProvider()).create(exception, "ADMIN", None)
    responses = await asyncio.gather(worker(), worker())
    assert len({str(item.id) for item in responses}) == 1
    async with sessions() as db:
        assert await db.scalar(select(func.count(AIInvestigationModel.id))) == 1
    await race_engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_opposite_decisions_allow_exactly_one_terminal_state(tmp_path):
    race_engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'decision-race.db').as_posix()}")
    sessions = async_sessionmaker(race_engine, expire_on_commit=False)
    async with race_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with sessions() as db:
        erp = ERPRecordModel(reference_id="ERP-DECIDE", amount=1, currency="INR", timestamp=datetime.utcnow(), type="SALE", status="PENDING")
        exception = ReconciliationExceptionModel(exception_key="c" * 64, exception_type="MISSING_RAZORPAY", severity="HIGH", description="Missing")
        db.add_all([erp, exception]); await db.flush()
        db.add(ExceptionEvidenceModel(exception_id=exception.id, record_type="ERP", record_id=erp.id, source_id=erp.reference_id))
        await db.commit()
        exception = await db.scalar(select(ReconciliationExceptionModel).where(
            ReconciliationExceptionModel.id == exception.id).options(selectinload(ReconciliationExceptionModel.evidence)))
        item = await module.InvestigationService(db, StubProvider()).create(exception, "ADMIN", None)
        investigation_id = item.id
    async def worker(decision):
        async with sessions() as db:
            item = await db.get(AIInvestigationModel, investigation_id)
            try:
                await module.InvestigationService(db, StubProvider()).decide(item, decision, "ADMIN", None, None)
                return "ok"
            except ValueError:
                return "conflict"
    responses = await asyncio.gather(worker("APPROVED"), worker("REJECTED"))
    assert sorted(responses) == ["conflict", "ok"]
    async with sessions() as db:
        approval = await db.scalar(select(AIInvestigationApprovalModel))
        assert approval.status in {"APPROVED", "REJECTED"}
    await race_engine.dispose()
