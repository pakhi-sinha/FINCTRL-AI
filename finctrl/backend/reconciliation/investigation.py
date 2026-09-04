"""Advisory AI investigation of authoritative reconciliation exceptions."""
from __future__ import annotations

import asyncio
import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Literal
from urllib import request as urlrequest

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from finctrl.backend.config import settings
from finctrl.backend.database.database import async_session_maker
from finctrl.backend.database.models import (
    AIInvestigationApprovalModel, AIInvestigationModel, AuditLogModel,
    BankRecordModel, ERPRecordModel, ExceptionEvidenceModel, FinancialEventModel,
    RazorpayOrderModel, RazorpayPaymentModel, RazorpayRefundModel,
    RazorpaySettlementModel, ReconciliationCandidateModel,
    ReconciliationExceptionModel, ReconciliationMatchModel,
)
from finctrl.backend.recovery.leases import Lease, claim, db_now, heartbeat_loop, owned


class InvestigationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    classification: Literal["MATCHING_ERROR", "MISSING_RECORD", "TIMING_DIFFERENCE", "AMOUNT_DIFFERENCE", "DUPLICATE", "REFUND_OR_SETTLEMENT", "UNDETERMINED"]
    root_cause: str = Field(min_length=1, max_length=2000)
    summary: str = Field(min_length=1, max_length=2000)
    recommended_action: Literal["MANUAL_REVIEW", "REQUEST_EVIDENCE", "DETERMINISTIC_REPROCESS", "DISMISS_IF_VERIFIED"]
    confidence: float = Field(ge=0, le=1)
    evidence_references: list[str] = Field(min_length=1, max_length=100)
    requires_human_approval: Literal[True]


class InvestigationProviderError(Exception):
    """Sanitized provider boundary error."""


class InvestigationValidationError(Exception):
    """Sanitized structured-result or evidence validation error."""


class InvestigationProvider(ABC):
    name: str
    model: str

    @abstractmethod
    async def investigate(self, case_payload: dict) -> InvestigationResult: ...


_SYSTEM = "Return only JSON matching the supplied schema. Use only evidence_references present in the case. AI is advisory and human approval is mandatory."


def _parse_result(value) -> InvestigationResult:
    try:
        if isinstance(value, str):
            value = json.loads(value)
        return InvestigationResult.model_validate(value)
    except (ValueError, TypeError, ValidationError, json.JSONDecodeError) as exc:
        raise InvestigationValidationError("AI investigation result failed validation") from None


class GeminiInvestigationProvider(InvestigationProvider):
    name = "gemini"

    def __init__(self):
        self.model = settings.GEMINI_MODEL
        self._key = settings.GEMINI_API_KEY

    async def investigate(self, case_payload: dict) -> InvestigationResult:
        if not self._key:
            raise InvestigationProviderError("AI provider is not configured")
        schema = InvestigationResult.model_json_schema()
        body = {"system_instruction": {"parts": [{"text": _SYSTEM}]},
                "contents": [{"role": "user", "parts": [{"text": json.dumps(case_payload, sort_keys=True)}]}],
                "generationConfig": {"responseMimeType": "application/json", "responseJsonSchema": schema}}
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self._key}"
        try:
            raw = await asyncio.to_thread(_post_json, endpoint, body, {})
        except Exception:
            raise InvestigationProviderError("AI provider request failed") from None
        try:
            return _parse_result(raw["candidates"][0]["content"]["parts"][0]["text"])
        except InvestigationValidationError:
            raise
        except (KeyError, IndexError, TypeError):
            raise InvestigationValidationError("AI investigation result failed validation") from None


class OpenRouterInvestigationProvider(InvestigationProvider):
    name = "openrouter"

    def __init__(self):
        self.model = settings.OPENROUTER_MODEL
        self._key = settings.OPENROUTER_API_KEY

    async def investigate(self, case_payload: dict) -> InvestigationResult:
        if not self._key:
            raise InvestigationProviderError("AI provider is not configured")
        body = {"model": self.model, "messages": [{"role": "system", "content": _SYSTEM},
                {"role": "user", "content": json.dumps(case_payload, sort_keys=True)}],
                "response_format": {"type": "json_schema", "json_schema": {"name": "investigation", "strict": True, "schema": InvestigationResult.model_json_schema()}}}
        try:
            raw = await asyncio.to_thread(_post_json, "https://openrouter.ai/api/v1/chat/completions", body,
                                          {"Authorization": f"Bearer {self._key}"})
        except Exception:
            raise InvestigationProviderError("AI provider request failed") from None
        try:
            return _parse_result(raw["choices"][0]["message"]["content"])
        except InvestigationValidationError:
            raise
        except (KeyError, IndexError, TypeError):
            raise InvestigationValidationError("AI investigation result failed validation") from None


def _post_json(endpoint: str, body: dict, headers: dict) -> dict:
    req = urlrequest.Request(endpoint, data=json.dumps(body).encode(), method="POST",
                             headers={"Content-Type": "application/json", **headers})
    with urlrequest.urlopen(req, timeout=30) as response:
        return json.loads(response.read())


def get_investigation_provider() -> InvestigationProvider:
    name = settings.AI_PROVIDER.lower().strip()
    if name == "gemini":
        return GeminiInvestigationProvider()
    if name == "openrouter":
        return OpenRouterInvestigationProvider()
    raise InvestigationProviderError("Unsupported AI provider configuration")


_RECORD_MODELS = {"ERP": (ERPRecordModel,), "BANK": (BankRecordModel,), "RZP": (
    RazorpayPaymentModel, RazorpaySettlementModel, RazorpayRefundModel, RazorpayOrderModel),
    "FINANCIAL_EVENT": (FinancialEventModel,), "RECONCILIATION_MATCH": (ReconciliationMatchModel,),
    "RECONCILIATION_CANDIDATE": (ReconciliationCandidateModel,)}
_FIELDS = ("reference_id", "amount", "currency", "timestamp", "status", "transaction_ref",
           "rzp_payment_id", "rzp_order_id", "rzp_settlement_id", "rzp_refund_id", "created_at_ts",
           "candidate_key", "candidate_type", "score", "evidence_payload", "match_key", "match_type")


async def build_case_payload(db: AsyncSession, exception: ReconciliationExceptionModel) -> dict:
    evidence = []
    for link in exception.evidence:
        record = None
        for model in _RECORD_MODELS.get(link.record_type, ()):
            record = await db.get(model, link.record_id)
            if record is not None:
                break
        if record is None:
            continue
        facts = {}
        for field in _FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                facts[field] = value.isoformat() if isinstance(value, datetime) else value
        evidence.append({"reference": f"{link.record_type}:{link.record_id}", "source_id": link.source_id, "facts": facts})
    return {"exception": {"id": str(exception.id), "type": exception.exception_type, "severity": exception.severity,
                           "status": exception.status, "description": exception.description,
                           "created_at": exception.created_at.isoformat()}, "evidence": evidence}


def _canonical_hash(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _audit(entity_id, action, actor, correlation_id=None, **changes):
    return AuditLogModel(entity_type="AI_INVESTIGATION", entity_id=entity_id, action=action, actor=actor,
                         changes={"correlation_id": correlation_id, **changes})


class InvestigationService:
    def __init__(self, db: AsyncSession, provider: InvestigationProvider | None = None,
                 session_factory=async_session_maker, worker_id="api"):
        self.db, self.provider = db, provider or get_investigation_provider()
        self.session_factory, self.worker_id = session_factory, worker_id

    async def create(self, exception: ReconciliationExceptionModel, actor: str, correlation_id: str | None):
        payload = await build_case_payload(self.db, exception)
        input_hash = _canonical_hash(payload)
        request_key = hashlib.sha256(f"{exception.id}:{input_hash}".encode()).hexdigest()
        existing = await self.db.scalar(select(AIInvestigationModel).where(AIInvestigationModel.request_key == request_key))
        if existing:
            return existing
        item = AIInvestigationModel(exception_id=exception.id, request_key=request_key, provider=self.provider.name,
                                    model=self.provider.model, input_hash=input_hash, requested_by=actor,
                                    correlation_id=correlation_id)
        self.db.add(item)
        try:
            await self.db.flush()
        except IntegrityError:
            await self.db.rollback()
            return await self.db.scalar(select(AIInvestigationModel).where(AIInvestigationModel.request_key == request_key))
        self.db.add(_audit(item.id, "INVESTIGATION_REQUESTED", actor, correlation_id, exception_id=str(exception.id)))
        await self.db.commit()
        return await self._execute(item.id, payload, actor, correlation_id)

    async def _execute(self, investigation_id, payload, actor, correlation_id):
        lease = Lease.new(self.worker_id)
        before = await self.db.get(AIInvestigationModel, investigation_id, populate_existing=True)
        was_takeover = before is not None and before.status == "RUNNING"
        if not await claim(self.db, AIInvestigationModel, investigation_id, lease,
                settings.AI_INVESTIGATION_LEASE_SECONDS,
                eligible_statuses={"REQUESTED"}, active_status="RUNNING"):
            await self.db.rollback()
            return await self.db.get(AIInvestigationModel, investigation_id, populate_existing=True)
        await self.db.execute(update(AIInvestigationModel).where(
            AIInvestigationModel.id == investigation_id).values(started_at=db_now()))
        action = "INVESTIGATION_TAKEN_OVER" if was_takeover else "INVESTIGATION_CLAIMED"
        self.db.add(_audit(investigation_id, action, actor, correlation_id,
                           attempt_id=lease.attempt_id, owner=lease.owner))
        self.db.add(_audit(investigation_id, "INVESTIGATION_STARTED", actor, correlation_id,
                           attempt_id=lease.attempt_id))
        await self.db.commit()
        async with heartbeat_loop(self.session_factory, AIInvestigationModel, investigation_id,
                lease, settings.AI_INVESTIGATION_LEASE_SECONDS,
                settings.AI_INVESTIGATION_HEARTBEAT_SECONDS, "RUNNING") as ownership_lost:
            return await self._execute_owned(investigation_id, payload, actor,
                                             correlation_id, lease, ownership_lost)

    async def _execute_owned(self, investigation_id, payload, actor, correlation_id,
                             lease, ownership_lost):
        try:
            result = await self.provider.investigate(payload)
            allowed = {entry["reference"] for entry in payload["evidence"]}
            if not set(result.evidence_references).issubset(allowed):
                raise InvestigationValidationError("AI investigation result failed evidence validation")
            result_dict = result.model_dump()
            result_hash = _canonical_hash(result_dict)
            if ownership_lost.is_set():
                raise InvestigationProviderError("Investigation ownership lost")
            terminal = await self.db.execute(update(AIInvestigationModel).where(
                owned(AIInvestigationModel, investigation_id, lease, active_status="RUNNING")
            ).values(status="COMPLETED", completed_at=db_now(),
                classification=result.classification, root_cause=result.root_cause,
                summary=result.summary, recommended_action=result.recommended_action,
                confidence=round(result.confidence * 10000),
                evidence_references=result.evidence_references, result_hash=result_hash,
                lease_owner=None, lease_expires_at=None))
            if terminal.rowcount != 1:
                await self.db.rollback()
                return await self.db.get(AIInvestigationModel, investigation_id, populate_existing=True)
            self.db.add(AIInvestigationApprovalModel(investigation_id=investigation_id, correlation_id=correlation_id))
            self.db.add(_audit(investigation_id, "INVESTIGATION_COMPLETED", actor, correlation_id,
                               result_hash=result_hash, attempt_id=lease.attempt_id))
            self.db.add(_audit(investigation_id, "APPROVAL_REQUESTED", actor, correlation_id))
        except InvestigationProviderError:
            await self._fail_owned(investigation_id, lease, actor, correlation_id, "PROVIDER_FAILURE")
        except InvestigationValidationError:
            await self._fail_owned(investigation_id, lease, actor, correlation_id,
                                   "INVESTIGATION_VALIDATION_FAILURE")
        except Exception:
            await self.db.rollback()
            await self._fail_owned(investigation_id, lease, actor, correlation_id,
                                   "UNEXPECTED_FAILURE")
            raise
        await self.db.commit()
        return await self.db.get(AIInvestigationModel, investigation_id, populate_existing=True)

    async def _fail_owned(self, investigation_id, lease, actor, correlation_id, code):
        terminal = await self.db.execute(update(AIInvestigationModel).where(
            owned(AIInvestigationModel, investigation_id, lease, active_status="RUNNING")
        ).values(status="FAILED", completed_at=db_now(), failure_code=code,
                 lease_owner=None, lease_expires_at=None))
        if terminal.rowcount == 1:
            self.db.add(_audit(investigation_id, "INVESTIGATION_FAILED", actor, correlation_id,
                               failure_code=code, attempt_id=lease.attempt_id))
            await self.db.commit()
            return True
        await self.db.rollback()
        return False

    async def recover_eligible(self, worker_id, limit=None):
        limit = limit or settings.RECOVERY_BATCH_SIZE
        ids = list((await self.db.scalars(select(AIInvestigationModel.id).where(or_(
            AIInvestigationModel.status == "REQUESTED",
            (AIInvestigationModel.status == "RUNNING") &
            (or_(AIInvestigationModel.lease_expires_at.is_(None),
                 AIInvestigationModel.lease_expires_at <= db_now())),
        )).order_by(AIInvestigationModel.created_at).limit(limit))).all())
        recovered = 0
        for investigation_id in ids:
            item = await self.db.get(AIInvestigationModel, investigation_id)
            previous_attempt = item.execution_attempt_id
            exception = await self.db.scalar(select(ReconciliationExceptionModel).where(
                ReconciliationExceptionModel.id == item.exception_id).options(
                    selectinload(ReconciliationExceptionModel.evidence)))
            payload = await build_case_payload(self.db, exception)
            self.worker_id = worker_id
            result = await self._execute(investigation_id, payload, "SYSTEM", item.correlation_id)
            recovered += int(result.execution_attempt_id != previous_attempt)
        return recovered

    async def decide(self, investigation: AIInvestigationModel, decision: str, actor: str,
                     reason: str | None, correlation_id: str | None):
        if investigation.status != "COMPLETED":
            raise ValueError("Only completed investigations can be decided")
        approval = await self.db.scalar(select(AIInvestigationApprovalModel).where(
            AIInvestigationApprovalModel.investigation_id == investigation.id))
        if approval.status == decision:
            return approval
        if approval.status != "PENDING":
            raise ValueError("Investigation already has a conflicting decision")
        approval_id = approval.id
        changed = await self.db.execute(update(AIInvestigationApprovalModel).where(
            AIInvestigationApprovalModel.id == approval_id, AIInvestigationApprovalModel.status == "PENDING"
        ).values(status=decision, actor=actor, decision_at=datetime.now(timezone.utc), reason=reason,
                 correlation_id=correlation_id))
        if changed.rowcount != 1:
            await self.db.rollback()
            current = await self.db.get(AIInvestigationApprovalModel, approval_id)
            if current.status == decision:
                return current
            raise ValueError("Investigation already has a conflicting decision")
        self.db.add(_audit(investigation.id, f"INVESTIGATION_{decision}", actor, correlation_id,
                           reason=reason))
        await self.db.commit()
        return await self.db.get(AIInvestigationApprovalModel, approval_id)
