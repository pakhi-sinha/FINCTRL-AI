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
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from finctrl.backend.config import settings
from finctrl.backend.database.models import (
    AIInvestigationApprovalModel, AIInvestigationModel, AuditLogModel,
    BankRecordModel, ERPRecordModel, ExceptionEvidenceModel, FinancialEventModel,
    RazorpayOrderModel, RazorpayPaymentModel, RazorpayRefundModel,
    RazorpaySettlementModel, ReconciliationCandidateModel,
    ReconciliationExceptionModel, ReconciliationMatchModel,
)


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
    def __init__(self, db: AsyncSession, provider: InvestigationProvider | None = None):
        self.db, self.provider = db, provider or get_investigation_provider()

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
        item.status, item.started_at = "RUNNING", datetime.now(timezone.utc)
        self.db.add(_audit(item.id, "INVESTIGATION_STARTED", actor, correlation_id))
        await self.db.commit()
        try:
            result = await self.provider.investigate(payload)
            allowed = {entry["reference"] for entry in payload["evidence"]}
            if not set(result.evidence_references).issubset(allowed):
                raise InvestigationValidationError("AI investigation result failed evidence validation")
            result_dict = result.model_dump()
            item.status = "COMPLETED"; item.completed_at = datetime.now(timezone.utc)
            item.classification = result.classification; item.root_cause = result.root_cause
            item.summary = result.summary; item.recommended_action = result.recommended_action
            item.confidence = round(result.confidence * 10000); item.evidence_references = result.evidence_references
            item.result_hash = _canonical_hash(result_dict)
            self.db.add(AIInvestigationApprovalModel(investigation_id=item.id, correlation_id=correlation_id))
            self.db.add(_audit(item.id, "INVESTIGATION_COMPLETED", actor, correlation_id, result_hash=item.result_hash))
            self.db.add(_audit(item.id, "APPROVAL_REQUESTED", actor, correlation_id))
        except InvestigationProviderError:
            item.status = "FAILED"; item.completed_at = datetime.now(timezone.utc); item.failure_code = "PROVIDER_FAILURE"
            self.db.add(_audit(item.id, "INVESTIGATION_FAILED", actor, correlation_id, failure_code=item.failure_code))
        except InvestigationValidationError:
            item.status = "FAILED"; item.completed_at = datetime.now(timezone.utc); item.failure_code = "INVESTIGATION_VALIDATION_FAILURE"
            self.db.add(_audit(item.id, "INVESTIGATION_FAILED", actor, correlation_id, failure_code=item.failure_code))
        except Exception:
            await self.db.rollback()
            raise
        await self.db.commit()
        return item

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
