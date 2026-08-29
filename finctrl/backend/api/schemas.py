from pydantic import BaseModel, ConfigDict
from typing import List, Optional, Any
from datetime import datetime
from finctrl.backend.schemas.models import ERPRecord, RazorpayRecord, BankRecord
from uuid import UUID

class HealthCheckResponse(BaseModel):
    status: str

class BulkIngestResponse(BaseModel):
    received: int
    inserted: int
    skipped: int
    errors: int

class ERPBatchPayload(BaseModel):
    records: List[ERPRecord]

class RZPBatchPayload(BaseModel):
    records: List[RazorpayRecord]

class BankBatchPayload(BaseModel):
    records: List[BankRecord]


class WebhookEventPayload(BaseModel):
    provider: str
    provider_event_id: str
    event_type: str
    raw_payload: dict

class CandidateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    candidate_key: Optional[str] = None
    candidate_type: str
    score: int = 0
    evidence_payload: Any
    status: str
    created_at: datetime
    updated_at: datetime

class MatchEvidenceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    record_type: str
    record_id: UUID
    source_id: Optional[str] = None

class MatchResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    match_type: str
    status: str
    created_at: datetime
    updated_at: datetime
    evidence: List[MatchEvidenceResponse]

class RunReconciliationResponse(BaseModel):
    matches_created: int
    candidates_created: int
    exceptions_created: int


class ReconciliationStageRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    stage_name: str
    sequence: int
    status: str
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_ms: int
    records_examined: int
    matches_created: int
    candidates_created: int
    exceptions_created: int
    error_message: Optional[str]


class ReconciliationRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    run_key: str
    status: str
    requested_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    initiated_by: Optional[str]
    correlation_id: Optional[str]
    from_ts: Optional[int]
    to_ts: Optional[int]
    current_stage: Optional[str]
    matches_created: int
    candidates_created: int
    exceptions_created: int
    records_examined: int
    errors_count: int
    duration_ms: int
    error_message: Optional[str]
    retry_of_id: Optional[UUID]
    attempt: int
    stages: List[ReconciliationStageRunResponse] = []


class ReconciliationPeriodResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    period_key: str
    from_ts: int
    to_ts: int
    status: str
    created_at: datetime
    opened_at: datetime
    closed_at: Optional[datetime]
    created_by: Optional[str]
    closed_by: Optional[str]
    correlation_id: Optional[str]
    latest_run_id: Optional[UUID]
    notes: Optional[str]


class ExceptionEvidenceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    record_type: str
    record_id: UUID
    source_id: str
    created_at: datetime


class ExceptionAuditResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    previous_status: str
    new_status: str
    resolution_type: Optional[str]
    resolution_note: Optional[str]
    actor: Optional[str]
    timestamp: datetime


class ReconciliationExceptionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    exception_key: str
    exception_type: str
    status: str
    severity: str
    description: str
    created_at: datetime
    updated_at: datetime
    resolved_at: Optional[datetime]
    resolution_type: Optional[str]
    resolution_note: Optional[str]
    evidence: List[ExceptionEvidenceResponse] = []
    audit_entries: List[ExceptionAuditResponse] = []


class ExceptionResolutionRequest(BaseModel):
    resolution_type: Optional[str] = None
    resolution_note: Optional[str] = None
