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
    candidate_type: str
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
