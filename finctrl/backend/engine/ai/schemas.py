from pydantic import BaseModel, Field
from typing import List, Optional, Literal

class ProposedMatchSchema(BaseModel):
    classification: Literal["MATCH", "EXCEPTION", "UNRESOLVED"]
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    supporting_evidence: List[str] = Field(description="List of IDs for evidence used")
    missing_evidence: Optional[str] = None
    recommended_action: Literal["AUTO_RESOLVE", "HUMAN_REVIEW_REQUIRED", "EXCEPTION"]
    risk_level: Literal["LOW", "MEDIUM", "HIGH"]
    requires_human_approval: bool = False

    # Keeping old fields as optional for backwards compatibility during testing
    decision: Optional[str] = None
    match_type: Optional[str] = None
    evidence_ids: Optional[List[str]] = Field(default_factory=list)
    reasoning: Optional[str] = None
