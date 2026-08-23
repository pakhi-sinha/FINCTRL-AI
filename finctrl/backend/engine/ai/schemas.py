from pydantic import BaseModel, Field
from typing import List, Optional, Literal

class ProposedMatchSchema(BaseModel):
    decision: Literal["PROPOSE_MATCH", "PROPOSE_EXCEPTION", "NO_MATCH"]
    match_type: Literal["ONE_TO_ONE", "ONE_TO_MANY", "FEE_DISCREPANCY", "PARTIAL", "NO_MATCH"]
    evidence_ids: List[str]
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str
    discrepancy: Optional[int] = None
    unresolved_reason: Optional[str] = None
