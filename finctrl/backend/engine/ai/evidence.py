from typing import Dict, Any, List, Optional
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from finctrl.backend.database.models import (
    ReconciliationCandidateModel,
    ERPRecordModel,
    RazorpayRecordModel,
    BankRecordModel
)

class EvidencePackage:
    def __init__(self, candidate: Dict[str, Any]):
        self.candidate = candidate
        self.erp_records: List[Dict[str, Any]] = []
        self.rzp_records: List[Dict[str, Any]] = []
        self.bank_records: List[Dict[str, Any]] = []
        # Additional optional relations or sandbox fetches could go here

    def to_dict(self) -> Dict[str, Any]:
        return {
            "candidate": self.candidate,
            "erp_records": self.erp_records,
            "rzp_records": self.rzp_records,
            "bank_records": self.bank_records
        }

def _serialize_model(record: Any) -> Dict[str, Any]:
    if not record:
        return {}
    data = {c.name: getattr(record, c.name) for c in record.__table__.columns if c.name != "created_at" and c.name != "updated_at"}
    for k, v in data.items():
        if hasattr(v, "isoformat"):
            data[k] = v.isoformat()
        else:
            data[k] = str(v)
    return data

async def collect_evidence(db: AsyncSession, candidate_id: str) -> Optional[EvidencePackage]:
    # 1. Load Candidate
    result = await db.execute(select(ReconciliationCandidateModel).filter_by(id=UUID(candidate_id)))
    candidate_record = result.scalar_one_or_none()

    if not candidate_record:
        return None

    candidate_data = {
        "id": str(candidate_record.id),
        "candidate_type": candidate_record.candidate_type,
        "evidence_payload": candidate_record.evidence_payload,
        "status": candidate_record.status
    }

    package = EvidencePackage(candidate=candidate_data)

    # Extract known related IDs from payload
    payload = candidate_record.evidence_payload or {}
    erp_ids = payload.get("erp_ids", [])
    if "erp_id" in payload:
        erp_ids.append(payload["erp_id"])

    rzp_ids = payload.get("rzp_ids", [])
    if "rzp_id" in payload:
        rzp_ids.append(payload["rzp_id"])

    bank_ids = payload.get("bank_ids", [])
    if "bank_id" in payload:
        bank_ids.append(payload["bank_id"])

    # 2. Load associated records
    for eid in erp_ids:
        res = await db.execute(select(ERPRecordModel).filter_by(id=UUID(eid)))
        r = res.scalar_one_or_none()
        if r:
            package.erp_records.append(_serialize_model(r))

    for rid in rzp_ids:
        res = await db.execute(select(RazorpayRecordModel).filter_by(id=UUID(rid)))
        r = res.scalar_one_or_none()
        if r:
            package.rzp_records.append(_serialize_model(r))

    for bid in bank_ids:
        res = await db.execute(select(BankRecordModel).filter_by(id=UUID(bid)))
        r = res.scalar_one_or_none()
        if r:
            package.bank_records.append(_serialize_model(r))

    # No ground truth exposed. Only bounded DB queries based on the candidate.

    return package
