from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from uuid import UUID
from datetime import datetime

from finctrl.backend.engine.policy import PolicyDecision
from finctrl.backend.engine.ai.schemas import ProposedMatchSchema
from finctrl.backend.engine.ai.evidence import EvidencePackage
from finctrl.backend.database.models import (
    ReconciliationCandidateModel,
    ReconciliationMatchModel,
    MatchEvidenceModel,
    ExceptionModel,
    AuditLogModel,
    ERPRecordModel,
    RazorpayRecordModel,
    BankRecordModel
)

def _create_audit(db: AsyncSession, entity_type: str, entity_id: UUID, action: str, changes: dict):
    log = AuditLogModel(
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        changes=changes
    )
    db.add(log)

async def apply_action(db: AsyncSession, candidate_id: str, decision: PolicyDecision, proposal: ProposedMatchSchema, evidence: EvidencePackage):
    if not decision.is_valid or decision.action == "REJECTED":
        _create_audit(db, "CANDIDATE", UUID(candidate_id), "POLICY_REJECTED", {"reason": decision.reason})
        return

    res = await db.execute(select(ReconciliationCandidateModel).filter_by(id=UUID(candidate_id)))
    candidate = res.scalar_one_or_none()
    if not candidate or candidate.status != "PENDING_INVESTIGATION":
        _create_audit(db, "CANDIDATE", UUID(candidate_id), "ACTION_FAILED", {"reason": "Candidate not in valid state"})
        return

    # Verify all records are still unresolved
    for eid in proposal.evidence_ids:
        record = None
        if any(r["id"] == eid for r in evidence.erp_records):
            record = (await db.execute(select(ERPRecordModel).filter_by(id=UUID(eid)))).scalar_one_or_none()
        elif any(r["id"] == eid for r in evidence.rzp_records):
            record = (await db.execute(select(RazorpayRecordModel).filter_by(id=UUID(eid)))).scalar_one_or_none()
        elif any(r["id"] == eid for r in evidence.bank_records):
            record = (await db.execute(select(BankRecordModel).filter_by(id=UUID(eid)))).scalar_one_or_none()

        if not record or record.status == "RECONCILED":
            _create_audit(db, "CANDIDATE", UUID(candidate_id), "ACTION_FAILED", {"reason": f"Record {eid} already reconciled or missing"})
            return

    if decision.action == "AUTO_RESOLVE":
        match = ReconciliationMatchModel(match_type=proposal.match_type, status="RESOLVED")
        db.add(match)
        await db.flush()

        for eid in proposal.evidence_ids:
            record_type = None
            if any(r["id"] == eid for r in evidence.erp_records): record_type = "ERP"
            elif any(r["id"] == eid for r in evidence.rzp_records): record_type = "RZP"
            elif any(r["id"] == eid for r in evidence.bank_records): record_type = "BANK"

            if record_type:
                me = MatchEvidenceModel(match_id=match.id, record_type=record_type, record_id=UUID(eid))
                db.add(me)

                if record_type == "ERP":
                    r = (await db.execute(select(ERPRecordModel).filter_by(id=UUID(eid)))).scalar_one()
                    r.status = "RECONCILED"
                elif record_type == "RZP":
                    r = (await db.execute(select(RazorpayRecordModel).filter_by(id=UUID(eid)))).scalar_one()
                    r.status = "RECONCILED"
                elif record_type == "BANK":
                    r = (await db.execute(select(BankRecordModel).filter_by(id=UUID(eid)))).scalar_one()
                    r.status = "RECONCILED"

        candidate.status = "RESOLVED"
        _create_audit(db, "MATCH", match.id, "AUTO_RESOLVED", {"candidate_id": candidate_id, "confidence": proposal.confidence})
        _create_audit(db, "CANDIDATE", UUID(candidate_id), "POLICY_APPROVED", {"action": "AUTO_RESOLVE"})

    elif decision.action == "HUMAN_REVIEW_REQUIRED":
        candidate.status = "HUMAN_REVIEW_REQUIRED"
        _create_audit(db, "CANDIDATE", UUID(candidate_id), "HUMAN_REVIEW_REQUIRED", {"reason": decision.reason, "proposal": proposal.model_dump()})

    elif decision.action == "EXCEPTION":
        candidate.status = "EXCEPTION"
        exc = ExceptionModel(
            record_type="CANDIDATE",
            record_id=UUID(candidate_id),
            anomaly_type="AI_IDENTIFIED_EXCEPTION",
            severity="HIGH",
            status="OPEN"
        )
        db.add(exc)
        await db.flush()
        _create_audit(db, "EXCEPTION", exc.id, "EXCEPTION_CREATED", {"reason": decision.reason, "candidate_id": candidate_id})
        _create_audit(db, "CANDIDATE", UUID(candidate_id), "EXCEPTION_CREATED", {"reason": decision.reason})

    await db.commit()
