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
    RazorpayPaymentModel, RazorpaySettlementModel, RazorpayRefundModel, RazorpayOrderModel,
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
            record = (await db.execute(select(RazorpayPaymentModel).filter_by(id=UUID(eid)))).scalar_one_or_none()
            if not record:
                record = (await db.execute(select(RazorpaySettlementModel).filter_by(id=UUID(eid)))).scalar_one_or_none()
            if not record:
                record = (await db.execute(select(RazorpayRefundModel).filter_by(id=UUID(eid)))).scalar_one_or_none()
            if not record:
                record = (await db.execute(select(RazorpayOrderModel).filter_by(id=UUID(eid)))).scalar_one_or_none()

            recon_status = getattr(record, "reconciliation_status", getattr(record, "status", None))
            if recon_status == "RECONCILED":
                _create_audit(db, "CANDIDATE", UUID(candidate_id), "ACTION_FAILED", {"reason": f"Record {eid} already reconciled or missing"})
                return

    if decision.action == "AUTO_RESOLVE":
        # Legacy AI is advisory only. Authoritative resolution is exclusively
        # owned by the Phase 6D investigation/approval workflow.
        candidate.status = "HUMAN_REVIEW_REQUIRED"
        _create_audit(db, "CANDIDATE", UUID(candidate_id), "LEGACY_AUTO_RESOLVE_BLOCKED",
                      {"reason": "Phase 6D approval is required", "confidence": proposal.confidence})

    elif decision.action == "HUMAN_REVIEW_REQUIRED":
        candidate.status = "HUMAN_REVIEW_REQUIRED"
        _create_audit(db, "CANDIDATE", UUID(candidate_id), "HUMAN_REVIEW_REQUIRED", {"reason": decision.reason, "proposal": proposal.model_dump()})

    elif decision.action == "EXCEPTION":
        candidate.status = "HUMAN_REVIEW_REQUIRED"
        _create_audit(db, "CANDIDATE", UUID(candidate_id), "LEGACY_EXCEPTION_ADVISORY",
                      {"reason": decision.reason, "proposal": proposal.model_dump()})

    await db.commit()
