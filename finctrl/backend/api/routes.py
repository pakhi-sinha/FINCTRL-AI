from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List

from finctrl.backend.database.database import get_db_session
from finctrl.backend.database.models import (
    ERPRecordModel,
    RazorpayRecordModel,
    BankRecordModel,
    ReconciliationMatchModel,
    ReconciliationCandidateModel,
    ExceptionModel
)
from finctrl.backend.api.schemas import (
    HealthCheckResponse,
    BulkIngestResponse,
    ERPBatchPayload,
    RZPBatchPayload,
    BankBatchPayload,
    MatchResponse,
    CandidateResponse,
    RunReconciliationResponse
)
from finctrl.backend.reconciliation.engine import run_reconciliation

router = APIRouter()

@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    return {"status": "ok"}

@router.post("/ingest/erp", response_model=BulkIngestResponse)
async def ingest_erp(payload: ERPBatchPayload, db: AsyncSession = Depends(get_db_session)):
    received = len(payload.records)
    inserted = 0
    skipped = 0

    # Simple check for dupes based on ID (since Phase 1 data has UUIDs)
    # Production would typically do batch inserts and handle constraint violations,
    # but looping is fine here for explicit control over skipped.
    for record in payload.records:
        existing = await db.execute(select(ERPRecordModel).filter_by(id=record.id))
        if existing.scalar_one_or_none():
            skipped += 1
            continue

        db_record = ERPRecordModel(
            id=record.id,
            reference_id=record.reference_id,
            amount=record.amount,
            currency=record.currency,
            timestamp=record.timestamp,
            type=record.type,
            status=record.status
        )
        db.add(db_record)
        inserted += 1

    await db.commit()
    return BulkIngestResponse(received=received, inserted=inserted, skipped=skipped, errors=0)

@router.post("/ingest/rzp", response_model=BulkIngestResponse)
async def ingest_rzp(payload: RZPBatchPayload, db: AsyncSession = Depends(get_db_session)):
    received = len(payload.records)
    inserted = 0
    skipped = 0

    for record in payload.records:
        existing = await db.execute(select(RazorpayRecordModel).filter_by(id=record.id))
        if existing.scalar_one_or_none():
            skipped += 1
            continue

        db_record = RazorpayRecordModel(
            id=record.id,
            rzp_payment_id=record.rzp_payment_id,
            rzp_settlement_id=record.rzp_settlement_id,
            order_receipt=record.order_receipt,
            gross_amount=record.gross_amount,
            fee=record.fee,
            tax=record.tax,
            net_amount=record.net_amount,
            type=record.type,
            timestamp=record.timestamp,
            status=record.status
        )
        db.add(db_record)
        inserted += 1

    await db.commit()
    return BulkIngestResponse(received=received, inserted=inserted, skipped=skipped, errors=0)

@router.post("/ingest/bank", response_model=BulkIngestResponse)
async def ingest_bank(payload: BankBatchPayload, db: AsyncSession = Depends(get_db_session)):
    received = len(payload.records)
    inserted = 0
    skipped = 0

    for record in payload.records:
        existing = await db.execute(select(BankRecordModel).filter_by(id=record.id))
        if existing.scalar_one_or_none():
            skipped += 1
            continue

        db_record = BankRecordModel(
            id=record.id,
            transaction_ref=record.transaction_ref,
            description=record.description,
            amount=record.amount,
            type=record.type,
            timestamp=record.timestamp,
            status=record.status
        )
        db.add(db_record)
        inserted += 1

    await db.commit()
    return BulkIngestResponse(received=received, inserted=inserted, skipped=skipped, errors=0)

@router.post("/reconciliation/run", response_model=RunReconciliationResponse)
async def trigger_reconciliation(db: AsyncSession = Depends(get_db_session)):
    stats = await run_reconciliation(db)
    return stats

# Adding some basic retrieval endpoints for E2E tests and debugging
@router.get("/matches", response_model=List[MatchResponse])
async def get_matches(db: AsyncSession = Depends(get_db_session)):
    # Needed to eagerly load evidence for the response
    from sqlalchemy.orm import selectinload
    result = await db.execute(
        select(ReconciliationMatchModel).options(selectinload(ReconciliationMatchModel.evidence))
    )
    return result.scalars().all()

@router.get("/candidates", response_model=List[CandidateResponse])
async def get_candidates(db: AsyncSession = Depends(get_db_session)):
    result = await db.execute(select(ReconciliationCandidateModel))
    return result.scalars().all()

from finctrl.backend.engine.ai.agent import AIAgent

@router.post("/ai/investigate/{candidate_id}", response_model=dict)
async def ai_investigate_candidate(candidate_id: str, db: AsyncSession = Depends(get_db_session)):
    agent = AIAgent(db)
    await agent.investigate_candidate(candidate_id)
    return {"status": "investigation_completed", "candidate_id": candidate_id}

@router.post("/ai/process/{candidate_id}", response_model=dict)
async def ai_process_candidate(candidate_id: str, db: AsyncSession = Depends(get_db_session)):
    agent = AIAgent(db)
    await agent.investigate_candidate(candidate_id)
    return {"status": "processed", "candidate_id": candidate_id}

@router.post("/ai/process-pending", response_model=dict)
async def ai_process_pending(db: AsyncSession = Depends(get_db_session)):
    query = select(ReconciliationCandidateModel).filter_by(status="PENDING_INVESTIGATION").limit(10)
    res = await db.execute(query)
    candidates = res.scalars().all()

    agent = AIAgent(db)
    processed = []
    for c in candidates:
        await agent.investigate_candidate(str(c.id))
        processed.append(str(c.id))

    return {"status": "processed_pending", "count": len(processed), "processed_ids": processed}

@router.get("/ai/investigations/{candidate_id}", response_model=dict)
async def get_investigation_logs(candidate_id: str, db: AsyncSession = Depends(get_db_session)):
    from finctrl.backend.database.models import AuditLogModel
    query = select(AuditLogModel).filter(
        AuditLogModel.entity_type == "CANDIDATE",
        AuditLogModel.entity_id == UUID(candidate_id)
    ).order_by(AuditLogModel.timestamp)
    res = await db.execute(query)
    logs = res.scalars().all()
    return {
        "candidate_id": candidate_id,
        "logs": [
            {
                "action": log.action,
                "timestamp": log.timestamp.isoformat(),
                "changes": log.changes
            } for log in logs
        ]
    }
