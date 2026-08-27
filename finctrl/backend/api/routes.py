from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List

from finctrl.backend.database.database import get_db_session


from finctrl.backend.database.models import (
    FinancialEventModel,
    ERPRecordModel,
    RazorpayOrderModel,
    RazorpayPaymentModel,
    RazorpaySettlementModel,
    RazorpayRefundModel,
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
    WebhookEventPayload,
    MatchResponse,
    CandidateResponse,
    RunReconciliationResponse
)
import hashlib
import json
import hmac
from fastapi import Request
from finctrl.backend.config import settings

from finctrl.backend.reconciliation.engine import run_reconciliation

router = APIRouter()


def verify_signature(payload_body: bytes, signature: str, secret: str) -> bool:
    expected_signature = hmac.new(
        key=secret.encode('utf-8'),
        msg=payload_body,
        digestmod=hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected_signature, signature)

@router.post("/webhooks/razorpay")
async def razorpay_webhook(request: Request, db: AsyncSession = Depends(get_db_session)):
    body_bytes = await request.body()
    signature = request.headers.get("x-razorpay-signature")
    event_id = request.headers.get("x-razorpay-event-id")

    if not signature:
        raise HTTPException(status_code=400, detail="Missing signature")

    if not event_id:
        raise HTTPException(status_code=400, detail="Missing event ID")

    if settings.RAZORPAY_KEY_SECRET:
        if not verify_signature(body_bytes, signature, settings.RAZORPAY_KEY_SECRET):
            raise HTTPException(status_code=400, detail="Invalid signature")

    try:
        payload = json.loads(body_bytes)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    existing = await db.execute(select(FinancialEventModel).filter_by(provider="razorpay", provider_event_id=event_id))
    if existing.scalar_one_or_none():
        return {"status": "already_processed"}

    payload_hash = hashlib.sha256(body_bytes).hexdigest()
    event_type = payload.get("event", "unknown")

    event_model = FinancialEventModel(
        provider="razorpay",
        provider_event_id=event_id,
        event_type=event_type,
        payload_hash=payload_hash,
        raw_payload=payload
    )
    db.add(event_model)
    await db.flush()

    try:
        if event_type == "payment.captured":
            payment_data = payload.get("payload", {}).get("payment", {}).get("entity", {})
            if payment_data:
                pm = RazorpayPaymentModel(
                    source_event_id=event_model.id,
                    rzp_payment_id=payment_data.get("id"),
                    rzp_order_id=payment_data.get("order_id"),
                    amount=payment_data.get("amount", 0),
                    currency=payment_data.get("currency", "INR"),
                    status=payment_data.get("status"),
                    created_at_ts=payment_data.get("created_at", 0)
                )
                db.add(pm)
        elif event_type == "settlement.processed":
            settlement_data = payload.get("payload", {}).get("settlement", {}).get("entity", {})
            if settlement_data:
                sm = RazorpaySettlementModel(
                    source_event_id=event_model.id,
                    rzp_settlement_id=settlement_data.get("id"),
                    amount=settlement_data.get("amount", 0),
                    status=settlement_data.get("status"),
                    fees=settlement_data.get("fees", 0),
                    tax=settlement_data.get("tax", 0),
                    created_at_ts=settlement_data.get("created_at", 0)
                )
                db.add(sm)
        event_model.processing_status = "PROCESSED"
    except Exception as e:
        event_model.processing_status = "FAILED"
        event_model.error_message = str(e)

    await db.commit()
    return {"status": "ok", "event_id": str(event_model.id)}


@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    return {"status": "ok"}

@router.post("/ingest/erp", response_model=BulkIngestResponse)
async def ingest_erp(payload: ERPBatchPayload, db: AsyncSession = Depends(get_db_session)):
    received = len(payload.records)
    inserted = 0
    skipped = 0

    for record in payload.records:
        existing = await db.execute(select(ERPRecordModel).filter_by(id=record.id))
        if existing.scalar_one_or_none():
            skipped += 1
            continue

        raw_payload = record.model_dump(mode="json")
        payload_hash = hashlib.sha256(json.dumps(raw_payload, sort_keys=True).encode()).hexdigest()

        event_model = FinancialEventModel(
            provider="erp",
            provider_event_id=str(record.id),
            event_type="erp.upload",
            payload_hash=payload_hash,
            raw_payload=raw_payload,
            processing_status="PROCESSED"
        )
        db.add(event_model)
        await db.flush()

        db_record = ERPRecordModel(
            id=record.id,
            source_event_id=event_model.id,
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
        existing = await db.execute(select(RazorpayPaymentModel).filter_by(rzp_payment_id=record.rzp_payment_id))
        if existing.scalar_one_or_none():
            skipped += 1
            continue

        raw_payload = record.model_dump(mode="json")
        payload_hash = hashlib.sha256(json.dumps(raw_payload, sort_keys=True).encode()).hexdigest()

        event_model = FinancialEventModel(
            provider="razorpay_legacy",
            provider_event_id=str(record.id),
            event_type="legacy.upload",
            payload_hash=payload_hash,
            raw_payload=raw_payload,
            processing_status="PROCESSED"
        )
        db.add(event_model)
        await db.flush()

        # Create Order
        om = RazorpayOrderModel(
            source_event_id=event_model.id,
            rzp_order_id=f"order_{record.id}",
            receipt=record.order_receipt,
            amount=record.gross_amount,
            amount_due=0,
            status="paid",
            created_at_ts=int(record.timestamp.timestamp())
        )
        db.add(om)

        # Create Payment
        pm = RazorpayPaymentModel(
            source_event_id=event_model.id,
            rzp_payment_id=record.rzp_payment_id,
            rzp_order_id=f"order_{record.id}",
            rzp_settlement_id=record.rzp_settlement_id,
            amount=record.gross_amount,
            currency="INR",
            status=record.status,
            captured=1 if record.status == "captured" else 0,
            fee=record.fee,
            tax=record.tax,
            created_at_ts=int(record.timestamp.timestamp())
        )
        db.add(pm)

        # Create Settlement if ID exists
        if record.rzp_settlement_id:
            sm = RazorpaySettlementModel(
                source_event_id=event_model.id,
                rzp_settlement_id=record.rzp_settlement_id,
                amount=record.net_amount,
                status="processed",
                fees=record.fee,
                tax=record.tax,
                created_at_ts=int(record.timestamp.timestamp())
            )
            db.add(sm)

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

        raw_payload = record.model_dump(mode="json")
        payload_hash = hashlib.sha256(json.dumps(raw_payload, sort_keys=True).encode()).hexdigest()

        event_model = FinancialEventModel(
            provider="bank",
            provider_event_id=str(record.id),
            event_type="bank.upload",
            payload_hash=payload_hash,
            raw_payload=raw_payload,
            processing_status="PROCESSED"
        )
        db.add(event_model)
        await db.flush()

        db_record = BankRecordModel(
            id=record.id,
            source_event_id=event_model.id,
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
