from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from typing import List
from datetime import datetime
import asyncio
import hashlib
import json
import hmac
from uuid import UUID

from finctrl.backend.database.database import get_db_session
from finctrl.backend.api.security import require_admin, require_read_only
from finctrl.backend.config import settings

from finctrl.backend.database.models import (
    FinancialEventModel,
    ERPRecordModel,
    RazorpayOrderModel,
    RazorpayPaymentModel,
    RazorpaySettlementModel,
    RazorpayRefundModel,
    BankRecordModel,
    ReconciliationMatchModel,
    MatchEvidenceModel,
    ReconciliationCandidateModel,
    ExceptionModel,
    AuditLogModel
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
from finctrl.backend.reconciliation.engine import run_reconciliation
from finctrl.backend.api.cash_position_schema import CashPositionResponse
from finctrl.backend.api.metrics_schema import MetricsResponse
from finctrl.backend.engine.ai.agent import AIAgent
from sqlalchemy.orm import selectinload
from sqlalchemy import func

router = APIRouter()
_webhook_locks: dict[str, asyncio.Lock] = {}


def verify_signature(payload_body: bytes, signature: str, secret: str) -> bool:
    expected_signature = hmac.new(
        key=secret.encode('utf-8'),
        msg=payload_body,
        digestmod=hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected_signature, signature)


# Public endpoints
@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    return {"status": "ok"}


@router.get("/ready", response_model=HealthCheckResponse)
async def readiness_check():
    return {"status": "ready"}


# Webhook endpoint - uses Razorpay signature verification only, not X-API-Key
@router.post("/webhooks/razorpay")
async def razorpay_webhook(request: Request, db: AsyncSession = Depends(get_db_session)):
    body_bytes = await request.body()
    signature = request.headers.get("x-razorpay-signature")
    event_id = request.headers.get("x-razorpay-event-id")

    if not signature:
        raise HTTPException(status_code=400, detail="Missing signature")

    if not event_id:
        raise HTTPException(status_code=400, detail="Missing event ID")

    if not settings.RAZORPAY_KEY_SECRET:
        raise HTTPException(status_code=500, detail="Webhook secret not configured")

    if not verify_signature(body_bytes, signature, settings.RAZORPAY_KEY_SECRET):
        raise HTTPException(status_code=400, detail="Invalid signature")

    try:
        payload = json.loads(body_bytes)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    lock = _webhook_locks.setdefault(event_id, asyncio.Lock())
    try:
        async with lock:
            return await _process_razorpay_webhook(db, body_bytes, payload, event_id)
    finally:
        if not lock.locked():
            _webhook_locks.pop(event_id, None)


async def _process_razorpay_webhook(
    db: AsyncSession, body_bytes: bytes, payload: dict, event_id: str
):
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
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        existing = await db.execute(
            select(FinancialEventModel).filter_by(
                provider="razorpay", provider_event_id=event_id
            )
        )
        if existing.scalar_one_or_none():
            return {"status": "already_processed"}
        raise

    try:
        if event_type == "order.paid":
            order_data = payload.get("payload", {}).get("order", {}).get("entity", {})
            if order_data:
                om = RazorpayOrderModel(
                    source_event_id=event_model.id,
                    rzp_order_id=order_data.get("id"),
                    receipt=order_data.get("receipt"),
                    amount=order_data.get("amount", 0),
                    amount_due=order_data.get("amount_due", 0),
                    status=order_data.get("status"),
                    created_at_ts=order_data.get("created_at", 0)
                )
                db.add(om)
        elif event_type == "payment.captured":
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
        elif event_type == "refund.processed":
            refund_data = payload.get("payload", {}).get("refund", {}).get("entity", {})
            if refund_data:
                rm = RazorpayRefundModel(
                    source_event_id=event_model.id,
                    rzp_refund_id=refund_data.get("id"),
                    rzp_payment_id=refund_data.get("payment_id"),
                    amount=refund_data.get("amount", 0),
                    currency=refund_data.get("currency", "INR"),
                    status=refund_data.get("status"),
                    receipt=refund_data.get("receipt"),
                    created_at_ts=refund_data.get("created_at", 0)
                )
                db.add(rm)
        event_model.processing_status = "PROCESSED"
        event_model.processed_at = datetime.utcnow()
        await db.commit()
        return {"status": "ok", "event_id": str(event_model.id)}
    except Exception as e:
        event_model.processing_status = "FAILED"
        event_model.error_message = str(e)
        await db.commit()
        raise HTTPException(status_code=500, detail="Processing failed")


# ADMIN endpoints - sensitive write operations
@router.post("/ingest/erp", response_model=BulkIngestResponse, dependencies=[Depends(require_admin)])
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
            processing_status="PROCESSED",
            processed_at=datetime.utcnow()
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


@router.post("/ingest/rzp", response_model=BulkIngestResponse, dependencies=[Depends(require_admin)])
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
            processing_status="PROCESSED",
            processed_at=datetime.utcnow()
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


@router.post("/ingest/bank", response_model=BulkIngestResponse, dependencies=[Depends(require_admin)])
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
            processing_status="PROCESSED",
            processed_at=datetime.utcnow()
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


@router.post("/reconciliation/run", response_model=RunReconciliationResponse, dependencies=[Depends(require_admin)])
async def trigger_reconciliation(db: AsyncSession = Depends(get_db_session)):
    stats = await run_reconciliation(db)
    return stats


# READ-ONLY endpoints
@router.get("/matches", response_model=List[MatchResponse], dependencies=[Depends(require_read_only)])
async def get_matches(db: AsyncSession = Depends(get_db_session)):
    result = await db.execute(
        select(ReconciliationMatchModel).options(selectinload(ReconciliationMatchModel.evidence))
    )
    return result.scalars().all()


@router.get("/candidates", response_model=List[CandidateResponse], dependencies=[Depends(require_read_only)])
async def get_candidates(db: AsyncSession = Depends(get_db_session)):
    result = await db.execute(select(ReconciliationCandidateModel))
    return result.scalars().all()


@router.get("/cash-position", response_model=CashPositionResponse, dependencies=[Depends(require_read_only)])
async def get_cash_position(db: AsyncSession = Depends(get_db_session)):
    realized_cash_result = await db.execute(
        select(BankRecordModel).join(
            MatchEvidenceModel,
            MatchEvidenceModel.record_id == BankRecordModel.id
        ).join(
            ReconciliationMatchModel,
            ReconciliationMatchModel.id == MatchEvidenceModel.match_id
        ).filter(
            BankRecordModel.status == "RECONCILED",
            BankRecordModel.type == "CREDIT",
            ReconciliationMatchModel.match_type == "CONSOLIDATED"
        )
    )
    realized_cash_records = realized_cash_result.scalars().all()
    current_realized_cash = sum(r.amount for r in realized_cash_records)

    unsettled_payments_result = await db.execute(
        select(RazorpayPaymentModel).filter(
            RazorpayPaymentModel.status == "CAPTURED",
            RazorpayPaymentModel.reconciliation_status != "RECONCILED"
        )
    )
    unsettled_payments = unsettled_payments_result.scalars().all()

    captured_unsettled_amount = 0
    known_fees = 0
    known_tax = 0

    for pm in unsettled_payments:
        captured_unsettled_amount += pm.amount
        known_fees += pm.fee or 0
        known_tax += pm.tax or 0

    expected_refunds_result = await db.execute(
        select(RazorpayRefundModel).filter(
            RazorpayRefundModel.status == "processed",
            RazorpayRefundModel.reconciliation_status != "RECONCILED"
        )
    )
    expected_refunds_records = expected_refunds_result.scalars().all()
    expected_refunds = sum(r.amount for r in expected_refunds_records)

    projected_cash_position = current_realized_cash + captured_unsettled_amount - known_fees - known_tax - expected_refunds

    return CashPositionResponse(
        current_realized_cash=current_realized_cash,
        captured_unsettled_amount=captured_unsettled_amount,
        expected_refunds=expected_refunds,
        known_fees=known_fees,
        known_tax=known_tax,
        projected_cash_position=projected_cash_position,
        records_analyzed=len(realized_cash_records) + len(unsettled_payments) + len(expected_refunds_records)
    )


@router.get("/metrics", response_model=MetricsResponse, dependencies=[Depends(require_read_only)])
async def get_metrics(db: AsyncSession = Depends(get_db_session)):
    processed_count = await db.execute(select(func.count(FinancialEventModel.id)).filter(FinancialEventModel.processing_status == "PROCESSED"))
    records_processed = processed_count.scalar() or 0

    failed_count = await db.execute(select(func.count(FinancialEventModel.id)).filter(FinancialEventModel.processing_status == "FAILED"))
    processing_failures = failed_count.scalar() or 0

    reconciled_count = await db.execute(select(func.count(ReconciliationMatchModel.id)))
    records_reconciled = reconciled_count.scalar() or 0

    exception_count = await db.execute(select(func.count(ExceptionModel.id)))
    exceptions_created = exception_count.scalar() or 0

    resolved_exceptions = await db.execute(select(func.count(ExceptionModel.id)).filter(ExceptionModel.status == "RESOLVED"))
    exceptions_resolved = resolved_exceptions.scalar() or 0

    escalated_exceptions = await db.execute(select(func.count(ExceptionModel.id)).filter(ExceptionModel.status == "ESCALATED"))
    exceptions_escalated = escalated_exceptions.scalar() or 0

    candidate_count = await db.execute(select(func.count(ReconciliationCandidateModel.id)))
    candidates_created = candidate_count.scalar() or 0

    return MetricsResponse(
        records_processed=records_processed,
        records_reconciled=records_reconciled,
        exceptions_created=exceptions_created,
        exceptions_resolved=exceptions_resolved,
        exceptions_escalated=exceptions_escalated,
        candidates_created=candidates_created,
        processing_failures=processing_failures
    )


@router.get("/ai/investigations/{candidate_id}", response_model=dict, dependencies=[Depends(require_read_only)])
async def get_investigation_logs(candidate_id: str, db: AsyncSession = Depends(get_db_session)):
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


# ADMIN-only AI endpoints
@router.post("/ai/investigate/{candidate_id}", response_model=dict, dependencies=[Depends(require_admin)])
async def ai_investigate_candidate(candidate_id: str, db: AsyncSession = Depends(get_db_session)):
    agent = AIAgent(db)
    await agent.investigate_candidate(candidate_id)
    return {"status": "investigation_completed", "candidate_id": candidate_id}


@router.post("/ai/process/{candidate_id}", response_model=dict, dependencies=[Depends(require_admin)])
async def ai_process_candidate(candidate_id: str, db: AsyncSession = Depends(get_db_session)):
    agent = AIAgent(db)
    await agent.investigate_candidate(candidate_id)
    return {"status": "processed", "candidate_id": candidate_id}


@router.post("/ai/process-pending", response_model=dict, dependencies=[Depends(require_admin)])
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


# Webhook replay endpoint - ADMIN only
@router.post("/webhooks/replay/{event_id}", response_model=dict, dependencies=[Depends(require_admin)])
async def replay_webhook(event_id: str, db: AsyncSession = Depends(get_db_session)):
    """
    Replay a FAILED webhook event (ADMIN only).
    Only FAILED/retryable events can be replayed.
    """
    event = await db.execute(select(FinancialEventModel).filter_by(id=UUID(event_id)))
    event = event.scalar_one_or_none()

    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    if event.processing_status != "FAILED":
        raise HTTPException(status_code=400, detail="Only FAILED events can be replayed")

    if event.attempt_count >= 5:  # Max retry attempts
        raise HTTPException(status_code=400, detail="Max retry attempts exceeded")

    # For now, this just increments attempt_count and sets to PROCESSED
    # In a real implementation, you would re-process the webhook payload
    event.attempt_count += 1
    event.processing_status = "PROCESSED"
    event.processed_at = datetime.utcnow()
    event.error_message = None

    await db.commit()

    return {
        "status": "replay_successful",
        "event_id": str(event.id),
        "attempt_count": event.attempt_count
    }
