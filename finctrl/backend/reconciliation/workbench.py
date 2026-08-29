"""Deterministic exception and evidence workbench built on authoritative records."""

import hashlib
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from finctrl.backend.database.models import (
    BankRecordModel,
    ERPRecordModel,
    ExceptionAuditModel,
    ExceptionEvidenceModel,
    ExceptionModel,
    FinancialEventModel,
    RazorpayPaymentModel,
    RazorpayRefundModel,
    RazorpaySettlementModel,
    ReconciliationCandidateModel,
    ReconciliationExceptionModel,
)

EXCEPTION_STATUSES = {"OPEN", "INVESTIGATING", "RESOLVED", "DISMISSED"}
TERMINAL_STATUSES = {"RESOLVED", "DISMISSED"}
EXCEPTION_TYPES = {
    "MISSING_ERP", "MISSING_RAZORPAY", "MISSING_BANK", "AMOUNT_MISMATCH",
    "REFERENCE_MISMATCH", "TIMING_MISMATCH", "SETTLEMENT_MISMATCH",
    "REFUND_MISMATCH", "DUPLICATE_CANDIDATE", "AMBIGUOUS_MATCH", "UNMATCHED",
}


def stable_source_id(record: Any) -> str:
    for field in (
        "reference_id", "rzp_refund_id", "rzp_payment_id", "rzp_settlement_id",
        "rzp_order_id", "transaction_ref", "provider_event_id", "candidate_key", "match_key",
    ):
        value = getattr(record, field, None)
        if value:
            return str(value)
    return str(record.id)


def deterministic_key(kind: str, identities: Iterable[str]) -> str:
    canonical = "|".join([kind, *sorted(str(value) for value in identities)])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def candidate_signals(erp: ERPRecordModel, payment: RazorpayPaymentModel) -> tuple[list[str], int]:
    signals: list[str] = []
    score = 0
    if erp.reference_id == payment.rzp_order_id:
        signals.append("reference_exact")
        score += 40
    if erp.amount == payment.amount:
        signals.append("amount_exact")
        score += 40
    if erp.currency == payment.currency:
        signals.append("currency_exact")
        score += 20
    payment_time = datetime.fromtimestamp(payment.created_at_ts) if payment.created_at_ts else None
    erp_time = erp.timestamp.replace(tzinfo=None) if erp.timestamp.tzinfo else erp.timestamp
    if payment_time and abs((erp_time - payment_time).total_seconds()) <= 86400:
        signals.append("timestamp_within_24h")
        score += 10
    return signals, min(score, 100)


async def generate_candidates(db: AsyncSession) -> list[ReconciliationCandidateModel]:
    erps = list((await db.scalars(select(ERPRecordModel).where(ERPRecordModel.status != "RECONCILED"))).all())
    payments = list((await db.scalars(select(RazorpayPaymentModel).where(
        RazorpayPaymentModel.reconciliation_status != "RECONCILED"
    ))).all())
    created: list[ReconciliationCandidateModel] = []
    existing_keys = set((await db.scalars(select(ReconciliationCandidateModel.candidate_key))).all())

    for erp in erps:
        for payment in payments:
            signals, score = candidate_signals(erp, payment)
            if "amount_exact" not in signals and "reference_exact" not in signals:
                continue
            key = deterministic_key("POTENTIAL_1_1", [erp.reference_id, payment.rzp_payment_id])
            if key in existing_keys:
                continue
            candidate = ReconciliationCandidateModel(
                candidate_key=key,
                candidate_type="POTENTIAL_1_1",
                score=score,
                evidence_payload={
                    "erp_id": str(erp.id),
                    "erp_source_id": erp.reference_id,
                    "rzp_id": str(payment.id),
                    "rzp_source_id": payment.rzp_payment_id,
                    "signals": signals,
                },
            )
            db.add(candidate)
            created.append(candidate)
            existing_keys.add(key)
    await db.flush()
    return created


async def _upsert_exception(
    db: AsyncSession,
    exception_type: str,
    severity: str,
    description: str,
    evidence: list[tuple[str, Any]],
) -> tuple[ReconciliationExceptionModel, bool]:
    identities = [f"{record_type}:{stable_source_id(record)}" for record_type, record in evidence]
    key = deterministic_key(exception_type, identities)
    existing = await db.scalar(select(ReconciliationExceptionModel).where(
        ReconciliationExceptionModel.exception_key == key
    ))
    if existing:
        return existing, False
    exception = ReconciliationExceptionModel(
        exception_key=key,
        exception_type=exception_type,
        status="OPEN",
        severity=severity,
        description=description,
    )
    db.add(exception)
    await db.flush()
    for record_type, record in evidence:
        db.add(ExceptionEvidenceModel(
            exception_id=exception.id,
            record_type=record_type,
            record_id=record.id,
            source_id=stable_source_id(record),
        ))
    return exception, True


async def generate_exceptions(db: AsyncSession) -> int:
    candidates = list((await db.scalars(select(ReconciliationCandidateModel).where(
        ReconciliationCandidateModel.status == "PENDING_INVESTIGATION"
    ))).all())
    erps = list((await db.scalars(select(ERPRecordModel).where(ERPRecordModel.status != "RECONCILED"))).all())
    payments = list((await db.scalars(select(RazorpayPaymentModel).where(
        RazorpayPaymentModel.reconciliation_status != "RECONCILED"
    ))).all())
    created = 0
    candidates_by_erp: dict[str, list[ReconciliationCandidateModel]] = {}
    for candidate in candidates:
        candidates_by_erp.setdefault(candidate.evidence_payload["erp_id"], []).append(candidate)

    payments_by_order = {payment.rzp_order_id: payment for payment in payments if payment.rzp_order_id}
    for erp in erps:
        related = candidates_by_erp.get(str(erp.id), [])
        exact_payment = payments_by_order.get(erp.reference_id)
        if exact_payment:
            _, was_created = await _upsert_exception(
                db, "MISSING_BANK", "HIGH", "No authoritative bank record completed this ERP/Razorpay pair.",
                [("ERP", erp), ("RZP", exact_payment)],
            )
        elif len(related) > 1:
            evidence = [("ERP", erp), *(("RECONCILIATION_CANDIDATE", item) for item in related)]
            _, was_created = await _upsert_exception(
                db, "AMBIGUOUS_MATCH", "HIGH", "Multiple deterministic candidates remain unresolved.", evidence,
            )
        elif related:
            candidate = related[0]
            payment = next((p for p in payments if str(p.id) == candidate.evidence_payload["rzp_id"]), None)
            evidence = [("ERP", erp), ("RECONCILIATION_CANDIDATE", candidate)]
            if payment:
                evidence.append(("RZP", payment))
            _, was_created = await _upsert_exception(
                db, "REFERENCE_MISMATCH", "MEDIUM", "Amount-based candidate has conflicting source references.", evidence,
            )
        else:
            _, was_created = await _upsert_exception(
                db, "MISSING_RAZORPAY", "HIGH", "No authoritative Razorpay record was found for the ERP record.",
                [("ERP", erp)],
            )
        created += int(was_created)

    erp_refs = {erp.reference_id for erp in erps}
    for payment in payments:
        if payment.rzp_order_id not in erp_refs:
            _, was_created = await _upsert_exception(
                db, "MISSING_ERP", "HIGH", "No authoritative ERP record was found for the Razorpay payment.",
                [("RZP", payment)],
            )
            created += int(was_created)

    legacy_exceptions = list((await db.scalars(select(ExceptionModel).where(
        ExceptionModel.status == "OPEN"
    ))).all())
    legacy_type_map = {
        "SETTLEMENT_SHORTFALL": "SETTLEMENT_MISMATCH",
        "SETTLEMENT_EXCESS": "SETTLEMENT_MISMATCH",
        "MISSING_PAYMENTS_FOR_SETTLEMENT": "SETTLEMENT_MISMATCH",
        "MISSING_BANK_TRANSACTION_FOR_SETTLEMENT": "MISSING_BANK",
        "REFUND_AMOUNT_MISMATCH": "REFUND_MISMATCH",
        "REFUND_EXCEEDS_GROSS": "REFUND_MISMATCH",
        "REFUND_STATUS_MISMATCH": "REFUND_MISMATCH",
        "ORPHAN_REFUND": "REFUND_MISMATCH",
        "REFUND_AFTER_SETTLEMENT": "REFUND_MISMATCH",
    }
    rzp_models = (RazorpayPaymentModel, RazorpaySettlementModel, RazorpayRefundModel)
    for legacy in legacy_exceptions:
        exception_type = legacy_type_map.get(legacy.anomaly_type, "AMOUNT_MISMATCH")
        record = None
        for model in rzp_models:
            record = await db.get(model, legacy.record_id)
            if record is not None:
                break
        if record is None:
            continue
        _, was_created = await _upsert_exception(
            db, exception_type, legacy.severity,
            f"Deterministic reconciliation anomaly: {legacy.anomaly_type}.",
            [(legacy.record_type, record)],
        )
        created += int(was_created)
    return created


async def run_exception_workbench(db: AsyncSession) -> tuple[int, int]:
    candidates = await generate_candidates(db)
    exceptions = await generate_exceptions(db)
    return len(candidates), exceptions


async def transition_exception(
    db: AsyncSession,
    exception: ReconciliationExceptionModel,
    new_status: str,
    actor: str | None,
    resolution_type: str | None = None,
    resolution_note: str | None = None,
) -> None:
    allowed = {
        "OPEN": {"INVESTIGATING", "RESOLVED", "DISMISSED"},
        "INVESTIGATING": {"RESOLVED", "DISMISSED"},
        "RESOLVED": set(),
        "DISMISSED": set(),
    }
    if new_status not in allowed.get(exception.status, set()):
        raise ValueError(f"Invalid exception transition: {exception.status} -> {new_status}")
    previous = exception.status
    exception.status = new_status
    exception.updated_at = datetime.utcnow()
    if new_status in TERMINAL_STATUSES:
        exception.resolved_at = datetime.utcnow()
        exception.resolution_type = resolution_type or new_status
        exception.resolution_note = resolution_note
    db.add(ExceptionAuditModel(
        exception_id=exception.id,
        previous_status=previous,
        new_status=new_status,
        resolution_type=resolution_type,
        resolution_note=resolution_note,
        actor=actor,
    ))
