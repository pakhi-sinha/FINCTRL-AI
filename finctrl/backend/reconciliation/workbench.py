"""Deterministic exception and evidence workbench built on authoritative records."""

import hashlib
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

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


def provider_timestamp_utc(timestamp: int) -> datetime | None:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc) if timestamp else None


def _scoped_select(model, scope=None):
    query = select(model)
    if scope is None:
        return query
    column = model.timestamp if model in {ERPRecordModel, BankRecordModel} else model.created_at_ts
    if scope.from_ts is not None:
        lower = datetime.fromtimestamp(scope.from_ts, timezone.utc) if model in {ERPRecordModel, BankRecordModel} else scope.from_ts
        query = query.where(column >= lower)
    if scope.to_ts is not None:
        upper = datetime.fromtimestamp(scope.to_ts, timezone.utc) if model in {ERPRecordModel, BankRecordModel} else scope.to_ts
        query = query.where(column <= upper)
    return query


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
    payment_time = provider_timestamp_utc(payment.created_at_ts)
    erp_time = erp.timestamp
    erp_time = erp_time.replace(tzinfo=timezone.utc) if erp_time.tzinfo is None else erp_time.astimezone(timezone.utc)
    if payment_time and abs((erp_time - payment_time).total_seconds()) <= 86400:
        signals.append("timestamp_within_24h")
        score += 10
    return signals, min(score, 100)


async def generate_candidates(db: AsyncSession, scope=None) -> list[ReconciliationCandidateModel]:
    erps = list((await db.scalars(_scoped_select(ERPRecordModel, scope).where(ERPRecordModel.status != "RECONCILED"))).all())
    payments = list((await db.scalars(_scoped_select(RazorpayPaymentModel, scope).where(
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
    try:
        async with db.begin_nested():
            db.add(exception)
            await db.flush([exception])
    except IntegrityError:
        existing = await db.scalar(select(ReconciliationExceptionModel).where(
            ReconciliationExceptionModel.exception_key == key
        ))
        if existing is None:
            raise
        return existing, False
    for record_type, record in evidence:
        db.add(ExceptionEvidenceModel(
            exception_id=exception.id,
            record_type=record_type,
            record_id=record.id,
            source_id=stable_source_id(record),
        ))
    await db.flush()
    return exception, True


async def generate_exceptions(db: AsyncSession, scope=None) -> int:
    erps = list((await db.scalars(_scoped_select(ERPRecordModel, scope).where(ERPRecordModel.status != "RECONCILED"))).all())
    payments = list((await db.scalars(_scoped_select(RazorpayPaymentModel, scope).where(
        RazorpayPaymentModel.reconciliation_status != "RECONCILED"
    ))).all())
    source_ids = {str(item.id) for item in [*erps, *payments]}
    candidates = list((await db.scalars(select(ReconciliationCandidateModel).where(
        ReconciliationCandidateModel.status == "PENDING_INVESTIGATION"
    ))).all())
    candidates = [item for item in candidates if
                  str(item.evidence_payload.get("erp_id")) in source_ids or
                  str(item.evidence_payload.get("rzp_id")) in source_ids]
    all_payments = list((await db.scalars(_scoped_select(RazorpayPaymentModel, scope))).all())
    # Reconciled bank rows remain authoritative evidence and must not be hidden
    # from exception evaluation.
    banks = list((await db.scalars(_scoped_select(BankRecordModel, scope))).all())
    settlements = list((await db.scalars(_scoped_select(RazorpaySettlementModel, scope))).all())
    refunds = list((await db.scalars(_scoped_select(RazorpayRefundModel, scope))).all())
    scoped_rzp_ids = {item.id for item in [*all_payments, *settlements, *refunds]}
    created = 0
    candidates_by_erp: dict[str, list[ReconciliationCandidateModel]] = {}
    for candidate in candidates:
        candidates_by_erp.setdefault(candidate.evidence_payload["erp_id"], []).append(candidate)

    payments_by_order: dict[str, list[RazorpayPaymentModel]] = {}
    for payment in payments:
        if payment.rzp_order_id:
            payments_by_order.setdefault(payment.rzp_order_id, []).append(payment)

    def valid_bank_evidence(payment: RazorpayPaymentModel) -> list[BankRecordModel]:
        settlement = next((
            item for item in settlements
            if payment.rzp_settlement_id and item.rzp_settlement_id == payment.rzp_settlement_id
        ), None)
        if settlement is not None:
            linked_payments = [
                item for item in all_payments
                if item.rzp_settlement_id == settlement.rzp_settlement_id
            ]
            calculated_net = 0
            for linked_payment in linked_payments:
                contribution = linked_payment.amount - (linked_payment.fee or 0) - (linked_payment.tax or 0)
                contribution -= sum(
                    refund.amount for refund in refunds
                    if refund.rzp_payment_id == linked_payment.rzp_payment_id
                    and refund.status == "processed"
                    and refund.created_at_ts <= settlement.created_at_ts
                )
                calculated_net += contribution
            # Preserve Phase 6A settlement arithmetic: an inconsistent
            # settlement is not accepted as valid bank evidence here.
            if calculated_net != settlement.amount:
                return []
            expected_amount = settlement.amount
        else:
            expected_amount = payment.amount - (payment.fee or 0) - (payment.tax or 0)

        def has_reference(bank: BankRecordModel) -> bool:
            transaction_ref = bank.transaction_ref or ""
            description = bank.description or ""
            if settlement is not None:
                if settlement.utr and settlement.utr == transaction_ref:
                    return True
                if settlement.rzp_settlement_id in transaction_ref:
                    return True
                if settlement.rzp_settlement_id in description:
                    return True
            if payment.rzp_payment_id in transaction_ref:
                return True
            return payment.rzp_payment_id in description

        return [
            bank for bank in banks
            if bank.amount == expected_amount and has_reference(bank)
        ]

    for erp in erps:
        related = candidates_by_erp.get(str(erp.id), [])
        exact_payments = [
            payment for payment in payments_by_order.get(erp.reference_id, [])
            if payment.amount == erp.amount and payment.currency == erp.currency
        ]
        if len(exact_payments) > 1:
            evidence = [("ERP", erp), *(("RZP", payment) for payment in exact_payments)]
            evidence.extend(("RECONCILIATION_CANDIDATE", item) for item in related)
            _, was_created = await _upsert_exception(
                db, "AMBIGUOUS_MATCH", "HIGH",
                "Multiple authoritative Razorpay payments remain valid for the ERP order.", evidence,
            )
        elif len(exact_payments) == 1 and not valid_bank_evidence(exact_payments[0]):
            exact_payment = exact_payments[0]
            _, was_created = await _upsert_exception(
                db, "MISSING_BANK", "HIGH", "No authoritative bank record completed this ERP/Razorpay pair.",
                [("ERP", erp), ("RZP", exact_payment)],
            )
        elif len(exact_payments) == 1:
            continue
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
        if scope is not None and legacy.record_id not in scoped_rzp_ids:
            continue
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


async def run_exception_workbench(db: AsyncSession, scope=None) -> tuple[int, int]:
    candidates = await generate_candidates(db, scope)
    exceptions = await generate_exceptions(db, scope)
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
    exception_id = exception.id
    previous = exception.status
    values = {"status": new_status, "updated_at": datetime.utcnow()}
    if new_status in TERMINAL_STATUSES:
        values.update(resolved_at=datetime.utcnow(),
                      resolution_type=resolution_type or new_status,
                      resolution_note=resolution_note)
    changed = await db.execute(update(ReconciliationExceptionModel).where(
        ReconciliationExceptionModel.id == exception_id,
        ReconciliationExceptionModel.status == previous,
    ).values(**values))
    if changed.rowcount != 1:
        await db.rollback()
        current = await db.get(ReconciliationExceptionModel, exception_id)
        if current is not None and current.status == new_status:
            return
        current_status = current.status if current is not None else "MISSING"
        raise ValueError(f"Invalid exception transition: {current_status} -> {new_status}")
    for key, value in values.items():
        setattr(exception, key, value)
    db.add(ExceptionAuditModel(
        exception_id=exception.id,
        previous_status=previous,
        new_status=new_status,
        resolution_type=resolution_type,
        resolution_note=resolution_note,
        actor=actor,
    ))
