from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple
from uuid import UUID
import hashlib

from finctrl.backend.database.models import (
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
    FinancialEventModel,
)
from finctrl.backend.api.schemas import RunReconciliationResponse
from finctrl.backend.reconciliation.workbench import (
    candidate_signals,
    deterministic_key,
    generate_candidates as generate_workbench_candidates,
    generate_exceptions,
)

@dataclass(frozen=True)
class ReconciliationScope:
    from_ts: int | None = None
    to_ts: int | None = None


def scoped_select(model, scope: ReconciliationScope | None = None):
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


async def get_unresolved_erp(db: AsyncSession, scope=None) -> List[ERPRecordModel]:
    result = await db.execute(scoped_select(ERPRecordModel, scope).where(ERPRecordModel.status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_rzp_orders(db: AsyncSession, scope=None) -> List[RazorpayOrderModel]:
    result = await db.execute(scoped_select(RazorpayOrderModel, scope))
    return list(result.scalars().all())

async def get_unresolved_rzp_payments(db: AsyncSession, scope=None) -> List[RazorpayPaymentModel]:
    result = await db.execute(scoped_select(RazorpayPaymentModel, scope).where(RazorpayPaymentModel.reconciliation_status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_rzp_settlements(db: AsyncSession, scope=None) -> List[RazorpaySettlementModel]:
    result = await db.execute(scoped_select(RazorpaySettlementModel, scope).where(RazorpaySettlementModel.reconciliation_status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_rzp_refunds(db: AsyncSession, scope=None) -> List[RazorpayRefundModel]:
    result = await db.execute(scoped_select(RazorpayRefundModel, scope).where(RazorpayRefundModel.reconciliation_status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_bank(db: AsyncSession, scope=None) -> List[BankRecordModel]:
    result = await db.execute(scoped_select(BankRecordModel, scope).where(BankRecordModel.status != "RECONCILED"))
    return list(result.scalars().all())

def _source_id(record) -> str:
    for field in (
        "reference_id", "rzp_refund_id", "rzp_payment_id",
        "rzp_settlement_id", "rzp_order_id", "transaction_ref",
    ):
        value = getattr(record, field, None)
        if value:
            return str(value)
    return str(record.id)


def _match_key(match_type: str, evidence: List[Tuple[str, Any]]) -> str:
    identities = sorted(f"{kind}:{_source_id(record)}" for kind, record in evidence)
    canonical = "|".join([match_type, *identities])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def create_match_evidence(db: AsyncSession, match: ReconciliationMatchModel, record_type: str, record):
    evidence = MatchEvidenceModel(
        match_id=match.id,
        record_type=record_type,
        record_id=record.id,
        source_id=_source_id(record),
    )
    db.add(evidence)

async def _mark_reconciled(db: AsyncSession, model_class, record_id: UUID, is_rzp=False):
    result = await db.execute(select(model_class).filter_by(id=record_id))
    record = result.scalar_one_or_none()
    if record:
        if is_rzp:
            record.reconciliation_status = "RECONCILED"
        else:
            record.status = "RECONCILED"

def create_exception(db: AsyncSession, record_type: str, record_id: UUID, anomaly: str, severity: str = "HIGH"):
    exc = ExceptionModel(
        record_type=record_type,
        record_id=record_id,
        anomaly_type=anomaly,
        severity=severity,
        status="OPEN"
    )
    db.add(exc)
    return exc


async def stage_a_exact_match(db: AsyncSession, scope=None) -> int:
    matches_created = 0
    erp_records = await get_unresolved_erp(db, scope)
    payments = await get_unresolved_rzp_payments(db, scope)
    orders = await get_unresolved_rzp_orders(db, scope)
    bank_records = await get_unresolved_bank(db, scope)

    order_map = {o.rzp_order_id: o for o in orders if o.rzp_order_id}

    for pm in payments:
        receipt = pm.rzp_order_id
        if not receipt:
            continue

        # In legacy mode, receipt might be mapped to order_receipt in the order model, not directly in rzp_order_id
        if receipt in order_map and order_map[receipt].receipt:
             receipt = order_map[receipt].receipt

        for erp in erp_records:
            if erp.reference_id == receipt and erp.amount == pm.amount:
                # Check for Bank Match
                expected_net = pm.amount - (pm.fee or 0) - (pm.tax or 0)
                bank_match = None

                for bank in bank_records:
                    if bank.amount == expected_net and ((pm.rzp_settlement_id and pm.rzp_settlement_id in bank.description) or pm.rzp_payment_id in bank.description):
                        bank_match = bank
                        break

                if bank_match:
                    evidence_records = [("ERP", erp), ("RZP", pm), ("BANK", bank_match)]
                    match = ReconciliationMatchModel(
                        match_type="EXACT_1_1",
                        match_key=_match_key("EXACT_1_1", evidence_records),
                    )
                    db.add(match)
                    await db.flush()

                    for record_type, record in evidence_records:
                        create_match_evidence(db, match, record_type, record)

                    await _mark_reconciled(db, ERPRecordModel, erp.id)
                    await _mark_reconciled(db, RazorpayPaymentModel, pm.id, True)
                    await _mark_reconciled(db, BankRecordModel, bank_match.id)

                    matches_created += 1
                    bank_records.remove(bank_match)
                    break

    return matches_created



async def stage_b_payment_arithmetic(db: AsyncSession, scope=None) -> int:
    exceptions_created = 0
    payments = await get_unresolved_rzp_payments(db, scope)

    for pm in payments:
        if pm.amount is not None:
            if pm.amount < 0:
                create_exception(db, "RZP", pm.id, "NEGATIVE_GROSS_AMOUNT", "HIGH")
                exceptions_created += 1

        if pm.fee is not None and pm.tax is not None and pm.amount is not None:
            if pm.fee + pm.tax > pm.amount:
                create_exception(db, "RZP", pm.id, "FEE_TAX_EXCEEDS_GROSS", "HIGH")
                exceptions_created += 1

            expected_net = pm.amount - pm.fee - pm.tax
            if expected_net < 0:
                create_exception(db, "RZP", pm.id, "NEGATIVE_EXPECTED_NET", "HIGH")
                exceptions_created += 1

    return exceptions_created


async def stage_c_settlement_reconciliation(db: AsyncSession, scope=None) -> Tuple[int, int]:
    matches_created = 0
    exceptions_created = 0

    settlements = await get_unresolved_rzp_settlements(db, scope)
    bank_records = await get_unresolved_bank(db, scope)

    # We need ALL payments (even if resolved by Stage A) to calculate the settlement net correctly
    result = await db.execute(scoped_select(RazorpayPaymentModel, scope))
    payments = list(result.scalars().all())

    # Re-fetch ERP records in case we need to match them or mark them
    result_erp = await db.execute(scoped_select(ERPRecordModel, scope))
    erp_records = list(result_erp.scalars().all())

    result_orders = await db.execute(scoped_select(RazorpayOrderModel, scope))
    orders = list(result_orders.scalars().all())

    erp_by_ref = {e.reference_id: e for e in erp_records}
    order_by_id = {order.rzp_order_id: order for order in orders}

    for sm in settlements:
        actual_settlement_amount = sm.amount

        # Find ALL linked payments
        linked_payments = [pm for pm in payments if pm.rzp_settlement_id == sm.rzp_settlement_id]

        if not linked_payments:
            create_exception(db, "RZP", sm.id, "MISSING_PAYMENTS_FOR_SETTLEMENT", "HIGH")
            exceptions_created += 1
            continue

        if len({payment.currency for payment in linked_payments}) != 1:
            create_exception(db, "RZP", sm.id, "SETTLEMENT_CURRENCY_MISMATCH", "HIGH")
            exceptions_created += 1
            continue

        matched_erps = []
        missing_required_erp = False
        for payment in linked_payments:
            direct_erp = erp_by_ref.get(payment.rzp_order_id)
            if direct_erp is not None:
                if direct_erp.currency != payment.currency or direct_erp.amount != payment.amount:
                    missing_required_erp = True
                    break
                matched_erps.append(direct_erp)
                continue
            order = order_by_id.get(payment.rzp_order_id)
            if order is None:
                # Direct provider/bank settlement reconciliation remains an
                # explicitly supported population when no ERP-linked order fact exists.
                continue
            erp = erp_by_ref.get(order.receipt)
            if erp is None or erp.currency != payment.currency or erp.amount != payment.amount:
                missing_required_erp = True
                break
            matched_erps.append(erp)
        if missing_required_erp:
            create_exception(db, "RZP", sm.id, "MISSING_REQUIRED_ERP_FOR_SETTLEMENT", "HIGH")
            exceptions_created += 1
            continue

        # Get refunds for all linked payments to explicitly determine applicable ones based on timestamps.
        # MUST include all refunds, not just unresolved ones, to maintain idempotency regardless of Stage D execution state.
        result_refunds = await db.execute(scoped_select(RazorpayRefundModel, scope))
        refunds = list(result_refunds.scalars().all())

        calculated_net = 0
        for pm in linked_payments:
            # gross - fee - tax
            pm_contribution = pm.amount - (pm.fee or 0) - (pm.tax or 0)

            # subtract applicable refunds deterministically (refund happened before or exactly at settlement creation)
            pm_refunds = [rm for rm in refunds if rm.rzp_payment_id == pm.rzp_payment_id]
            for rm in pm_refunds:
                if rm.status == "processed" and rm.created_at_ts <= sm.created_at_ts:
                    pm_contribution -= rm.amount

            calculated_net += pm_contribution

        # Arithmetic match Check
        if calculated_net > actual_settlement_amount:
            create_exception(db, "RZP", sm.id, "SETTLEMENT_SHORTFALL", "HIGH")
            exceptions_created += 1
            continue
        elif calculated_net < actual_settlement_amount:
            create_exception(db, "RZP", sm.id, "SETTLEMENT_EXCESS", "HIGH")
            exceptions_created += 1
            continue

        # Look for Bank Match only after arithmetic passes
        bank_matched = False
        for bank in bank_records:
            if bank.amount == actual_settlement_amount and (sm.rzp_settlement_id in bank.description or (sm.utr and sm.utr in bank.transaction_ref)):
                bank_matched = True

                evidence_records = [("RZP", sm), ("BANK", bank)]
                evidence_records.extend(("RZP", p) for p in linked_payments)
                evidence_records.extend(("ERP", e) for e in matched_erps)
                match = ReconciliationMatchModel(
                    match_type="CONSOLIDATED",
                    match_key=_match_key("CONSOLIDATED", evidence_records),
                )
                db.add(match)
                await db.flush()

                create_match_evidence(db, match, "RZP", sm)
                create_match_evidence(db, match, "BANK", bank)
                await _mark_reconciled(db, RazorpaySettlementModel, sm.id, True)
                await _mark_reconciled(db, BankRecordModel, bank.id)

                for p in linked_payments:
                    create_match_evidence(db, match, "RZP", p)
                    await _mark_reconciled(db, RazorpayPaymentModel, p.id, True)

                for e in matched_erps:
                    create_match_evidence(db, match, "ERP", e)
                    await _mark_reconciled(db, ERPRecordModel, e.id)

                matches_created += 1
                bank_records.remove(bank)
                break

        if not bank_matched:
            create_exception(db, "RZP", sm.id, "MISSING_BANK_TRANSACTION_FOR_SETTLEMENT", "HIGH")
            exceptions_created += 1

    return matches_created, exceptions_created


async def stage_d_refund_aware_reconciliation(db: AsyncSession, scope=None) -> Tuple[int, int]:
    matches_created = 0
    exceptions_created = 0

    refunds = await get_unresolved_rzp_refunds(db, scope)
    # We may need to find payments even if they are reconciled (e.g. refund after settlement)
    result = await db.execute(scoped_select(RazorpayPaymentModel, scope))
    payments = list(result.scalars().all())

    payment_map = {pm.rzp_payment_id: pm for pm in payments}

    refunds_by_payment = defaultdict(list)
    for rm in refunds:
        refunds_by_payment[rm.rzp_payment_id].append(rm)

    for rzp_payment_id, related_refunds in refunds_by_payment.items():
        if rzp_payment_id not in payment_map:
            for rm in related_refunds:
                create_exception(db, "RZP", rm.id, "ORPHAN_REFUND", "HIGH")
                exceptions_created += 1
            continue

        pm = payment_map[rzp_payment_id]
        total_refunded = sum(r.amount for r in related_refunds)

        if total_refunded > pm.amount:
            for rm in related_refunds:
                create_exception(db, "RZP", rm.id, "REFUND_EXCEEDS_GROSS", "HIGH")
                exceptions_created += 1
            continue

        # If payment refund tracking states amount_refunded is zero, but we have refund records, this is a mismatch
        if pm.amount_refunded == 0 and total_refunded > 0:
            for rm in related_refunds:
                create_exception(db, "RZP", rm.id, "REFUND_AMOUNT_MISMATCH", "HIGH")
                exceptions_created += 1
            continue

        if total_refunded != pm.amount_refunded:
            for rm in related_refunds:
                create_exception(db, "RZP", rm.id, "REFUND_AMOUNT_MISMATCH", "HIGH")
                exceptions_created += 1
            continue

        for rm in related_refunds:
            if rm.status != "processed":
                create_exception(db, "RZP", rm.id, "REFUND_STATUS_MISMATCH", "MEDIUM")
                exceptions_created += 1
                continue

            if getattr(pm, "reconciliation_status", None) == "RECONCILED":
                create_exception(db, "RZP", rm.id, "REFUND_AFTER_SETTLEMENT", "HIGH")
                exceptions_created += 1
                # Mark as reconciled because we captured the exception, but it is an anomaly
                await _mark_reconciled(db, RazorpayRefundModel, rm.id, True)
                continue

            # If all good, match
            evidence_records = [("RZP", rm), ("RZP", pm)]
            match = ReconciliationMatchModel(
                match_type="REFUND_MATCH",
                match_key=_match_key("REFUND_MATCH", evidence_records),
            )
            db.add(match)
            await db.flush()
            create_match_evidence(db, match, "RZP", rm)
            create_match_evidence(db, match, "RZP", pm)
            await _mark_reconciled(db, RazorpayRefundModel, rm.id, True)
            matches_created += 1

    return matches_created, exceptions_created


async def stage_d_consolidated_legacy_refunds(db: AsyncSession, scope=None) -> Tuple[int, int]:
    """Reconcile refund batches imported through the typed legacy contract."""
    refunds = await get_unresolved_rzp_refunds(db, scope)
    events = {event.id: event for event in (await db.scalars(select(FinancialEventModel))).all()}
    erps = list((await db.scalars(scoped_select(ERPRecordModel, scope).where(
        ERPRecordModel.status != "RECONCILED"))).all())
    banks = await get_unresolved_bank(db, scope)
    erp_by_ref = {erp.reference_id: erp for erp in erps}
    grouped = defaultdict(list)

    for refund in refunds:
        event = events.get(refund.source_event_id)
        raw = event.raw_payload if event and event.provider == "razorpay_legacy" else None
        settlement_id = raw.get("rzp_settlement_id") if isinstance(raw, dict) else None
        if settlement_id and raw.get("type", "").lower() == "refund":
            grouped[settlement_id].append(refund)

    matches_created = exceptions_created = 0
    for settlement_id, related_refunds in grouped.items():
        currencies = {refund.currency for refund in related_refunds}
        matched_erps = [erp_by_ref.get(refund.receipt) for refund in related_refunds]
        if len(currencies) != 1 or any(erp is None for erp in matched_erps):
            for refund in related_refunds:
                create_exception(db, "RZP", refund.id, "MISSING_OR_MISMATCHED_REFUND_ERP", "HIGH")
                exceptions_created += 1
            continue
        if any(erp.currency != refund.currency or erp.amount != refund.amount
               for erp, refund in zip(matched_erps, related_refunds)):
            for refund in related_refunds:
                create_exception(db, "RZP", refund.id, "REFUND_AMOUNT_MISMATCH", "HIGH")
                exceptions_created += 1
            continue

        total = sum(refund.amount for refund in related_refunds)
        bank_matches = [bank for bank in banks if bank.amount == total and
                        (settlement_id in bank.transaction_ref or settlement_id in bank.description)]
        if len(bank_matches) != 1:
            for refund in related_refunds:
                create_exception(db, "RZP", refund.id,
                                 "AMBIGUOUS_REFUND_BANK" if bank_matches else "MISSING_REFUND_BANK", "HIGH")
                exceptions_created += 1
            continue

        bank = bank_matches[0]
        evidence_records = [("RZP", refund) for refund in related_refunds]
        evidence_records.extend(("ERP", erp) for erp in matched_erps)
        evidence_records.append(("BANK", bank))
        match = ReconciliationMatchModel(
            match_type="REFUND_MATCH",
            match_key=_match_key("REFUND_MATCH", evidence_records),
        )
        db.add(match)
        await db.flush()
        for record_type, record in evidence_records:
            create_match_evidence(db, match, record_type, record)
        for refund in related_refunds:
            await _mark_reconciled(db, RazorpayRefundModel, refund.id, True)
        for erp in matched_erps:
            await _mark_reconciled(db, ERPRecordModel, erp.id)
        await _mark_reconciled(db, BankRecordModel, bank.id)
        banks.remove(bank)
        matches_created += 1

    return matches_created, exceptions_created
async def generate_candidates(db: AsyncSession, scope=None) -> int:
    """Phase 6A amount-based candidate path, enriched with Phase 6B metadata."""
    candidates_created = 0

    erp_records = await get_unresolved_erp(db, scope)
    payments = await get_unresolved_rzp_payments(db, scope)

    # Phase 6A candidate behavior: same amount remains an investigative
    # candidate and never becomes a reconciliation match automatically.
    pm_by_amount = defaultdict(list)
    for pm in payments:
        pm_by_amount[pm.amount].append(pm)

    existing_keys = set((await db.scalars(select(ReconciliationCandidateModel.candidate_key))).all())
    for erp in erp_records:
        if erp.amount in pm_by_amount:
            for pm in pm_by_amount[erp.amount]:
                key = deterministic_key("POTENTIAL_1_1", [erp.reference_id, pm.rzp_payment_id])
                if key in existing_keys:
                    continue
                signals, score = candidate_signals(erp, pm)
                payload = {
                    "erp_id": str(erp.id),
                    "rzp_id": str(pm.id),
                    "signal": "AMOUNT_MATCH_REF_MISMATCH",
                    "erp_source_id": erp.reference_id,
                    "rzp_source_id": pm.rzp_payment_id,
                    "signals": signals,
                }
                candidate = ReconciliationCandidateModel(
                    candidate_key=key,
                    candidate_type="POTENTIAL_1_1",
                    score=score,
                    evidence_payload=payload,
                )
                db.add(candidate)
                existing_keys.add(key)
                candidates_created += 1

    await db.flush()
    return candidates_created


async def stage_e_candidates_and_exceptions(db: AsyncSession, scope=None) -> Tuple[int, int]:
    """Run the existing Phase 6A candidate and Phase 6B workbench paths."""
    candidates = await generate_candidates(db, scope)
    additional_candidates = await generate_workbench_candidates(db, scope)
    exceptions = await generate_exceptions(db, scope)
    return candidates + len(additional_candidates), exceptions


async def run_reconciliation(db: AsyncSession) -> RunReconciliationResponse:
    matches = 0
    exceptions = 0

    # Stage A
    m = await stage_a_exact_match(db)
    matches += m

    # Stage B
    e = await stage_b_payment_arithmetic(db)
    exceptions += e

    # Stage C
    m, e = await stage_c_settlement_reconciliation(db)
    matches += m
    exceptions += e

    # Stage D
    m, e = await stage_d_consolidated_legacy_refunds(db)
    matches += m
    exceptions += e

    m, e = await stage_d_refund_aware_reconciliation(db)
    matches += m
    exceptions += e

    # Stage E - Candidate Generation (only for unresolved)
    c, _ = await stage_e_candidates_and_exceptions(db)

    await db.commit()

    return RunReconciliationResponse(
        matches_created=matches,
        candidates_created=c,
        exceptions_created=exceptions
    )
