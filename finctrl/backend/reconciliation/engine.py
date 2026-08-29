from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from collections import defaultdict
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
    ExceptionModel
)
from finctrl.backend.api.schemas import RunReconciliationResponse

async def get_unresolved_erp(db: AsyncSession) -> List[ERPRecordModel]:
    result = await db.execute(select(ERPRecordModel).filter(ERPRecordModel.status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_rzp_orders(db: AsyncSession) -> List[RazorpayOrderModel]:
    result = await db.execute(select(RazorpayOrderModel))
    return list(result.scalars().all())

async def get_unresolved_rzp_payments(db: AsyncSession) -> List[RazorpayPaymentModel]:
    result = await db.execute(select(RazorpayPaymentModel).filter(RazorpayPaymentModel.reconciliation_status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_rzp_settlements(db: AsyncSession) -> List[RazorpaySettlementModel]:
    result = await db.execute(select(RazorpaySettlementModel).filter(RazorpaySettlementModel.reconciliation_status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_rzp_refunds(db: AsyncSession) -> List[RazorpayRefundModel]:
    result = await db.execute(select(RazorpayRefundModel).filter(RazorpayRefundModel.reconciliation_status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_bank(db: AsyncSession) -> List[BankRecordModel]:
    result = await db.execute(select(BankRecordModel).filter(BankRecordModel.status != "RECONCILED"))
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


async def stage_a_exact_match(db: AsyncSession) -> int:
    matches_created = 0
    erp_records = await get_unresolved_erp(db)
    payments = await get_unresolved_rzp_payments(db)
    orders = await get_unresolved_rzp_orders(db)
    bank_records = await get_unresolved_bank(db)

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



async def stage_b_payment_arithmetic(db: AsyncSession) -> int:
    exceptions_created = 0
    payments = await get_unresolved_rzp_payments(db)

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


async def stage_c_settlement_reconciliation(db: AsyncSession) -> Tuple[int, int]:
    matches_created = 0
    exceptions_created = 0

    settlements = await get_unresolved_rzp_settlements(db)
    bank_records = await get_unresolved_bank(db)

    # We need ALL payments (even if resolved by Stage A) to calculate the settlement net correctly
    result = await db.execute(select(RazorpayPaymentModel))
    payments = list(result.scalars().all())

    # Re-fetch ERP records in case we need to match them or mark them
    result_erp = await db.execute(select(ERPRecordModel))
    erp_records = list(result_erp.scalars().all())

    erp_by_ref = {e.reference_id: e for e in erp_records}

    for sm in settlements:
        actual_settlement_amount = sm.amount

        # Find ALL linked payments
        linked_payments = [pm for pm in payments if pm.rzp_settlement_id == sm.rzp_settlement_id]

        if not linked_payments:
            create_exception(db, "RZP", sm.id, "MISSING_PAYMENTS_FOR_SETTLEMENT", "HIGH")
            exceptions_created += 1
            continue

        # Get refunds for all linked payments to explicitly determine applicable ones based on timestamps.
        # MUST include all refunds, not just unresolved ones, to maintain idempotency regardless of Stage D execution state.
        result_refunds = await db.execute(select(RazorpayRefundModel))
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

                matched_erps = [
                    erp_by_ref[p.rzp_order_id]
                    for p in linked_payments
                    if p.rzp_order_id and p.rzp_order_id in erp_by_ref
                ]
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

                    # Find ERP
                    if p.rzp_order_id and p.rzp_order_id in erp_by_ref:
                        e = erp_by_ref[p.rzp_order_id]
                        create_match_evidence(db, match, "ERP", e)
                        await _mark_reconciled(db, ERPRecordModel, e.id)

                matches_created += 1
                bank_records.remove(bank)
                break

        if not bank_matched:
            create_exception(db, "RZP", sm.id, "MISSING_BANK_TRANSACTION_FOR_SETTLEMENT", "HIGH")
            exceptions_created += 1

    return matches_created, exceptions_created


async def stage_d_refund_aware_reconciliation(db: AsyncSession) -> Tuple[int, int]:
    matches_created = 0
    exceptions_created = 0

    refunds = await get_unresolved_rzp_refunds(db)
    # We may need to find payments even if they are reconciled (e.g. refund after settlement)
    result = await db.execute(select(RazorpayPaymentModel))
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
async def generate_candidates(db: AsyncSession) -> int:
    candidates_created = 0

    erp_records = await get_unresolved_erp(db)
    payments = await get_unresolved_rzp_payments(db)

    # Candidate: Same amount, different ref
    pm_by_amount = defaultdict(list)
    for pm in payments:
        pm_by_amount[pm.amount].append(pm)

    for erp in erp_records:
        if erp.amount in pm_by_amount:
            for pm in pm_by_amount[erp.amount]:
                # If they share the same amount, this is ambiguous.
                payload = {
                    "erp_id": str(erp.id),
                    "rzp_id": str(pm.id),
                    "signal": "AMOUNT_MATCH_REF_MISMATCH"
                }
                candidate = ReconciliationCandidateModel(
                    candidate_type="POTENTIAL_1_1",
                    evidence_payload=payload
                )
                db.add(candidate)
                candidates_created += 1

    return candidates_created


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
    m, e = await stage_d_refund_aware_reconciliation(db)
    matches += m
    exceptions += e

    # Stage E - Candidate Generation (only for unresolved)
    c = await generate_candidates(db)

    await db.commit()

    return RunReconciliationResponse(
        matches_created=matches,
        candidates_created=c,
        exceptions_created=exceptions
    )
