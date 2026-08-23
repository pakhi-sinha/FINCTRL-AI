from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from collections import defaultdict
from typing import List, Dict, Any, Tuple
from uuid import UUID

from finctrl.backend.database.models import (
    ERPRecordModel,
    RazorpayRecordModel,
    BankRecordModel,
    ReconciliationMatchModel,
    MatchEvidenceModel,
    ReconciliationCandidateModel
)
from finctrl.backend.api.schemas import RunReconciliationResponse

async def get_unresolved_erp(db: AsyncSession) -> List[ERPRecordModel]:
    result = await db.execute(select(ERPRecordModel).filter(ERPRecordModel.status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_rzp(db: AsyncSession) -> List[RazorpayRecordModel]:
    result = await db.execute(select(RazorpayRecordModel).filter(RazorpayRecordModel.status != "RECONCILED"))
    return list(result.scalars().all())

async def get_unresolved_bank(db: AsyncSession) -> List[BankRecordModel]:
    result = await db.execute(select(BankRecordModel).filter(BankRecordModel.status != "RECONCILED"))
    return list(result.scalars().all())

def create_match_evidence(db: AsyncSession, match: ReconciliationMatchModel, record_type: str, record_id: UUID):
    evidence = MatchEvidenceModel(
        match_id=match.id,
        record_type=record_type,
        record_id=record_id
    )
    db.add(evidence)

async def _mark_reconciled(db: AsyncSession, model_class, record_id: UUID):
    result = await db.execute(select(model_class).filter_by(id=record_id))
    record = result.scalar_one_or_none()
    if record:
        record.status = "RECONCILED"

async def pass_1_exact_match(db: AsyncSession) -> int:
    """
    Pass 1 - Exact 1:1 Matching
    Rules:
      - ERP reference_id == RZP order_receipt
      - ERP amount == RZP gross_amount
      - RZP rzp_payment_id/rzp_settlement_id in Bank description (or RZP payment evidence in bank ref)
      - RZP net_amount == Bank amount
    Require multi-field evidence. Amount only or date only matching is forbidden.
    """
    matches_created = 0

    erp_records = await get_unresolved_erp(db)
    rzp_records = await get_unresolved_rzp(db)
    bank_records = await get_unresolved_bank(db)

    # Simple index structures for fast lookup (1:1 candidate pairs)
    rzp_by_receipt = {r.order_receipt: r for r in rzp_records if r.order_receipt}
    bank_by_amount = defaultdict(list)
    for b in bank_records:
        bank_by_amount[b.amount].append(b)

    for erp in erp_records:
        if erp.reference_id in rzp_by_receipt:
            rzp = rzp_by_receipt[erp.reference_id]
            # Verify amounts match (multi-field evidence check)
            if erp.amount == rzp.gross_amount:
                # Look for bank matching RZP net amount and payment evidence
                potential_banks = bank_by_amount.get(rzp.net_amount, [])
                for bank in potential_banks:
                    # Check reference evidence (rzp_payment_id or settlement_id in bank desc or ref)
                    # Often banks combine things, so a simple substring inclusion check is robust deterministic evidence
                    has_ref_evidence = (
                        (rzp.rzp_payment_id and rzp.rzp_payment_id in bank.description) or
                        (rzp.rzp_settlement_id and rzp.rzp_settlement_id in bank.description) or
                        (rzp.rzp_payment_id and rzp.rzp_payment_id in bank.transaction_ref)
                    )

                    if has_ref_evidence:
                        # Success! 1:1 match across all three sources.
                        match = ReconciliationMatchModel(match_type="EXACT_1_1")
                        db.add(match)
                        await db.flush() # get ID

                        create_match_evidence(db, match, "ERP", erp.id)
                        create_match_evidence(db, match, "RZP", rzp.id)
                        create_match_evidence(db, match, "BANK", bank.id)

                        await _mark_reconciled(db, ERPRecordModel, erp.id)
                        await _mark_reconciled(db, RazorpayRecordModel, rzp.id)
                        await _mark_reconciled(db, BankRecordModel, bank.id)

                        matches_created += 1

                        # Remove from indices to prevent duplicate matching in loop
                        del rzp_by_receipt[erp.reference_id]
                        bank_by_amount[rzp.net_amount].remove(bank)
                        break

    return matches_created


async def pass_2_consolidated_settlement(db: AsyncSession) -> int:
    """
    Pass 2 - Consolidated Settlement (1:N or N:M mapped via Settlement ID)
    Rules:
      - Group RZP records by rzp_settlement_id
      - Sum net_amounts for the group
      - Find Bank record matching the exact sum AND having settlement_id in description/reference
      - Equal sums alone are NOT sufficient (need settlement ID evidence)
      - Map back to ERP records using reference_id
      - Complete group accepted. Incomplete groups remain unresolved.
    """
    matches_created = 0

    erp_records = await get_unresolved_erp(db)
    rzp_records = await get_unresolved_rzp(db)
    bank_records = await get_unresolved_bank(db)

    erp_by_ref = {e.reference_id: e for e in erp_records}

    # Group RZP by settlement id
    rzp_by_settlement = defaultdict(list)
    for r in rzp_records:
        if r.rzp_settlement_id:
            rzp_by_settlement[r.rzp_settlement_id].append(r)

    for settlement_id, rzp_group in rzp_by_settlement.items():
        total_net_amount = sum(r.net_amount for r in rzp_group)

        # Check if we have ERP records for ALL these RZP records (complete group)
        erp_group = []
        is_complete_group = True
        for rzp in rzp_group:
            if rzp.order_receipt in erp_by_ref:
                erp_group.append(erp_by_ref[rzp.order_receipt])
            else:
                is_complete_group = False
                break

        if not is_complete_group:
            continue

        # Check for Bank record
        for bank in bank_records:
            if bank.amount == total_net_amount and settlement_id in bank.description:
                # Success! Consolidated match
                match = ReconciliationMatchModel(match_type="CONSOLIDATED")
                db.add(match)
                await db.flush()

                # Add evidence
                create_match_evidence(db, match, "BANK", bank.id)
                await _mark_reconciled(db, BankRecordModel, bank.id)

                for erp in erp_group:
                    create_match_evidence(db, match, "ERP", erp.id)
                    await _mark_reconciled(db, ERPRecordModel, erp.id)

                for rzp in rzp_group:
                    create_match_evidence(db, match, "RZP", rzp.id)
                    await _mark_reconciled(db, RazorpayRecordModel, rzp.id)

                matches_created += 1
                bank_records.remove(bank) # Prevent duplicate matching
                break

    return matches_created


async def generate_candidates(db: AsyncSession) -> int:
    """
    Candidate Generation for unresolved cases.
    Generate deterministic candidates (e.g. potential matching but missing piece)
    which will be stored in reconciliation_candidates for AI investigation.
    """
    candidates_created = 0

    erp_records = await get_unresolved_erp(db)
    rzp_records = await get_unresolved_rzp(db)

    # 1. Potential 1:1 where ERP amount == RZP gross_amount but references don't match (anomaly candidate)
    # (Just an example of a candidate. We will limit this to avoid explosion, e.g. matching timestamps within some window)

    # 2. ERP to RZP partial match based on reference similarity (e.g., typos in order_receipt)
    # We will implement a simplified check: same amounts, different refs.
    rzp_by_amount = defaultdict(list)
    for r in rzp_records:
        rzp_by_amount[r.gross_amount].append(r)

    for erp in erp_records:
        if erp.amount in rzp_by_amount:
            for rzp in rzp_by_amount[erp.amount]:
                if erp.reference_id != rzp.order_receipt:
                    # Potential candidate due to amount match but ref mismatch
                    payload = {
                        "erp_id": str(erp.id),
                        "rzp_id": str(rzp.id),
                        "signal": "AMOUNT_MATCH_REF_MISMATCH",
                        "erp_ref": erp.reference_id,
                        "rzp_ref": rzp.order_receipt
                    }
                    candidate = ReconciliationCandidateModel(
                        candidate_type="POTENTIAL_1_1",
                        evidence_payload=payload
                    )
                    db.add(candidate)
                    candidates_created += 1

    # Generate bank exceptions/candidates if needed, but for now just the above is sufficient.

    return candidates_created


async def run_reconciliation(db: AsyncSession) -> RunReconciliationResponse:
    # 1. Exact 1:1 Matching
    pass1_matches = await pass_1_exact_match(db)

    # 2. Consolidated Settlement
    pass2_matches = await pass_2_consolidated_settlement(db)

    # 3. Candidate Generation (for unresolved)
    candidates_created = await generate_candidates(db)

    await db.commit()

    return RunReconciliationResponse(
        matches_created=pass1_matches + pass2_matches,
        candidates_created=candidates_created,
        exceptions_created=0 # Not generating hard exceptions yet in this phase
    )
