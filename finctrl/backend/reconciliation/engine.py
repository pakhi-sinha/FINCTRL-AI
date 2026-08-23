import uuid
from typing import List, Dict, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func
from database.models import (
    ERPRecordModel,
    RazorpayRecordModel,
    BankRecordModel,
    ReconciliationMatch,
    MatchEvidence,
    ExceptionModel
)
from datetime import timedelta

class DeterministicReconciliationEngine:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _get_unreconciled_records(self, model, id_field="id"):
        # Helper to get records not in evidence
        subquery = select(MatchEvidence.record_id).where(MatchEvidence.record_type == model.__tablename__)
        stmt = select(model).where(getattr(model, id_field).notin_(subquery))
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def _save_match(self, match_type: str, evidence: List[Tuple[str, uuid.UUID]], score: int = 100):
        match = ReconciliationMatch(
            match_type=match_type,
            status="DETERMINISTIC_MATCH",
            confidence_score=score
        )
        self.db.add(match)
        # Flush to get match.id
        await self.db.flush()

        for record_type, record_id in evidence:
            ev = MatchEvidence(
                match_id=match.id,
                record_type=record_type,
                record_id=record_id
            )
            self.db.add(ev)

            # Update status of original records
            if record_type == "erp_records":
                stmt = select(ERPRecordModel).where(ERPRecordModel.id == record_id)
                res = await self.db.execute(stmt)
                rec = res.scalar_one()
                rec.status = "reconciled"
            elif record_type == "rzp_records":
                stmt = select(RazorpayRecordModel).where(RazorpayRecordModel.id == record_id)
                res = await self.db.execute(stmt)
                rec = res.scalar_one()
                rec.status = "reconciled"
            elif record_type == "bank_records":
                stmt = select(BankRecordModel).where(BankRecordModel.id == record_id)
                res = await self.db.execute(stmt)
                rec = res.scalar_one()
                rec.status = "reconciled"

    async def run_pass_1_exact_1_to_1(self) -> int:
        """
        Pass 1 - EXACT 1:1 match.
        Requires ERP reference_id == RZP order_receipt
        AND RZP rzp_settlement_id == Bank transaction_ref
        AND ERP amount == RZP gross_amount
        AND RZP net_amount == Bank amount
        AND timing window (Bank date >= RZP date)
        """
        matches_found = 0
        unreconciled_erp = await self._get_unreconciled_records(ERPRecordModel)
        unreconciled_rzp = {r.order_receipt: r for r in await self._get_unreconciled_records(RazorpayRecordModel)}
        unreconciled_bank = {r.transaction_ref: r for r in await self._get_unreconciled_records(BankRecordModel)}

        for erp in unreconciled_erp:
            rzp = unreconciled_rzp.get(erp.reference_id)
            if rzp:
                bank = unreconciled_bank.get(rzp.rzp_settlement_id)
                if bank:
                    # Check exact multi-field constraints
                    if (erp.amount == rzp.gross_amount and
                        rzp.net_amount == bank.amount and
                        bank.timestamp >= rzp.timestamp and
                        (bank.timestamp - rzp.timestamp).days <= 14): # Time window

                        await self._save_match(
                            match_type="1:1",
                            evidence=[
                                ("erp_records", erp.id),
                                ("rzp_records", rzp.id),
                                ("bank_records", bank.id)
                            ]
                        )
                        matches_found += 1

                        # Remove from working set so we don't match again in this pass
                        del unreconciled_rzp[erp.reference_id]
                        del unreconciled_bank[rzp.rzp_settlement_id]

        await self.db.commit()
        return matches_found

    async def run_pass_2_consolidated_1_to_n(self) -> int:
        """
        Pass 2 - CONSOLIDATED SETTLEMENT.
        Multiple ERP/RZP grouped by rzp_settlement_id matching a single Bank record.
        """
        matches_found = 0
        unreconciled_erp = await self._get_unreconciled_records(ERPRecordModel)
        unreconciled_rzp = await self._get_unreconciled_records(RazorpayRecordModel)
        unreconciled_bank = {r.transaction_ref: r for r in await self._get_unreconciled_records(BankRecordModel)}

        # Group RZP by settlement_id
        rzp_by_settlement = {}
        for rzp in unreconciled_rzp:
            if rzp.rzp_settlement_id:
                if rzp.rzp_settlement_id not in rzp_by_settlement:
                    rzp_by_settlement[rzp.rzp_settlement_id] = []
                rzp_by_settlement[rzp.rzp_settlement_id].append(rzp)

        # Group ERP by reference_id for quick lookup
        erp_by_ref = {e.reference_id: e for e in unreconciled_erp}

        for settlement_id, rzps in rzp_by_settlement.items():
            if len(rzps) > 1 and settlement_id in unreconciled_bank:
                bank = unreconciled_bank[settlement_id]

                # Check that all RZPs have matching ERPs and the sum matches exactly
                total_net = sum(r.net_amount for r in rzps)
                if total_net == bank.amount:
                    valid = True
                    erps_to_match = []
                    for r in rzps:
                        erp = erp_by_ref.get(r.order_receipt)
                        if not erp or erp.amount != r.gross_amount:
                            valid = False
                            break
                        erps_to_match.append(erp)

                    if valid:
                        evidence = [("bank_records", bank.id)]
                        for r in rzps:
                            evidence.append(("rzp_records", r.id))
                        for e in erps_to_match:
                            evidence.append(("erp_records", e.id))

                        await self._save_match("1:N", evidence)
                        matches_found += 1
                        del unreconciled_bank[settlement_id]

        await self.db.commit()
        return matches_found

    async def run_candidate_generation(self):
        """
        Pass 4 - UNRESOLVED / CANDIDATES.
        Records not cleanly matched remain unresolved. Create exceptions/candidates for investigation.
        """
        unreconciled_erp = await self._get_unreconciled_records(ERPRecordModel)
        unreconciled_rzp = await self._get_unreconciled_records(RazorpayRecordModel)
        unreconciled_bank = await self._get_unreconciled_records(BankRecordModel)

        exceptions_created = 0

        # Simple candidate logic: just flag them as PENDING_INVESTIGATION
        for erp in unreconciled_erp:
            exc = ExceptionModel(
                record_type="erp_records",
                record_id=erp.id,
                anomaly_type="UNRESOLVED_ERP",
                severity="HIGH",
                status="PENDING_INVESTIGATION"
            )
            self.db.add(exc)
            exceptions_created += 1

        for rzp in unreconciled_rzp:
            exc = ExceptionModel(
                record_type="rzp_records",
                record_id=rzp.id,
                anomaly_type="UNRESOLVED_RZP",
                severity="HIGH",
                status="PENDING_INVESTIGATION"
            )
            self.db.add(exc)
            exceptions_created += 1

        for bank in unreconciled_bank:
            exc = ExceptionModel(
                record_type="bank_records",
                record_id=bank.id,
                anomaly_type="UNRESOLVED_BANK",
                severity="HIGH",
                status="PENDING_INVESTIGATION"
            )
            self.db.add(exc)
            exceptions_created += 1

        await self.db.commit()
        return exceptions_created

    async def run_all_passes(self) -> Dict[str, Any]:
        pass_1 = await self.run_pass_1_exact_1_to_1()
        pass_2 = await self.run_pass_2_consolidated_1_to_n()
        candidates = await self.run_candidate_generation()

        return {
            "pass_1_matches": pass_1,
            "pass_2_matches": pass_2,
            "exceptions_created": candidates
        }

    async def get_matches(self):
        stmt = select(ReconciliationMatch)
        res = await self.db.execute(stmt)
        return list(res.scalars().all())

    async def get_unresolved(self):
        stmt = select(ExceptionModel)
        res = await self.db.execute(stmt)
        return list(res.scalars().all())
