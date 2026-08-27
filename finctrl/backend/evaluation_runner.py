import os
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"

import asyncio
import json
import time
from typing import Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from finctrl.backend.database.database import get_db_session, init_db
from finctrl.backend.api.schemas import ERPBatchPayload, RZPBatchPayload, BankBatchPayload
from finctrl.backend.api.routes import ingest_erp, ingest_rzp, ingest_bank
from finctrl.backend.reconciliation.engine import run_reconciliation

async def run_evaluation(dataset_path: str) -> Dict[str, Any]:
    with open(dataset_path, "r") as f:
        dataset = json.load(f)

    ground_truth_path = dataset_path.replace("dataset.json", "ground_truth.json")
    with open(ground_truth_path, "r") as f:
        ground_truth = json.load(f)

    await init_db()

    start_time = time.time()

    async for db in get_db_session():
        # Ingestion
        await ingest_erp(ERPBatchPayload(records=dataset.get("erp_records", [])), db)
        await ingest_rzp(RZPBatchPayload(records=dataset.get("rzp_records", [])), db)
        await ingest_bank(BankBatchPayload(records=dataset.get("bank_records", [])), db)

        # Recon
        response = await run_reconciliation(db)

        end_time = time.time()
        latency = end_time - start_time
        records_processed = len(dataset.get("erp_records", [])) + len(dataset.get("rzp_records", [])) + len(dataset.get("bank_records", []))


        # Ground Truth checking
        correct_matches = 0
        false_resolutions = 0
        from finctrl.backend.database.models import ReconciliationMatchModel, MatchEvidenceModel
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload

        matches_q = await db.execute(select(ReconciliationMatchModel).options(selectinload(ReconciliationMatchModel.evidence)))
        all_matches = matches_q.scalars().all()

        from finctrl.backend.database.models import ERPRecordModel, RazorpayPaymentModel, RazorpaySettlementModel, BankRecordModel
        # Build map of what we matched
        # Our match -> set of stable external/business IDs to compare against ground truth
        our_matches = []
        for match in all_matches:
            s = set()
            for ev in match.evidence:
                if ev.record_type == "ERP":
                    rec = await db.execute(select(ERPRecordModel).filter_by(id=ev.record_id))
                    p = rec.scalar_one_or_none()
                    if p: s.add(str(p.reference_id)) # ERP -> reference_id
                elif ev.record_type == "RZP":
                    rec = await db.execute(select(RazorpayPaymentModel).filter_by(id=ev.record_id))
                    p = rec.scalar_one_or_none()
                    if p:
                        s.add(str(p.rzp_payment_id))
                    else:
                        rec = await db.execute(select(RazorpaySettlementModel).filter_by(id=ev.record_id))
                        p = rec.scalar_one_or_none()
                        if p:
                            s.add(str(p.rzp_settlement_id))
                elif ev.record_type == "BANK":
                    rec = await db.execute(select(BankRecordModel).filter_by(id=ev.record_id))
                    p = rec.scalar_one_or_none()
                    if p: s.add(str(p.transaction_ref)) # Bank -> transaction_ref
            our_matches.append(s)

        # Build ground truth sets normalizing all sets to stable business IDs to match our logic
        with open(dataset_path, "r") as ds_f:
            ds_data = json.load(ds_f)

        rzp_event_to_payment_id = {}
        for r in ds_data.get("rzp_records", []):
            if "rzp_payment_id" in r:
                rzp_event_to_payment_id[r["id"]] = r["rzp_payment_id"]
            elif "id" in r and r.get("type") == "settlement":
                rzp_event_to_payment_id[r["id"]] = r.get("rzp_settlement_id", r["id"])

        erp_event_to_ref = {}
        for r in ds_data.get("erp_records", []):
             erp_event_to_ref[r["id"]] = r["reference_id"]

        bank_event_to_ref = {}
        for r in ds_data.get("bank_records", []):
             bank_event_to_ref[r["id"]] = r["transaction_ref"]

        for group in ground_truth.get("groups", []):
            if group.get("expected_outcome") == "MATCH":
                # Find exactly the set of expected stable business IDs
                expected_set = set()
                for e_id in group.get("erp_record_ids", []): expected_set.add(erp_event_to_ref.get(e_id, e_id))
                for b_id in group.get("bank_record_ids", []): expected_set.add(bank_event_to_ref.get(b_id, b_id))
                for rzp_ev_id in group.get("rzp_record_ids", []): expected_set.add(rzp_event_to_payment_id.get(rzp_ev_id, rzp_ev_id))

                # Check if we have this exact match
                if expected_set in our_matches:
                    correct_matches += 1

        # Total generated matches vs correct matches gives us precision
        total_generated = len(all_matches)
        false_resolutions = total_generated - correct_matches
        precision = correct_matches / total_generated if total_generated > 0 else 1.0

        expected_matches = len([g for g in ground_truth.get("groups", []) if g.get("expected_outcome") == "MATCH"])
        overall_resolution_rate = correct_matches / expected_matches if expected_matches > 0 else 0.0

        metrics = {
            "records_processed": records_processed,
            "records_reconciled": response.matches_created,
            "records_escalated": response.candidates_created + response.exceptions_created,
            "exceptions_created": response.exceptions_created,
            "candidates_created": response.candidates_created,
            "overall_resolution_rate": overall_resolution_rate,
            "throughput_records_per_second": records_processed / max(latency, 0.001),
            "false_resolutions": false_resolutions,
            "precision": precision
        }
        return metrics

if __name__ == "__main__":
    result = asyncio.run(run_evaluation("finctrl/backend/data/dev/dataset.json"))
    print(json.dumps(result, indent=2))
