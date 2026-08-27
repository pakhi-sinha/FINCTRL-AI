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

    # Initialize fresh in-memory DB for evaluation execution
    import os
    os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
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

        metrics = {
            "records_processed": records_processed,
            "records_reconciled": response.matches_created,
            "records_escalated": response.candidates_created + response.exceptions_created,
            "exceptions_created": response.exceptions_created,
            "candidates_created": response.candidates_created,
            "overall_resolution_rate": response.matches_created / max(len(dataset.get("erp_records", [])), 1),
            "throughput_records_per_second": records_processed / max(latency, 0.001)
        }
        return metrics

if __name__ == "__main__":
    result = asyncio.run(run_evaluation("finctrl/backend/data/dev/dataset.json"))
    print(json.dumps(result, indent=2))
