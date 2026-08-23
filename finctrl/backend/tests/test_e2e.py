import pytest
import json
import asyncio
from httpx import AsyncClient, ASGITransport
from finctrl.backend.api.main import app
from finctrl.backend.database.database import get_db_session

@pytest.mark.asyncio
async def test_e2e_reconciliation():
    # Load Phase 1 DEV data
    with open("finctrl/backend/data/dev/dataset.json", "r") as f:
        dataset = json.load(f)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Ingest ERP
        resp_erp = await ac.post("/ingest/erp", json={"records": dataset["erp_records"]})
        assert resp_erp.status_code == 200

        # Ingest RZP
        resp_rzp = await ac.post("/ingest/rzp", json={"records": dataset["rzp_records"]})
        assert resp_rzp.status_code == 200

        # Ingest Bank
        resp_bank = await ac.post("/ingest/bank", json={"records": dataset["bank_records"]})
        assert resp_bank.status_code == 200

        # Run reconciliation
        resp_recon = await ac.post("/reconciliation/run")
        assert resp_recon.status_code == 200

        recon_data = resp_recon.json()
        # Some matches should be created deterministically since synthetic data contains happy paths
        assert recon_data["matches_created"] > 0
        assert recon_data["candidates_created"] >= 0

        # Verify get candidates
        resp_cand = await ac.get("/candidates")
        assert resp_cand.status_code == 200

        # Verify get matches
        resp_matches = await ac.get("/matches")
        assert resp_matches.status_code == 200
