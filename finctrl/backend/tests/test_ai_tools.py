import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

import pytest
from finctrl.backend.engine.ai.tools import execute_tool, calculate_fee_discrepancy
from finctrl.backend.integrations.razorpay.client import razorpay_client
from unittest.mock import patch

@pytest.mark.asyncio
async def test_calculate_fee_discrepancy():
    res = await calculate_fee_discrepancy(gross_amount=1000, fee=20, tax=5, expected_net=975)
    assert res["calculated_net"] == 975
    assert res["is_matching"] is True

@pytest.mark.asyncio
async def test_fetch_razorpay_mocked():
    with patch.object(razorpay_client, "fetch_payment") as mock_fetch:
        from finctrl.backend.integrations.razorpay.schemas import RazorpayEvidence
        mock_fetch.return_value = RazorpayEvidence(
            payment_id="pay_123", order_id=None, settlement_id=None, amount=100, fee=2, tax=0, net_amount=98,
            currency="INR", status="captured", created_at=123
        )
        res = await execute_tool(None, "fetch_razorpay_payment", {"payment_id": "pay_123"})
        assert res["payment_id"] == "pay_123"
        assert res["amount"] == 100
