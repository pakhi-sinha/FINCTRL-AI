from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, select

from finctrl.backend.api.main import app
from finctrl.backend.database.database import async_session_maker
from finctrl.backend.database.models import BankRecordModel
from finctrl.backend.reconciliation.forecasting import CashForecastService, METHOD


def ts(day):
    return int(datetime(2026, 8, day, 12, tzinfo=timezone.utc).timestamp())


@pytest_asyncio.fixture(autouse=True)
async def clean_bank_records():
    async with async_session_maker() as db:
        await db.execute(delete(BankRecordModel)); await db.commit()


async def seed():
    async with async_session_maker() as db:
        db.add_all([
            BankRecordModel(transaction_ref="credit-1", description="Settlement", amount=1001, type="CREDIT", timestamp=datetime.fromtimestamp(ts(1), timezone.utc), status="CLEARED"),
            BankRecordModel(transaction_ref="debit-1", description="Refund", amount=201, type="DEBIT", timestamp=datetime.fromtimestamp(ts(1), timezone.utc), status="CLEARED"),
            BankRecordModel(transaction_ref="credit-2", description="Settlement", amount=700, type="CR", timestamp=datetime.fromtimestamp(ts(2), timezone.utc), status="RECONCILED"),
            BankRecordModel(transaction_ref="ignored", description="Pending", amount=9999, type="CREDIT", timestamp=datetime.fromtimestamp(ts(2), timezone.utc), status="PENDING"),
        ]); await db.commit()


@pytest.mark.asyncio
async def test_deterministic_integer_aggregation_horizon_and_metadata():
    await seed()
    async with async_session_maker() as db:
        first = await CashForecastService(db).forecast(ts(1), ts(3), 4, "INR")
        second = await CashForecastService(db).forecast(ts(1), ts(3), 4, "INR")
    inr = first["currencies"]["INR"]
    assert inr["totals"] == {"historical_inflow": 1701, "historical_outflow": 201,
                              "historical_net": 1500, "forecast_net": 2000}
    assert [point["net"] for point in inr["forecast"]] == [500] * 4
    assert first["method"] == second["method"] == METHOD
    assert first["currencies"] == second["currencies"]
    assert all(type(value) is int for value in inr["totals"].values())
    assert "integer smallest-unit" in first["methodology"]


@pytest.mark.asyncio
async def test_empty_and_insufficient_history_are_explicit():
    async with async_session_maker() as db:
        empty = await CashForecastService(db).forecast(ts(1), ts(2), 7)
    assert empty["currencies"]["INR"]["forecast"] == []
    assert empty["currencies"]["INR"]["forecast_available"] is False
    await seed()
    async with async_session_maker() as db:
        short = await CashForecastService(db).forecast(ts(1), ts(1), 7)
    assert short["currencies"]["INR"]["forecast"] == []
    assert "two populated" in short["currencies"]["INR"]["unavailable_reason"]


@pytest.mark.asyncio
async def test_currency_separation_and_invalid_parameters():
    async with async_session_maker() as db:
        service = CashForecastService(db)
        with pytest.raises(ValueError): await service.forecast(ts(2), ts(1), 1)
        with pytest.raises(ValueError): await service.forecast(ts(1), ts(2), 0)
        with pytest.raises(ValueError): await service.forecast(ts(1), ts(2), 1, "USD")


@pytest.mark.asyncio
async def test_forecast_api_schema_auth_and_existing_reporting_endpoint():
    await seed()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        missing_auth = await client.get(f"/forecast/cash?from_ts={ts(1)}&to_ts={ts(3)}")
        response = await client.get(f"/forecast/cash?from_ts={ts(1)}&to_ts={ts(3)}&horizon_days=2",
                                    headers={"X-API-Key": "test_readonly_key"})
        summary = await client.get(f"/forecast/cash/summary?from_ts={ts(1)}&to_ts={ts(3)}&horizon_days=2",
                                   headers={"X-API-Key": "test_readonly_key"})
        reports = await client.get("/reconciliation/reports", headers={"X-API-Key": "test_readonly_key"})
    assert missing_auth.status_code == 401
    assert response.status_code == summary.status_code == reports.status_code == 200
    assert response.json()["currencies"]["INR"]["historical"][0].keys() == {"date", "inflow", "outflow", "net"}


@pytest.mark.asyncio
async def test_forecast_does_not_mutate_financial_records():
    await seed()
    async with async_session_maker() as db:
        before = [(row.id, row.amount, row.status) for row in (await db.scalars(select(BankRecordModel).order_by(BankRecordModel.id))).all()]
        await CashForecastService(db).forecast(ts(1), ts(3), 2)
        after = [(row.id, row.amount, row.status) for row in (await db.scalars(select(BankRecordModel).order_by(BankRecordModel.id))).all()]
    assert before == after
