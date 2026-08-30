"""Deterministic, read-only cash forecasting over authoritative bank movements."""
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from finctrl.backend.database.models import BankRecordModel

METHOD = "daily_mean_net_cash_v1"
SUPPORTED_CURRENCIES = {"INR"}
MAX_HORIZON_DAYS = 365


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _trunc_div(numerator: int, denominator: int) -> int:
    return (1 if numerator >= 0 else -1) * (abs(numerator) // denominator)


class CashForecastService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def forecast(self, from_ts: int, to_ts: int, horizon_days: int, currency: str | None = None) -> dict:
        if from_ts > to_ts:
            raise ValueError("from_ts must be less than or equal to to_ts")
        if not 1 <= horizon_days <= MAX_HORIZON_DAYS:
            raise ValueError(f"horizon_days must be between 1 and {MAX_HORIZON_DAYS}")
        selected = currency.upper() if currency else "INR"
        if selected not in SUPPORTED_CURRENCIES:
            raise ValueError(f"Unsupported currency: {selected}")
        start = datetime.fromtimestamp(from_ts, timezone.utc)
        end = datetime.fromtimestamp(to_ts, timezone.utc)
        rows = (await self.db.scalars(select(BankRecordModel).where(
            BankRecordModel.timestamp >= start, BankRecordModel.timestamp <= end,
            BankRecordModel.status.in_(("CLEARED", "RECONCILED", "C")),
        ).order_by(BankRecordModel.timestamp, BankRecordModel.id))).all()
        by_day = defaultdict(lambda: {"inflow": 0, "outflow": 0})
        for row in rows:
            day = _utc(row.timestamp).date().isoformat()
            if row.type.upper() in {"CREDIT", "CR"}:
                by_day[day]["inflow"] += row.amount
            elif row.type.upper() in {"DEBIT", "DR"}:
                by_day[day]["outflow"] += abs(row.amount)
        first_day, last_day = start.date(), end.date()
        day_count = (last_day - first_day).days + 1
        historical = []
        for offset in range(day_count):
            day = (first_day + timedelta(days=offset)).isoformat()
            values = by_day[day]
            historical.append({"date": day, **values, "net": values["inflow"] - values["outflow"]})
        total_inflow = sum(point["inflow"] for point in historical)
        total_outflow = sum(point["outflow"] for point in historical)
        total_net = total_inflow - total_outflow
        populated_days = sum(bool(point["inflow"] or point["outflow"]) for point in historical)
        sufficient = populated_days >= 2
        forecast = []
        if sufficient:
            daily_net = _trunc_div(total_net, day_count)
            forecast = [{"date": (last_day + timedelta(days=offset)).isoformat(), "net": daily_net}
                        for offset in range(1, horizon_days + 1)]
        reason = None if sufficient else ("No authoritative cash movements in the selected window" if not rows
                                           else "At least two populated historical days are required")
        return {
            "generated_at": datetime.now(timezone.utc), "from_ts": from_ts, "to_ts": to_ts,
            "horizon_days": horizon_days, "method": METHOD,
            "methodology": "Arithmetic mean of daily net authoritative bank cash movements across the complete selected calendar window; integer smallest-unit arithmetic with truncation toward zero.",
            "source": {"record_type": "bank_records", "statuses": ["C", "CLEARED", "RECONCILED"],
                       "record_count": len(rows), "calendar_days": day_count, "populated_days": populated_days,
                       "currency_assumption": "Bank records have no currency field and are contractually treated as INR."},
            "currencies": {selected: {"historical": historical, "forecast": forecast,
                "totals": {"historical_inflow": total_inflow, "historical_outflow": total_outflow,
                           "historical_net": total_net, "forecast_net": sum(point["net"] for point in forecast)},
                "forecast_available": sufficient, "unavailable_reason": reason}},
        }

    async def summary(self, from_ts: int, to_ts: int, horizon_days: int, currency: str | None = None) -> dict:
        result = await self.forecast(from_ts, to_ts, horizon_days, currency)
        return {key: result[key] for key in ("generated_at", "from_ts", "to_ts", "horizon_days", "method", "source")} | {
            "currencies": {code: {"totals": data["totals"], "forecast_available": data["forecast_available"],
                                  "unavailable_reason": data["unavailable_reason"]}
                           for code, data in result["currencies"].items()}}
