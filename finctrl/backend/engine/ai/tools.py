from typing import Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from datetime import datetime
import asyncio

from finctrl.backend.database.models import ERPRecordModel, RazorpayPaymentModel, RazorpaySettlementModel, RazorpayRefundModel, RazorpayOrderModel, BankRecordModel
from finctrl.backend.integrations.razorpay.client import razorpay_client

async def search_records(db: AsyncSession, record_type: str, status: str = "PENDING_INVESTIGATION", limit: int = 10) -> List[Dict[str, Any]]:
    # Read-only DB search bounded by limit
    model = None
    if record_type == "ERP":
        model = ERPRecordModel
    elif record_type == "RZP":
        model = RazorpayPaymentModel
    elif record_type == "BANK":
        model = BankRecordModel

    if not model:
        return []

    query = select(model).filter(model.status == status).limit(limit)
    result = await db.execute(query)
    records = result.scalars().all()

    # Serialize for AI consumption safely
    serialized = []
    for r in records:
        data = {c.name: getattr(r, c.name) for c in r.__table__.columns if c.name != "created_at" and c.name != "updated_at"}
        # Convert UUID/datetime to string
        for k, v in data.items():
            if hasattr(v, "isoformat"):
                data[k] = v.isoformat()
            else:
                data[k] = str(v)
        serialized.append(data)

    return serialized

async def calculate_fee_discrepancy(gross_amount: int, fee: int, tax: int, expected_net: int) -> Dict[str, Any]:
    # Deterministic helper to avoid AI inventing math
    calculated_net = gross_amount - fee - tax
    discrepancy = calculated_net - expected_net
    return {
        "calculated_net": calculated_net,
        "expected_net": expected_net,
        "discrepancy": discrepancy,
        "is_matching": discrepancy == 0
    }

async def fetch_razorpay_payment(payment_id: str) -> Dict[str, Any]:
    # Make synchronous SDK call non-blocking by running in thread pool
    evidence = await asyncio.to_thread(razorpay_client.fetch_payment, payment_id)
    if evidence:
        return evidence.model_dump()
    return {"error": "Payment not found or client unavailable"}

async def fetch_razorpay_settlement(settlement_id: str) -> Dict[str, Any]:
    evidence = await asyncio.to_thread(razorpay_client.fetch_settlement, settlement_id)
    if evidence:
        return evidence
    return {"error": "Settlement not found or client unavailable"}


async def fetch_unreconciled_in_window(db: AsyncSession, record_type: str, start_time: str, end_time: str, limit: int = 10) -> List[Dict[str, Any]]:
    model = None
    if record_type == "ERP":
        model = ERPRecordModel
    elif record_type == "RZP":
        model = RazorpayPaymentModel
    elif record_type == "BANK":
        model = BankRecordModel

    if not model:
        return []

    try:
        start_dt = datetime.fromisoformat(start_time)
        end_dt = datetime.fromisoformat(end_time)
    except ValueError:
        return [{"error": "Invalid date format. Use ISO format."}]

    query = select(model).filter(
        and_(
            model.status == "PENDING_INVESTIGATION",
            model.timestamp >= start_dt,
            model.timestamp <= end_dt
        )
    ).limit(limit)

    result = await db.execute(query)
    records = result.scalars().all()

    serialized = []
    for r in records:
        data = {c.name: getattr(r, c.name) for c in r.__table__.columns if c.name != "created_at" and c.name != "updated_at"}
        for k, v in data.items():
            if hasattr(v, "isoformat"):
                data[k] = v.isoformat()
            else:
                data[k] = str(v)
        serialized.append(data)

    return serialized


TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "search_records",
            "description": "Search internal database records that are unresolved.",
            "parameters": {
                "type": "object",
                "properties": {
                    "record_type": {
                        "type": "string",
                        "enum": ["ERP", "RZP", "BANK"],
                        "description": "The type of record to search."
                    },
                    "status": {
                        "type": "string",
                        "description": "Status to filter by (default PENDING_INVESTIGATION or UNRESOLVED)."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max number of records to return."
                    }
                },
                "required": ["record_type"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "calculate_fee_discrepancy",
            "description": "Deterministically calculate if amounts align according to gross - fee - tax = net.",
            "parameters": {
                "type": "object",
                "properties": {
                    "gross_amount": {"type": "integer"},
                    "fee": {"type": "integer"},
                    "tax": {"type": "integer"},
                    "expected_net": {"type": "integer"}
                },
                "required": ["gross_amount", "fee", "tax", "expected_net"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "fetch_razorpay_payment",
            "description": "Fetch normalized payment evidence from the Razorpay Sandbox.",
            "parameters": {
                "type": "object",
                "properties": {
                    "payment_id": {"type": "string"}
                },
                "required": ["payment_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "fetch_razorpay_settlement",
            "description": "Fetch settlement info from the Razorpay Sandbox.",
            "parameters": {
                "type": "object",
                "properties": {
                    "settlement_id": {"type": "string"}
                },
                "required": ["settlement_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "fetch_unreconciled_in_window",
            "description": "Fetch unreconciled records within a specific time window.",
            "parameters": {
                "type": "object",
                "properties": {
                    "record_type": {
                        "type": "string",
                        "enum": ["ERP", "RZP", "BANK"],
                    },
                    "start_time": {"type": "string", "description": "ISO format datetime"},
                    "end_time": {"type": "string", "description": "ISO format datetime"},
                    "limit": {"type": "integer"}
                },
                "required": ["record_type", "start_time", "end_time"]
            }
        }
    }
]

async def execute_tool(db: AsyncSession, name: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    if name == "search_records":
        return {"result": await search_records(db, **kwargs)}
    elif name == "calculate_fee_discrepancy":
        return await calculate_fee_discrepancy(**kwargs)
    elif name == "fetch_razorpay_payment":
        return await fetch_razorpay_payment(**kwargs)
    elif name == "fetch_razorpay_settlement":
        return await fetch_razorpay_settlement(**kwargs)
    elif name == "fetch_unreconciled_in_window":
        return {"result": await fetch_unreconciled_in_window(db, **kwargs)}
    else:
        return {"error": f"Unknown tool: {name}"}
