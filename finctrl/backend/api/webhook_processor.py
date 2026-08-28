import json
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from finctrl.backend.database.models import (
    FinancialEventModel,
    RazorpayOrderModel,
    RazorpayPaymentModel,
    RazorpaySettlementModel,
    RazorpayRefundModel,
)


async def process_razorpay_event(db: AsyncSession, event_model: FinancialEventModel) -> bool:
    """
    Processes a Razorpay financial event.
    Returns True if successfully processed, False otherwise.
    """
    event_type = event_model.event_type
    payload = event_model.raw_payload

    try:
        if event_type == "order.paid":
            order_data = payload.get("payload", {}).get("order", {}).get("entity", {})
            if order_data:
                om = RazorpayOrderModel(
                    source_event_id=event_model.id,
                    rzp_order_id=order_data.get("id"),
                    receipt=order_data.get("receipt"),
                    amount=order_data.get("amount", 0),
                    amount_due=order_data.get("amount_due", 0),
                    status=order_data.get("status"),
                    created_at_ts=order_data.get("created_at", 0)
                )
                db.add(om)
        elif event_type == "payment.captured":
            payment_data = payload.get("payload", {}).get("payment", {}).get("entity", {})
            if payment_data:
                pm = RazorpayPaymentModel(
                    source_event_id=event_model.id,
                    rzp_payment_id=payment_data.get("id"),
                    rzp_order_id=payment_data.get("order_id"),
                    amount=payment_data.get("amount", 0),
                    currency=payment_data.get("currency", "INR"),
                    status=payment_data.get("status"),
                    created_at_ts=payment_data.get("created_at", 0)
                )
                db.add(pm)
        elif event_type == "settlement.processed":
            settlement_data = payload.get("payload", {}).get("settlement", {}).get("entity", {})
            if settlement_data:
                sm = RazorpaySettlementModel(
                    source_event_id=event_model.id,
                    rzp_settlement_id=settlement_data.get("id"),
                    amount=settlement_data.get("amount", 0),
                    status=settlement_data.get("status"),
                    fees=settlement_data.get("fees", 0),
                    tax=settlement_data.get("tax", 0),
                    created_at_ts=settlement_data.get("created_at", 0)
                )
                db.add(sm)
        elif event_type == "refund.processed":
            refund_data = payload.get("payload", {}).get("refund", {}).get("entity", {})
            if refund_data:
                rm = RazorpayRefundModel(
                    source_event_id=event_model.id,
                    rzp_refund_id=refund_data.get("id"),
                    rzp_payment_id=refund_data.get("payment_id"),
                    amount=refund_data.get("amount", 0),
                    currency=refund_data.get("currency", "INR"),
                    status=refund_data.get("status"),
                    receipt=refund_data.get("receipt"),
                    created_at_ts=refund_data.get("created_at", 0)
                )
                db.add(rm)

        event_model.processing_status = "PROCESSED"
        # DeprecationWarning fix: Use timezone-aware datetime
        from datetime import timezone
        event_model.processed_at = datetime.now(timezone.utc)
        return True
    except IntegrityError as e:
        # Handles concurrent identical webhook deliveries cleanly
        event_model.processing_status = "FAILED"
        event_model.error_message = f"IntegrityError: {str(e)}"
        return False
    except Exception as e:
        event_model.processing_status = "FAILED"
        event_model.error_message = str(e)
        return False
