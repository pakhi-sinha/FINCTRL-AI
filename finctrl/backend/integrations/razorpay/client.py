import razorpay
from typing import Optional, Dict, Any, List
from finctrl.backend.config import settings
from finctrl.backend.integrations.razorpay.schemas import RazorpayEvidence
from finctrl.backend.database.models import RazorpayOrderModel, RazorpayPaymentModel, RazorpaySettlementModel, RazorpayRefundModel
from pydantic import BaseModel

class RazorpayOrderResponse(BaseModel):
    id: str
    amount: int
    amount_due: int
    currency: str
    receipt: Optional[str] = None
    status: str
    created_at: int

class RazorpaySettlementResponse(BaseModel):
    id: str
    amount: int
    status: str
    fees: int
    tax: int
    utr: Optional[str] = None
    created_at: int

class RazorpayRefundResponse(BaseModel):
    id: str
    payment_id: str
    amount: int
    currency: str
    status: str
    receipt: Optional[str] = None
    created_at: int

class RazorpayClient:
    def __init__(self):
        self.key_id = settings.RAZORPAY_KEY_ID
        self.key_secret = settings.RAZORPAY_KEY_SECRET
        self.client = None
        if self.key_id and self.key_secret:
            self.client = razorpay.Client(auth=(self.key_id, self.key_secret))

    def fetch_order(self, order_id: str) -> Optional[RazorpayOrderResponse]:
        if not self.client:
            return None
        try:
            response = self.client.order.fetch(order_id)
            return RazorpayOrderResponse(
                id=response.get("id"),
                amount=response.get("amount", 0),
                amount_due=response.get("amount_due", 0),
                currency=response.get("currency", "INR"),
                receipt=response.get("receipt"),
                status=response.get("status", ""),
                created_at=response.get("created_at", 0)
            )
        except Exception:
            return None

    def fetch_payment(self, payment_id: str) -> Optional[RazorpayEvidence]:
        if not self.client:
            return None
        try:
            response = self.client.payment.fetch(payment_id)
            return self._normalize_payment(response)
        except Exception:
            return None

    def fetch_settlement(self, settlement_id: str) -> Optional[RazorpaySettlementResponse]:
        if not self.client:
            return None
        try:
            response = self.client.settlement.fetch(settlement_id)
            return RazorpaySettlementResponse(
                id=response.get("id"),
                amount=response.get("amount", 0),
                status=response.get("status", ""),
                fees=response.get("fees", 0),
                tax=response.get("tax", 0),
                utr=response.get("utr"),
                created_at=response.get("created_at", 0)
            )
        except Exception:
            return None

    def fetch_refund(self, refund_id: str) -> Optional[RazorpayRefundResponse]:
        if not self.client:
            return None
        try:
            response = self.client.refund.fetch(refund_id)
            return RazorpayRefundResponse(
                id=response.get("id"),
                payment_id=response.get("payment_id"),
                amount=response.get("amount", 0),
                currency=response.get("currency", "INR"),
                status=response.get("status", ""),
                receipt=response.get("receipt"),
                created_at=response.get("created_at", 0)
            )
        except Exception:
            return None

    def _normalize_payment(self, raw_payment: Dict[str, Any]) -> RazorpayEvidence:
        return RazorpayEvidence(
            payment_id=raw_payment.get("id", ""),
            order_id=raw_payment.get("order_id"),
            settlement_id=None,
            amount=raw_payment.get("amount", 0),
            fee=raw_payment.get("fee", 0),
            tax=raw_payment.get("tax", 0),
            net_amount=raw_payment.get("amount", 0) - raw_payment.get("fee", 0) - raw_payment.get("tax", 0),
            currency=raw_payment.get("currency", "INR"),
            status=raw_payment.get("status", ""),
            created_at=raw_payment.get("created_at", 0),
            metadata={
                "method": raw_payment.get("method"),
                "email": raw_payment.get("email"),
                "contact": raw_payment.get("contact"),
                "notes": raw_payment.get("notes", {})
            }
        )

razorpay_client = RazorpayClient()
