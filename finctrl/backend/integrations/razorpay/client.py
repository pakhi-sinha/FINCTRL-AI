import razorpay
from typing import Optional, Dict, Any, List
from finctrl.backend.config import settings
from finctrl.backend.integrations.razorpay.schemas import RazorpayEvidence

class RazorpayClient:
    def __init__(self):
        self.key_id = settings.RAZORPAY_KEY_ID
        self.key_secret = settings.RAZORPAY_KEY_SECRET
        self.client = None
        if self.key_id and self.key_secret:
            self.client = razorpay.Client(auth=(self.key_id, self.key_secret))

    def fetch_payment(self, payment_id: str) -> Optional[RazorpayEvidence]:
        if not self.client:
            return None
        try:
            # READ ONLY operation
            response = self.client.payment.fetch(payment_id)
            return self._normalize_payment(response)
        except Exception:
            return None

    def fetch_settlement(self, settlement_id: str) -> Optional[Dict[str, Any]]:
        # This might return standard settlement info, we'll keep it simple for now
        if not self.client:
            return None
        try:
            # READ ONLY operation
            response = self.client.settlement.fetch(settlement_id)
            return response
        except Exception:
            return None

    def _normalize_payment(self, raw_payment: Dict[str, Any]) -> RazorpayEvidence:
        return RazorpayEvidence(
            payment_id=raw_payment.get("id", ""),
            order_id=raw_payment.get("order_id"),
            settlement_id=None, # Settlement info isn't always directly on the payment fetch
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

# Global read-only client instance
razorpay_client = RazorpayClient()
