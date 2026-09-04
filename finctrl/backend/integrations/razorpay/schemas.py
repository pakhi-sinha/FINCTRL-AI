from pydantic import BaseModel
from typing import Optional, Any, Dict
from datetime import datetime

class RazorpayEvidence(BaseModel):
    payment_id: str
    order_id: Optional[str]
    settlement_id: Optional[str]
    amount: int
    fee: int
    tax: int
    net_amount: int
    currency: str
    status: str
    created_at: int
    metadata: Dict[str, Any] = {}
