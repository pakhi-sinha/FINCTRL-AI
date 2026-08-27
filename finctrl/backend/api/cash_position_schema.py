from pydantic import BaseModel

class CashPositionResponse(BaseModel):
    current_realized_cash: int
    captured_unsettled_amount: int
    expected_refunds: int
    known_fees: int
    known_tax: int
    projected_cash_position: int
    records_analyzed: int
