from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any
from uuid import UUID
from datetime import datetime

class ERPRecord(BaseModel):
    id: UUID
    reference_id: str
    amount: int
    currency: str = "INR"
    timestamp: datetime
    type: str
    status: str

class RazorpayRecord(BaseModel):
    id: UUID
    rzp_payment_id: str
    rzp_settlement_id: Optional[str]
    order_receipt: str
    gross_amount: int
    fee: int
    tax: int
    net_amount: int
    type: str
    timestamp: datetime
    status: str

class BankRecord(BaseModel):
    id: UUID
    transaction_ref: str
    description: str
    amount: int
    type: str
    timestamp: datetime
    status: str

class GroundTruthGroup(BaseModel):
    group_id: UUID
    scenario: str
    erp_record_ids: List[UUID]
    rzp_record_ids: List[UUID]
    bank_record_ids: List[UUID]
    expected_outcome: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

class DatasetMetadata(BaseModel):
    dataset_name: str
    generator_version: str
    random_seed: int
    generation_id: str
    record_counts: Dict[str, int]
    scenario_counts: Dict[str, int]

class FinctrlDataset(BaseModel):
    metadata: DatasetMetadata
    erp_records: List[ERPRecord]
    rzp_records: List[RazorpayRecord]
    bank_records: List[BankRecord]

class GroundTruthDataset(BaseModel):
    metadata: DatasetMetadata
    groups: List[GroundTruthGroup]
