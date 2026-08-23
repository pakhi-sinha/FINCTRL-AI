from datetime import datetime
from uuid import UUID, uuid4
from typing import Any
import os

from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

# If using SQLite for testing, we must fall back to generic JSON
DATABASE_URL = os.environ.get("DATABASE_URL", "")
is_sqlite = DATABASE_URL.startswith("sqlite")

if is_sqlite:
    from sqlalchemy import JSON as JSONType
    # Custom type decorator to handle UUID to str conversion for SQLite
    import sqlalchemy.types as types
    class UUIDType(types.TypeDecorator):
        impl = types.String
        cache_ok = True

        def process_bind_param(self, value, dialect):
            if value is None:
                return value
            return str(value)

        def process_result_value(self, value, dialect):
            if value is None:
                return value
            return UUID(value)

    def get_uuid():
        return str(uuid4())

    def _uuid_col(*args, **kwargs):
        return Column(UUIDType(), *args, **kwargs)
else:
    from sqlalchemy.dialects.postgresql import JSONB as JSONType
    from sqlalchemy.dialects.postgresql import UUID as UUIDType

    def get_uuid():
        return uuid4()

    def _uuid_col(*args, **kwargs):
        return Column(UUIDType(as_uuid=True), *args, **kwargs)

class ERPRecordModel(Base):
    __tablename__ = "erp_records"

    id = _uuid_col(primary_key=True, default=get_uuid)
    reference_id = Column(String, index=True, nullable=False)
    amount = Column(Integer, nullable=False)  # subunits
    currency = Column(String, default="INR", nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    type = Column(String, nullable=False)
    status = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class RazorpayRecordModel(Base):
    __tablename__ = "razorpay_records"

    id = _uuid_col(primary_key=True, default=get_uuid)
    rzp_payment_id = Column(String, index=True, nullable=False)
    rzp_settlement_id = Column(String, index=True, nullable=True)
    order_receipt = Column(String, nullable=False)
    gross_amount = Column(Integer, nullable=False)
    fee = Column(Integer, nullable=False)
    tax = Column(Integer, nullable=False)
    net_amount = Column(Integer, nullable=False)
    type = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    status = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class BankRecordModel(Base):
    __tablename__ = "bank_records"

    id = _uuid_col(primary_key=True, default=get_uuid)
    transaction_ref = Column(String, index=True, nullable=False)
    description = Column(Text, nullable=False)
    amount = Column(Integer, nullable=False)
    type = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    status = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class ReconciliationMatchModel(Base):
    __tablename__ = "reconciliation_matches"

    id = _uuid_col(primary_key=True, default=get_uuid)
    match_type = Column(String, nullable=False) # e.g. "EXACT_1_1", "CONSOLIDATED"
    status = Column(String, nullable=False, default="RESOLVED")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    evidence = relationship("MatchEvidenceModel", back_populates="match", cascade="all, delete-orphan")

class MatchEvidenceModel(Base):
    __tablename__ = "match_evidence"

    id = _uuid_col(primary_key=True, default=get_uuid)
    match_id = _uuid_col(ForeignKey("reconciliation_matches.id"), nullable=False)
    record_type = Column(String, nullable=False) # "ERP", "RZP", "BANK"
    record_id = _uuid_col(nullable=False)

    match = relationship("ReconciliationMatchModel", back_populates="evidence")

class ReconciliationCandidateModel(Base):
    __tablename__ = "reconciliation_candidates"

    id = _uuid_col(primary_key=True, default=get_uuid)
    candidate_type = Column(String, nullable=False) # e.g. "POTENTIAL_1_1", "PARTIAL_SETTLEMENT"
    evidence_payload = Column(JSONType, nullable=False) # Source IDs and signals
    status = Column(String, nullable=False, default="PENDING_INVESTIGATION")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class ExceptionModel(Base):
    __tablename__ = "exceptions"

    id = _uuid_col(primary_key=True, default=get_uuid)
    record_type = Column(String, nullable=False)
    record_id = _uuid_col(nullable=False)
    anomaly_type = Column(String, nullable=False)
    severity = Column(String, nullable=False)
    status = Column(String, nullable=False, default="OPEN")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class AuditLogModel(Base):
    __tablename__ = "audit_logs"

    id = _uuid_col(primary_key=True, default=get_uuid)
    entity_type = Column(String, nullable=False)
    entity_id = _uuid_col(nullable=False)
    action = Column(String, nullable=False)
    actor = Column(String, nullable=False, default="SYSTEM")
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)
    changes = Column(JSONType, nullable=False)
