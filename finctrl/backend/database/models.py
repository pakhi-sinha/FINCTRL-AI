from datetime import datetime
import uuid
from typing import Optional, List, Any
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text, JSON, func, BigInteger
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, relationship
from sqlalchemy import UUID
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class ERPRecordModel(Base, TimestampMixin):
    __tablename__ = "erp_records"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    reference_id: Mapped[str] = mapped_column(String, index=True)
    amount: Mapped[int] = mapped_column(BigInteger)
    currency: Mapped[str] = mapped_column(String, default="INR")
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    type: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String)

class RazorpayRecordModel(Base, TimestampMixin):
    __tablename__ = "rzp_records"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    rzp_payment_id: Mapped[str] = mapped_column(String, index=True)
    rzp_settlement_id: Mapped[Optional[str]] = mapped_column(String, index=True, nullable=True)
    order_receipt: Mapped[str] = mapped_column(String, index=True)
    gross_amount: Mapped[int] = mapped_column(BigInteger)
    fee: Mapped[int] = mapped_column(BigInteger)
    tax: Mapped[int] = mapped_column(BigInteger)
    net_amount: Mapped[int] = mapped_column(BigInteger)
    type: Mapped[str] = mapped_column(String)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String)

class BankRecordModel(Base, TimestampMixin):
    __tablename__ = "bank_records"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    transaction_ref: Mapped[str] = mapped_column(String, index=True)
    description: Mapped[str] = mapped_column(Text)
    amount: Mapped[int] = mapped_column(BigInteger)
    type: Mapped[str] = mapped_column(String)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String)

class MatchEvidence(Base, TimestampMixin):
    __tablename__ = "match_evidence"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    match_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("reconciliation_matches.id"))
    record_type: Mapped[str] = mapped_column(String) # 'erp', 'rzp', 'bank'
    record_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))

class ReconciliationMatch(Base, TimestampMixin):
    __tablename__ = "reconciliation_matches"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    match_type: Mapped[str] = mapped_column(String) # '1:1', '1:N'
    status: Mapped[str] = mapped_column(String) # 'DETERMINISTIC_MATCH', 'PENDING_INVESTIGATION'
    confidence_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True) # deterministic scale e.g. 100

    evidence: Mapped[List[MatchEvidence]] = relationship("MatchEvidence", backref="match", cascade="all, delete-orphan")

class ExceptionModel(Base, TimestampMixin):
    __tablename__ = "exceptions"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    record_type: Mapped[str] = mapped_column(String)
    record_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    anomaly_type: Mapped[str] = mapped_column(String)
    severity: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String)

class AuditLog(Base, TimestampMixin):
    __tablename__ = "audit_log"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_type: Mapped[str] = mapped_column(String)
    entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    action: Mapped[str] = mapped_column(String)
    actor: Mapped[str] = mapped_column(String)
    changes: Mapped[dict] = mapped_column(JSONB)
