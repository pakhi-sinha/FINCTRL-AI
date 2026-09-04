
from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB

JSONType = JSON().with_variant(JSONB, "postgresql")

from datetime import datetime, timezone
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL
from typing import Any

from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text, UniqueConstraint, CheckConstraint, event, inspect
from sqlalchemy.types import TypeDecorator
from sqlalchemy.orm import declarative_base, relationship
from finctrl.backend.config import settings

Base = declarative_base()


class UTCDateTime(TypeDecorator):
    """Persist UTC and restore timezone information on dialects such as SQLite."""
    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("UTCDateTime requires a timezone-aware value")
        return value.astimezone(timezone.utc)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

# If using SQLite for testing, we must fall back to generic JSON
DATABASE_URL = settings.DATABASE_URL
is_sqlite = DATABASE_URL.startswith("sqlite")

if is_sqlite:
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
    from sqlalchemy.dialects.postgresql import UUID as UUIDType

    def get_uuid():
        return uuid4()

    def _uuid_col(*args, **kwargs):
        return Column(UUIDType(as_uuid=True), *args, **kwargs)


def financial_event_id(provider: str, provider_event_id: str):
    """Return the stable ledger identity for a provider event."""
    value = uuid5(NAMESPACE_URL, f"finctrl:financial-event:{provider.lower()}:{provider_event_id}")
    return str(value) if is_sqlite else value


def razorpay_source_event_key(entity_type: str, provider_object_id: str) -> str:
    """Canonical ledger key shared by Razorpay API and webhook ingestion."""
    return f"{entity_type.lower()}:{provider_object_id}"


def razorpay_payload_event_key(payload: dict, fallback_event_id: str) -> str:
    """Resolve an object identity, retaining delivery identity for object-less events."""
    event_type = payload.get("event", "")
    entity_type = event_type.split(".", 1)[0]
    entity = payload.get("payload", {}).get(entity_type, {}).get("entity", {}) if isinstance(payload.get("payload"), dict) else {}
    provider_object_id = entity.get("id") if isinstance(entity, dict) else None
    return razorpay_source_event_key(entity_type, provider_object_id) if entity_type and provider_object_id else fallback_event_id


class FinancialEventModel(Base):
    __tablename__ = "financial_events"

    id = _uuid_col(primary_key=True, default=get_uuid)
    provider = Column(String, nullable=False, index=True)
    provider_event_id = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False)
    payload_hash = Column(String, nullable=False)
    raw_payload = Column(JSONType, nullable=False)
    received_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    processed_at = Column(DateTime(timezone=True), nullable=True)
    processing_status = Column(String, nullable=False, default="PENDING")
    attempt_count = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)
    lease_owner = Column(String(64), nullable=True, index=True)
    execution_attempt_id = Column(String(64), nullable=True, index=True)
    lease_expires_at = Column(UTCDateTime(), nullable=True, index=True)
    heartbeat_at = Column(UTCDateTime(), nullable=True)
    schema_version = Column(String, nullable=False, default="1.0")
    __table_args__ = (UniqueConstraint('provider', 'provider_event_id', name='uix_provider_event_id'),)


@event.listens_for(FinancialEventModel, "before_update")
def _prevent_financial_event_mutation(mapper, connection, target):
    """Ledger source facts are immutable; only processing state may change."""
    state = inspect(target)
    immutable_fields = (
        "id", "provider", "provider_event_id", "event_type", "payload_hash",
        "raw_payload", "received_at", "schema_version",
    )
    changed = [name for name in immutable_fields if state.attrs[name].history.has_changes()]
    if changed:
        raise ValueError(f"Financial event fields are immutable: {', '.join(changed)}")

class ERPRecordModel(Base):
    __tablename__ = "erp_records"

    id = _uuid_col(primary_key=True, default=get_uuid)
    source_event_id = _uuid_col(ForeignKey("financial_events.id"), nullable=True)
    reference_id = Column(String, index=True, nullable=False)
    amount = Column(Integer, nullable=False)  # subunits
    currency = Column(String, default="INR", nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    type = Column(String, nullable=False)
    status = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class RazorpayOrderModel(Base):
    __tablename__ = "razorpay_orders"

    id = _uuid_col(primary_key=True, default=get_uuid)
    source_event_id = _uuid_col(ForeignKey("financial_events.id"), nullable=True)
    rzp_order_id = Column(String, index=True, unique=True, nullable=False)
    receipt = Column(String, index=True, nullable=False)
    amount = Column(Integer, nullable=False)
    amount_paid = Column(Integer, nullable=False, default=0)
    amount_due = Column(Integer, nullable=False)
    currency = Column(String, default="INR", nullable=False)
    status = Column(String, nullable=False)
    created_at_ts = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class RazorpayPaymentModel(Base):
    __tablename__ = "razorpay_payments"

    id = _uuid_col(primary_key=True, default=get_uuid)
    source_event_id = _uuid_col(ForeignKey("financial_events.id"), nullable=True)
    rzp_payment_id = Column(String, index=True, unique=True, nullable=False)
    rzp_order_id = Column(String, index=True, nullable=True)
    rzp_settlement_id = Column(String, index=True, nullable=True)
    amount = Column(Integer, nullable=False)
    currency = Column(String, default="INR", nullable=False)
    status = Column(String, nullable=False)
    method = Column(String, nullable=True)
    amount_refunded = Column(Integer, nullable=False, default=0)
    refund_status = Column(String, nullable=True)
    captured = Column(Integer, nullable=False, default=0)
    email = Column(String, nullable=True)
    contact = Column(String, nullable=True)
    fee = Column(Integer, nullable=True)
    tax = Column(Integer, nullable=True)
    error_code = Column(String, nullable=True)
    error_description = Column(String, nullable=True)
    created_at_ts = Column(Integer, nullable=False)
    reconciliation_status = Column(String, nullable=False, default="UNRECONCILED")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class RazorpaySettlementModel(Base):
    __tablename__ = "razorpay_settlements"

    id = _uuid_col(primary_key=True, default=get_uuid)
    source_event_id = _uuid_col(ForeignKey("financial_events.id"), nullable=True)
    rzp_settlement_id = Column(String, index=True, unique=True, nullable=False)
    amount = Column(Integer, nullable=False)
    status = Column(String, nullable=False)
    fees = Column(Integer, nullable=False)
    tax = Column(Integer, nullable=False)
    utr = Column(String, index=True, nullable=True)
    created_at_ts = Column(Integer, nullable=False)
    reconciliation_status = Column(String, nullable=False, default="UNRECONCILED")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class RazorpayRefundModel(Base):
    __tablename__ = "razorpay_refunds"

    id = _uuid_col(primary_key=True, default=get_uuid)
    source_event_id = _uuid_col(ForeignKey("financial_events.id"), nullable=True)
    rzp_refund_id = Column(String, index=True, unique=True, nullable=False)
    rzp_payment_id = Column(String, index=True, nullable=False)
    amount = Column(Integer, nullable=False)
    currency = Column(String, default="INR", nullable=False)
    status = Column(String, nullable=False)
    receipt = Column(String, nullable=True)
    created_at_ts = Column(Integer, nullable=False)
    reconciliation_status = Column(String, nullable=False, default="UNRECONCILED")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class BankRecordModel(Base):
    __tablename__ = "bank_records"

    id = _uuid_col(primary_key=True, default=get_uuid)
    source_event_id = _uuid_col(ForeignKey("financial_events.id"), nullable=True)
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
    match_key = Column(String, nullable=True, unique=True)
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
    source_id = Column(String, nullable=True)

    match = relationship("ReconciliationMatchModel", back_populates="evidence")
    __table_args__ = (
        UniqueConstraint('match_id', 'record_type', 'record_id', name='uix_match_evidence_record'),
    )

class ReconciliationCandidateModel(Base):
    __tablename__ = "reconciliation_candidates"

    id = _uuid_col(primary_key=True, default=get_uuid)
    candidate_key = Column(String, nullable=True, unique=True)
    candidate_type = Column(String, nullable=False) # e.g. "POTENTIAL_1_1", "PARTIAL_SETTLEMENT"
    score = Column(Integer, nullable=False, default=0)
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


class ReconciliationExceptionModel(Base):
    __tablename__ = "reconciliation_exceptions"

    id = _uuid_col(primary_key=True, default=get_uuid)
    exception_key = Column(String, nullable=False, unique=True, index=True)
    exception_type = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, default="OPEN", index=True)
    severity = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolution_type = Column(String, nullable=True)
    resolution_note = Column(Text, nullable=True)

    evidence = relationship("ExceptionEvidenceModel", back_populates="exception", cascade="all, delete-orphan")
    audit_entries = relationship("ExceptionAuditModel", back_populates="exception", cascade="all, delete-orphan")
    __table_args__ = (
        CheckConstraint("status IN ('OPEN','INVESTIGATING','RESOLVED','DISMISSED')", name="ck_reconciliation_exception_status"),
        CheckConstraint("severity IN ('LOW','MEDIUM','HIGH','CRITICAL')", name="ck_reconciliation_exception_severity"),
        CheckConstraint(
            "exception_type IN ('MISSING_ERP','MISSING_RAZORPAY','MISSING_BANK','AMOUNT_MISMATCH','REFERENCE_MISMATCH','TIMING_MISMATCH','SETTLEMENT_MISMATCH','REFUND_MISMATCH','DUPLICATE_CANDIDATE','AMBIGUOUS_MATCH','UNMATCHED')",
            name="ck_reconciliation_exception_type",
        ),
    )


class ExceptionEvidenceModel(Base):
    __tablename__ = "exception_evidence"

    id = _uuid_col(primary_key=True, default=get_uuid)
    exception_id = _uuid_col(ForeignKey("reconciliation_exceptions.id"), nullable=False, index=True)
    record_type = Column(String, nullable=False)
    record_id = _uuid_col(nullable=False)
    source_id = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    exception = relationship("ReconciliationExceptionModel", back_populates="evidence")
    __table_args__ = (
        UniqueConstraint("exception_id", "record_type", "record_id", name="uix_exception_evidence_record"),
    )


class ExceptionAuditModel(Base):
    __tablename__ = "exception_audits"

    id = _uuid_col(primary_key=True, default=get_uuid)
    exception_id = _uuid_col(ForeignKey("reconciliation_exceptions.id"), nullable=False, index=True)
    previous_status = Column(String, nullable=False)
    new_status = Column(String, nullable=False)
    resolution_type = Column(String, nullable=True)
    resolution_note = Column(Text, nullable=True)
    actor = Column(String, nullable=True)
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    exception = relationship("ReconciliationExceptionModel", back_populates="audit_entries")

class AuditLogModel(Base):
    __tablename__ = "audit_logs"

    id = _uuid_col(primary_key=True, default=get_uuid)
    entity_type = Column(String, nullable=False)
    entity_id = _uuid_col(nullable=False)
    action = Column(String, nullable=False)
    actor = Column(String, nullable=False, default="SYSTEM")
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)
    changes = Column(JSONType, nullable=False)


class AIInvestigationModel(Base):
    __tablename__ = "ai_investigations"

    id = _uuid_col(primary_key=True, default=get_uuid)
    exception_id = _uuid_col(ForeignKey("reconciliation_exceptions.id"), nullable=False, index=True)
    request_key = Column(String(64), nullable=False, unique=True)
    provider = Column(String(32), nullable=False)
    model = Column(String(128), nullable=False)
    status = Column(String(16), nullable=False, default="REQUESTED", index=True)
    classification = Column(String(64), nullable=True)
    root_cause = Column(Text, nullable=True)
    summary = Column(Text, nullable=True)
    recommended_action = Column(String(64), nullable=True)
    confidence = Column(Integer, nullable=True)  # basis points, 0..10000
    evidence_references = Column(JSONType, nullable=False, default=list)
    requires_human_approval = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    started_at = Column(UTCDateTime(), nullable=True)
    completed_at = Column(UTCDateTime(), nullable=True)
    input_hash = Column(String(64), nullable=False)
    result_hash = Column(String(64), nullable=True)
    failure_code = Column(String(64), nullable=True)
    requested_by = Column(String, nullable=False)
    correlation_id = Column(String, nullable=True, index=True)
    lease_owner = Column(String(64), nullable=True, index=True)
    execution_attempt_id = Column(String(64), nullable=True, index=True)
    lease_expires_at = Column(UTCDateTime(), nullable=True, index=True)
    heartbeat_at = Column(UTCDateTime(), nullable=True)
    __table_args__ = (
        CheckConstraint("status IN ('REQUESTED','RUNNING','COMPLETED','FAILED')", name="ck_ai_investigation_status"),
        CheckConstraint("confidence IS NULL OR (confidence >= 0 AND confidence <= 10000)", name="ck_ai_investigation_confidence"),
        CheckConstraint("requires_human_approval = 1", name="ck_ai_investigation_human_approval"),
    )


class AIInvestigationApprovalModel(Base):
    __tablename__ = "ai_investigation_approvals"

    id = _uuid_col(primary_key=True, default=get_uuid)
    investigation_id = _uuid_col(ForeignKey("ai_investigations.id"), nullable=False, unique=True, index=True)
    status = Column(String(16), nullable=False, default="PENDING")
    actor = Column(String, nullable=True)
    decision_at = Column(UTCDateTime(), nullable=True)
    reason = Column(Text, nullable=True)
    correlation_id = Column(String, nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    __table_args__ = (
        CheckConstraint("status IN ('PENDING','APPROVED','REJECTED')", name="ck_ai_approval_status"),
    )


class RazorpaySyncStateModel(Base):
    __tablename__ = "razorpay_sync_state"

    id = _uuid_col(primary_key=True, default=get_uuid)
    resource_type = Column(String, nullable=False, unique=True, index=True)
    last_from_ts = Column(Integer, nullable=True)
    last_to_ts = Column(Integer, nullable=True)
    last_provider_timestamp = Column(Integer, nullable=True)
    last_status = Column(String, nullable=False, default="NEVER_RUN")
    last_error = Column(Text, nullable=True)
    records_fetched = Column(Integer, nullable=False, default=0)
    records_created = Column(Integer, nullable=False, default=0)
    records_updated = Column(Integer, nullable=False, default=0)
    duplicates_ignored = Column(Integer, nullable=False, default=0)
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class ReconciliationRunModel(Base):
    __tablename__ = "reconciliation_runs"

    id = _uuid_col(primary_key=True, default=get_uuid)
    run_key = Column(String, nullable=False, unique=True, index=True)
    status = Column(String, nullable=False, default="REQUESTED", index=True)
    requested_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    initiated_by = Column(String, nullable=True)
    correlation_id = Column(String, nullable=True, index=True)
    from_ts = Column(Integer, nullable=True)
    to_ts = Column(Integer, nullable=True)
    current_stage = Column(String, nullable=True)
    matches_created = Column(Integer, nullable=False, default=0)
    candidates_created = Column(Integer, nullable=False, default=0)
    exceptions_created = Column(Integer, nullable=False, default=0)
    records_examined = Column(Integer, nullable=False, default=0)
    errors_count = Column(Integer, nullable=False, default=0)
    duration_ms = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)
    retry_of_id = _uuid_col(ForeignKey("reconciliation_runs.id"), nullable=True)
    attempt = Column(Integer, nullable=False, default=1)
    lease_owner = Column(String(64), nullable=True, index=True)
    execution_attempt_id = Column(String(64), nullable=True, index=True)
    lease_expires_at = Column(UTCDateTime(), nullable=True, index=True)
    heartbeat_at = Column(UTCDateTime(), nullable=True)

    stages = relationship("ReconciliationStageRunModel", back_populates="run", cascade="all, delete-orphan")
    __table_args__ = (
        CheckConstraint("status IN ('REQUESTED','RUNNING','SUCCEEDED','PARTIAL','FAILED','CANCELLED')", name="ck_reconciliation_run_status"),
    )


class ReconciliationStageRunModel(Base):
    __tablename__ = "reconciliation_stage_runs"

    id = _uuid_col(primary_key=True, default=get_uuid)
    run_id = _uuid_col(ForeignKey("reconciliation_runs.id"), nullable=False, index=True)
    stage_name = Column(String, nullable=False)
    sequence = Column(Integer, nullable=False)
    status = Column(String, nullable=False, default="REQUESTED")
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    duration_ms = Column(Integer, nullable=False, default=0)
    records_examined = Column(Integer, nullable=False, default=0)
    matches_created = Column(Integer, nullable=False, default=0)
    candidates_created = Column(Integer, nullable=False, default=0)
    exceptions_created = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)

    run = relationship("ReconciliationRunModel", back_populates="stages")
    __table_args__ = (
        UniqueConstraint("run_id", "stage_name", name="uq_reconciliation_stage_run"),
        CheckConstraint("status IN ('REQUESTED','RUNNING','SUCCEEDED','FAILED','SKIPPED')", name="ck_reconciliation_stage_status"),
    )


class ReconciliationPeriodModel(Base):
    __tablename__ = "reconciliation_periods"

    id = _uuid_col(primary_key=True, default=get_uuid)
    period_key = Column(String, nullable=False, unique=True, index=True)
    from_ts = Column(Integer, nullable=False)
    to_ts = Column(Integer, nullable=False)
    status = Column(String, nullable=False, default="OPEN", index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    opened_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    closed_at = Column(DateTime(timezone=True), nullable=True)
    created_by = Column(String, nullable=True)
    closed_by = Column(String, nullable=True)
    correlation_id = Column(String, nullable=True, index=True)
    latest_run_id = _uuid_col(ForeignKey("reconciliation_runs.id"), nullable=True)
    notes = Column(Text, nullable=True)
    __table_args__ = (
        CheckConstraint("status IN ('OPEN','READY','BLOCKED','CLOSED')", name="ck_reconciliation_period_status"),
        CheckConstraint("from_ts <= to_ts", name="ck_reconciliation_period_window"),
    )
