"""Phase 6B exception and evidence workbench.

Revision ID: 20260829_02
Revises: 20260829_01
"""

from alembic import op
import sqlalchemy as sa


revision = "20260829_02"
down_revision = "20260829_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("reconciliation_candidates") as batch_op:
        batch_op.add_column(sa.Column("candidate_key", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("score", sa.Integer(), nullable=False, server_default="0"))
        batch_op.create_unique_constraint("uq_reconciliation_candidates_candidate_key", ["candidate_key"])

    op.create_table(
        "reconciliation_exceptions",
        sa.Column("id", _uuid_type(), nullable=False),
        sa.Column("exception_key", sa.String(), nullable=False),
        sa.Column("exception_type", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution_type", sa.String(), nullable=True),
        sa.Column("resolution_note", sa.Text(), nullable=True),
        sa.CheckConstraint("status IN ('OPEN','INVESTIGATING','RESOLVED','DISMISSED')", name="ck_reconciliation_exception_status"),
        sa.CheckConstraint("severity IN ('LOW','MEDIUM','HIGH','CRITICAL')", name="ck_reconciliation_exception_severity"),
        sa.CheckConstraint(
            "exception_type IN ('MISSING_ERP','MISSING_RAZORPAY','MISSING_BANK','AMOUNT_MISMATCH','REFERENCE_MISMATCH','TIMING_MISMATCH','SETTLEMENT_MISMATCH','REFUND_MISMATCH','DUPLICATE_CANDIDATE','AMBIGUOUS_MATCH','UNMATCHED')",
            name="ck_reconciliation_exception_type",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("exception_key", name="uq_reconciliation_exceptions_exception_key"),
    )
    op.create_index("ix_reconciliation_exceptions_exception_key", "reconciliation_exceptions", ["exception_key"])
    op.create_index("ix_reconciliation_exceptions_exception_type", "reconciliation_exceptions", ["exception_type"])
    op.create_index("ix_reconciliation_exceptions_status", "reconciliation_exceptions", ["status"])

    op.create_table(
        "exception_evidence",
        sa.Column("id", _uuid_type(), nullable=False),
        sa.Column("exception_id", _uuid_type(), nullable=False),
        sa.Column("record_type", sa.String(), nullable=False),
        sa.Column("record_id", _uuid_type(), nullable=False),
        sa.Column("source_id", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["exception_id"], ["reconciliation_exceptions.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("exception_id", "record_type", "record_id", name="uix_exception_evidence_record"),
    )
    op.create_index("ix_exception_evidence_exception_id", "exception_evidence", ["exception_id"])

    op.create_table(
        "exception_audits",
        sa.Column("id", _uuid_type(), nullable=False),
        sa.Column("exception_id", _uuid_type(), nullable=False),
        sa.Column("previous_status", sa.String(), nullable=False),
        sa.Column("new_status", sa.String(), nullable=False),
        sa.Column("resolution_type", sa.String(), nullable=True),
        sa.Column("resolution_note", sa.Text(), nullable=True),
        sa.Column("actor", sa.String(), nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["exception_id"], ["reconciliation_exceptions.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_exception_audits_exception_id", "exception_audits", ["exception_id"])


def _uuid_type():
    return sa.UUID(as_uuid=True) if op.get_bind().dialect.name == "postgresql" else sa.String(36)


def downgrade() -> None:
    op.drop_table("exception_audits")
    op.drop_table("exception_evidence")
    op.drop_table("reconciliation_exceptions")
    with op.batch_alter_table("reconciliation_candidates") as batch_op:
        batch_op.drop_constraint("uq_reconciliation_candidates_candidate_key", type_="unique")
        batch_op.drop_column("score")
        batch_op.drop_column("candidate_key")
