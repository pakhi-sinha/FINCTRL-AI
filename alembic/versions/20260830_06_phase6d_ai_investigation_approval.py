"""Phase 6D AI investigation and approval workflow.

Revision ID: 20260830_06
Revises: 20260829_05
"""
from alembic import op
import sqlalchemy as sa

revision = "20260830_06"
down_revision = "20260829_05"
branch_labels = None
depends_on = None


def _uuid_type():
    return sa.UUID(as_uuid=True) if op.get_bind().dialect.name == "postgresql" else sa.String(36)


def upgrade() -> None:
    op.create_table(
        "ai_investigations",
        sa.Column("id", _uuid_type(), nullable=False),
        sa.Column("exception_id", _uuid_type(), nullable=False),
        sa.Column("request_key", sa.String(64), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("model", sa.String(128), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="REQUESTED"),
        sa.Column("classification", sa.String(64)), sa.Column("root_cause", sa.Text()),
        sa.Column("summary", sa.Text()), sa.Column("recommended_action", sa.String(64)),
        sa.Column("confidence", sa.Integer()),
        sa.Column("evidence_references", sa.JSON(), nullable=False),
        sa.Column("requires_human_approval", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)), sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("input_hash", sa.String(64), nullable=False), sa.Column("result_hash", sa.String(64)),
        sa.Column("failure_code", sa.String(64)), sa.Column("requested_by", sa.String(), nullable=False),
        sa.Column("correlation_id", sa.String()),
        sa.CheckConstraint("status IN ('REQUESTED','RUNNING','COMPLETED','FAILED')", name="ck_ai_investigation_status"),
        sa.CheckConstraint("confidence IS NULL OR (confidence >= 0 AND confidence <= 10000)", name="ck_ai_investigation_confidence"),
        sa.CheckConstraint("requires_human_approval = 1", name="ck_ai_investigation_human_approval"),
        sa.ForeignKeyConstraint(["exception_id"], ["reconciliation_exceptions.id"]),
        sa.PrimaryKeyConstraint("id"), sa.UniqueConstraint("request_key", name="uq_ai_investigation_request_key"),
    )
    op.create_index("ix_ai_investigations_exception_id", "ai_investigations", ["exception_id"])
    op.create_index("ix_ai_investigations_status", "ai_investigations", ["status"])
    op.create_index("ix_ai_investigations_correlation_id", "ai_investigations", ["correlation_id"])
    op.create_table(
        "ai_investigation_approvals",
        sa.Column("id", _uuid_type(), nullable=False), sa.Column("investigation_id", _uuid_type(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="PENDING"),
        sa.Column("actor", sa.String()), sa.Column("decision_at", sa.DateTime(timezone=True)),
        sa.Column("reason", sa.Text()), sa.Column("correlation_id", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("status IN ('PENDING','APPROVED','REJECTED')", name="ck_ai_approval_status"),
        sa.ForeignKeyConstraint(["investigation_id"], ["ai_investigations.id"]),
        sa.PrimaryKeyConstraint("id"), sa.UniqueConstraint("investigation_id", name="uq_ai_approval_investigation"),
    )
    op.create_index("ix_ai_investigation_approvals_investigation_id", "ai_investigation_approvals", ["investigation_id"])
    op.create_index("ix_ai_investigation_approvals_correlation_id", "ai_investigation_approvals", ["correlation_id"])


def downgrade() -> None:
    op.drop_table("ai_investigation_approvals")
    op.drop_table("ai_investigations")
