"""Phase 6E reconciliation reporting and close controls.

Revision ID: 20260829_05
Revises: 20260829_04
"""
from alembic import op
import sqlalchemy as sa

revision = "20260829_05"
down_revision = "20260829_04"
branch_labels = None
depends_on = None


def _uuid_type():
    return sa.UUID(as_uuid=True) if op.get_bind().dialect.name == "postgresql" else sa.String(36)


def upgrade() -> None:
    op.create_table(
        "reconciliation_periods",
        sa.Column("id", _uuid_type(), nullable=False),
        sa.Column("period_key", sa.String(), nullable=False),
        sa.Column("from_ts", sa.Integer(), nullable=False),
        sa.Column("to_ts", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="OPEN"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
        sa.Column("created_by", sa.String()), sa.Column("closed_by", sa.String()),
        sa.Column("correlation_id", sa.String()),
        sa.Column("latest_run_id", _uuid_type()), sa.Column("notes", sa.Text()),
        sa.CheckConstraint("status IN ('OPEN','READY','BLOCKED','CLOSED')", name="ck_reconciliation_period_status"),
        sa.CheckConstraint("from_ts <= to_ts", name="ck_reconciliation_period_window"),
        sa.ForeignKeyConstraint(["latest_run_id"], ["reconciliation_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("period_key", name="uq_reconciliation_periods_period_key"),
    )
    op.create_index("ix_reconciliation_periods_period_key", "reconciliation_periods", ["period_key"])
    op.create_index("ix_reconciliation_periods_status", "reconciliation_periods", ["status"])
    op.create_index("ix_reconciliation_periods_correlation_id", "reconciliation_periods", ["correlation_id"])


def downgrade() -> None:
    op.drop_table("reconciliation_periods")
