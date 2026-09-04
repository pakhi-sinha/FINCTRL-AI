"""Phase 6D reconciliation operations and run control.

Revision ID: 20260829_04
Revises: 20260829_03
"""
from alembic import op
import sqlalchemy as sa

revision = "20260829_04"
down_revision = "20260829_03"
branch_labels = None
depends_on = None


def _uuid_type():
    return sa.UUID(as_uuid=True) if op.get_bind().dialect.name == "postgresql" else sa.String(36)


def upgrade() -> None:
    op.create_table(
        "reconciliation_runs",
        sa.Column("id", _uuid_type(), nullable=False),
        sa.Column("run_key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="REQUESTED"),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("initiated_by", sa.String()), sa.Column("correlation_id", sa.String()),
        sa.Column("from_ts", sa.Integer()), sa.Column("to_ts", sa.Integer()),
        sa.Column("current_stage", sa.String()),
        sa.Column("matches_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("candidates_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("exceptions_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_examined", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("errors_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text()),
        sa.Column("retry_of_id", _uuid_type()),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.CheckConstraint("status IN ('REQUESTED','RUNNING','SUCCEEDED','PARTIAL','FAILED','CANCELLED')", name="ck_reconciliation_run_status"),
        sa.ForeignKeyConstraint(["retry_of_id"], ["reconciliation_runs.id"]),
        sa.PrimaryKeyConstraint("id"), sa.UniqueConstraint("run_key", name="uq_reconciliation_runs_run_key"),
    )
    op.create_index("ix_reconciliation_runs_run_key", "reconciliation_runs", ["run_key"])
    op.create_index("ix_reconciliation_runs_status", "reconciliation_runs", ["status"])
    op.create_index("ix_reconciliation_runs_correlation_id", "reconciliation_runs", ["correlation_id"])
    op.create_table(
        "reconciliation_stage_runs",
        sa.Column("id", _uuid_type(), nullable=False),
        sa.Column("run_id", _uuid_type(), nullable=False),
        sa.Column("stage_name", sa.String(), nullable=False), sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="REQUESTED"),
        sa.Column("started_at", sa.DateTime(timezone=True)), sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_examined", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("matches_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("candidates_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("exceptions_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text()),
        sa.CheckConstraint("status IN ('REQUESTED','RUNNING','SUCCEEDED','FAILED','SKIPPED')", name="ck_reconciliation_stage_status"),
        sa.ForeignKeyConstraint(["run_id"], ["reconciliation_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "stage_name", name="uq_reconciliation_stage_run"),
    )
    op.create_index("ix_reconciliation_stage_runs_run_id", "reconciliation_stage_runs", ["run_id"])


def downgrade() -> None:
    op.drop_table("reconciliation_stage_runs")
    op.drop_table("reconciliation_runs")
