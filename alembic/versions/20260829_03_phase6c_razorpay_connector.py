"""Phase 6C production Razorpay connector expansion.

Revision ID: 20260829_03
Revises: 20260829_02
"""

from alembic import op
import sqlalchemy as sa


revision = "20260829_03"
down_revision = "20260829_02"
branch_labels = None
depends_on = None


def _uuid_type():
    return sa.UUID(as_uuid=True) if op.get_bind().dialect.name == "postgresql" else sa.String(36)


def upgrade() -> None:
    op.create_table(
        "razorpay_sync_state",
        sa.Column("id", _uuid_type(), nullable=False),
        sa.Column("resource_type", sa.String(), nullable=False),
        sa.Column("last_from_ts", sa.Integer(), nullable=True),
        sa.Column("last_to_ts", sa.Integer(), nullable=True),
        sa.Column("last_provider_timestamp", sa.Integer(), nullable=True),
        sa.Column("last_status", sa.String(), nullable=False, server_default="NEVER_RUN"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("records_fetched", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_updated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicates_ignored", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("resource_type", name="uq_razorpay_sync_state_resource_type"),
    )
    op.create_index("ix_razorpay_sync_state_resource_type", "razorpay_sync_state", ["resource_type"])


def downgrade() -> None:
    op.drop_table("razorpay_sync_state")
