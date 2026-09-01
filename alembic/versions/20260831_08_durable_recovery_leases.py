"""Add durable recovery lease ownership.

Revision ID: 20260831_08
Revises: 20260831_07
"""
from alembic import op
import sqlalchemy as sa

revision = "20260831_08"
down_revision = "20260831_07"
branch_labels = None
depends_on = None

TABLES = ("financial_events", "ai_investigations", "reconciliation_runs")


def upgrade():
    for table in TABLES:
        op.add_column(table, sa.Column("lease_owner", sa.String(64), nullable=True))
        op.add_column(table, sa.Column("execution_attempt_id", sa.String(64), nullable=True))
        op.add_column(table, sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
        op.add_column(table, sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True))
        op.create_index(f"ix_{table}_lease_owner", table, ["lease_owner"])
        op.create_index(f"ix_{table}_execution_attempt_id", table, ["execution_attempt_id"])
        op.create_index(f"ix_{table}_lease_expires_at", table, ["lease_expires_at"])


def downgrade():
    for table in reversed(TABLES):
        op.drop_index(f"ix_{table}_lease_expires_at", table_name=table)
        op.drop_index(f"ix_{table}_execution_attempt_id", table_name=table)
        op.drop_index(f"ix_{table}_lease_owner", table_name=table)
        op.drop_column(table, "heartbeat_at")
        op.drop_column(table, "lease_expires_at")
        op.drop_column(table, "execution_attempt_id")
        op.drop_column(table, "lease_owner")
