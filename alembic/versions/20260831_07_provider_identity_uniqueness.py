"""Enforce unique Razorpay provider object identities.

Revision ID: 20260831_07
Revises: 20260830_06
"""
from alembic import op
import sqlalchemy as sa

revision = "20260831_07"
down_revision = "20260830_06"
branch_labels = None
depends_on = None

_IDENTITIES = (
    ("razorpay_orders", "rzp_order_id"),
    ("razorpay_payments", "rzp_payment_id"),
    ("razorpay_settlements", "rzp_settlement_id"),
    ("razorpay_refunds", "rzp_refund_id"),
)


def upgrade():
    connection = op.get_bind()
    conflicts = []
    for table, column in _IDENTITIES:
        duplicate = connection.execute(sa.text(
            f"SELECT {column}, COUNT(*) FROM {table} GROUP BY {column} HAVING COUNT(*) > 1 LIMIT 1"
        )).first()
        if duplicate:
            conflicts.append(f"{table}.{column}={duplicate[0]!r} ({duplicate[1]} rows)")
    if conflicts:
        raise RuntimeError("Razorpay provider identity duplicates must be resolved before migration: " + "; ".join(conflicts))
    for table, column in _IDENTITIES:
        name = f"ix_{table}_{column}"
        op.drop_index(name, table_name=table)
        op.create_index(name, table, [column], unique=True)


def downgrade():
    for table, column in reversed(_IDENTITIES):
        name = f"ix_{table}_{column}"
        op.drop_index(name, table_name=table)
        op.create_index(name, table, [column], unique=False)
