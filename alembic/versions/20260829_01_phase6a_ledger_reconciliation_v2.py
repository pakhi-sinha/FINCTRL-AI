"""Phase 6A ledger and reconciliation V2 constraints.

Revision ID: 20260829_01
Revises: 8c95c70a6e6c
"""

from alembic import op
import sqlalchemy as sa


revision = "20260829_01"
down_revision = "8c95c70a6e6c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("reconciliation_matches") as batch_op:
        batch_op.add_column(sa.Column("match_key", sa.String(), nullable=True))
        batch_op.create_unique_constraint("uq_reconciliation_matches_match_key", ["match_key"])

    with op.batch_alter_table("match_evidence") as batch_op:
        batch_op.add_column(sa.Column("source_id", sa.String(), nullable=True))
        batch_op.create_unique_constraint(
            "uix_match_evidence_record", ["match_id", "record_type", "record_id"]
        )


def downgrade() -> None:
    with op.batch_alter_table("match_evidence") as batch_op:
        batch_op.drop_constraint("uix_match_evidence_record", type_="unique")
        batch_op.drop_column("source_id")

    with op.batch_alter_table("reconciliation_matches") as batch_op:
        batch_op.drop_constraint("uq_reconciliation_matches_match_key", type_="unique")
        batch_op.drop_column("match_key")
