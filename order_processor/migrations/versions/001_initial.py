"""Initial migration

Revision ID: 001
Revises:
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "orders",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("product_category", sa.String(), nullable=True),
        sa.Column("is_w_telegram_id", sa.Boolean(), nullable=True),
        sa.Column("last_ts", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "products",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("category", sa.String(), nullable=True),
        sa.Column("data", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    products_table = sa.table(
        "products",
        sa.column("id", sa.String()),
        sa.column("category", sa.String()),
        sa.column("data", sa.JSON()),
    )
    op.bulk_insert(
        products_table,
        [
            {"id": "gcrystals", "category": "gcrystals", "data": {}},
            {"id": "coins", "category": "coins", "data": {}},
            {"id": "premium", "category": "premium", "data": {}},
        ],
    )


def downgrade() -> None:
    op.drop_table("products")
    op.drop_table("orders")
