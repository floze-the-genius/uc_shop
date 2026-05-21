from typing import Any

from sqlalchemy import select

from src.models.products import Product


class ProductRepository:
    def __init__(self, session_maker):
        self._session_maker = session_maker

    async def get_product(self, session, product_id: str) -> dict[str, Any] | None:
        result = await session.execute(select(Product).where(Product.id == product_id))
        row = result.scalar_one_or_none()
        if not row:
            return None
        item: dict[str, Any] = {"id": row.id, "category": row.category}
        return item

    async def get_products_by_category(self, session, category: str) -> list[dict[str, Any]]:
        result = await session.execute(
            select(Product).where(Product.category == category)
        )
        rows = result.scalars().all()
        return [
            {"id": row.id, "category": row.category, **(row.data or {})}
            for row in rows
        ]
