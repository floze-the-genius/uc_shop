from __future__ import annotations

from order_pipeline.domain.order import Order
from order_pipeline.infrastructure.postgres.mappers import to_order


class ReadOrdersMixin:
    async def get_order(self, order_id: int) -> Order | None:
        async with self._pool.acquire() as conn:
            return to_order(
                await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
            )

    async def list_orders(self, limit: int) -> list[Order]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM orders ORDER BY id DESC LIMIT $1",
                max(1, min(limit, 200)),
            )
            return [order for row in rows if (order := to_order(row)) is not None]
