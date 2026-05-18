from __future__ import annotations

from typing import Any

from order_pipeline.domain.events import OrderPaidEvent
from order_pipeline.domain.order import Order
from order_pipeline.infrastructure.postgres.codec import dump_json
from order_pipeline.infrastructure.postgres.mappers import to_order


class CreateOrderMixin:
    async def create_paid_order(
        self,
        category: str,
        telegram_id: str | None,
        payload: dict[str, Any],
        idempotency_key: str,
        topic: str,
        trace_id: str | None,
    ) -> Order:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                inserted = await conn.fetchrow(
                    _INSERT_ORDER,
                    category,
                    telegram_id,
                    dump_json(payload),
                    idempotency_key,
                )
                if inserted is None:
                    return await self._existing_by_idempotency(conn, idempotency_key)
                event = OrderPaidEvent(
                    order_id=inserted["id"],
                    category=category,
                    telegram_id=telegram_id,
                    payload=payload,
                    idempotency_key=idempotency_key,
                    trace_id=trace_id,
                )
                await self._insert_outbox(conn, topic, event)
                order = to_order(inserted)
                if order is None:
                    raise RuntimeError("created order was not returned")
                return order

    async def _existing_by_idempotency(self, conn, idempotency_key: str) -> Order:
        row = await conn.fetchrow(
            "SELECT * FROM orders WHERE idempotency_key = $1", idempotency_key
        )
        order = to_order(row)
        if order is None:
            raise RuntimeError("idempotency conflict without existing order")
        return order


_INSERT_ORDER = """
INSERT INTO orders (category, telegram_id, status, payload, idempotency_key)
VALUES ($1, $2, 'PAID', $3::jsonb, $4)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *
"""
