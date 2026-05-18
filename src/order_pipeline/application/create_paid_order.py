from __future__ import annotations

from order_pipeline.application.ports.order_store import OrderStore
from order_pipeline.domain.order import Order
from order_pipeline.platform.ids import new_id


class CreatePaidOrder:
    def __init__(self, store: OrderStore, paid_topic: str):
        self._store = store
        self._paid_topic = paid_topic

    async def execute(
        self,
        category: str,
        telegram_id: str | None,
        payload: dict,
        idempotency_key: str | None,
        trace_id: str | None,
    ) -> Order:
        return await self._store.create_paid_order(
            category=category,
            telegram_id=telegram_id,
            payload=payload,
            idempotency_key=idempotency_key or new_id(),
            topic=self._paid_topic,
            trace_id=trace_id,
        )
