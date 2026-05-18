from __future__ import annotations

from datetime import datetime, timedelta
from typing import Protocol

from order_pipeline.domain.events import OrderPaidEvent
from order_pipeline.domain.order import ClaimedOrder, Order, ProviderResult


class OrderStore(Protocol):
    async def create_paid_order(
        self,
        category: str,
        telegram_id: str | None,
        payload: dict,
        idempotency_key: str,
        topic: str,
        trace_id: str | None,
    ) -> Order: ...

    async def get_order(self, order_id: int) -> Order | None: ...

    async def list_orders(self, limit: int) -> list[Order]: ...

    async def claim_order(
        self, order_id: int, owner: str, stale_after: timedelta
    ) -> ClaimedOrder | None: ...

    async def complete_order(
        self,
        order_id: int,
        result: ProviderResult,
    ) -> None: ...

    async def retry_order(
        self,
        order_id: int,
        error: str,
        retry_event: OrderPaidEvent,
        retry_topic: str,
        publish_after: datetime,
    ) -> None: ...

    async def fail_order(
        self, order_id: int, error: str, dlq_event: OrderPaidEvent, dlq_topic: str
    ) -> None: ...
