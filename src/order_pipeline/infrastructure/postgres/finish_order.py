from __future__ import annotations

from datetime import datetime

from order_pipeline.domain.events import OrderPaidEvent
from order_pipeline.domain.order import ProviderResult
from order_pipeline.infrastructure.postgres.codec import dump_json


class FinishOrderMixin:
    async def complete_order(
        self,
        order_id: int,
        result: ProviderResult,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(_COMPLETE_ORDER, order_id, dump_json(result.to_dict()))

    async def retry_order(
        self,
        order_id: int,
        error: str,
        retry_event: OrderPaidEvent,
        retry_topic: str,
        publish_after: datetime,
    ) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(_RETRY_ORDER, order_id, error)
                await self._insert_outbox(conn, retry_topic, retry_event, publish_after)

    async def fail_order(
        self, order_id: int, error: str, dlq_event: OrderPaidEvent, dlq_topic: str
    ) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(_FAIL_ORDER, order_id, error)
                await self._insert_outbox(conn, dlq_topic, dlq_event)


_COMPLETE_ORDER = """
UPDATE orders SET status = 'COMPLETED', result = $2::jsonb, last_error = NULL,
processing_owner = NULL, processing_started_at = NULL, completed_at = now(), updated_at = now()
WHERE id = $1
"""

_RETRY_ORDER = """
UPDATE orders SET status = 'API_PENDING', last_error = $2,
processing_owner = NULL, processing_started_at = NULL, updated_at = now()
WHERE id = $1
"""

_FAIL_ORDER = """
UPDATE orders SET status = 'FAILED', last_error = $2,
processing_owner = NULL, processing_started_at = NULL, updated_at = now()
WHERE id = $1
"""
