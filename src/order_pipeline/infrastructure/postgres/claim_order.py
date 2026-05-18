from __future__ import annotations

from datetime import timedelta

from order_pipeline.domain.order import ClaimedOrder
from order_pipeline.infrastructure.postgres.mappers import to_claimed


class ClaimOrderMixin:
    async def claim_order(
        self, order_id: int, owner: str, stale_after: timedelta
    ) -> ClaimedOrder | None:
        async with self._pool.acquire() as conn:
            return to_claimed(
                await conn.fetchrow(_CLAIM_ORDER, order_id, owner, stale_after)
            )


_CLAIM_ORDER = """
UPDATE orders
SET status = 'PROCESSING', attempts = attempts + 1,
    processing_owner = $2, processing_started_at = now(), updated_at = now()
WHERE id = $1 AND (
  status IN ('PAID', 'API_PENDING')
  OR (status = 'PROCESSING' AND processing_started_at < now() - $3::interval)
)
RETURNING id, category, payload, attempts, idempotency_key
"""
