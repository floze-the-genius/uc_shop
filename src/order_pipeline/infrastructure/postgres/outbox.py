from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import asyncpg

from order_pipeline.domain.events import OrderPaidEvent
from order_pipeline.infrastructure.postgres.codec import dump_json, load_json


class OutboxMixin:
    async def claim_outbox(
        self, relay_id: str, limit: int, stale_after: timedelta
    ) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(_CLAIM_OUTBOX, relay_id, limit, stale_after)
                return [_outbox_row(row) for row in rows]

    async def mark_outbox_published(self, event_id: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(_MARK_PUBLISHED, event_id)

    async def _insert_outbox(
        self,
        conn: asyncpg.Connection,
        topic: str,
        event: OrderPaidEvent,
        publish_after: datetime | None = None,
    ) -> None:
        await conn.execute(
            _INSERT_OUTBOX,
            event.event_id,
            topic,
            event.event_type,
            str(event.order_id),
            dump_json(event.to_dict()),
            publish_after,
        )


def _outbox_row(row: asyncpg.Record) -> dict[str, Any]:
    event = dict(row)
    event["payload"] = load_json(event["payload"])
    return event


_CLAIM_OUTBOX = """
WITH selected AS (
  SELECT id FROM order_events_outbox
  WHERE published_at IS NULL AND publish_after <= now()
    AND (locked_at IS NULL OR locked_at < now() - $3::interval)
  ORDER BY id FOR UPDATE SKIP LOCKED LIMIT $2
)
UPDATE order_events_outbox outbox
SET locked_by = $1, locked_at = now()
FROM selected WHERE outbox.id = selected.id
RETURNING outbox.*
"""

_MARK_PUBLISHED = "UPDATE order_events_outbox SET published_at = now(), locked_by = NULL, locked_at = NULL WHERE id = $1"

_INSERT_OUTBOX = """
INSERT INTO order_events_outbox (event_id, topic, event_type, aggregate_id, payload, publish_after)
VALUES ($1, $2, $3, $4, $5::jsonb, COALESCE($6::timestamptz, now()))
ON CONFLICT (event_id) DO NOTHING
"""
