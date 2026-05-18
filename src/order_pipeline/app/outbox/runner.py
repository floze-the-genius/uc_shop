from __future__ import annotations

import asyncio
from datetime import timedelta

from order_pipeline.app.outbox.publisher import publish_row
from order_pipeline.infrastructure.kafka import KafkaPublisher
from order_pipeline.infrastructure.postgres import PostgresStore
from order_pipeline.platform.config import settings


async def relay_forever(
    relay_id: str, store: PostgresStore, publisher: KafkaPublisher
) -> None:
    semaphore = asyncio.Semaphore(settings.outbox_concurrency)

    async def limited(row: dict) -> None:
        async with semaphore:
            await publish_row(row, store, publisher)

    while True:
        rows = await store.claim_outbox(
            relay_id=relay_id,
            limit=settings.outbox_batch_size,
            stale_after=timedelta(seconds=60),
        )
        if not rows:
            await asyncio.sleep(settings.outbox_idle_seconds)
            continue
        await asyncio.gather(*(limited(row) for row in rows))
