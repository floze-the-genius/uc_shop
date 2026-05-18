from __future__ import annotations

import asyncio

from order_pipeline.app.outbox.runner import relay_forever
from order_pipeline.infrastructure.kafka import KafkaPublisher
from order_pipeline.infrastructure.postgres import PostgresStore
from order_pipeline.platform.config import settings
from order_pipeline.platform.ids import process_id
from order_pipeline.platform.logging import configure_logging


async def run() -> None:
    configure_logging()
    store = await PostgresStore.connect(
        settings.database_url,
        settings.postgres_pool_min_size,
        settings.postgres_pool_max_size,
    )
    publisher = KafkaPublisher(settings.kafka_bootstrap_servers)
    await publisher.start()
    try:
        await relay_forever(process_id("outbox"), store, publisher)
    finally:
        await publisher.stop()
        await store.close()


if __name__ == "__main__":
    asyncio.run(run())
