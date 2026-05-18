from __future__ import annotations

import asyncio

from order_pipeline.app.consumer.dependencies import create_store, create_use_case
from order_pipeline.app.consumer.runner import consume_forever
from order_pipeline.infrastructure.kafka import KafkaPublisher
from order_pipeline.platform.config import settings
from order_pipeline.platform.ids import process_id
from order_pipeline.platform.logging import configure_logging


async def run() -> None:
    configure_logging()
    store = await create_store()
    publisher = KafkaPublisher(settings.kafka_bootstrap_servers)
    await publisher.start()
    try:
        await consume_forever(
            process_id("processor"), create_use_case(store), publisher
        )
    finally:
        await publisher.stop()
        await store.close()


if __name__ == "__main__":
    asyncio.run(run())
