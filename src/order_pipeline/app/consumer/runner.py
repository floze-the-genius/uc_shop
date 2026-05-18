from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer

from order_pipeline.app.consumer.handler import handle_message
from order_pipeline.infrastructure.kafka import KafkaPublisher
from order_pipeline.platform.config import settings

logger = logging.getLogger(__name__)


async def consume_forever(
    consumer_id: str, use_case, publisher: KafkaPublisher
) -> None:
    consumer = AIOKafkaConsumer(
        settings.orders_paid_topic,
        *settings.orders_retry_topics,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.order_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=settings.order_consumer_batch_size,
    )
    await consumer.start()
    logger.info("Order processor started as %s", consumer_id)
    try:
        await _loop(consumer, consumer_id, use_case, publisher)
    finally:
        await consumer.stop()


async def _loop(
    consumer, consumer_id: str, use_case, publisher: KafkaPublisher
) -> None:
    semaphore = asyncio.Semaphore(settings.order_consumer_concurrency)

    async def limited(message) -> None:
        async with semaphore:
            await handle_message(message, use_case, consumer_id, publisher)

    while True:
        batches = await consumer.getmany(
            timeout_ms=settings.order_consumer_poll_ms,
            max_records=settings.order_consumer_batch_size,
        )
        messages = [message for batch in batches.values() for message in batch]
        if messages:
            await asyncio.gather(*(limited(message) for message in messages))

            await consumer.commit()
