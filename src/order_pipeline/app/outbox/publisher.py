from __future__ import annotations

import logging

from order_pipeline.domain.events import OrderPaidEvent
from order_pipeline.infrastructure.kafka import KafkaPublisher
from order_pipeline.infrastructure.postgres import PostgresStore

logger = logging.getLogger(__name__)


async def publish_row(
    row: dict, store: PostgresStore, publisher: KafkaPublisher
) -> None:
    event = OrderPaidEvent.from_dict(row["payload"])
    await publisher.publish(row["topic"], event)

    await store.mark_outbox_published(row["id"])
    logger.debug(
        "Published outbox id=%s event_id=%s order_id=%s topic=%s",
        row["id"],
        event.event_id,
        event.order_id,
        row["topic"],
    )
