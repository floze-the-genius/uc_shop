from __future__ import annotations

import logging

from order_pipeline.application.process_order_event import ProcessOrderEvent
from order_pipeline.domain.events import OrderPaidEvent
from order_pipeline.infrastructure.kafka import KafkaPublisher
from order_pipeline.platform.config import settings

logger = logging.getLogger(__name__)


async def handle_message(
    message, use_case: ProcessOrderEvent, owner: str, publisher: KafkaPublisher
) -> None:
    try:
        event = OrderPaidEvent.from_bytes(message.value)
    except Exception as exc:
        await publisher.send_raw(
            topic=settings.orders_dlq_topic,
            key=message.key,
            value=message.value,
            headers=[("error", str(exc).encode("utf-8", errors="replace"))],
        )
        logger.exception(
            "Invalid event topic=%s partition=%s offset=%s",
            message.topic,
            message.partition,
            message.offset,
        )
        return

    outcome = await use_case.execute(event, owner)
    logger.debug(
        "Handled order_id=%s status=%s detail=%s",
        outcome.order_id,
        outcome.status,
        outcome.detail,
    )
