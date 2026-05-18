from __future__ import annotations

from order_pipeline.domain.events import OrderPaidEvent


def dlq_event(event: OrderPaidEvent) -> OrderPaidEvent:
    return OrderPaidEvent(
        order_id=event.order_id,
        category=event.category,
        event_type="order.dlq",
        telegram_id=event.telegram_id,
        payload=event.payload,
        attempt=event.attempt,
        idempotency_key=event.idempotency_key,
        trace_id=event.trace_id,
        created_at=event.created_at,
    )
