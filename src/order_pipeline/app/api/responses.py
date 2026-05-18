from __future__ import annotations

from typing import Any

from order_pipeline.domain.order import Order


def order_response(order: Order) -> dict[str, Any]:
    return {
        "id": order.id,
        "category": order.category,
        "telegram_id": order.telegram_id,
        "status": order.status,
        "payload": order.payload,
        "result": order.result,
        "attempts": order.attempts,
        "idempotency_key": order.idempotency_key,
        "last_error": order.last_error,
        "created_at": order.created_at,
        "updated_at": order.updated_at,
    }
