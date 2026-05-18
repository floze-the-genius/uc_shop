from __future__ import annotations

import asyncpg

from order_pipeline.domain.order import ClaimedOrder, Order, OrderStatus
from order_pipeline.infrastructure.postgres.codec import load_json


def to_order(row: asyncpg.Record | None) -> Order | None:
    if row is None:
        return None
    return Order(
        id=row["id"],
        category=row["category"],
        telegram_id=row["telegram_id"],
        status=OrderStatus(row["status"]),
        payload=load_json(row["payload"]) or {},
        result=load_json(row["result"]) if row["result"] is not None else None,
        attempts=row["attempts"],
        idempotency_key=row["idempotency_key"],
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def to_claimed(row: asyncpg.Record | None) -> ClaimedOrder | None:
    if row is None:
        return None
    return ClaimedOrder(
        id=row["id"],
        category=row["category"],
        payload=load_json(row["payload"]) or {},
        attempts=row["attempts"],
        idempotency_key=row["idempotency_key"],
    )
