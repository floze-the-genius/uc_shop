from __future__ import annotations

from order_pipeline.application.create_paid_order import CreatePaidOrder
from order_pipeline.infrastructure.postgres import PostgresStore
from order_pipeline.platform.config import settings


async def create_store() -> PostgresStore:
    return await PostgresStore.connect(
        settings.database_url,
        settings.postgres_pool_min_size,
        settings.postgres_pool_max_size,
    )


def create_order_use_case(store: PostgresStore) -> CreatePaidOrder:
    return CreatePaidOrder(store, settings.orders_paid_topic)
