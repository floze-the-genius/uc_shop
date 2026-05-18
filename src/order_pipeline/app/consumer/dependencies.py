from __future__ import annotations

from order_pipeline.application.process_order_event import ProcessOrderEvent
from order_pipeline.application.retry_policy import RetryPolicy
from order_pipeline.infrastructure.postgres import PostgresStore
from order_pipeline.infrastructure.providers import OrderProviderClient
from order_pipeline.platform.config import settings


async def create_store() -> PostgresStore:
    return await PostgresStore.connect(
        settings.database_url,
        settings.postgres_pool_min_size,
        settings.postgres_pool_max_size,
    )


def create_use_case(store: PostgresStore) -> ProcessOrderEvent:
    return ProcessOrderEvent(
        store=store,
        provider=OrderProviderClient(settings.provider_timeout_seconds),
        retry_policy=RetryPolicy(
            settings.order_max_attempts,
            settings.retry_delays_seconds,
            settings.orders_retry_topics,
        ),
        stale_after_seconds=settings.order_processing_stale_seconds,
        dlq_topic=settings.orders_dlq_topic,
    )
