from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _csv(name: str, default: str) -> tuple[str, ...]:
    raw = os.getenv(name, default)
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _float_csv(name: str, default: str) -> tuple[float, ...]:
    values: list[float] = []
    for item in _csv(name, default):
        try:
            values.append(float(item))
        except ValueError:
            continue
    return tuple(values)


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://uc_shop:uc_shop@localhost:5432/uc_shop"
    )
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    orders_paid_topic: str = os.getenv("ORDERS_PAID_TOPIC", "orders.paid")

    orders_retry_topics: tuple[str, ...] = _csv(
        "ORDERS_RETRY_TOPICS",
        "orders.retry.10s,orders.retry.1m,orders.retry.5m",
    )
    orders_dlq_topic: str = os.getenv("ORDERS_DLQ_TOPIC", "orders.dlq")
    order_consumer_group: str = os.getenv(
        "ORDER_CONSUMER_GROUP", "uc-shop-order-processors"
    )
    order_processing_stale_seconds: int = _int("ORDER_PROCESSING_STALE_SECONDS", 600)
    order_max_attempts: int = _int("ORDER_MAX_ATTEMPTS", 5)
    order_consumer_batch_size: int = _int("ORDER_CONSUMER_BATCH_SIZE", 500)
    order_consumer_concurrency: int = _int("ORDER_CONSUMER_CONCURRENCY", 100)
    order_consumer_poll_ms: int = _int("ORDER_CONSUMER_POLL_MS", 500)
    retry_delays_seconds: tuple[float, ...] = _float_csv(
        "RETRY_DELAYS_SECONDS", "10,60,300"
    )
    outbox_batch_size: int = _int("OUTBOX_BATCH_SIZE", 50)
    outbox_concurrency: int = _int("OUTBOX_CONCURRENCY", 20)
    outbox_idle_seconds: float = _float("OUTBOX_IDLE_SECONDS", 1.0)
    postgres_pool_min_size: int = _int("POSTGRES_POOL_MIN_SIZE", 2)
    postgres_pool_max_size: int = _int("POSTGRES_POOL_MAX_SIZE", 30)
    provider_timeout_seconds: float = _float("PROVIDER_TIMEOUT_SECONDS", 10.0)


settings = Settings()
