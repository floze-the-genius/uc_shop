from __future__ import annotations

import asyncpg

from order_pipeline.infrastructure.postgres.claim_order import ClaimOrderMixin
from order_pipeline.infrastructure.postgres.create_order import CreateOrderMixin
from order_pipeline.infrastructure.postgres.finish_order import FinishOrderMixin
from order_pipeline.infrastructure.postgres.outbox import OutboxMixin
from order_pipeline.infrastructure.postgres.read_orders import ReadOrdersMixin


class PostgresStore(
    CreateOrderMixin,
    ReadOrdersMixin,
    ClaimOrderMixin,
    FinishOrderMixin,
    OutboxMixin,
):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @classmethod
    async def connect(
        cls, database_url: str, min_size: int, max_size: int
    ) -> "PostgresStore":
        return cls(
            await asyncpg.create_pool(
                dsn=database_url, min_size=min_size, max_size=max_size
            )
        )

    async def close(self) -> None:
        await self._pool.close()
