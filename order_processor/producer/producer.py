import asyncio, json, logging
from typing import Any
import redis.asyncio as redis
from config import REDIS_URL, REDIS_STREAM_KEY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class OrderProducer:
    def __init__(
        self,
        redis_url: str = REDIS_URL,
        stream_key: str = REDIS_STREAM_KEY,
    ):
        self.redis_url = redis_url
        self.stream_key = stream_key
        self._redis: redis.Redis | None = None

    async def connect(self) -> None:
        self._redis = redis.from_url(self.redis_url, decode_responses=True)
        await self._redis.ping()
        logger.info(f"Connected to Redis at {self.redis_url}")

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.close()
            logger.info("Disconnected from Redis")

    async def publish_order(self, order: dict[str, Any]) -> None:
        if not self._redis:
            raise RuntimeError("Redis producer not connected. Call connect() first.")

        await self._redis.xadd(self.stream_key, {"data": json.dumps(order)})
        logger.info(f"Published order {order.get('id')} to stream '{self.stream_key}'")

    async def publish_orders_batch(self, orders: list[dict[str, Any]]) -> None:
        for order in orders:
            await self.publish_order(order)
