import json
import logging
import asyncio
from typing import Dict, Any, Optional
import redis.asyncio as redis
from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class OrderProducer:
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        stream_name: str = "orders_stream",
        consumer_group: str = "order_processors"
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self._redis: Optional[Redis] = None
        
    async def connect(self) -> None:
        self._redis = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            decode_responses=True
        )
        await self._redis.ping()
        logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
        
    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.close()
            logger.info("Disconnected from Redis")
    
    async def _ensure_consumer_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                name=self.stream_name,
                groupname=self.consumer_group,
                id="0",
                mkstream=True
            )
            logger.info(f"Created consumer group '{self.consumer_group}' for stream '{self.stream_name}'")
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
            logger.info(f"Consumer group '{self.consumer_group}' already exists")
    
    async def publish_order(self, order: Dict[str, Any]) -> str:
        if not self._redis:
            raise RuntimeError("Redis connection not established. Call connect() first.")
        
        order_data = {
            "order_id": order.get("id"),
            "status": order.get("status"),
            "product_category": order.get("product_category"),
            "is_w_telegram_id": str(order.get("is_w_telegram_id", False)),
            "metadata": json.dumps(order.get("metadata", {}))
        }
        
        message_id = await self._redis.xadd(
            name=self.stream_name,
            fields=order_data
        )
        
        logger.info(f"Published order {order.get('id')} to stream with message_id={message_id}")
        return message_id
    
    async def publish_orders_batch(self, orders: list[Dict[str, Any]]) -> list[str]:
        message_ids = []
        for order in orders:
            message_id = await self.publish_order(order)
            message_ids.append(message_id)
        return message_ids
    
    async def get_stream_info(self) -> Dict[str, Any]:
        if not self._redis:
            raise RuntimeError("Redis connection not established. Call connect() first.")
        
        info = await self._redis.xinfo_stream(self.stream_name)
        return info
    
    async def get_stream_length(self) -> int:
        if not self._redis:
            raise RuntimeError("Redis connection not established. Call connect() first.")
        
        return await self._redis.xlen(self.stream_name)


async def main():
    logging.basicConfig(level=logging.INFO)
    
    producer = OrderProducer()
    await producer.connect()
    await producer._ensure_consumer_group()
    
    sample_order = {
        "id": "order_123",
        "status": "paid",
        "product_category": "gcrystals",
        "is_w_telegram_id": False,
        "metadata": {"user_id": "user_456", "amount": 100}
    }
    
    message_id = await producer.publish_order(sample_order)
    print(f"Published with message_id: {message_id}")
    
    print(f"Stream length: {await producer.get_stream_length()}")
    
    await producer.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
