import asyncio
import json
import logging
import signal
import traceback
from typing import Optional
import redis.asyncio as redis
from redis.asyncio import Redis

from mocks.repositories import OrdersRepository, ProductRepository
from mocks.processor import OrderProcessorFactory
from mocks.models import OrderStatus, OrderDict, OrderUpdate

logger = logging.getLogger(__name__)


class OrderConsumerWorker:
    def __init__(
        self,
        redis_host: str = "redis",
        redis_port: int = 6379,
        redis_db: int = 0,
        stream_name: str = "orders_stream",
        consumer_group: str = "order_processors",
        consumer_name: str = "worker_1",
        max_parallel_orders: int = 10,
        poll_timeout_ms: int = 5000,
        block_timeout_ms: int = 2000
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.max_parallel_orders = max_parallel_orders
        self.poll_timeout_ms = poll_timeout_ms
        self.block_timeout_ms = block_timeout_ms
        self._redis: Optional[Redis] = None
        self._running = False
        self._orders_repo = OrdersRepository()
        self._product_repo = ProductRepository()
        
    async def connect(self) -> None:
        self._redis = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            decode_responses=True
        )
        await self._redis.ping()
        logger.info(
            f"Worker {self.consumer_name} connected to Redis at "
            f"{self.redis_host}:{self.redis_port}"
        )
        
    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.close()
            logger.info(f"Worker {self.consumer_name} disconnected from Redis")
    
    async def _ensure_consumer_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                name=self.stream_name,
                groupname=self.consumer_group,
                id="0",
                mkstream=True
            )
            logger.info(f"Created consumer group '{self.consumer_group}'")
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
            logger.info(f"Consumer group '{self.consumer_group}' already exists")
    
    def _parse_order_from_message(self, message: dict) -> OrderDict:
        return {
            "id": message.get("order_id"),
            "status": message.get("status"),
            "product_category": message.get("product_category"),
            "is_w_telegram_id": message.get("is_w_telegram_id", "false").lower() == "true",
            "metadata": json.loads(message.get("metadata", "{}"))
        }
    
    async def _process_single_order(self, order: OrderDict) -> None:
        try:
            logger.info(f"Processing order {order['id']}")
            
            await self._orders_repo.create_order(order)
            
            factory = OrderProcessorFactory(self._orders_repo, self._product_repo)
            processor = await factory.get_processor_for_order(order["id"])
            
            result = await processor.process_order(order["id"])
            
            order_update = OrderUpdate(status=OrderStatus.COMPLETED)
            await self._orders_repo.update_order(order["id"], order_update)
            
            logger.info(f"Successfully processed order {order['id']}: {result}")
            
        except Exception as e:
            logger.error(
                f"Error processing order {order.get('id', 'unknown')}: "
                f"{traceback.format_exc()}"
            )
    
    async def _process_messages(self, messages: list) -> None:
        semaphore = asyncio.Semaphore(self.max_parallel_orders)
        
        async def process_with_limit(message):
            async with semaphore:
                message_id, fields = message
                try:
                    order = self._parse_order_from_message(fields)
                    await self._process_single_order(order)
                    
                    await self._redis.xack(
                        self.stream_name,
                        self.consumer_group,
                        message_id
                    )
                    logger.info(f"ACK message {message_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing message {message_id}: {e}")
        
        await asyncio.gather(
            *[process_with_limit(msg) for msg in messages],
            return_exceptions=True
        )
    
    async def _read_messages(self) -> list:
        try:
            messages = await self._redis.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={self.stream_name: ">"},
                count=self.max_parallel_orders,
                block=self.block_timeout_ms
            )
            return messages
        except Exception as e:
            logger.error(f"Error reading messages: {e}")
            return []
    
    async def _process_pending_messages(self) -> None:
        try:
            pending = await self._redis.xpending_range(
                name=self.stream_name,
                groupname=self.consumer_group,
                min="-",
                max="+",
                count=self.max_parallel_orders
            )
            
            if not pending:
                return
            
            logger.info(f"Found {len(pending)} pending messages")
            
            for item in pending:
                message_id = item['message_id']
                try:
                    messages = await self._redis.xclaim(
                        name=self.stream_name,
                        groupname=self.consumer_group,
                        consumername=self.consumer_name,
                        min_idle_time=0,
                        message_ids=[message_id]
                    )
                    
                    if messages:
                        await self._process_messages(messages)
                        
                except Exception as e:
                    logger.error(f"Error claiming pending message {message_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error processing pending messages: {e}")
    
    async def run(self) -> None:
        await self.connect()
        await self._ensure_consumer_group()
        
        self._running = True
        logger.info(f"Worker {self.consumer_name} started")
        
        await self._process_pending_messages()
        
        while self._running:
            try:
                messages = await self._read_messages()
                
                if messages:
                    for stream_name, stream_messages in messages:
                        logger.info(f"Received {len(stream_messages)} messages from {stream_name}")
                        await self._process_messages(stream_messages)
                
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                logger.info(f"Worker {self.consumer_name} cancelled")
                break
            except Exception as e:
                logger.error(f"Error in worker loop: {traceback.format_exc()}")
                await asyncio.sleep(1)
    
    async def stop(self) -> None:
        logger.info(f"Stopping worker {self.consumer_name}...")
        self._running = False
        await self.disconnect()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    worker = OrderConsumerWorker(
        consumer_name="worker_1",
        max_parallel_orders=5
    )
    
    def signal_handler():
        asyncio.create_task(worker.stop())
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda s, f: signal_handler())
    
    try:
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
