import asyncio, json, logging, signal, socket, traceback
import redis.asyncio as redis
from redis.exceptions import ResponseError, LockError
from config import (
    REDIS_URL,
    REDIS_STREAM_KEY,
    REDIS_GROUP_NAME,
    REDIS_DLQ_STREAM_KEY,
    CONSUMER_NAME,
    MAX_PARALLEL_ORDERS,
    POLL_TIMEOUT_MS,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
)
from db import async_session_maker, transactional
from src.repositories import OrdersRepository, ProductRepository
from src.processor import OrderProcessorFactory
from src.models import OrderStatus, OrderUpdate, OrderFSM
from src.models.orders import Order
from .wrapper import retry, RetryExhaustedError

logger = logging.getLogger(__name__)


class OrderConsumerWorker:
    def __init__(
        self,
        redis_url: str = REDIS_URL,
        stream_key: str = REDIS_STREAM_KEY,
        group_name: str = REDIS_GROUP_NAME,
        dlq_stream_key: str = REDIS_DLQ_STREAM_KEY,
        consumer_name: str | None = None,
        max_parallel_orders: int = MAX_PARALLEL_ORDERS,
        poll_timeout_ms: int = POLL_TIMEOUT_MS,
        max_retries: int = MAX_RETRIES,
        retry_delay: int = RETRY_DELAY_SECONDS,
    ):
        self.redis_url = redis_url
        self.stream_key = stream_key
        self.group_name = group_name
        self.dlq_stream_key = dlq_stream_key
        self.consumer_name = consumer_name or f"{CONSUMER_NAME}_{socket.gethostname()}"
        self.max_parallel_orders = max_parallel_orders
        self.poll_timeout_ms = poll_timeout_ms
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._redis: redis.Redis | None = None
        self._running = False
        self._orders_repo: OrdersRepository | None = None
        self._product_repo: ProductRepository | None = None

    async def connect(self) -> None:
        self._redis = redis.from_url(self.redis_url, decode_responses=True)
        await self._redis.ping()
        try:
            await self._redis.xgroup_create(self.stream_key, self.group_name, id="0", mkstream=True)
            logger.info(f"Created consumer group '{self.group_name}' for stream '{self.stream_key}'")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{self.group_name}' already exists")
            else:
                raise

        self._orders_repo = OrdersRepository(async_session_maker)
        self._product_repo = ProductRepository(async_session_maker)

        logger.info(
            f"Worker {self.consumer_name} connected to Redis at {self.redis_url} "
            f"(stream={self.stream_key}, group={self.group_name})"
        )

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.close()
            logger.info(f"Worker {self.consumer_name} disconnected from Redis")

    def _parse_order_from_fields(self, fields: dict) -> Order:
        data = fields.get("data")
        value = json.loads(data)
        return Order(
            id=value.get("id") or value.get("order_id"),
            status=value.get("status"),
            product_category=value.get("product_category"),
            is_w_telegram_id=value.get("is_w_telegram_id", False),
            _metadata=value.get("metadata", {}),
        )

    @retry(retries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS)
    @transactional(async_session_maker)
    async def _process_single_order(self, order: Order, *, session) -> bool:
        logger.info(f"Processing order {order.id}")

        existing = await self._orders_repo.get_order_by_id(order.id, session=session)
        if existing:
            await session.refresh(existing)
            if existing.status == order.status or not OrderFSM.is_transition_allowed(existing.status, order.status):
                logger.warning(f"Order {existing.id} incorrect status transition: {existing.status} -> {order.status}")
                return False

        if not existing:
            await self._orders_repo.create_order(order, session=session)

        factory = OrderProcessorFactory(self._orders_repo, self._product_repo)
        processor = await factory.get_processor_for_order(order.id)
        result = await processor.process_order(order.id)

        if existing:
            order_update = OrderUpdate(status=OrderStatus(order.status))
            updated = await self._orders_repo.update_order(order.id, order_update, session=session)
            if not updated:
                logger.warning(f"Could not update order {order.id}")
                return False

        logger.info(f"Successfully processed order {order.id}: {result}")
        return True

    async def _send_to_dlq(self, fields: dict, error_reason: str) -> None:
        data = fields.get("data")
        original_message = json.loads(data) if data else None
        dlq_payload = {
            "original_message": original_message,
            "error_reason": error_reason,
            "consumer_name": self.consumer_name,
            "retry_count": self.max_retries,
            "failed_at": asyncio.get_event_loop().time(),
        }
        await self._redis.xadd(self.dlq_stream_key, {"data": json.dumps(dlq_payload)})
        logger.warning(
            f"Sent message to DLQ stream '{self.dlq_stream_key}': {error_reason}"
        )

    async def _try_process_message(self, message_id: str, fields: dict, order: Order) -> bool:
        try:
            return await self._process_single_order(order)
        except RetryExhaustedError as e:
            logger.error(f"Message exhausted all retries. Sending to DLQ.")
            try:
                await self._send_to_dlq(fields, str(e))
                return True
            except Exception as dlq_err:
                logger.error(
                    f"Failed to send message to DLQ: {dlq_err}. Message will not be acknowledged to avoid data loss."
                )
                return False
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}. Message will be retried on next poll (not acknowledged).")
            return False

    async def _process_and_ack(self, message_id: str, fields: dict) -> bool:
        order = self._parse_order_from_fields(fields)
        lock_key = f"order_lock:{order.id}"
        lock = self._redis.lock(lock_key, timeout=60, blocking_timeout=None)
        try:
            async with lock:
                needs_ack = await self._try_process_message(message_id, fields, order)
                if needs_ack:
                    await self._redis.xack(self.stream_key, self.group_name, message_id)
                    logger.info(f"Acknowledged message {message_id} on {self.stream_key}")
                    return True
                return False
        except LockError:
            logger.warning(f"Could not acquire lock for order {order.id}, message will be retried on next poll")
            return False

    async def run(self) -> None:
        await self.connect()

        self._running = True
        logger.info(f"Worker {self.consumer_name} started")

        while self._running:
            try:
                pending = await self._redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_key: "0"},
                    count=self.max_parallel_orders,
                )
                if pending and pending[0][1]:
                    for message_id, fields in pending[0][1]:
                        acked = await self._process_and_ack(message_id, fields)
                        if not acked:
                            await asyncio.sleep(1)
                    continue

                result = await self._redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_key: ">"},
                    count=self.max_parallel_orders,
                    block=self.poll_timeout_ms,
                )

                if not result:
                    continue

                for stream_name, messages in result:
                    if messages:
                        logger.info(
                            f"Received {len(messages)} messages from {stream_name}"
                        )
                        for message_id, fields in messages:
                            await self._process_and_ack(message_id, fields)

            except asyncio.CancelledError:
                logger.info(f"Worker {self.consumer_name} cancelled")
                break
            except Exception:
                logger.error(f"Error in worker loop: {traceback.format_exc()}")
                await asyncio.sleep(1)

    async def stop(self) -> None:
        logger.info(f"Stopping worker {self.consumer_name}...")
        self._running = False
        await self.disconnect()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    worker = OrderConsumerWorker(
        redis_url=REDIS_URL,
        max_parallel_orders=MAX_PARALLEL_ORDERS,
        max_retries=MAX_RETRIES,
        retry_delay=RETRY_DELAY_SECONDS,
    )

    loop = asyncio.get_running_loop()

    def signal_handler():
        asyncio.create_task(worker.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
