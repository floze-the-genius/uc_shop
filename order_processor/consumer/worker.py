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
from db import async_session_maker
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

    def _parse_order_from_fields(self, fields: dict, message_id: str) -> tuple[Order, int]:
        data = fields.get("data")
        value = json.loads(data)
        msg_parts = message_id.split("-")
        msg_ts = int(msg_parts[0])
        msg_seq = int(msg_parts[1])
        score = msg_ts * 1000 + msg_seq
        return Order(
            id=value.get("id") or value.get("order_id"),
            status=value.get("status"),
            product_category=value.get("product_category"),
            is_w_telegram_id=value.get("is_w_telegram_id", False),
            _metadata=value.get("metadata", {}),
        ), score

    @retry(retries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS)
    async def _process_single_order(self, order: Order, message_ts: int) -> bool:
        async with async_session_maker() as session:
            try:
                success, buffered_members = await self._process_order_logic(order, message_ts, session=session)
                if success:
                    await session.commit()
                    if buffered_members:
                        key = f"order_buffer:{order.id}"
                        pipe = self._redis.pipeline()
                        for member in buffered_members:
                            pipe.zrem(key, member)
                        await pipe.execute()
                else:
                    await session.rollback()
                return success
            except Exception:
                await session.rollback()
                raise

    async def _process_order_logic(self, order: Order, message_ts: int, *, session) -> tuple[bool, list[str]]:
        existing = await self._orders_repo.get_order_by_id(order.id, session=session)

        if not existing:
            order.last_ts = message_ts
            await self._orders_repo.create_order(order, session=session)

            factory = OrderProcessorFactory(self._orders_repo, self._product_repo)
            processor = await factory.get_processor_for_order(order.id)
            await processor.process_order(order.id)

            logger.info(f"Created order {order.id} with status {order.status} and ts {message_ts}")
            return True, []

        await session.refresh(existing)

        if existing.status == order.status:
            logger.warning(f"Order {existing.id} same status transition: ({existing.status}), skipping processing")
            return True, []

        current_enum = OrderStatus(existing.status) if existing.status else None
        target_enum = OrderStatus(order.status) if order.status else None

        if OrderFSM.is_transition_allowed(current_enum, target_enum):
            last_ts = existing.last_ts or 0
            if message_ts < last_ts:
                logger.warning(
                    f"Order {order.id} received stale update {order.status} with ts {message_ts} < last_ts {existing.last_ts}. Ignoring."
                )
                return True, []

            update = OrderUpdate(status=target_enum, metadata=order._metadata, last_ts=message_ts)
            await self._orders_repo.apply_status_update(order.id, update, session=session)

            factory = OrderProcessorFactory(self._orders_repo, self._product_repo)
            processor = await factory.get_processor_for_order(order.id)
            await processor.process_order(order.id)

            logger.info(f"Order {order.id} transitioned {existing.status} -> {order.status} (ts {message_ts})")

            buffered_members = await self._drain_buffer(order.id, order.status, message_ts, session=session)
            return True, buffered_members
        else:
            last_ts = existing.last_ts or 0
            if message_ts > last_ts:
                key = f"order_buffer:{order.id}"
                member = json.dumps({"status": order.status, "metadata": order._metadata}, default=str)
                await self._redis.zadd(key, {member: message_ts})
                logger.info(f"Order {order.id} buffered future update {order.status} (ts {message_ts})")
                return True, []
            else:
                logger.warning(
                    f"Order {order.id} incorrect and stale status transition: {existing.status} -> {order.status} (ts {message_ts}). Dropping."
                )
                return True, []

    async def _drain_buffer(self, order_id: str, current_status: str, last_ts: int, *, session) -> list[str]:
        key = f"order_buffer:{order_id}"
        items = await self._redis.zrange(key, 0, -1, withscores=True)
        removed_members = []
        for member, score in items:
            data = json.loads(member)
            target_status = OrderStatus(data["status"])
            target_ts = int(score)

            if target_ts <= last_ts:
                removed_members.append(member)
                continue

            if OrderFSM.is_transition_allowed(OrderStatus(current_status), target_status):
                update = OrderUpdate(status=target_status, metadata=data.get("metadata"), last_ts=target_ts)
                await self._orders_repo.apply_status_update(order_id, update, session=session)
                removed_members.append(member)
                current_status = target_status.value
                last_ts = target_ts
                logger.info(f"Order {order_id} applied buffered transition -> {target_status.value} (ts {target_ts})")
            else:
                break
        return removed_members

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

    async def _try_process_message(self, message_id: str, fields: dict, order: Order, message_ts: int) -> bool:
        try:
            return await self._process_single_order(order, message_ts)
        except RetryExhaustedError as e:
            logger.error(f"Message exhausted all retries. Sending to DLQ.")
            try:
                await self._send_to_dlq(fields, str(e))
                return True
            except Exception as dlq_err:
                logger.error(f"Failed to send message to DLQ: {dlq_err}")
                return True
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}. Message will be retried on next poll (not acknowledged).")
            return False

    async def _process_and_ack(self, message_id: str, fields: dict) -> bool:
        order, message_ts = self._parse_order_from_fields(fields, message_id)
        lock_key = f"order_lock:{order.id}"
        lock = self._redis.lock(lock_key, timeout=60, blocking_timeout=None)
        try:
            async with lock:
                needs_ack = await self._try_process_message(message_id, fields, order, message_ts)
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
                        logger.info(f"Received {len(pending[0][1])} messages from PEL")
                        await self._process_and_ack(message_id, fields)

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
                        logger.info(f"Received {len(messages)} messages from {stream_name}")
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
