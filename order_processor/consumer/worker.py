import asyncio, json, logging, signal, traceback
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_DLQ_TOPIC,
    CONSUMER_NAME,
    MAX_PARALLEL_ORDERS,
    POLL_TIMEOUT_MS,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
)
from mocks.repositories import OrdersRepository, ProductRepository
from mocks.processor import OrderProcessorFactory
from mocks.models import OrderStatus, OrderDict, OrderUpdate
from .wrapper import retry, dlq_safe

logger = logging.getLogger(__name__)


class OrderConsumerWorker:
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = KAFKA_GROUP_ID,
        dlq_topic: str = KAFKA_DLQ_TOPIC,
        consumer_name: str | None = None,
        max_parallel_orders: int = MAX_PARALLEL_ORDERS,
        poll_timeout_ms: int = POLL_TIMEOUT_MS,
        max_retries: int = MAX_RETRIES,
        retry_delay: int = RETRY_DELAY_SECONDS,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.dlq_topic = dlq_topic
        self.consumer_name = consumer_name or CONSUMER_NAME
        self.max_parallel_orders = max_parallel_orders
        self.poll_timeout_ms = poll_timeout_ms
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self._running = False
        self._orders_repo = OrdersRepository()
        self._product_repo = ProductRepository()

    async def connect(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=self.max_parallel_orders,
            client_id=self.consumer_name,
        )
        await self._consumer.start()
        logger.info(
            f"Worker {self.consumer_name} connected to Kafka at {self.bootstrap_servers} "
            f"(topic={self.topic}, group_id={self.group_id})"
        )

        self._dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await self._dlq_producer.start()
        logger.info(
            f"Worker {self.consumer_name} DLQ producer connected (dlq_topic={self.dlq_topic})"
        )

    async def disconnect(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            logger.info(f"Worker {self.consumer_name} consumer disconnected from Kafka")
        if self._dlq_producer:
            await self._dlq_producer.stop()
            logger.info(f"Worker {self.consumer_name} DLQ producer disconnected")

    def _parse_order_from_message(self, message) -> OrderDict:
        value = message.value
        return {
            "id": value.get("id") or value.get("order_id"),
            "status": value.get("status"),
            "product_category": value.get("product_category"),
            "is_w_telegram_id": value.get("is_w_telegram_id", False),
            "metadata": value.get("metadata", {}),
        }

    @retry(retries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS)
    async def _process_single_order(self, order: OrderDict) -> None:
        logger.info(f"Processing order {order['id']}")

        await self._orders_repo.get_or_create(order)

        factory = OrderProcessorFactory(self._orders_repo, self._product_repo)
        processor = await factory.get_processor_for_order(order["id"])

        result = await processor.process_order(order["id"])

        order_update = OrderUpdate(status=OrderStatus.COMPLETED)
        await self._orders_repo.update_order(order["id"], order_update)

        logger.info(f"Successfully processed order {order['id']}: {result}")

    async def _send_to_dlq(self, message, error_reason: str) -> None:
        dlq_payload = {
            "original_message": message.value,
            "error_reason": error_reason,
            "consumer_name": self.consumer_name,
            "retry_count": self.max_retries,
            "failed_at": asyncio.get_event_loop().time(),
        }
        await self._dlq_producer.send(
            self.dlq_topic,
            key=message.key,
            value=dlq_payload,
        )
        logger.warning(
            f"Sent message for order {message.value.get('id') or message.value.get('order_id')} "
            f"to DLQ topic '{self.dlq_topic}': {error_reason}"
        )

    @dlq_safe
    async def _try_process_message(self, message, tp: TopicPartition) -> None:
        order = self._parse_order_from_message(message)
        await self._process_single_order(order)

    async def _process_partition_messages(self, tp: TopicPartition, messages: list) -> None:
        for message in messages:
            if not self._running:
                break

            needs_commit = await self._try_process_message(message, tp)
            if needs_commit:
                await self._consumer.commit({tp: message.offset + 1})
                logger.info(f"Committed offset {message.offset + 1} for {tp}")

    async def run(self) -> None:
        await self.connect()

        self._running = True
        logger.info(f"Worker {self.consumer_name} started")

        while self._running:
            try:
                result = await self._consumer.getmany(
                    timeout_ms=self.poll_timeout_ms,
                    max_records=self.max_parallel_orders,
                )

                if not result:
                    continue

                tasks = []
                for tp, messages in result.items():
                    if messages:
                        logger.info(
                            f"Received {len(messages)} messages from {tp}"
                        )
                        tasks.append(
                            self._process_partition_messages(tp, messages)
                        )

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

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
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        consumer_name=CONSUMER_NAME,
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
