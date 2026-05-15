import asyncio, json, logging, signal, traceback
from aiokafka import AIOKafkaConsumer, TopicPartition
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID, CONSUMER_NAME, MAX_PARALLEL_ORDERS, POLL_TIMEOUT_MS
from mocks.repositories import OrdersRepository, ProductRepository
from mocks.processor import OrderProcessorFactory
from mocks.models import OrderStatus, OrderDict, OrderUpdate

logger = logging.getLogger(__name__)


class OrderConsumerWorker:
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = KAFKA_GROUP_ID,
        consumer_name: str | None = None,
        max_parallel_orders: int = MAX_PARALLEL_ORDERS,
        poll_timeout_ms: int = POLL_TIMEOUT_MS,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer_name = consumer_name or CONSUMER_NAME
        self.max_parallel_orders = max_parallel_orders
        self.poll_timeout_ms = poll_timeout_ms
        self._consumer: AIOKafkaConsumer | None = None
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

    async def disconnect(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            logger.info(f"Worker {self.consumer_name} disconnected from Kafka")

    def _parse_order_from_message(self, message) -> OrderDict:
        value = message.value
        return {
            "id": value.get("id") or value.get("order_id"),
            "status": value.get("status"),
            "product_category": value.get("product_category"),
            "is_w_telegram_id": value.get("is_w_telegram_id", False),
            "metadata": value.get("metadata", {}),
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

        except Exception:
            logger.error(
                f"Error processing order {order.get('id', 'unknown')}: "
                f"{traceback.format_exc()}"
            )
            raise

    async def _process_partition_messages(
        self, tp: TopicPartition, messages: list
    ) -> None:
        for message in messages:
            if not self._running:
                break
            try:
                order = self._parse_order_from_message(message)
                await self._process_single_order(order)

                await self._consumer.commit({tp: message.offset + 1})
                logger.info(
                    f"Committed offset {message.offset + 1} for {tp}"
                )
            except Exception:
                logger.error(
                    f"Error processing message at offset {message.offset} in {tp}. "
                    f"Stopping further commits for this partition."
                )
                break

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
