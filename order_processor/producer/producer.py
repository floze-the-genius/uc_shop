import asyncio, json, logging
from typing import Any
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from mocks.config import BOOTSTRAP_SERVER

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OrderProducer:
    def __init__(
        self,
        bootstrap_server: str = "localhost:9092",
        topic: str = "orders",
    ):
        self.bootstrap_servers = bootstrap_server
        self.topic = topic
        self._producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await self._producer.start()
        logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
        await self._ensure_topic()

    async def disconnect(self) -> None:
        if self._producer:
            await self._producer.stop()
            logger.info("Disconnected from Kafka")

    async def _ensure_topic(self) -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            topics = await admin.list_topics()
            if self.topic not in set(topics):
                await admin.create_topics([
                    NewTopic(
                        name=self.topic,
                        num_partitions=3,
                        replication_factor=1,
                    )
                ])
                logger.info(f"Created topic '{self.topic}'")
            else:
                logger.info(f"Topic '{self.topic}' already exists")
        except Exception as e:
            logger.warning(f"Could not ensure topic exists: {e}")
        finally:
            await admin.close()

    async def publish_order(self, order: dict[str, Any]) -> None:
        if not self._producer:
            raise RuntimeError("Kafka producer not connected. Call connect() first.")

        await self._producer.send(
            self.topic,
            key=order.get("id"),
            value=order,
        )
        logger.info(f"Published order {order.get('id')} to topic '{self.topic}'")

    async def publish_orders_batch(self, orders: list[dict[str, Any]]) -> None:
        for order in orders:
            await self.publish_order(order)

    async def get_topic_info(self) -> dict[str, Any]:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            topics = await admin.list_topics()
            return {
                "topic": self.topic,
                "exists": self.topic in set(topics),
                "all_topics": list(topics),
            }
        finally:
            await admin.close()

    async def get_topic_partitions_count(self) -> int:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            topics = await admin.describe_topics([self.topic])
            if topics:
                return len(topics[0]["partitions"])
            return 0
        except Exception:
            return 0
        finally:
            await admin.close()


async def main():
    producer = OrderProducer(bootstrap_server=BOOTSTRAP_SERVER)
    await producer.connect()

    sample_order = {
        "id": "order_123",
        "status": "paid",
        "product_category": "gcrystals",
        "is_w_telegram_id": False,
        "metadata": {"user_id": "user_456", "amount": 100},
    }

    await producer.publish_order(sample_order)
    logger.info("Published sample order")

    info = await producer.get_topic_info()
    logger.info(f"Topic info: {info}")

    partitions = await producer.get_topic_partitions_count()
    logger.info(f"Topic partitions: {partitions}")

    await producer.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
