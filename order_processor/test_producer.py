import asyncio, random, logging
from datetime import datetime
from producer import OrderProducer
from src.models import OrderStatus, ProductCategory
from config import REDIS_URL, REDIS_STREAM_KEY, TEST_NUM_ORDERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def generate_test_order(order_id: int) -> dict:
    statuses = [OrderStatus.API_PENDING, OrderStatus.PENDING, OrderStatus.PAID]

    return {
        "id": f"order_{order_id}",
        "status": random.choice(statuses),
        "product_category": random.choice(list(ProductCategory)),
        "is_w_telegram_id": random.choice([True, False]),
        "metadata": {
            "user_id": f"user_{random.randint(1000, 9999)}",
            "amount": random.randint(10, 1000),
            "created_at": datetime.now().isoformat(),
            "test": True,
        },
    }


async def main():
    producer = OrderProducer(
        redis_url=REDIS_URL,
        stream_key=REDIS_STREAM_KEY,
    )

    try:
        await producer.connect()

        logger.info("Starting test producer...")

        orders = []

        for i in range(1, TEST_NUM_ORDERS + 1):
            order = generate_test_order(i)
            orders.append(order)
            logger.info(f"Generated test order: {order['id']}")

        logger.info(f"Publishing {len(orders)} orders to Redis stream '{REDIS_STREAM_KEY}'...")
        await producer.publish_orders_batch(orders)

        logger.info(f"Successfully published {len(orders)} orders")

    except Exception as e:
        logger.error(f"Error in test producer: {e}")
        raise
    finally:
        await producer.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
