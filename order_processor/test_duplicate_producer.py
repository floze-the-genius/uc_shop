import asyncio, logging
from producer import OrderProducer
from config import REDIS_URL, REDIS_STREAM_KEY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    producer = OrderProducer(
        redis_url=REDIS_URL,
        stream_key=REDIS_STREAM_KEY,
    )

    try:
        await producer.connect()

        duplicate_orders = [
            {"id": "dup_order_1", "status": "pending", "product_category": "gcrystals", "is_w_telegram_id": False, "metadata": {"step": 1}},
            {"id": "dup_order_1", "status": "paid", "product_category": "gcrystals", "is_w_telegram_id": False, "metadata": {"step": 2}},
            {"id": "dup_order_1", "status": "completed", "product_category": "gcrystals", "is_w_telegram_id": False, "metadata": {"step": 3}},
            {"id": "dup_order_2", "status": "api_pending", "product_category": "coins", "is_w_telegram_id": True, "metadata": {"step": 1}},
            {"id": "dup_order_2", "status": "processing", "product_category": "coins", "is_w_telegram_id": True, "metadata": {"step": 2}},
            {"id": "dup_order_2", "status": "failed", "product_category": "coins", "is_w_telegram_id": True, "metadata": {"step": 3}},
            {"id": "dup_order_2", "status": "pending", "product_category": "coins", "is_w_telegram_id": True, "metadata": {"step": 4}},
            {"id": "dup_order_2", "status": "completed", "product_category": "coins", "is_w_telegram_id": True, "metadata": {"step": 5}},
        ]

        logger.info(f"Publishing {len(duplicate_orders)} duplicate test orders...")
        await producer.publish_orders_batch(duplicate_orders)
        logger.info("Duplicate orders published")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        await producer.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
