import asyncio
import random
import logging
from datetime import datetime
from producer import OrderProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_test_order(order_id: int) -> dict:
    categories = ["gcrystals", "coins", "premium"]
    statuses = ["paid", "processing"]
    
    return {
        "id": f"order_{order_id}",
        "status": random.choice(statuses),
        "product_category": random.choice(categories),
        "is_w_telegram_id": random.choice([True, False]),
        "metadata": {
            "user_id": f"user_{random.randint(1000, 9999)}",
            "amount": random.randint(10, 1000),
            "created_at": datetime.now().isoformat(),
            "test": True
        }
    }


async def main():
    producer = OrderProducer(
        redis_host="redis",
        redis_port=6379,
        stream_name="orders_stream"
    )
    
    try:
        await producer.connect()
        await producer._ensure_consumer_group()
        
        logger.info("Starting test producer...")
        
        num_orders = 20
        orders = []
        
        for i in range(1, num_orders + 1):
            order = generate_test_order(i)
            orders.append(order)
            logger.info(f"Generated test order: {order['id']}")
        
        logger.info(f"Publishing {len(orders)} orders to Redis Stream...")
        message_ids = await producer.publish_orders_batch(orders)
        
        logger.info(f"Successfully published {len(message_ids)} orders")
        logger.info(f"Stream length: {await producer.get_stream_length()}")
        
        stream_info = await producer.get_stream_info()
        logger.info(f"Stream info: {stream_info}")
        
    except Exception as e:
        logger.error(f"Error in test producer: {e}")
        raise
    finally:
        await producer.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
