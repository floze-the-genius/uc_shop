import asyncio, random
from mocks.repositories import OrdersRepository, ProductRepository
from .order_processor import OrderProcessor
from .failing_order_processor import FailingOrderProcessor

class OrderProcessorFactory:
    def __init__(self, orders_repo: OrdersRepository, product_repo: ProductRepository):
        self.orders_repo = orders_repo
        self.product_repo = product_repo

    async def get_processor_for_order(self, order_id: str) -> OrderProcessor | FailingOrderProcessor:
        await asyncio.sleep(0.1)
        processor = random.choice([OrderProcessor, FailingOrderProcessor])
        return processor(self.orders_repo, self.product_repo)
