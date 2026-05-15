import asyncio
from mocks.repositories import OrdersRepository, ProductRepository
from .order_processor import OrderProcessor

class OrderProcessorFactory:
    def __init__(self, orders_repo: OrdersRepository, product_repo: ProductRepository):
        self.orders_repo = orders_repo
        self.product_repo = product_repo

    async def get_processor_for_order(self, order_id: str) -> OrderProcessor:
        await asyncio.sleep(0.1)
        return OrderProcessor(self.orders_repo, self.product_repo)
