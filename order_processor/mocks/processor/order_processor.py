import asyncio
from mocks.repositories import OrdersRepository, ProductRepository
from mocks.models import OrderStatus
from typing import Any

class OrderProcessor:
    def __init__(self, orders_repo: OrdersRepository, product_repo: ProductRepository):
        self.orders_repo = orders_repo
        self.product_repo = product_repo

    async def process_order(self, order_id: str) -> dict[str, Any]:
        await asyncio.sleep(0.5)
        return {"success": True, "order_id": order_id, "processed": True}

    async def update_order_status(self, order_id: str, status: OrderStatus) -> dict[str, Any]:
        await asyncio.sleep(0.2)
        return {"success": True, "order_id": order_id, "status": status.value}

