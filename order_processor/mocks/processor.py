import asyncio
from typing import Dict, Any, Optional
from .models import OrderStatus
from .repositories import OrdersRepository, ProductRepository


class OrderProcessorUtils:
    @staticmethod
    def sanitize_for_json(data: Any) -> Any:
        if isinstance(data, dict):
            return {k: OrderProcessorUtils.sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [OrderProcessorUtils.sanitize_for_json(item) for item in data]
        elif isinstance(data, (str, int, float, bool, type(None))):
            return data
        else:
            return str(data)


class OrderProcessor:
    def __init__(self, orders_repo: OrdersRepository, product_repo: ProductRepository):
        self.orders_repo = orders_repo
        self.product_repo = product_repo

    async def process_order(self, order_id: str) -> Dict[str, Any]:
        await asyncio.sleep(0.5)
        return {"success": True, "order_id": order_id, "processed": True}

    async def update_order_status(self, order_id: str, status: OrderStatus) -> Dict[str, Any]:
        await asyncio.sleep(0.2)
        return {"success": True, "order_id": order_id, "status": status.value}


class OrderProcessorFactory:
    def __init__(self, orders_repo: OrdersRepository, product_repo: ProductRepository):
        self.orders_repo = orders_repo
        self.product_repo = product_repo

    async def get_processor_for_order(self, order_id: str) -> OrderProcessor:
        await asyncio.sleep(0.1)
        return OrderProcessor(self.orders_repo, self.product_repo)
