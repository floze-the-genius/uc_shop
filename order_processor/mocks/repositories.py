import asyncio
from typing import List, Optional, Dict, Any
from .models import OrderStatus, OrderUpdate, OrderDict, ProductCategory


class OrdersRepository:
    def __init__(self):
        self._orders: Dict[str, OrderDict] = {}
        self._lock = asyncio.Lock()

    async def get_order_by_id(self, order_id: str) -> Optional[OrderDict]:
        async with self._lock:
            return self._orders.get(order_id)

    async def update_order(self, order_id: str, order_data: OrderUpdate) -> bool:
        async with self._lock:
            if order_id not in self._orders:
                return False
            
            order = self._orders[order_id]
            if order_data.status:
                order["status"] = order_data.status.value
            if order_data.metadata is not None:
                order["metadata"] = order_data.metadata
            return True

    async def get_orders_by_statuses(
        self,
        statuses: List[OrderStatus],
        include_only: Optional[List[ProductCategory]] = None,
        is_w_telegram_id: Optional[bool] = None,
        limit: int = 20
    ) -> List[OrderDict]:
        async with self._lock:
            result = []
            status_values = [s.value for s in statuses]
            
            for order in self._orders.values():
                if order["status"] not in status_values:
                    continue
                
                if include_only:
                    categories = [c.value for c in include_only]
                    if order.get("product_category") not in categories:
                        continue
                
                if is_w_telegram_id is not None:
                    if order.get("is_w_telegram_id") != is_w_telegram_id:
                        continue
                
                result.append(order)
                if len(result) >= limit:
                    break
            
            return result

    async def create_order(self, order: OrderDict) -> bool:
        async with self._lock:
            if order["id"] in self._orders:
                return False
            self._orders[order["id"]] = order
            return True


class ProductRepository:
    def __init__(self):
        self._products: Dict[str, Dict[str, Any]] = {}

    async def get_product(self, product_id: str) -> Optional[Dict[str, Any]]:
        return self._products.get(product_id)

    async def get_products_by_category(self, category: ProductCategory) -> List[Dict[str, Any]]:
        return [p for p in self._products.values() if p.get("category") == category.value]
