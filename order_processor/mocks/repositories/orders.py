import logging
from mocks.models import OrderDict, OrderUpdate, OrderStatus, ProductCategory, OrderFSM

logger = logging.getLogger(__name__)

class OrdersRepository:
    def __init__(self):
        self._orders: dict[str, OrderDict] = {}

    async def get_order_by_id(self, order_id: str) -> OrderDict | None:
        return self._orders.get(order_id)

    async def update_order(self, order_id: str, order_data: OrderUpdate) -> bool:
        if order_id not in self._orders:
            return False

        order = self._orders[order_id]
        current_status = order.get("status")
        if order_data.status and current_status != order_data.status:
            target_status = order_data.status.value

            if not OrderFSM.is_transition_allowed(current_status, target_status):
                logger.warning(
                    f"Order status transition rejected for order {order_id}: "
                    f"{current_status if current_status else 'None'} -> {target_status}"
                )
                return False

            order["status"] = target_status
            logger.info(f"Updated order {order_id} status from {current_status} to {target_status}")

        if order_data.metadata is not None:
            order["metadata"] = order_data.metadata
        return True

    async def get_orders_by_statuses(
        self,
        statuses: list[OrderStatus],
        include_only: list[ProductCategory] | None = None,
        is_w_telegram_id: bool | None = None,
        limit: int = 20
    ) -> list[OrderDict]:
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
        if order.get("id") in self._orders:
            return False
        self._orders[order["id"]] = order
        return True

    async def get_or_create(self, order: OrderDict) -> bool:
        result = await self.get_order_by_id(order.get("id"))
        if result:
            return result
        
        return await self.create_order(order)
