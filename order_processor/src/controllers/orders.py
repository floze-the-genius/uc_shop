from src.repositories import OrdersRepository
from src.models.orders import Order
from sqlalchemy.exc import IntegrityError
from db import transactional, async_session_maker


class OrdersController:
    def __init__(self, orders_repository: OrdersRepository):
        self.orders_repo = orders_repository

    @transactional(async_session_maker)
    async def create_order(self, order_data: dict) -> dict:
        order = Order(
            id=order_data.get("id") or order_data.get("order_id"),
            status=order_data.get("status"),
            _metadata=order_data.get("metadata"),
            product_category=order_data.get("product_category"),
            is_w_telegram_id=order_data.get("is_w_telegram_id"),
        )
        try:
            created = await self.orders_repo.create_order(order)
            return {"id": created.id, "status": created.status}
        except IntegrityError:
            return {"error": "Order already exists"}

    @transactional(async_session_maker)
    async def get_order(self, order_id: str) -> dict:
        order = await self.orders_repo.get_order_by_id(order_id)
        if order is None:
            return {}
        return {
            "id": order.id,
            "status": order.status,
            "metadata": order._metadata,
            "product_category": order.product_category,
            "is_w_telegram_id": order.is_w_telegram_id,
        }
