from mocks.repositories import OrdersRepository

class OrdersController:
    def __init__(self, orders_repository: OrdersRepository):
        self.orders_repo = orders_repository

    async def create_order(self, order_data: dict) -> dict:
        return {"id": "mock_order_id", "status": "created"}

    async def get_order(self, order_id: str) -> dict:
        order = await self.orders_repo.get_order_by_id(order_id)
        return order or {}
