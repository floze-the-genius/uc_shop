import logging

from sqlalchemy import select

from src.models.orders import Order
from src.models import OrderUpdate, OrderStatus, OrderFSM

logger = logging.getLogger(__name__)


class OrdersRepository:
    def __init__(self, session_maker):
        self._session_maker = session_maker

    async def get_order_by_id(self, order_id: str, *, session) -> Order | None:
        result = await session.execute(select(Order).where(Order.id == order_id))
        return result.scalar_one_or_none()

    async def update_order(self, order_id: str, order_data: OrderUpdate, *, session) -> bool:
        result = await session.execute(select(Order).where(Order.id == order_id))
        row = result.scalar_one_or_none()
        if not row:
            return False

        current_status = row.status
        if order_data.status and current_status != order_data.status.value:
            target_status = order_data.status.value
            current_enum = OrderStatus(current_status) if current_status else None

            if not OrderFSM.is_transition_allowed(current_enum, order_data.status):
                logger.warning(
                    f"Order status transition rejected for order {order_id}: "
                    f"{current_status if current_status else 'None'} -> {target_status}"
                )
                return False

            row.status = target_status
            if order_data.metadata is not None:
                row._metadata = order_data.metadata

            logger.info(
                f"Updated order {order_id} status from {current_status} to {target_status}"
            )

        if order_data.metadata is not None:
            row._metadata = order_data.metadata

        return True

    async def apply_status_update(self, order_id: str, order_data: OrderUpdate, *, session) -> Order | None:
        result = await session.execute(select(Order).where(Order.id == order_id))
        row = result.scalar_one_or_none()
        if not row:
            return None
        if order_data.status:
            row.status = order_data.status.value
        if order_data.last_ts is not None:
            row.last_ts = order_data.last_ts
        if order_data.metadata is not None:
            row._metadata = order_data.metadata
        return row

    async def create_order(self, order: Order, *, session) -> Order:
        session.add(order)
        return order
