import logging
from typing import Any

from sqlalchemy import select

from db import with_session
from src.models.orders import Order
from src.models import OrderUpdate, OrderStatus, OrderFSM

logger = logging.getLogger(__name__)


class OrdersRepository:
    def __init__(self, session_maker):
        self._session_maker = session_maker

    @with_session()
    async def get_order_by_id(self, session, order_id: str) -> Order | None:
        result = await session.execute(select(Order).where(Order.id == order_id))
        return result.scalar_one_or_none()

    @with_session()
    async def update_order(self, session, order_id: str, order_data: OrderUpdate) -> bool:
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

    @with_session()
    async def get_orders_by_statuses(
        self,
        session,
        statuses: list[OrderStatus],
        include_only: list[Any] | None = None,
        is_w_telegram_id: bool | None = None,
        limit: int = 20,
    ) -> list[Order]:
        status_values = [s.value for s in statuses]
        query = select(Order).where(Order.status.in_(status_values))
        if include_only:
            query = query.where(Order.product_category.in_(include_only))
        if is_w_telegram_id is not None:
            query = query.where(Order.is_w_telegram_id == is_w_telegram_id)
        query = query.limit(limit)
        result = await session.execute(query)
        return list(result.scalars().all())

    @with_session()
    async def create_order(self, session, order: Order) -> Order:
        existing = await self.get_order_by_id(order.id)
        if existing:
            return
        
        session.add(order)
        return order

    async def get_or_create(self, order: Order) -> Order:
        result = await self.get_order_by_id(order.id)
        if result:
            return result
        return await self.create_order(order)
