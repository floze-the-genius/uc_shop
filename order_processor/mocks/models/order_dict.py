from typing import TypedDict, Any
from .order_status import OrderStatus

class OrderDict(TypedDict):
    id: str
    status: OrderStatus
    metadata: dict[str, Any] | None
    product_category: str | None
    is_w_telegram_id: bool | None
