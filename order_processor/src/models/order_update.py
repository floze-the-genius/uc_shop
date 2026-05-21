from dataclasses import dataclass
from .order_status import OrderStatus
from typing import Any

@dataclass
class OrderUpdate:
    status: OrderStatus | None = None
    metadata: dict[str, Any] | None = None
    last_ts: int | None = None
