from enum import Enum
from typing import TypedDict, Optional, Dict, Any
from dataclasses import dataclass


class OrderStatus(str, Enum):
    PENDING = "pending"
    PAID = "paid"
    PROCESSING = "processing"
    API_PENDING = "api_pending"
    COMPLETED = "completed"
    FAILED = "failed"


class ProductCategory(str, Enum):
    GCRYSTALS = "gcrystals"
    COINS = "coins"
    PREMIUM = "premium"


@dataclass
class OrderUpdate:
    status: Optional[OrderStatus] = None
    metadata: Optional[Dict[str, Any]] = None


class OrderDict(TypedDict):
    id: str
    status: str
    metadata: Optional[Dict[str, Any]]
    product_category: Optional[str]
    is_w_telegram_id: Optional[bool]
