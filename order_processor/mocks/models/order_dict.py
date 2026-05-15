from typing import TypedDict, Any

class OrderDict(TypedDict):
    id: str
    status: str
    metadata: dict[str, Any] | None
    product_category: str | None
    is_w_telegram_id: bool | None
