from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class OrderStatus(StrEnum):
    PAID = "PAID"
    PROCESSING = "PROCESSING"
    API_PENDING = "API_PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class Order:
    id: int
    category: str
    status: OrderStatus
    payload: dict[str, Any]
    telegram_id: str | None = None
    attempts: int = 0
    idempotency_key: str | None = None
    last_error: str | None = None
    result: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ClaimedOrder:
    id: int
    category: str
    payload: dict[str, Any]
    attempts: int
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class ProviderResult:
    external_order_id: str
    provider: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "external_order_id": self.external_order_id,
            "provider": self.provider,
            "payload": self.payload,
        }
