from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from order_pipeline.platform.clock import iso_now, utc_now


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass(frozen=True, slots=True)
class OrderPaidEvent:
    order_id: int
    category: str
    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_type: str = "order.paid"
    schema_version: int = 1
    telegram_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    attempt: int = 1
    idempotency_key: str | None = None
    trace_id: str | None = None
    not_before: str | None = None
    created_at: str = field(default_factory=iso_now)

    @property
    def key(self) -> bytes:
        return str(self.order_id).encode("utf-8")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_bytes(self) -> bytes:
        return json.dumps(
            self.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode()

    def seconds_until_ready(self) -> float:
        ready_at = _parse_datetime(self.not_before)
        return (
            0.0
            if ready_at is None
            else max(0.0, (ready_at - utc_now()).total_seconds())
        )

    def next_retry(self, delay_seconds: float) -> "OrderPaidEvent":
        return OrderPaidEvent(
            order_id=self.order_id,
            category=self.category,
            event_type="order.retry",
            telegram_id=self.telegram_id,
            payload=self.payload,
            attempt=self.attempt + 1,
            idempotency_key=self.idempotency_key,
            trace_id=self.trace_id,
            not_before=(utc_now() + timedelta(seconds=delay_seconds)).isoformat(),
            created_at=self.created_at,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OrderPaidEvent":
        return cls(
            order_id=int(data["order_id"]),
            category=str(data["category"]),
            event_id=str(data.get("event_id") or uuid4()),
            event_type=str(data.get("event_type") or "order.paid"),
            schema_version=int(data.get("schema_version", 1)),
            telegram_id=data.get("telegram_id"),
            payload=dict(data.get("payload") or {}),
            attempt=int(data.get("attempt", 1)),
            idempotency_key=data.get("idempotency_key"),
            trace_id=data.get("trace_id"),
            not_before=data.get("not_before"),
            created_at=str(data.get("created_at") or iso_now()),
        )

    @classmethod
    def from_bytes(cls, raw: bytes) -> "OrderPaidEvent":
        return cls.from_dict(json.loads(raw.decode("utf-8")))
