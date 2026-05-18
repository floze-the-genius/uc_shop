from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CreateOrderRequest(BaseModel):
    category: str = Field(default="GCRYSTALS", min_length=1)
    telegram_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
