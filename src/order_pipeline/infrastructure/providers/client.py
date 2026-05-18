from __future__ import annotations

import asyncio

from order_pipeline.domain.errors import PermanentProviderError, TransientProviderError
from order_pipeline.domain.order import ClaimedOrder, ProviderResult


class OrderProviderClient:
    def __init__(self, timeout_seconds: float):
        self._timeout_seconds = timeout_seconds

    async def fulfill(self, order: ClaimedOrder) -> ProviderResult:
        return await asyncio.wait_for(
            self._fulfill(order), timeout=self._timeout_seconds
        )

    async def _fulfill(self, order: ClaimedOrder) -> ProviderResult:
        if order.payload.get("force_permanent_fail"):
            raise PermanentProviderError("Provider rejected order permanently")
        if order.attempts <= int(order.payload.get("fail_until_attempt", 0)):
            raise TransientProviderError("Provider is temporarily unavailable")
        if order.payload.get("force_fail"):
            raise TransientProviderError("Provider is temporarily unavailable")
        await asyncio.sleep(float(order.payload.get("provider_latency", 0.015)))
        return ProviderResult(
            external_order_id=f"ext-{order.id}",
            provider="order-provider",
            payload={"category": order.category},
        )
