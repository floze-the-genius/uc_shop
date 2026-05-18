from __future__ import annotations

from typing import Protocol

from order_pipeline.domain.order import ClaimedOrder, ProviderResult


class ProviderClient(Protocol):
    async def fulfill(self, order: ClaimedOrder) -> ProviderResult: ...
