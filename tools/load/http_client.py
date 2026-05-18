from __future__ import annotations

import time

import aiohttp


class ApiClient:
    def __init__(self, base_url: str, timeout: float):
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def __aenter__(self) -> "ApiClient":
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(
            timeout=self._timeout, connector=connector
        )
        return self

    async def __aexit__(self, *_args) -> None:
        await self._session.close()

    async def create_order(self, key: str, latency: float) -> tuple[int, float]:
        started = time.perf_counter()
        payload = {
            "category": "GCRYSTALS",
            "telegram_id": "load",
            "payload": {"amount": 100, "provider_latency": latency},
        }
        async with self._session.post(
            f"{self._base_url}/orders",
            json=payload,
            headers={"Idempotency-Key": key},
        ) as response:
            await response.read()
            return response.status, time.perf_counter() - started
