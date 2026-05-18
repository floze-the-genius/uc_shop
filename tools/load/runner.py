from __future__ import annotations

import asyncio
import time
from uuid import uuid4

from tools.load.http_client import ApiClient
from tools.load.stats import Stats


async def run_load(
    base_url: str,
    rps: int,
    duration: int,
    concurrency: int,
    timeout: float,
    provider_latency: float,
    prefix: str,
) -> dict:
    stats = Stats()
    queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=concurrency * 4)

    async with ApiClient(base_url, timeout) as client:
        started = time.perf_counter()
        workers = [
            asyncio.create_task(_worker(queue, client, stats, provider_latency))
            for _ in range(concurrency)
        ]
        produced = await _produce(queue, rps, duration, prefix)
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)
        wall_seconds = time.perf_counter() - started

    result = stats.summary()
    result["target_rps"] = rps
    result["duration_s"] = duration
    result["produced"] = produced
    result["actual_submit_rps"] = round(produced / duration, 2)
    result["wall_seconds"] = round(wall_seconds, 2)
    result["completed_rps"] = (
        round(result["requests"] / wall_seconds, 2) if wall_seconds else 0
    )
    return result


async def _produce(queue: asyncio.Queue, rps: int, duration: int, prefix: str) -> int:
    started = time.perf_counter()
    target = rps * duration
    produced = 0
    while produced < target:
        elapsed = min(time.perf_counter() - started, duration)
        allowed = min(target, max(produced + 1, int(elapsed * rps)))
        while produced < allowed:
            await queue.put(f"{prefix}-{produced}-{uuid4()}")
            produced += 1
        if time.perf_counter() - started >= duration:
            break
        await asyncio.sleep(0.01)
    return produced


async def _worker(
    queue: asyncio.Queue, client: ApiClient, stats: Stats, latency: float
) -> None:
    while True:
        key = await queue.get()
        if key is None:
            queue.task_done()
            return
        try:
            status, elapsed = await client.create_order(key, latency)
        except Exception:
            status, elapsed = 599, 0
        stats.add(status, elapsed)
        queue.task_done()
