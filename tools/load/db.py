from __future__ import annotations

import asyncio

import asyncpg


async def reset_db(database_url: str) -> None:
    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute("TRUNCATE order_events_outbox, orders RESTART IDENTITY")
    finally:
        await conn.close()


async def status_counts(database_url: str) -> dict[str, int]:
    conn = await asyncpg.connect(database_url)
    try:
        rows = await conn.fetch(
            "SELECT status, count(*) AS count FROM orders GROUP BY status"
        )
        return {row["status"]: row["count"] for row in rows}
    finally:
        await conn.close()


async def wait_for_drain(database_url: str, expected: int, timeout: int) -> dict:
    started = asyncio.get_running_loop().time()
    while asyncio.get_running_loop().time() - started < timeout:
        counts = await status_counts(database_url)
        finished = counts.get("COMPLETED", 0) + counts.get("FAILED", 0)
        if finished >= expected:
            return {
                "drained": True,
                "seconds": round(asyncio.get_running_loop().time() - started, 2),
                "counts": counts,
            }
        await asyncio.sleep(1)
    return {
        "drained": False,
        "seconds": timeout,
        "counts": await status_counts(database_url),
    }
