from __future__ import annotations

import argparse
import asyncio
import json

from tools.load.db import reset_db, wait_for_drain
from tools.load.runner import run_load


async def main() -> None:
    args = parse_args()
    if args.reset:
        await reset_db(args.database_url)
    result = await run_load(
        base_url=args.base_url,
        rps=args.rps,
        duration=args.duration,
        concurrency=args.concurrency,
        timeout=args.timeout,
        provider_latency=args.provider_latency,
        prefix=args.prefix,
    )
    result["drain"] = await wait_for_drain(
        args.database_url, result["ok"], args.drain_timeout
    )
    print(json.dumps(result, indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8001")
    parser.add_argument(
        "--database-url", default="postgresql://uc_shop:uc_shop@localhost:5432/uc_shop"
    )
    parser.add_argument("--rps", type=int, required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--provider-latency", type=float, default=0.015)
    parser.add_argument("--prefix", default="load")
    parser.add_argument("--drain-timeout", type=int, default=120)
    parser.add_argument("--reset", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
