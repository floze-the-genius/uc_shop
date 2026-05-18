#!/usr/bin/env python3

from __future__ import annotations

import argparse, json, subprocess, sys, time, urllib.request
from datetime import datetime

C = {"r": "\033[31m", "g": "\033[32m", "y": "\033[33m", "c": "\033[36m",
     "w": "\033[37m", "bold": "\033[1m", "reset": "\033[0m", "clear": "\033[2J\033[H"}


def run(cmd: list[str], timeout: float = 4) -> tuple[str, bool]:
    try:
        p = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
        return p.stdout.strip(), p.returncode == 0
    except Exception as exc:
        return str(exc), False


def api(base_url: str) -> tuple[str, str]:
    try:
        with urllib.request.urlopen(f"{base_url.rstrip('/')}/health", timeout=2) as r:
            return "OK" if r.status == 200 else str(r.status), "g"
    except Exception:
        return "DOWN", "r"


def compose() -> tuple[str, str]:
    out, ok = run(["docker", "compose", "ps", "--format", "json"])
    if not ok or not out:
        return "docker compose недоступен", "r"
    rows = [json.loads(line) for line in out.splitlines() if line.strip()]
    running = sum(row.get("State") == "running" for row in rows)
    healthy = sum(row.get("Health") in ("healthy", "") for row in rows)
    color = "g" if running == len(rows) and healthy == len(rows) else "y"
    return f"{running}/{len(rows)} running, {healthy}/{len(rows)} healthy", color


def db() -> tuple[list[str], str]:
    sql = (
        "SELECT 'orders:'||COALESCE(string_agg(status||'='||cnt, ', ' ORDER BY status),'none') "
        "FROM (SELECT status, count(*) cnt FROM orders GROUP BY status) s;"
        "SELECT 'outbox:'||count(*) FILTER (WHERE published_at IS NULL)||' pending / '||count(*)||' total' "
        "FROM order_events_outbox;"
        "SELECT 'latest:'||COALESCE(string_agg(id||':'||status, ', '), 'none') "
        "FROM (SELECT id,status FROM orders ORDER BY id DESC LIMIT 3) s;"
    )
    cmd = ["docker", "exec", "uc_shop-postgres-1", "psql", "-U", "uc_shop", "-d", "uc_shop", "-Atc", sql]
    out, ok = run(cmd)
    return out.splitlines() if ok else [out], "g" if ok else "r"


def kafka_lag() -> tuple[str, str]:
    cmd = ["docker", "exec", "uc_shop-kafka-1-1", "/opt/kafka/bin/kafka-consumer-groups.sh",
           "--bootstrap-server", "kafka-1:19092", "--describe", "--group", "uc-shop-order-processors"]
    out, ok = run(cmd, timeout=8)
    if not ok:
        return out.splitlines()[-1] if out else "no data", "r"
    lag = sum(int(p[5]) for p in (line.split() for line in out.splitlines()[1:]) if len(p) > 5 and p[5].isdigit())
    return str(lag), "g" if lag == 0 else "y"


def card(title: str, value: str, color: str = "w") -> str:
    return f"{C['bold']}{title:<16}{C['reset']} {C[color]}{value}{C['reset']}"


def draw(args: argparse.Namespace) -> None:
    api_status, api_color = api(args.api_url)
    compose_status, compose_color = compose()
    lag, lag_color = kafka_lag()
    db_lines, db_color = db()
    print(C["clear"], end="")
    print(f"{C['bold']}{C['c']}UC Shop live monitor{C['reset']}  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 72)
    print(card("API", api_status, api_color))
    print(card("Containers", compose_status, compose_color))
    print(card("Kafka lag", lag, lag_color))
    print(card("Database", "OK" if db_color == "g" else "ERROR", db_color))
    for line in db_lines:
        print(f"  {C['w']}{line}{C['reset']}")
    print("-" * 72)
    print(f"{C['y']}Ctrl+C чтобы выйти. Interval: {args.interval}s{C['reset']}")
    sys.stdout.flush()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8001")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    try:
        while True:
            draw(args)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n{C['g']}monitor stopped{C['reset']}")


if __name__ == "__main__":
    main()
