from __future__ import annotations

from dataclasses import dataclass, field
from collections import Counter


@dataclass(slots=True)
class Stats:
    latencies: list[float] = field(default_factory=list)
    statuses: Counter = field(default_factory=Counter)
    ok: int = 0
    errors: int = 0

    def add(self, status: int, latency: float) -> None:
        self.latencies.append(latency)
        self.statuses[str(status)] += 1
        if 200 <= status < 300:
            self.ok += 1
        else:
            self.errors += 1

    def summary(self) -> dict:
        values = sorted(self.latencies)
        total = self.ok + self.errors
        return {
            "requests": total,
            "ok": self.ok,
            "errors": self.errors,
            "error_rate": round(self.errors / total, 6) if total else 0,
            "p50_ms": _percentile(values, 0.50),
            "p95_ms": _percentile(values, 0.95),
            "p99_ms": _percentile(values, 0.99),
            "max_ms": round(values[-1] * 1000, 2) if values else 0,
            "status_counts": dict(sorted(self.statuses.items())),
        }


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0
    index = min(len(values) - 1, int(len(values) * percentile))
    return round(values[index] * 1000, 2)
