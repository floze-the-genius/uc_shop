from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from order_pipeline.platform.clock import utc_now


@dataclass(frozen=True, slots=True)
class RetryDecision:
    should_retry: bool
    publish_after: datetime
    topic: str = ""


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    delays_seconds: tuple[float, ...]
    topics: tuple[str, ...]

    def decide(self, attempt: int) -> RetryDecision:
        if attempt >= self.max_attempts or not self.delays_seconds or not self.topics:
            return RetryDecision(False, utc_now())

        index = min(max(0, attempt - 1), len(self.delays_seconds) - 1)
        delay = self.delays_seconds[index]
        topic = self.topics[min(index, len(self.topics) - 1)]
        return RetryDecision(True, utc_now() + timedelta(seconds=delay), topic)
