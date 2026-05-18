from __future__ import annotations

from datetime import timedelta

from order_pipeline.application.event_factory import dlq_event
from order_pipeline.application.ports.order_store import OrderStore
from order_pipeline.application.ports.provider import ProviderClient
from order_pipeline.application.retry_policy import RetryPolicy
from order_pipeline.domain.errors import PermanentProviderError
from order_pipeline.domain.events import OrderPaidEvent
from order_pipeline.domain.outcome import ProcessingOutcome
from order_pipeline.platform.clock import utc_now


class ProcessOrderEvent:
    def __init__(
        self,
        store: OrderStore,
        provider: ProviderClient,
        retry_policy: RetryPolicy,
        stale_after_seconds: int,
        dlq_topic: str,
    ):
        self._store = store
        self._provider = provider
        self._retry_policy = retry_policy
        self._stale_after = timedelta(seconds=stale_after_seconds)
        self._dlq_topic = dlq_topic

    async def execute(self, event: OrderPaidEvent, owner: str) -> ProcessingOutcome:
        claimed = await self._store.claim_order(
            event.order_id, owner, self._stale_after
        )
        if claimed is None:
            return self._outcome(event.order_id, "skipped", "not claimable")

        try:
            result = await self._provider.fulfill(claimed)
        except PermanentProviderError as exc:
            await self._store.fail_order(
                claimed.id, str(exc), dlq_event(event), self._dlq_topic
            )
            return self._outcome(claimed.id, "failed", str(exc))
        except Exception as exc:
            decision = self._retry_policy.decide(claimed.attempts)
            if not decision.should_retry:
                await self._store.fail_order(
                    claimed.id, str(exc), dlq_event(event), self._dlq_topic
                )
                return self._outcome(claimed.id, "dlq", str(exc))

            retry_event = event.next_retry(
                max(0.0, (decision.publish_after - utc_now()).total_seconds())
            )
            await self._store.retry_order(
                claimed.id,
                str(exc),
                retry_event,
                decision.topic,
                decision.publish_after,
            )
            return self._outcome(claimed.id, "retry", str(exc))

        await self._store.complete_order(claimed.id, result)
        return self._outcome(claimed.id, "completed")

    def _outcome(
        self, order_id: int, status: str, detail: str = ""
    ) -> ProcessingOutcome:
        return ProcessingOutcome(order_id, status, detail)
