from __future__ import annotations

from typing import Iterable

from aiokafka import AIOKafkaProducer

from order_pipeline.domain.events import OrderPaidEvent


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, topic: str, event: OrderPaidEvent) -> None:
        await self._producer.send_and_wait(
            topic,
            key=event.key,
            value=event.to_bytes(),
            headers=[
                ("event_id", event.event_id.encode()),
                ("event_type", event.event_type.encode()),
            ],
        )

    async def send_raw(
        self,
        topic: str,
        key: bytes | None,
        value: bytes,
        headers: Iterable[tuple[str, bytes]] | None = None,
    ) -> None:
        await self._producer.send_and_wait(
            topic, key=key, value=value, headers=list(headers or [])
        )
