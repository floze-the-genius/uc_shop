from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ProcessingOutcome:
    order_id: int
    status: str
    detail: str = ""
