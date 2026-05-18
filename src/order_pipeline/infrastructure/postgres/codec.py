from __future__ import annotations

import json
from typing import Any


def dump_json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def load_json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
