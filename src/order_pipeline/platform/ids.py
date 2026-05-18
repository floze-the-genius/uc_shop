from __future__ import annotations

import os
import socket
from uuid import uuid4


def new_id() -> str:
    return str(uuid4())


def process_id(prefix: str) -> str:
    return f"{prefix}-{socket.gethostname()}-{os.getpid()}"
