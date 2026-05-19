import os

# ── Redis ─────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
REDIS_STREAM_KEY = os.getenv("REDIS_STREAM_KEY", "orders")
REDIS_DLQ_STREAM_KEY = os.getenv("REDIS_DLQ_STREAM_KEY", "orders_dlq")
REDIS_GROUP_NAME = os.getenv("REDIS_GROUP_NAME", "order_processors")

# ── Worker ────────────────────────────────────
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "worker_1")
MAX_PARALLEL_ORDERS = int(os.getenv("MAX_PARALLEL_ORDERS", "5"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "5000"))
WORKER_REPLICAS = int(os.getenv("WORKER_REPLICAS", "3"))

# ── Retry & DLQ ───────────────────────────────
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "3"))

# ── Test Producer ─────────────────────────────
TEST_NUM_ORDERS = int(os.getenv("TEST_NUM_ORDERS", "20"))
