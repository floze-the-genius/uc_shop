import os

# ── Kafka ─────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order_processors")
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
KAFKA_NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "3"))
KAFKA_AUTO_CREATE_TOPICS_ENABLE = os.getenv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
KAFKA_BROKER_ID = int(os.getenv("KAFKA_BROKER_ID", "1"))

# ── Zookeeper ─────────────────────────────────
ZOOKEEPER_CLIENT_PORT = int(os.getenv("ZOOKEEPER_CLIENT_PORT", "2181"))
ZOOKEEPER_TICK_TIME = int(os.getenv("ZOOKEEPER_TICK_TIME", "2000"))

# ── Worker ────────────────────────────────────
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "worker_1")
MAX_PARALLEL_ORDERS = int(os.getenv("MAX_PARALLEL_ORDERS", "5"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "5000"))
WORKER_REPLICAS = int(os.getenv("WORKER_REPLICAS", "3"))

# ── Retry & DLQ ───────────────────────────────
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "3"))
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "orders_dlq")

# ── Test Producer ─────────────────────────────
TEST_NUM_ORDERS = int(os.getenv("TEST_NUM_ORDERS", "20"))
