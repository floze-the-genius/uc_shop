class MockConfig:
    def __init__(self):
        self._config = {
            "testing.enabled": False,
            "order_processor.processing_lock_ttl_seconds": 600,
            "order_processor.max_parallel_orders": 5,
        }

    def get(self, key: str, default=None):
        return self._config.get(key, default)

    def set(self, key: str, value):
        self._config[key] = value


config = MockConfig()
