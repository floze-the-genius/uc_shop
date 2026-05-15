from enum import Enum

class OrderStatus(str, Enum):
    PENDING = "pending"
    PAID = "paid"
    PROCESSING = "processing"
    API_PENDING = "api_pending"
    COMPLETED = "completed"
    FAILED = "failed"
