from .order_status import OrderStatus


class InvalidStateTransitionError(Exception):
    pass


class OrderFSM:
    _transitions: dict[OrderStatus, set[OrderStatus]] = {
        OrderStatus.PENDING: {
            OrderStatus.PAID,
            OrderStatus.FAILED,
        },
        OrderStatus.PAID: {
            OrderStatus.API_PENDING,
            OrderStatus.FAILED,
        },
        OrderStatus.API_PENDING: {
            OrderStatus.PROCESSING,
            OrderStatus.FAILED,
        },
        OrderStatus.PROCESSING: {
            OrderStatus.COMPLETED,
            OrderStatus.FAILED,
        },
        OrderStatus.COMPLETED: set(),
        OrderStatus.FAILED: {
            OrderStatus.PENDING,
            OrderStatus.PAID,
            OrderStatus.API_PENDING,
            OrderStatus.PROCESSING,
        },
    }

    @classmethod
    def is_transition_allowed(cls, current: OrderStatus | None, target: OrderStatus) -> bool:
        if current is None:
            return True
        if current == target:
            return current != OrderStatus.COMPLETED
        return target in cls._transitions.get(current, set())

