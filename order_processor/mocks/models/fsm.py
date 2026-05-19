from .order_status import OrderStatus


class InvalidStateTransitionError(Exception):
    pass


class OrderFSM:
    _transitions: dict[OrderStatus, set[OrderStatus]] = {
        OrderStatus.PENDING: {
            OrderStatus.PAID,
            OrderStatus.PROCESSING,
            OrderStatus.API_PENDING,
            OrderStatus.COMPLETED,
            OrderStatus.FAILED,
        },
        OrderStatus.PAID: {
            OrderStatus.PROCESSING,
            OrderStatus.COMPLETED,
            OrderStatus.FAILED,
        },
        OrderStatus.API_PENDING: {
            OrderStatus.PROCESSING,
            OrderStatus.COMPLETED,
            OrderStatus.FAILED,
        },
        OrderStatus.PROCESSING: {
            OrderStatus.COMPLETED,
            OrderStatus.FAILED,
        },
        OrderStatus.COMPLETED: set(),
        OrderStatus.FAILED: {
            OrderStatus.PENDING,
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

    @classmethod
    def validate_transition(cls, current: OrderStatus | None, target: OrderStatus) -> None:
        if not cls.is_transition_allowed(current, target):
            current_str = current.value if current else "None"
            raise InvalidStateTransitionError(
                f"Transition from '{current_str}' to '{target.value}' is not allowed."
            )
