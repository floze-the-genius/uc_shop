from __future__ import annotations


class OrderPipelineError(Exception):
    pass


class TransientProviderError(OrderPipelineError):
    pass


class PermanentProviderError(OrderPipelineError):
    pass
