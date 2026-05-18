import functools, asyncio, logging

logger = logging.getLogger(__name__)


class RetryExhaustedError(Exception):
    pass


def retry(retries = 3, delay = 3):
    def wrap(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            func_logger = logging.getLogger(func.__name__)

            retry_count = 0
            while retry_count < retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    remaining = retries - retry_count - 1
                    if remaining == 0:
                        func_logger.error(
                            f"FINAL RETRY failed for {func.__name__}: {e}. "
                            f"No more retries left ({retries}/{retries})."
                        )
                        raise RetryExhaustedError(f"All {retries} retries exhausted for {func.__name__}: {e}") from e
                    else:
                        func_logger.warning(f"Retry error in {func.__name__}: {e}. Retrying in {delay}s... (Attempt {retry_count + 1}/{retries})")
                        await asyncio.sleep(delay)
                        retry_count += 1

            func_logger.error(f"Failed to execute function {func.__name__}")

        return wrapper
    return wrap


def dlq_safe(func):
    @functools.wraps(func)
    async def wrapper(self, message, tp, *args, **kwargs):
        try:
            await func(self, message, tp, *args, **kwargs)
            return True
        except RetryExhaustedError as e:
            logger.error(f"Message exhausted all retries. Sending to DLQ.")
            try:
                await self._send_to_dlq(message, str(e))
                return True
            except Exception as dlq_err:
                logger.error(
                    f"Failed to send message to DLQ: {dlq_err}. Offset will not be committed to avoid data loss.")
                return False
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}. Message will be retried on next poll (offset not committed).")
            return False
    return wrapper
