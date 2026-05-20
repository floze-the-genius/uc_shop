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
