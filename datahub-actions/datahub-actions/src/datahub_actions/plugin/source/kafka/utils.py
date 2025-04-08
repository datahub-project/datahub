import logging
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)


def with_retry(
    max_attempts: int, max_backoff: float, func: Callable, *args: Any, **kwargs: Any
) -> Any:  # type: ignore
    curr_attempt = 0
    backoff = 0.3

    while curr_attempt < max_attempts:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(str(e))

            curr_attempt = curr_attempt + 1
            if curr_attempt >= max_attempts:
                logger.warning("kafka event source: exhausted all attempts.")
                return

            backoff = backoff * 2
            if backoff > max_backoff:
                backoff = max_backoff
            time.sleep(backoff)
