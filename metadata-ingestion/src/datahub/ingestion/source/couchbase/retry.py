import time
import asyncio
import logging
import traceback
from typing import Callable
from functools import wraps
from couchbase.exceptions import CouchbaseException

logger = logging.getLogger(__name__)


def retry_inline(func, *args, retry_count=5, factor=0.02, **kwargs):
    for retry_number in range(retry_count + 1):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            if retry_number == retry_count:
                logger.debug(f"{func.__name__} retry limit exceeded: {err}")
                raise
            logger.debug(f"{func.__name__} will retry, number {retry_number + 1}")
            wait = factor
            wait *= (2 ** (retry_number + 1))
            time.sleep(wait)


def retry(retry_count=5,
          factor=0.02,
          allow_list=None,
          always_raise_list=None
          ) -> Callable:

    def retry_handler(func):
        if not asyncio.iscoroutinefunction(func):
            @wraps(func)
            def f_wrapper(*args, **kwargs):
                for retry_number in range(retry_count + 1):
                    try:
                        return func(*args, **kwargs)
                    except CouchbaseException as err:
                        if always_raise_list and isinstance(err, always_raise_list):
                            raise

                        if allow_list and not isinstance(err, allow_list):
                            raise

                        if retry_number == retry_count:
                            logger.debug(f"{func.__name__} retry limit exceeded")
                            logger.debug(f"Error: {err}")
                            logger.debug(traceback.format_exc())
                            raise

                        logger.debug(f"{func.__name__} will retry, number {retry_number + 1}")
                        wait = factor
                        wait *= (2 ** (retry_number + 1))
                        time.sleep(wait)

            return f_wrapper
        else:
            @wraps(func)
            async def f_wrapper(*args, **kwargs):
                for retry_number in range(retry_count + 1):
                    try:
                        return await func(*args, **kwargs)
                    except CouchbaseException as err:
                        if always_raise_list and isinstance(err, always_raise_list):
                            raise

                        if allow_list and not isinstance(err, allow_list):
                            raise

                        if retry_number == retry_count:
                            logger.debug(f"{func.__name__} retry limit exceeded")
                            logger.debug(f"Error: {err}")
                            logger.debug(traceback.format_exc())
                            raise

                        logger.debug(f"{func.__name__} will retry, number {retry_number + 1}")
                        wait = factor
                        wait *= (2 ** (retry_number + 1))
                        time.sleep(wait)

            return f_wrapper
    return retry_handler
