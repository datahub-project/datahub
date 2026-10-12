import logging
import time
from typing import Any, Tuple

logger = logging.getLogger(__name__)

MESSAGE: str = "OAuth token `create_token` callback"

_DUMMY_TOKEN = (
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9."
    "eyJjbGllbnRfaWQiOiJrYWZrYV9jbGllbnQiLCJleHAiOjE2OTg3NjYwMDB9."
    "dummy_sig_abcdef123456"
)


def _token_with_expiry() -> Tuple[str, float]:
    # confluent-kafka 2.13+ requires epoch seconds (time.time() + lifetime),
    # not a relative lifetime. The client constructor waits up to 10s for a
    # token whose expiry is still in the future on the wall clock.
    return _DUMMY_TOKEN, time.time() + 3600


def create_token(*args: Any, **kwargs: Any) -> Tuple[str, float]:
    logger.warning(MESSAGE)
    return _token_with_expiry()


def create_token_no_args() -> Tuple[str, float]:
    logger.warning(MESSAGE)
    return _token_with_expiry()


def create_token_only_kwargs(**kwargs: Any) -> Tuple[str, float]:
    logger.warning(MESSAGE)
    return _token_with_expiry()
