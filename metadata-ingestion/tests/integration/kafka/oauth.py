import logging
from typing import Any, Tuple

logger = logging.getLogger(__name__)

MESSAGE: str = "OAuth token `create_token` callback"


def create_token(*args: Any, **kwargs: Any) -> Tuple[str, int]:
    logger.warning(MESSAGE)
    return (
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjbGllbnRfaWQiOiJrYWZrYV9jbGllbnQiLCJleHAiOjE2OTg3NjYwMDB9.dummy_sig_abcdef123456",
        3600,
    )


def create_token_no_args() -> Tuple[str, int]:
    logger.warning(MESSAGE)
    return (
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjbGllbnRfaWQiOiJrYWZrYV9jbGllbnQiLCJleHAiOjE2OTg3NjYwMDB9.dummy_sig_abcdef123456",
        3600,
    )


def create_token_only_kwargs(**kwargs: Any) -> Tuple[str, int]:
    logger.warning(MESSAGE)
    return (
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjbGllbnRfaWQiOiJrYWZrYV9jbGllbnQiLCJleHAiOjE2OTg3NjYwMDB9.dummy_sig_abcdef123456",
        3600,
    )
