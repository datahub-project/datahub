# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
