# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging

from gql.transport.requests import log as requests_logger

from datahub.api.circuit_breaker.assertion_circuit_breaker import (
    AssertionCircuitBreaker,
    AssertionCircuitBreakerConfig,
)
from datahub.api.circuit_breaker.operation_circuit_breaker import (
    OperationCircuitBreaker,
    OperationCircuitBreakerConfig,
)

requests_logger.setLevel(logging.WARNING)

__all__ = [
    "AssertionCircuitBreaker",
    "AssertionCircuitBreakerConfig",
    "OperationCircuitBreaker",
    "OperationCircuitBreakerConfig",
]
