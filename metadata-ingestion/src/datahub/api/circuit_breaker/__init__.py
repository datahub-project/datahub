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
