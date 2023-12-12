import logging
from gql.transport.requests import log as requests_logger

requests_logger.setLevel(logging.WARNING)

from datahub.api.circuit_breaker.assertion_circuit_breaker import (
    AssertionCircuitBreaker,
    AssertionCircuitBreakerConfig,
)
from datahub.api.circuit_breaker.operation_circuit_breaker import (
    OperationCircuitBreaker,
    OperationCircuitBreakerConfig,
)
