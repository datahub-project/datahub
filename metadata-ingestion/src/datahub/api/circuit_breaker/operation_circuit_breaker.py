import logging
from datetime import datetime, timedelta
from typing import Optional

from pydantic import Field

from datahub.api.circuit_breaker.circuit_breaker import (
    AbstractCircuitBreaker,
    CircuitBreakerConfig,
)
from datahub.api.graphql import Operation

logger: logging.Logger = logging.getLogger(__name__)


class OperationCircuitBreakerConfig(CircuitBreakerConfig):
    time_delta: timedelta = Field(
        default=(timedelta(days=1)),
        description="In what timeframe should accept an operation result if updated field is not available for the dataset",
    )


class OperationCircuitBreaker(AbstractCircuitBreaker):
    r"""
    DataHub Operation Circuit Breaker

    The circuit breaker checks if there is an operation metadata for the dataset.
    If there is no valid Operation metadata then the circuit breaker fails.
    """

    config: OperationCircuitBreakerConfig
    operation_api: Operation

    def __init__(self, config: OperationCircuitBreakerConfig):
        super().__init__(config.datahub_host, config.datahub_token, config.timeout)
        self.config = config
        self.operation_api = Operation(
            datahub_host=config.datahub_host,
            datahub_token=config.datahub_token,
            timeout=config.timeout,
        )

    def is_circuit_breaker_active(
        self,
        urn: str,
        partition: Optional[str] = None,
        source_type: Optional[str] = None,
        operation_type: Optional[str] = None,
    ) -> bool:
        r"""
        Checks if the circuit breaker is active

        :param urn: The Datahub dataset unique identifier.
        :param datahub_rest_conn_id: The REST DataHub connection id to communicate with DataHub
            which is set as Airflow connection.
        :param partition: The partition to check the operation.
        :param source_type: The source type to filter on. If not set it will accept any source type.
            See valid types here: https://datahubproject.io/docs/graphql/enums#operationsourcetype
        :param operation_type: The operation type to filter on. If not set it will accept any source type.
            See valid types here: https://datahubproject.io/docs/graphql/enums/#operationtype
        """

        start_time_millis: int = int(
            (datetime.now() - self.config.time_delta).timestamp() * 1000
        )
        operations = self.operation_api.query_operations(
            urn,
            start_time_millis=start_time_millis,
            partition=partition,
            source_type=source_type,
            operation_type=operation_type,
        )
        logger.info(f"Operations: {operations}")
        for operation in operations:
            if (
                operation.get("lastUpdatedTimestamp")
                and operation["lastUpdatedTimestamp"] >= start_time_millis
            ):
                return False

        return True
