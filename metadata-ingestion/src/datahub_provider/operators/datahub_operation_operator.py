import datetime
from typing import Any, List, Optional, Sequence, Union

from airflow.sensors.base import BaseSensorOperator

from datahub.api.circuit_breaker import (
    OperationCircuitBreaker,
    OperationCircuitBreakerConfig,
)
from datahub_provider.hooks.datahub import DatahubRestHook


class DataHubOperationCircuitBreakerOperator(BaseSensorOperator):
    r"""
    DataHub Operation Circuit Breaker Operator.

    :param urn: The DataHub dataset unique identifier. (templated)
    :param datahub_rest_conn_id: The REST datahub connection id to communicate with DataHub
        which is set as Airflow connection.
    :param partition: The partition to check the operation.
    :param source_type: The partition to check the operation. :ref:`https://datahubproject.io/docs/graphql/enums#operationsourcetype`

    """

    template_fields: Sequence[str] = (
        "urn",
        "partition",
        "source_type",
        "operation_type",
    )
    circuit_breaker: OperationCircuitBreaker
    urn: Union[List[str], str]
    partition: Optional[str]
    source_type: Optional[str]
    operation_type: Optional[str]

    def __init__(  # type: ignore[no-untyped-def]
        self,
        *,
        urn: Union[List[str], str],
        datahub_rest_conn_id: Optional[str] = None,
        time_delta: Optional[datetime.timedelta] = datetime.timedelta(days=1),
        partition: Optional[str] = None,
        source_type: Optional[str] = None,
        operation_type: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        hook: DatahubRestHook
        if datahub_rest_conn_id is not None:
            hook = DatahubRestHook(datahub_rest_conn_id=datahub_rest_conn_id)
        else:
            hook = DatahubRestHook()

        host, password, timeout_sec = hook._get_config()

        self.urn = urn
        self.partition = partition
        self.operation_type = operation_type
        self.source_type = source_type

        config: OperationCircuitBreakerConfig = OperationCircuitBreakerConfig(
            datahub_host=host,
            datahub_token=password,
            timeout=timeout_sec,
            time_delta=time_delta,
        )

        self.circuit_breaker = OperationCircuitBreaker(config=config)

    def execute(self, context: Any) -> bool:
        if "datahub_silence_circuit_breakers" in context["dag_run"].conf:
            self.log.info(
                "Circuit breaker is silenced because datahub_silence_circuit_breakers config is set"
            )
            return True

        self.log.info(f"Checking if dataset {self.urn} is ready to be consumed")
        if type(self.urn) == str:
            urns = [self.urn]
        elif type(self.urn) == list:
            urns = self.urn
        else:
            raise Exception(f"urn parameter has invalid type {type(self.urn)}")

        for urn in urns:
            self.log.info(f"Checking if dataset {self.urn} is ready to be consumed")
            ret = self.circuit_breaker.is_circuit_breaker_active(
                urn=urn,
                partition=self.partition,
                operation_type=self.operation_type,
                source_type=self.source_type,
            )
            if ret:
                raise Exception(f"Dataset {self.urn} is not in consumable state")

        return True
