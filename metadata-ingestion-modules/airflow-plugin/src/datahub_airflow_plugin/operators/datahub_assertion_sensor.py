import datetime
from typing import Any, List, Optional, Sequence, Union

from airflow.sensors.base import BaseSensorOperator

from datahub.api.circuit_breaker import (
    AssertionCircuitBreaker,
    AssertionCircuitBreakerConfig,
)
from datahub_airflow_plugin.hooks.datahub import DatahubRestHook


class DataHubAssertionSensor(BaseSensorOperator):
    r"""
    DataHub Assertion Circuit Breaker Sensor.

    :param urn: The DataHub dataset unique identifier. (templated)
    :param datahub_rest_conn_id: The REST datahub connection id to communicate with DataHub
        which is set as Airflow connection.
    :param check_last_assertion_time: If set it checks assertions after the last operation was set on the dataset.
        By default it is True.
    :param time_delta: If verify_after_last_update is False it checks for assertion within the time delta.
    """

    template_fields: Sequence[str] = ("urn",)
    circuit_breaker: AssertionCircuitBreaker
    urn: Union[List[str], str]

    def __init__(  # type: ignore[no-untyped-def]
        self,
        *,
        urn: Union[List[str], str],
        datahub_rest_conn_id: Optional[str] = None,
        check_last_assertion_time: bool = True,
        time_delta: datetime.timedelta = datetime.timedelta(days=1),
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
        config: AssertionCircuitBreakerConfig = AssertionCircuitBreakerConfig(
            datahub_host=host,
            datahub_token=password,
            timeout=timeout_sec,
            verify_after_last_update=check_last_assertion_time,
            time_delta=time_delta,
        )
        self.circuit_breaker = AssertionCircuitBreaker(config=config)

    def poke(self, context: Any) -> bool:
        if "datahub_silence_circuit_breakers" in context["dag_run"].conf:
            self.log.info(
                "Circuit breaker is silenced because datahub_silence_circuit_breakers config is set"
            )
            return True

        self.log.info(f"Checking if dataset {self.urn} is ready to be consumed")
        if isinstance(self.urn, str):
            urns = [self.urn]
        elif isinstance(self.urn, list):
            urns = self.urn
        else:
            raise Exception(f"urn parameter has invalid type {type(self.urn)}")

        for urn in urns:
            self.log.info(f"Checking if dataset {self.urn} is ready to be consumed")
            ret = self.circuit_breaker.is_circuit_breaker_active(urn=urn)
            if ret:
                self.log.info(f"Dataset {self.urn} is not in consumable state")
                return False

        return True
