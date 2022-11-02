import json
from typing import TYPE_CHECKING, Dict, List, Optional

from airflow.configuration import conf
from airflow.lineage.backend import LineageBackend

from datahub_provider._lineage_core import (
    DatahubBasicLineageConfig,
    send_lineage_to_datahub,
)

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator


class DatahubLineageConfig(DatahubBasicLineageConfig):
    # If set to true, most runtime errors in the lineage backend will be
    # suppressed and will not cause the overall task to fail. Note that
    # configuration issues will still throw exceptions.
    graceful_exceptions: bool = True


def get_lineage_config() -> DatahubLineageConfig:
    """Load the lineage config from airflow.cfg."""

    # The kwargs pattern is also used for secret backends.
    kwargs_str = conf.get("lineage", "datahub_kwargs", fallback="{}")
    kwargs = json.loads(kwargs_str)

    # Continue to support top-level datahub_conn_id config.
    datahub_conn_id = conf.get("lineage", "datahub_conn_id", fallback=None)
    if datahub_conn_id:
        kwargs["datahub_conn_id"] = datahub_conn_id

    return DatahubLineageConfig.parse_obj(kwargs)


class DatahubLineageBackend(LineageBackend):
    """
    Sends lineage data from tasks to DataHub.

    Configurable via ``airflow.cfg`` as follows: ::

        # For REST-based:
        airflow connections add  --conn-type 'datahub_rest' 'datahub_rest_default' --conn-host 'http://localhost:8080'
        # For Kafka-based (standard Kafka sink config can be passed via extras):
        airflow connections add  --conn-type 'datahub_kafka' 'datahub_kafka_default' --conn-host 'broker:9092' --conn-extra '{}'

        [lineage]
        backend = datahub_provider.lineage.datahub.DatahubLineageBackend
        datahub_kwargs = {
            "datahub_conn_id": "datahub_rest_default",
            "capture_ownership_info": true,
            "capture_tags_info": true,
            "graceful_exceptions": true }
        # The above indentation is important!
    """

    def __init__(self) -> None:
        super().__init__()

        # By attempting to get and parse the config, we can detect configuration errors
        # ahead of time. The init method is only called in Airflow 2.x.
        _ = get_lineage_config()

    # With Airflow 2.0, this can be an instance method. However, with Airflow 1.10.x, this
    # method is used statically, even though LineageBackend declares it as an instance variable.
    @staticmethod
    def send_lineage(
        operator: "BaseOperator",
        inlets: Optional[List] = None,  # unused
        outlets: Optional[List] = None,  # unused
        context: Dict = None,
    ) -> None:
        config = get_lineage_config()
        if not config.enabled:
            return

        try:
            context = context or {}  # ensure not None to satisfy mypy
            send_lineage_to_datahub(
                config, operator, operator.inlets, operator.outlets, context
            )
        except Exception as e:
            if config.graceful_exceptions:
                operator.log.error(e)
                operator.log.info(
                    "Suppressing error because graceful_exceptions is set"
                )
            else:
                raise
