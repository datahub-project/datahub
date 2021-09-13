from typing import List, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub_provider.hooks.datahub import (
    DatahubGenericHook,
    DatahubKafkaHook,
    DatahubRestHook,
)


class DatahubBaseOperator(BaseOperator):
    """
    The DatahubBaseOperator is used as a base operator all DataHub operators.
    """

    ui_color = "#4398c8"

    hook: Union[DatahubRestHook, DatahubKafkaHook]

    # mypy is not a fan of this. Newer versions of Airflow support proper typing for the decorator
    # using PEP 612. However, there is not yet a good way to inherit the types of the kwargs from
    # the superclass.
    @apply_defaults  # type: ignore[misc]
    def __init__(  # type: ignore[no-untyped-def]
        self,
        *,
        datahub_conn_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.datahub_conn_id = datahub_conn_id
        self.generic_hook = DatahubGenericHook(datahub_conn_id)


class DatahubEmitterOperator(DatahubBaseOperator):
    """
    Emits a Metadata Change Event to DataHub using either a DataHub
    Rest or Kafka connection.

    :param datahub_conn_id: Reference to the DataHub Rest or Kafka Connection.
    :type datahub_conn_id: str
    """

    # See above for why these mypy type issues are ignored here.
    @apply_defaults  # type: ignore[misc]
    def __init__(  # type: ignore[no-untyped-def]
        self,
        mces: List[MetadataChangeEvent],
        datahub_conn_id: str,
        **kwargs,
    ):
        super().__init__(
            datahub_conn_id=datahub_conn_id,
            **kwargs,
        )
        self.mces = mces

    def execute(self, context):
        self.generic_hook.get_underlying_hook().emit_mces(self.mces)
