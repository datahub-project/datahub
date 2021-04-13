from typing import List, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.integrations.airflow.hooks import (
    DatahubGenericHook,
    DatahubKafkaHook,
    DatahubRestHook,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent


class DatahubBaseOperator(BaseOperator):
    ui_color = "#4398c8"

    hook: Union[DatahubRestHook, DatahubKafkaHook]

    @apply_defaults
    def __init__(
        self,
        *,
        datahub_conn_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.datahub_conn_id = datahub_conn_id
        self.generic_hook = DatahubGenericHook(datahub_conn_id)


class DatahubEmitterOperator(DatahubBaseOperator):
    @apply_defaults
    def __init__(
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
