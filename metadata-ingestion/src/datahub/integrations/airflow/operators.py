from typing import List, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.integrations.airflow.hooks import DatahubKafkaHook, DatahubRestHook
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent


class DatahubBaseOperator(BaseOperator):
    ui_color = "#4398c8"

    hook: Union[DatahubRestHook, DatahubKafkaHook]

    @apply_defaults
    def __init__(
        self,
        *,
        datahub_rest_conn_id: Optional[str] = None,
        datahub_kafka_conn_id: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if datahub_rest_conn_id and datahub_kafka_conn_id:
            raise AirflowException(
                "two hook conn id given when only exactly one required"
            )
        elif datahub_rest_conn_id:
            self.hook = DatahubRestHook(datahub_rest_conn_id)
        elif datahub_kafka_conn_id:
            self.hook = DatahubKafkaHook(datahub_kafka_conn_id)
        else:
            raise AirflowException("no hook conn id provided")


class DatahubEmitterOperator(DatahubBaseOperator):
    @apply_defaults
    def __init__(
        self,
        mces: List[MetadataChangeEvent],
        datahub_rest_conn_id: Optional[str] = None,
        datahub_kafka_conn_id: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            datahub_rest_conn_id=datahub_rest_conn_id,
            datahub_kafka_conn_id=datahub_kafka_conn_id,
            **kwargs,
        )
        self.mces = mces

    def execute(self, context):
        self.hook.emit_mces(self.mces)
