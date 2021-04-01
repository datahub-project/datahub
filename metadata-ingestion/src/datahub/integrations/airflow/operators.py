from typing import Optional, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from datahub.integrations.airflow.hooks import DatahubRestHook, DatahubKafkaHook


class DatahubBaseOperator(BaseOperator):
    ui_color = "#4398c8"

    hook: Union[DatahubRestHook, DatahubKafkaHook]

    @apply_defaults
    def __init__(
        self,
        datahub_rest_conn_id: Optional[str],
        datahub_kafka_conn_id: Optional[str],
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

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
