from typing import Any, Dict, List

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.sink.datahub_kafka import KafkaSinkConfig
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent


class DatahubRestHook(BaseHook):
    conn_name_attr = "datahub_rest_conn_id"
    default_conn_name = "datahub_rest_default"
    conn_type = "datahub_rest"
    hook_name = "DataHub REST Server"

    def __init__(self, datahub_rest_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.datahub_rest_conn_id = datahub_rest_conn_id

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        return {}

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behavior"""
        return {
            "hidden_fields": ["port", "schema", "login"],
            "relabeling": {
                "host": "Server Endpoint",
            },
        }

    def _gms_endpoint(self) -> str:
        conn = self.get_connection(self.datahub_rest_conn_id)
        host = conn.host
        if host is None:
            raise AirflowException("host parameter is required")
        return host

    def make_emitter(self) -> DatahubRestEmitter:
        return DatahubRestEmitter(self._gms_endpoint())

    def emit_mces(self, mces: List[MetadataChangeEvent]) -> None:
        emitter = self.make_emitter()

        for mce in mces:
            emitter.emit_mce(mce)


class DatahubKafkaHook(BaseHook):
    conn_name_attr = "datahub_kafka_conn_id"
    default_conn_name = "datahub_kafka_default"
    conn_type = "datahub_kafka"
    hook_name = "DataHub Kafka Sink"

    def __init__(self, datahub_kafka_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.datahub_kafka_conn_id = datahub_kafka_conn_id

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        return {}

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behavior"""
        return {
            "hidden_fields": ["port", "schema", "login", "password"],
            "relabeling": {
                "host": "Kafka Broker",
            },
        }

    def _get_config(self) -> KafkaSinkConfig:
        conn = self.get_connection(self.datahub_kafka_conn_id)
        obj = conn.extra_dejson
        obj.setdefault("connection", {})
        if conn.host is not None:
            if "bootstrap" in obj["connection"]:
                raise AirflowException(
                    "Kafka broker specified twice (present in host and extra)"
                )
            obj["connection"]["bootstrap"] = conn.host
        config = KafkaSinkConfig.parse_obj(obj)
        return config

    def make_emitter(self) -> DatahubKafkaEmitter:
        sink_config = self._get_config()
        return DatahubKafkaEmitter(sink_config)

    def emit_mces(self, mces: List[MetadataChangeEvent]) -> None:
        emitter = self.make_emitter()
        errors = []

        def callback(exc, msg):
            if exc:
                errors.append(exc)

        for mce in mces:
            emitter.emit_mce_async(mce, callback)

        emitter.flush()

        if errors:
            raise AirflowException(f"failed to push some MCEs: {errors}")
