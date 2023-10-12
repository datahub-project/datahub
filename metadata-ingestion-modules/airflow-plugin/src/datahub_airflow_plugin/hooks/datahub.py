from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Tuple, Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

if TYPE_CHECKING:
    from airflow.models.connection import Connection
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.emitter.synchronized_file_emitter import SynchronizedFileEmitter
    from datahub.ingestion.sink.datahub_kafka import KafkaSinkConfig


class DatahubRestHook(BaseHook):
    """
    Creates a DataHub Rest API connection used to send metadata to DataHub.
    Takes the endpoint for your DataHub Rest API in the Server Endpoint(host) field.

    URI example: ::

        AIRFLOW_CONN_DATAHUB_REST_DEFAULT='datahub-rest://rest-endpoint'

    :param datahub_rest_conn_id: Reference to the DataHub Rest connection.
    :type datahub_rest_conn_id: str
    """

    conn_name_attr = "datahub_rest_conn_id"
    default_conn_name = "datahub_rest_default"
    conn_type = "datahub-rest"
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

    def test_connection(self) -> Tuple[bool, str]:
        try:
            emitter = self.make_emitter()
            emitter.test_connection()
        except Exception as e:
            return False, str(e)

        return True, "Successfully connected to DataHub."

    def _get_config(self) -> Tuple[str, Optional[str], Optional[int]]:
        conn: "Connection" = self.get_connection(self.datahub_rest_conn_id)

        host = conn.host
        if not host:
            raise AirflowException("host parameter is required")
        if conn.port:
            if ":" in host:
                raise AirflowException(
                    "host parameter should not contain a port number if the port is specified separately"
                )
            host = f"{host}:{conn.port}"
        password = conn.password
        timeout_sec = conn.extra_dejson.get("timeout_sec")
        return (host, password, timeout_sec)

    def make_emitter(self) -> "DatahubRestEmitter":
        import datahub.emitter.rest_emitter

        return datahub.emitter.rest_emitter.DatahubRestEmitter(*self._get_config())

    def emit(
        self,
        items: Sequence[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
    ) -> None:
        emitter = self.make_emitter()

        for item in items:
            emitter.emit(item)

    # Retained for backwards compatibility.
    emit_mces = emit
    emit_mcps = emit


class DatahubKafkaHook(BaseHook):
    """
    Creates a DataHub Kafka connection used to send metadata to DataHub.
    Takes your kafka broker in the Kafka Broker(host) field.

    URI example: ::

        AIRFLOW_CONN_DATAHUB_KAFKA_DEFAULT='datahub-kafka://kafka-broker'

    :param datahub_kafka_conn_id: Reference to the DataHub Kafka connection.
    :type datahub_kafka_conn_id: str
    """

    conn_name_attr = "datahub_kafka_conn_id"
    default_conn_name = "datahub_kafka_default"
    conn_type = "datahub-kafka"
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

    def _get_config(self) -> "KafkaSinkConfig":
        import datahub.ingestion.sink.datahub_kafka

        conn = self.get_connection(self.datahub_kafka_conn_id)
        obj = conn.extra_dejson
        obj.setdefault("connection", {})
        if conn.host is not None:
            if "bootstrap" in obj["connection"]:
                raise AirflowException(
                    "Kafka broker specified twice (present in host and extra)"
                )
            obj["connection"]["bootstrap"] = ":".join(
                map(str, filter(None, [conn.host, conn.port]))
            )
        config = datahub.ingestion.sink.datahub_kafka.KafkaSinkConfig.parse_obj(obj)
        return config

    def make_emitter(self) -> "DatahubKafkaEmitter":
        import datahub.emitter.kafka_emitter

        sink_config = self._get_config()
        return datahub.emitter.kafka_emitter.DatahubKafkaEmitter(sink_config)

    def emit(
        self,
        items: Sequence[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
    ) -> None:
        emitter = self.make_emitter()
        errors = []

        def callback(exc, msg):
            if exc:
                errors.append(exc)

        for mce in items:
            emitter.emit(mce, callback)

        emitter.flush()

        if errors:
            raise AirflowException(f"failed to push some metadata: {errors}")

    # Retained for backwards compatibility.
    emit_mces = emit
    emit_mcps = emit


class SynchronizedFileHook(BaseHook):
    conn_type = "datahub-file"

    def __init__(self, datahub_conn_id: str) -> None:
        super().__init__()
        self.datahub_conn_id = datahub_conn_id

    def make_emitter(self) -> "SynchronizedFileEmitter":
        from datahub.emitter.synchronized_file_emitter import SynchronizedFileEmitter

        conn = self.get_connection(self.datahub_conn_id)
        filename = conn.host
        if not filename:
            raise AirflowException("filename parameter is required")

        return SynchronizedFileEmitter(filename=filename)

    def emit(
        self,
        items: Sequence[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
    ) -> None:
        emitter = self.make_emitter()

        for item in items:
            emitter.emit(item)


class DatahubGenericHook(BaseHook):
    """
    Emits Metadata Change Events using either the DatahubRestHook or the
    DatahubKafkaHook. Set up a DataHub Rest or Kafka connection to use.

    :param datahub_conn_id: Reference to the DataHub connection.
    :type datahub_conn_id: str
    """

    def __init__(self, datahub_conn_id: str) -> None:
        super().__init__()
        self.datahub_conn_id = datahub_conn_id

    def get_underlying_hook(
        self,
    ) -> Union[DatahubRestHook, DatahubKafkaHook, SynchronizedFileHook]:
        conn = self.get_connection(self.datahub_conn_id)

        # We need to figure out the underlying hook type. First check the
        # conn_type. If that fails, attempt to guess using the conn id name.
        if (
            conn.conn_type == DatahubRestHook.conn_type
            or conn.conn_type == DatahubRestHook.conn_type.replace("-", "_")
        ):
            return DatahubRestHook(self.datahub_conn_id)
        elif (
            conn.conn_type == DatahubKafkaHook.conn_type
            or conn.conn_type == DatahubKafkaHook.conn_type.replace("-", "_")
        ):
            return DatahubKafkaHook(self.datahub_conn_id)
        elif (
            conn.conn_type == SynchronizedFileHook.conn_type
            or conn.conn_type == SynchronizedFileHook.conn_type.replace("-", "_")
        ):
            return SynchronizedFileHook(self.datahub_conn_id)
        elif "rest" in self.datahub_conn_id:
            return DatahubRestHook(self.datahub_conn_id)
        elif "kafka" in self.datahub_conn_id:
            return DatahubKafkaHook(self.datahub_conn_id)
        else:
            raise AirflowException(
                f"DataHub cannot handle conn_type {conn.conn_type} in {conn}"
            )

    def make_emitter(self) -> Emitter:
        return self.get_underlying_hook().make_emitter()

    def emit(
        self,
        items: Sequence[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
    ) -> None:
        return self.get_underlying_hook().emit(items)

    # Retained for backwards compatibility.
    emit_mces = emit
