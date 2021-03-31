from typing import Any, Dict, List

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

from datahub.emitter.rest_emitter import DatahubRestEmitter
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

    def make_sink(self):
        TODO
        pass

    def emit_mces(self, mces: List[MetadataChangeEvent]) -> None:
        emitter = self.make_emitter()

        for mce in mces:
            emitter.emit_mce(mce)
