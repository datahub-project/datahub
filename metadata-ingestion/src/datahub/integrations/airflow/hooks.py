from typing import Any, Dict, List

from airflow.hooks.base import BaseHook

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent


class DatahubRestHook(BaseHook):
    conn_name_attr = "datahub_rest_conn_id"
    default_conn_name = "datahub_rest_default"
    conn_type = "datahub_rest"
    hook_name = "DataHub REST Server"

    def __init__(
        self,
        datahub_rest_conn_id: str = default_conn_name,
    ) -> None:
        super().__init__()
        self.datahub_rest_conn_id = datahub_rest_conn_id

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        return {}

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behavior"""
        # TODO: see customized_form_field_behaviors (https://apache.googlesource.com/airflow/+/refs/tags/2.0.0rc3/airflow/customized_form_field_behaviours.schema.json)
        return {
            "hidden_fields": ["port", "schema", "login"],
            "relabeling": {
                "host": "Server Endpoint",
            },
        }

    def _gms_endpoint(self) -> str:
        conn = self.get_connection()
        return conn.host

    def emit_mces(self, mces: List[MetadataChangeEvent]) -> None:
        emitter = DatahubRestEmitter(self._gms_endpoint())

        for mce in mces:
            emitter.emit_mce(mce)
