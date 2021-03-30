from typing import Any, Dict, Optional

from airflow.hooks.base import BaseHook


class DatahubRestHook(BaseHook):
    """
    TODO
    """

    conn_name_attr = "datahub_rest_conn_id"
    default_conn_name = "datahub_rest_default"
    conn_type = "datahub_rest"
    hook_name = "DataHub REST Server"

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        # TODO: see customized_form_field_behaviors (https://apache.googlesource.com/airflow/+/refs/tags/2.0.0rc3/airflow/customized_form_field_behaviours.schema.json)
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {"host": "DataHub REST Server (GMS) Endpoint"},
        }

    def get_conn(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        host: str = conn.host
        login: str = conn.login
        psw: str = conn.password
        jdbc_driver_loc: Optional[str] = conn.extra_dejson.get("extra__jdbc__drv_path")
        jdbc_driver_name: Optional[str] = conn.extra_dejson.get(
            "extra__jdbc__drv_clsname"
        )

        conn = jaydebeapi.connect(
            jclassname=jdbc_driver_name,
            url=str(host),
            driver_args=[str(login), str(psw)],
            jars=jdbc_driver_loc.split(",") if jdbc_driver_loc else None,
        )
        return conn
