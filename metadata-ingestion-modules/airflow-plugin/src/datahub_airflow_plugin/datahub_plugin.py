import logging
import os
from types import ModuleType
from typing import List

from airflow.plugins_manager import AirflowPlugin

from datahub_airflow_plugin._airflow_compat import AIRFLOW_PATCHED
from datahub_airflow_plugin._airflow_shims import IS_AIRFLOW_V23_PLUS

assert AIRFLOW_PATCHED
logger = logging.getLogger(__name__)


_USE_AIRFLOW_LISTENER_INTERFACE = IS_AIRFLOW_V23_PLUS and not os.getenv(
    "DATAHUB_AIRFLOW_PLUGIN_USE_V1_PLUGIN", "false"
).lower() in ("true", "1")


class DatahubPlugin(AirflowPlugin):
    name = "datahub_plugin"

    if _USE_AIRFLOW_LISTENER_INTERFACE:
        from datahub_airflow_plugin.datahub_listener import (  # type: ignore[misc]
            get_airflow_plugin_listener,
        )

        listeners: List[ModuleType] = list(
            filter(None, [get_airflow_plugin_listener()])
        )


if not _USE_AIRFLOW_LISTENER_INTERFACE:
    # Use the policy patcher mechanism on Airflow 2.2 and below.
    import datahub_airflow_plugin.datahub_plugin_v22  # noqa: F401
