import contextlib
import logging
import os

from airflow.plugins_manager import AirflowPlugin

from datahub_airflow_plugin._airflow_compat import AIRFLOW_PATCHED
from datahub_airflow_plugin._airflow_shims import HAS_AIRFLOW_LISTENER_API

assert AIRFLOW_PATCHED
logger = logging.getLogger(__name__)


_USE_AIRFLOW_LISTENER_INTERFACE = HAS_AIRFLOW_LISTENER_API and not os.getenv(
    "DATAHUB_AIRFLOW_PLUGIN_USE_V1_PLUGIN", "false"
).lower() in ("true", "1")

with contextlib.suppress(Exception):
    if not os.getenv("DATAHUB_AIRFLOW_PLUGIN_SKIP_FORK_PATCH", "false").lower() in (
        "true",
        "1",
    ):
        # From https://github.com/apache/airflow/discussions/24463#discussioncomment-4404542
        # I'm not exactly sure why this fixes it, but I suspect it's that this
        # forces the proxy settings to get cached before the fork happens.
        #
        # For more details, see https://github.com/python/cpython/issues/58037
        # and https://wefearchange.org/2018/11/forkmacos.rst.html
        # and https://bugs.python.org/issue30385#msg293958
        # An alternative fix is to set NO_PROXY='*'

        from _scproxy import _get_proxy_settings

        _get_proxy_settings()


class DatahubPlugin(AirflowPlugin):
    name = "datahub_plugin"

    if _USE_AIRFLOW_LISTENER_INTERFACE:
        from datahub_airflow_plugin.datahub_listener import (  # type: ignore[misc]
            get_airflow_plugin_listener,
        )

        listeners: list = list(filter(None, [get_airflow_plugin_listener()]))


if not _USE_AIRFLOW_LISTENER_INTERFACE:
    # Use the policy patcher mechanism on Airflow 2.2 and below.
    import datahub_airflow_plugin.datahub_plugin_v22  # noqa: F401
