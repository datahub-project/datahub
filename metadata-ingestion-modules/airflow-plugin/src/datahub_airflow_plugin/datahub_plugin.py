import contextlib
import logging
import os

from airflow.plugins_manager import AirflowPlugin

from datahub_airflow_plugin._airflow_compat import AIRFLOW_PATCHED
from datahub_airflow_plugin._airflow_shims import (
    HAS_AIRFLOW_LISTENER_API,
    NEEDS_AIRFLOW_LISTENER_MODULE,
)
from datahub_airflow_plugin._version import __package_name__

assert AIRFLOW_PATCHED
logger = logging.getLogger(__name__)


_USE_AIRFLOW_LISTENER_INTERFACE = HAS_AIRFLOW_LISTENER_API and os.getenv(
    "DATAHUB_AIRFLOW_PLUGIN_USE_V1_PLUGIN", "false"
).lower() not in ("true", "1")

if _USE_AIRFLOW_LISTENER_INTERFACE:
    try:
        from openlineage.airflow.utils import try_import_from_string  # noqa: F401
    except ImportError:
        # If v2 plugin dependencies are not installed, we fall back to v1.
        logger.warning(
            "Falling back to the v1 DataHub plugin due to missing dependencies. "
            f"Please install {__package_name__}[plugin-v2] to fix this."
        )
        _USE_AIRFLOW_LISTENER_INTERFACE = False


with contextlib.suppress(Exception):
    if os.getenv("DATAHUB_AIRFLOW_PLUGIN_SKIP_FORK_PATCH", "false").lower() not in (
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
        try:
            if not NEEDS_AIRFLOW_LISTENER_MODULE:
                from datahub_airflow_plugin.datahub_listener import (  # type: ignore[misc]
                    get_airflow_plugin_listener,
                )

                listeners: list = list(filter(None, [get_airflow_plugin_listener()]))

            else:
                # On Airflow < 2.5, we need the listener to be a module.
                # This is just a quick shim layer to make that work.
                #
                # Related Airflow change: https://github.com/apache/airflow/pull/27113.
                import datahub_airflow_plugin._datahub_listener_module as _listener_module  # type: ignore[misc]

                listeners = [_listener_module]
        except Exception as e:
            logger.warning(
                f"Failed to load the DataHub plugin's event listener: {e}",
                exc_info=True,
            )
            listeners = []


if not _USE_AIRFLOW_LISTENER_INTERFACE:
    # Use the policy patcher mechanism on Airflow 2.2 and below.
    import datahub_airflow_plugin.datahub_plugin_v22  # noqa: F401
