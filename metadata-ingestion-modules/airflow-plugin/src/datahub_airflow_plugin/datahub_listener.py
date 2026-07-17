"""DataHub Airflow Plugin Listener — re-exports the Airflow 3 listener implementation."""

from datahub_airflow_plugin.airflow3.datahub_listener import (
    DataHubListener,
    get_airflow_plugin_listener,
)

__all__ = ["DataHubListener", "get_airflow_plugin_listener"]
