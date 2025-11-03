"""
DataHub Airflow Plugin v2 for Airflow 3.x.

This module provides the DataHub listener implementation for Airflow 3.x,
using the native OpenLineage provider and SQL parser patches for lineage.
"""

from datahub_airflow_plugin.plugin_v2_airflow3.datahub_listener import (
    DataHubListener,
    get_airflow_plugin_listener,
)

__all__ = ["DataHubListener", "get_airflow_plugin_listener"]
