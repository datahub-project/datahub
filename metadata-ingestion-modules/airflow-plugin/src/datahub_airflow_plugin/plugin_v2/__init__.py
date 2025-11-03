"""
DataHub Airflow Plugin v2 for Airflow 2.x.

This module provides the DataHub listener implementation for Airflow 2.x,
using the legacy openlineage-airflow package and extractor-based lineage.
"""

from datahub_airflow_plugin.plugin_v2.datahub_listener import (
    DataHubListener,
    get_airflow_plugin_listener,
)

__all__ = ["DataHubListener", "get_airflow_plugin_listener"]
