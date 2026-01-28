"""
DataHub Airflow Plugin Listener - Version Dispatcher.

This module automatically imports the correct listener implementation based on
the installed Airflow version:
- Airflow 2.x: Uses airflow2 with extractor-based lineage
- Airflow 3.x: Uses airflow3 with native OpenLineage integration

This approach allows clean type checking for each version without conflicts.
"""

from datahub_airflow_plugin._airflow_version_specific import IS_AIRFLOW_3_OR_HIGHER

if IS_AIRFLOW_3_OR_HIGHER:
    from datahub_airflow_plugin.airflow3.datahub_listener import (  # type: ignore[assignment]
        DataHubListener,
        get_airflow_plugin_listener,
    )
else:
    from datahub_airflow_plugin.airflow2.datahub_listener import (  # type: ignore[assignment]
        DataHubListener,
        get_airflow_plugin_listener,
    )

__all__ = ["DataHubListener", "get_airflow_plugin_listener"]
