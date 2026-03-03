"""
Pure dispatcher for version-specific Airflow shims.

This module automatically imports the correct shims based on the installed
Airflow version, dispatching to either airflow2 or airflow3 implementations.

No logic lives here - just clean version detection and re-export.
"""

import packaging.version

from datahub_airflow_plugin._airflow_version_specific import (
    AIRFLOW_VERSION,
    IS_AIRFLOW_3_OR_HIGHER,
)

# Version feature flags - hardcode based on Airflow version
# These were previously in the old _airflow_shims but are better kept simple
HAS_AIRFLOW_STANDALONE_CMD = AIRFLOW_VERSION >= packaging.version.parse("2.2")
HAS_AIRFLOW_LISTENER_API = AIRFLOW_VERSION >= packaging.version.parse("2.3")
HAS_AIRFLOW_DAG_LISTENER_API = AIRFLOW_VERSION >= packaging.version.parse("2.5")
HAS_AIRFLOW_DATASET_LISTENER_API = AIRFLOW_VERSION >= packaging.version.parse("2.5")

if IS_AIRFLOW_3_OR_HIGHER:
    # Airflow 3.x - use airflow3 shims
    from datahub_airflow_plugin.airflow3._shims import (
        BaseOperator,
        ExternalTaskSensor,
        MappedOperator,
        OpenLineagePlugin,
        Operator,
        TaskHolder,
        get_operator_class,
        get_task_inlets,
        get_task_outlets,
        redact_with_exclusions,
        try_import_from_string,
    )
else:
    # Airflow 2.x - use airflow2 shims
    from datahub_airflow_plugin.airflow2._shims import (  # type: ignore[assignment]
        BaseOperator,
        ExternalTaskSensor,
        MappedOperator,
        OpenLineagePlugin,
        Operator,
        TaskHolder,
        get_operator_class,
        get_task_inlets,
        get_task_outlets,
        redact_with_exclusions,
        try_import_from_string,
    )

__all__ = [
    # Airflow version and feature flags
    "AIRFLOW_VERSION",
    "IS_AIRFLOW_3_OR_HIGHER",
    "HAS_AIRFLOW_STANDALONE_CMD",
    "HAS_AIRFLOW_LISTENER_API",
    "HAS_AIRFLOW_DAG_LISTENER_API",
    "HAS_AIRFLOW_DATASET_LISTENER_API",
    # Airflow objects
    "BaseOperator",
    "Operator",
    "MappedOperator",
    "ExternalTaskSensor",
    "TaskHolder",
    "OpenLineagePlugin",
    "get_operator_class",
    "try_import_from_string",
    "redact_with_exclusions",
    "get_task_inlets",
    "get_task_outlets",
]
