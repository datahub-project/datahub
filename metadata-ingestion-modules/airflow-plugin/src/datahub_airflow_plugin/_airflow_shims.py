"""Stable import surface for Airflow shims. Delegates to the airflow3 module."""

from datahub_airflow_plugin._airflow_version_specific import AIRFLOW_VERSION
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

__all__ = [
    "AIRFLOW_VERSION",
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
