"""Stable import surface for Airflow shims. Delegates to the airflow3 module."""

from datahub_airflow_plugin.airflow3._shims import (
    BaseOperator,
    ExternalTaskSensor,
    MappedOperator,
    OpenLineagePlugin,
    Operator,
    get_task_inlets,
    get_task_outlets,
    redact_with_exclusions,
)

__all__ = [
    "BaseOperator",
    "Operator",
    "MappedOperator",
    "ExternalTaskSensor",
    "OpenLineagePlugin",
    "redact_with_exclusions",
    "get_task_inlets",
    "get_task_outlets",
]
