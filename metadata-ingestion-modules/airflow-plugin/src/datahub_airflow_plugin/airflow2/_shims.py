"""
Airflow 2.x specific shims and imports.
Clean, simple imports without cross-version compatibility complexity.
"""

from typing import List

from airflow.models.baseoperator import BaseOperator

# Operator type alias - try airflow.models.operator.Operator first (Airflow 2.5-2.9)
# Fall back to BaseOperator for Airflow 2.10+ (transitioning to Airflow 3.x)
from airflow.models.mappedoperator import MappedOperator

try:
    from airflow.models.operator import Operator
except ImportError:
    # Airflow 2.10+ removed airflow.models.operator.Operator
    # Use BaseOperator as the Operator type alias instead
    Operator = BaseOperator  # type: ignore[misc]

# ExternalTaskSensor import
try:
    from airflow.sensors.external_task import ExternalTaskSensor
except ImportError:
    from airflow.sensors.external_task_sensor import ExternalTaskSensor  # type: ignore

# OpenLineage imports for Airflow 2.x
# Detect which OpenLineage package is available and load appropriate shims
_USE_LEGACY_OPENLINEAGE = False
try:
    # Check if legacy package (openlineage-airflow) is available
    import openlineage.airflow  # noqa: F401

    _USE_LEGACY_OPENLINEAGE = True
except ImportError:
    pass

if _USE_LEGACY_OPENLINEAGE:
    # Import from legacy openlineage-airflow package
    from datahub_airflow_plugin.airflow2._legacy_shims import (
        OpenLineagePlugin,
        TaskHolder,
        get_operator_class,
        redact_with_exclusions,
        try_import_from_string,
    )
else:
    # Import from native apache-airflow-providers-openlineage package
    from datahub_airflow_plugin.airflow2._provider_shims import (
        OpenLineagePlugin,
        TaskHolder,
        get_operator_class,
        redact_with_exclusions,
        try_import_from_string,
    )


def get_task_inlets(operator: "Operator") -> List:
    """Get task inlets, handling Airflow 2.x variations."""
    if hasattr(operator, "_inlets"):
        return operator._inlets  # type: ignore[attr-defined, union-attr]
    if hasattr(operator, "get_inlet_defs"):
        return operator.get_inlet_defs()  # type: ignore[attr-defined]
    return operator.inlets or []


def get_task_outlets(operator: "Operator") -> List:
    """Get task outlets, handling Airflow 2.x variations."""
    if hasattr(operator, "_outlets"):
        return operator._outlets  # type: ignore[attr-defined, union-attr]
    if hasattr(operator, "get_outlet_defs"):
        return operator.get_outlet_defs()
    return operator.outlets or []


__all__ = [
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
