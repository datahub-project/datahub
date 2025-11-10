"""
Airflow 2.x specific shims and imports.
Clean, simple imports without cross-version compatibility complexity.
"""

from typing import List

from airflow.models.baseoperator import BaseOperator

# Operator type alias - these always exist in Airflow 2.5.0+ (plugin's minimum version)
from airflow.models.mappedoperator import MappedOperator
from airflow.models.operator import Operator

# ExternalTaskSensor import
try:
    from airflow.sensors.external_task import ExternalTaskSensor
except ImportError:
    from airflow.sensors.external_task_sensor import ExternalTaskSensor  # type: ignore

# OpenLineage imports for Airflow 2.x
from openlineage.airflow.listener import TaskHolder
from openlineage.airflow.plugin import OpenLineagePlugin
from openlineage.airflow.utils import (
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
