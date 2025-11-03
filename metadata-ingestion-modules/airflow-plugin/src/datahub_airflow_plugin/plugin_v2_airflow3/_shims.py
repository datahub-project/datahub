"""
Airflow 3.x specific shims and imports.
Clean, simple imports without cross-version compatibility complexity.
"""

from typing import List

# Airflow 3.x SDK imports
try:
    from airflow.sdk.bases.operator import BaseOperator
except (ModuleNotFoundError, ImportError):
    from airflow.models.baseoperator import BaseOperator  # type: ignore

try:
    from airflow.sdk.types import Operator
except (ModuleNotFoundError, ImportError):
    Operator = BaseOperator  # type: ignore

# MappedOperator may or may not exist in Airflow 3.x
try:
    from airflow.models.mappedoperator import MappedOperator
except (ModuleNotFoundError, ImportError):
    MappedOperator = None  # type: ignore

# OpenLineage imports for Airflow 3.x (native provider)
try:
    from airflow.providers.openlineage.plugins.openlineage import (
        OpenLineageProviderPlugin as OpenLineagePlugin,
    )
    from airflow.providers.openlineage.utils.utils import (
        get_operator_class,
        try_import_from_string,
    )

    # Native provider doesn't need TaskHolder, use dict as placeholder
    TaskHolder = dict  # type: ignore

    def redact_with_exclusions(source: dict) -> dict:
        """Compatibility shim - native provider doesn't expose this."""
        return source

except ImportError:
    # Native provider not installed
    TaskHolder = dict  # type: ignore
    OpenLineagePlugin = None  # type: ignore
    get_operator_class = None  # type: ignore
    try_import_from_string = None  # type: ignore

    def redact_with_exclusions(source: dict) -> dict:
        return source


def get_task_inlets(operator: "Operator") -> List:
    """Get task inlets for Airflow 3.x."""
    if hasattr(operator, "get_inlet_defs"):
        return operator.get_inlet_defs()  # type: ignore[attr-defined]
    return operator.inlets or []


def get_task_outlets(operator: "Operator") -> List:
    """Get task outlets for Airflow 3.x."""
    if hasattr(operator, "get_outlet_defs"):
        return operator.get_outlet_defs()
    return operator.outlets or []


__all__ = [
    "BaseOperator",
    "Operator",
    "MappedOperator",
    "TaskHolder",
    "OpenLineagePlugin",
    "get_operator_class",
    "try_import_from_string",
    "redact_with_exclusions",
    "get_task_inlets",
    "get_task_outlets",
]
