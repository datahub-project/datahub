"""
Airflow 3.x specific shims and imports.
Clean, simple imports without cross-version compatibility complexity.
"""

from typing import List, Union

# MappedOperator type alias - try airflow.models.mappedoperator first (Airflow 3.0-3.1)
# Fall back to airflow.serialization.definitions.mappedoperator for Airflow 3.2+
try:
    from airflow.models.mappedoperator import MappedOperator
except ModuleNotFoundError:
    from airflow.serialization.definitions.mappedoperator import (  # type: ignore[no-redef]
        SerializedMappedOperator as MappedOperator,
    )

from airflow.sdk import BaseOperator

# Operator type represents any operator (regular or mapped)
Operator = Union[BaseOperator, MappedOperator]

# ExternalTaskSensor import - uses standard provider in Airflow 3.x
try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
except ImportError:
    # Fallback for earlier Airflow 3 versions
    try:
        from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore[no-redef]  # noqa: I001
    except ImportError:
        from airflow.sensors.external_task_sensor import (  # type: ignore[no-redef]
            ExternalTaskSensor,
        )

# OpenLineage imports for Airflow 3.x (native provider)
try:
    from airflow.providers.openlineage.plugins.openlineage import (
        OpenLineageProviderPlugin as OpenLineagePlugin,
    )

    def redact_with_exclusions(source: dict) -> dict:
        """Compatibility shim - native provider doesn't expose this."""
        return source

except ImportError:
    # Native provider not installed
    OpenLineagePlugin = None  # type: ignore

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
    "ExternalTaskSensor",
    "OpenLineagePlugin",
    "redact_with_exclusions",
    "get_task_inlets",
    "get_task_outlets",
]
