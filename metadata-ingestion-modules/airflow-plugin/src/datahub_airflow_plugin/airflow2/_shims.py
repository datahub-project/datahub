"""
Airflow 2.x specific shims and imports.
Clean, simple imports without cross-version compatibility complexity.
"""

import warnings
from typing import List

import airflow
import packaging.version
from airflow.models.baseoperator import BaseOperator

# MappedOperator type alias - try airflow.models.mappedoperator first
# Fall back to airflow.serialization.definitions.mappedoperator for Airflow 3.2+
try:
    from airflow.models.mappedoperator import MappedOperator
except ModuleNotFoundError:
    from airflow.serialization.definitions.mappedoperator import (  # type: ignore[no-redef]
        SerializedMappedOperator as MappedOperator,
    )

# Operator type alias - try airflow.models.operator.Operator first (Airflow 2.5-2.9)
# Fall back to BaseOperator for Airflow 2.10+ (transitioning to Airflow 3.x)
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

# OpenLineage package selection for Airflow 2.x.
#
# Airflow 2.7 introduced the native apache-airflow-providers-openlineage package.
# The older openlineage-airflow standalone package still works on Airflow 2.7+ but
# emits a deprecation warning. On Airflow 2.7+ we therefore prefer the native
# provider when it is available.
#
# Priority:
#   Airflow >= 2.7 + native provider installed  → native provider (no warnings)
#   Airflow >= 2.7 + only legacy installed       → legacy (emit our own deprecation)
#   Airflow <  2.7 + legacy installed            → legacy (correct choice)
#   neither installed                            → provider path (fails gracefully)

_AIRFLOW_VERSION = packaging.version.parse(airflow.__version__)
_IS_AIRFLOW_27_OR_HIGHER = _AIRFLOW_VERSION >= packaging.version.parse("2.7.0")

_NATIVE_PROVIDER_AVAILABLE = False
try:
    from airflow.providers.openlineage.plugins.openlineage import (  # noqa: F401
        OpenLineageProviderPlugin as _,
    )

    _NATIVE_PROVIDER_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    pass

_LEGACY_AVAILABLE = False
try:
    import openlineage.airflow  # noqa: F401

    _LEGACY_AVAILABLE = True
except ImportError:
    pass

# Determine which package to use.
if _IS_AIRFLOW_27_OR_HIGHER and _NATIVE_PROVIDER_AVAILABLE:
    # Preferred path for Airflow 2.7+: native provider, no deprecation warning.
    _USE_LEGACY_OPENLINEAGE = False
elif _LEGACY_AVAILABLE:
    if _IS_AIRFLOW_27_OR_HIGHER:
        # Legacy package emits its own warning; surface a more actionable one.
        warnings.warn(
            "The openlineage-airflow package is deprecated for Airflow 2.7 and later. "
            "Install the native provider instead: "
            "pip install 'acryl-datahub-airflow-plugin[airflow27]'. "
            "See https://airflow.apache.org/docs/apache-airflow-providers-openlineage "
            "for details.",
            DeprecationWarning,
            stacklevel=2,
        )
    _USE_LEGACY_OPENLINEAGE = True
else:
    # Neither package installed — fall through to provider shims (will fail at call-site
    # if OpenLineage functionality is actually used, with a clear ImportError).
    _USE_LEGACY_OPENLINEAGE = False

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
