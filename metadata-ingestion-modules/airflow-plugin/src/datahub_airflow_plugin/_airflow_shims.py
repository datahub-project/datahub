# mypy: disable-error-code="no-redef, attr-defined"
from typing import List

import airflow.version
import packaging.version
import pluggy

from datahub_airflow_plugin._airflow_compat import AIRFLOW_PATCHED

# Determine Airflow version early for conditional imports
AIRFLOW_VERSION = packaging.version.parse(airflow.version.version)

# BaseOperator import - prefer new location in Airflow 3.x
try:
    from airflow.sdk.bases.operator import BaseOperator
except (ModuleNotFoundError, ImportError):
    from airflow.models.baseoperator import BaseOperator  # type: ignore

# Operator type alias and MappedOperator
try:
    # Airflow 3.x moved Operator to sdk.types
    from airflow.sdk.types import Operator

    try:
        from airflow.models.mappedoperator import MappedOperator
    except (ModuleNotFoundError, ImportError):
        MappedOperator = None  # type: ignore
except (ModuleNotFoundError, ImportError):
    try:
        # Airflow 2.x location
        from airflow.models.mappedoperator import MappedOperator
        from airflow.models.operator import Operator
    except (ModuleNotFoundError, ImportError):
        # Operator isn't a real class, but rather a type alias defined
        # as the union of BaseOperator and MappedOperator.
        # Since older versions of Airflow don't have MappedOperator, we can just use BaseOperator.
        try:
            from airflow.models.mappedoperator import MappedOperator
        except (ModuleNotFoundError, ImportError):
            MappedOperator = None  # type: ignore
        Operator = BaseOperator  # type: ignore

# ExternalTaskSensor import - prefer standard provider in Airflow 3.x
try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
except (ModuleNotFoundError, ImportError):
    try:
        from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore
    except ImportError:
        from airflow.sensors.external_task_sensor import (
            ExternalTaskSensor,  # type: ignore
        )

assert AIRFLOW_PATCHED

# Approach suggested by https://stackoverflow.com/a/11887885/5004662.
# AIRFLOW_VERSION already defined above for conditional imports
PLUGGY_VERSION = packaging.version.parse(pluggy.__version__)
HAS_AIRFLOW_STANDALONE_CMD = AIRFLOW_VERSION >= packaging.version.parse("2.2.0.dev0")
HAS_AIRFLOW_LISTENER_API = AIRFLOW_VERSION >= packaging.version.parse("2.3.0.dev0")
HAS_AIRFLOW_DAG_LISTENER_API = AIRFLOW_VERSION >= packaging.version.parse("2.5.0.dev0")
HAS_AIRFLOW_DATASET_LISTENER_API = AIRFLOW_VERSION >= packaging.version.parse(
    "2.8.0.dev0"
)
NEEDS_AIRFLOW_LISTENER_MODULE = AIRFLOW_VERSION < packaging.version.parse(
    "2.5.0.dev0"
) or PLUGGY_VERSION <= packaging.version.parse("1.0.0")

# OpenLineage compatibility - use native provider on Airflow 2.7+, old package otherwise
USE_NATIVE_OPENLINEAGE_PROVIDER = AIRFLOW_VERSION >= packaging.version.parse(
    "2.7.0.dev0"
)

if USE_NATIVE_OPENLINEAGE_PROVIDER:
    # Airflow 2.7+ with native OpenLineage provider
    try:
        from airflow.providers.openlineage.utils.utils import (
            get_operator_class,
            try_import_from_string,
        )

        # For compatibility, create TaskHolder alias
        # In native provider, we don't need TaskHolder as it uses different architecture
        TaskHolder = dict  # type: ignore

        def redact_with_exclusions(source: dict) -> dict:
            """Compatibility shim for redact_with_exclusions."""
            # Native provider doesn't expose this, so we just return the source as-is
            return source

        from airflow.providers.openlineage.plugins.openlineage import (
            OpenLineageProviderPlugin as OpenLineagePlugin,
        )

        HAS_OPENLINEAGE = True
    except ImportError:
        # Native provider not installed
        HAS_OPENLINEAGE = False
        TaskHolder = dict  # type: ignore
        OpenLineagePlugin = None  # type: ignore
        get_operator_class = None  # type: ignore
        try_import_from_string = None  # type: ignore

        def redact_with_exclusions(source: dict) -> dict:
            return source
else:
    # Airflow < 2.7 with old openlineage-airflow package
    try:
        from openlineage.airflow.listener import TaskHolder
        from openlineage.airflow.plugin import OpenLineagePlugin
        from openlineage.airflow.utils import (
            get_operator_class,
            redact_with_exclusions,
            try_import_from_string,
        )

        HAS_OPENLINEAGE = True
    except ImportError:
        # Old package not installed
        HAS_OPENLINEAGE = False
        TaskHolder = dict  # type: ignore
        OpenLineagePlugin = None  # type: ignore
        get_operator_class = None  # type: ignore
        try_import_from_string = None  # type: ignore

        def redact_with_exclusions(source: dict) -> dict:
            return source


def get_task_inlets(operator: "Operator") -> List:
    # From Airflow 2.4 _inlets is dropped and inlets used consistently. Earlier it was not the case, so we have to stick there to _inlets
    if hasattr(operator, "_inlets"):
        return operator._inlets  # type: ignore[attr-defined, union-attr]
    if hasattr(operator, "get_inlet_defs"):
        return operator.get_inlet_defs()  # type: ignore[attr-defined]
    return operator.inlets or []


def get_task_outlets(operator: "Operator") -> List:
    # From Airflow 2.4 _outlets is dropped and inlets used consistently. Earlier it was not the case, so we have to stick there to _outlets
    # We have to use _outlets because outlets is empty in Airflow < 2.4.0
    if hasattr(operator, "_outlets"):
        return operator._outlets  # type: ignore[attr-defined, union-attr]
    if hasattr(operator, "get_outlet_defs"):
        return operator.get_outlet_defs()
    return operator.outlets or []


__all__ = [
    "AIRFLOW_VERSION",
    "BaseOperator",
    "Operator",
    "MappedOperator",
    "ExternalTaskSensor",
    "HAS_AIRFLOW_STANDALONE_CMD",
    "HAS_AIRFLOW_LISTENER_API",
    "HAS_AIRFLOW_DAG_LISTENER_API",
    "HAS_AIRFLOW_DATASET_LISTENER_API",
    "TaskHolder",
    "OpenLineagePlugin",
    "get_operator_class",
    "try_import_from_string",
    "redact_with_exclusions",
    "get_task_inlets",
    "get_task_outlets",
]
