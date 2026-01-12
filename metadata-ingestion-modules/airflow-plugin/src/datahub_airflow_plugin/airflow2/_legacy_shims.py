"""
Shims for legacy openlineage-airflow package.
This module is used when openlineage-airflow is installed (Airflow 2.x with legacy OpenLineage).
"""

from openlineage.airflow.listener import TaskHolder
from openlineage.airflow.plugin import OpenLineagePlugin
from openlineage.airflow.utils import (
    get_operator_class,
    redact_with_exclusions,
    try_import_from_string,
)

__all__ = [
    "TaskHolder",
    "OpenLineagePlugin",
    "get_operator_class",
    "redact_with_exclusions",
    "try_import_from_string",
]
