"""
Shims for apache-airflow-providers-openlineage package.
This module is used when apache-airflow-providers-openlineage is installed
(Airflow 2.10+ with native OpenLineage provider).
"""

from airflow.providers.openlineage.plugins.openlineage import (
    OpenLineageProviderPlugin as OpenLineagePlugin,
)
from airflow.providers.openlineage.utils.utils import (
    get_operator_class,
    try_import_from_string,
)

# Provider package doesn't have TaskHolder - not needed with modern Airflow
# The task_instance.task attribute is directly available in Airflow 2.10+
TaskHolder = None  # type: ignore[misc,assignment]

# Provider package doesn't have redact_with_exclusions - not needed
# This was only used for logging/debugging in the legacy package
redact_with_exclusions = None  # type: ignore[misc,assignment]

__all__ = [
    "TaskHolder",
    "OpenLineagePlugin",
    "get_operator_class",
    "redact_with_exclusions",
    "try_import_from_string",
]
