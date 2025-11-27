"""
Shims for apache-airflow-providers-openlineage package.
This module is used when apache-airflow-providers-openlineage is installed
(Airflow 2.10+ with native OpenLineage provider).
"""

from typing import List

from airflow.providers.openlineage.plugins.openlineage import (
    OpenLineageProviderPlugin as OpenLineagePlugin,
)
from airflow.providers.openlineage.utils.utils import (
    get_operator_class,
    try_import_from_string,
)

# Provider package doesn't have TaskHolder, use dict as placeholder (similar to Airflow 3)
TaskHolder = dict  # type: ignore[misc,assignment]


# Provider package doesn't have redact_with_exclusions
# Define a simple passthrough function as fallback
def redact_with_exclusions(source: str, exclusions: List[str]) -> str:
    """
    Fallback for redact_with_exclusions when using provider package.

    The provider package doesn't include this function, but it's only used
    for logging/debugging purposes in the DataHub plugin, so a passthrough
    is acceptable.
    """
    return source


__all__ = [
    "TaskHolder",
    "OpenLineagePlugin",
    "get_operator_class",
    "redact_with_exclusions",
    "try_import_from_string",
]
