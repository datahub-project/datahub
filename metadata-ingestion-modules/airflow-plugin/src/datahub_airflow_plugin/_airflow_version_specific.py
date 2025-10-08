"""
Version-specific utilities for Airflow 2 vs 3 compatibility.
This module provides clean abstractions for version-specific behavior.
"""

import logging
from typing import TYPE_CHECKING, Dict

import airflow
import packaging.version

if TYPE_CHECKING:
    from airflow.models import TaskInstance

logger = logging.getLogger(__name__)

# Version detection
AIRFLOW_VERSION = packaging.version.parse(airflow.__version__)
IS_AIRFLOW_3_OR_HIGHER = AIRFLOW_VERSION >= packaging.version.parse("3.0.0")


def get_task_instance_attributes(ti: "TaskInstance") -> Dict[str, str]:  # noqa: C901
    """
    Extract attributes from a TaskInstance in a version-compatible way.

    Airflow 3.0 introduced RuntimeTaskInstance which has different attributes
    than Airflow 2.x TaskInstance.

    Returns a dict of attribute name -> string value.
    """
    attributes = {}

    # Common attributes (both Airflow 2 and 3)
    if hasattr(ti, "run_id"):
        attributes["run_id"] = str(ti.run_id)
    if hasattr(ti, "start_date") and ti.start_date:
        attributes["start_date"] = str(ti.start_date)
    if hasattr(ti, "try_number"):
        attributes["try_number"] = str(ti.try_number - 1)
    if hasattr(ti, "state"):
        attributes["state"] = str(ti.state)
    if hasattr(ti, "task_id"):
        attributes["task_id"] = str(ti.task_id)
    if hasattr(ti, "dag_id"):
        attributes["dag_id"] = str(ti.dag_id)

    # Airflow 2.x has duration as a direct attribute
    # Airflow 3.x doesn't have it, so we calculate it from end_date - start_date
    if hasattr(ti, "duration") and ti.duration is not None:
        attributes["duration"] = str(ti.duration)
    elif (
        hasattr(ti, "end_date")
        and ti.end_date
        and hasattr(ti, "start_date")
        and ti.start_date
    ):
        # Calculate duration for Airflow 3.x
        try:
            duration_seconds = (ti.end_date - ti.start_date).total_seconds()
            attributes["duration"] = str(duration_seconds)
        except Exception as e:
            logger.debug(f"Could not calculate duration: {e}")

    # end_date is available in both versions (when task is complete)
    if hasattr(ti, "end_date") and ti.end_date:
        attributes["end_date"] = str(ti.end_date)

    # execution_date was renamed to logical_date in Airflow 3.0
    if hasattr(ti, "execution_date"):
        attributes["execution_date"] = str(ti.execution_date)
    elif hasattr(ti, "logical_date"):
        attributes["logical_date"] = str(ti.logical_date)

    if hasattr(ti, "max_tries"):
        attributes["max_tries"] = str(ti.max_tries)

    if hasattr(ti, "external_executor_id") and ti.external_executor_id:
        attributes["external_executor_id"] = str(ti.external_executor_id)

    # operator field: In Airflow 2.x it's a direct attribute
    # In Airflow 3.x, we can get it from ti.task.__class__.__name__
    if hasattr(ti, "operator"):
        attributes["operator"] = str(ti.operator)
    elif hasattr(ti, "task") and ti.task is not None:
        try:
            attributes["operator"] = ti.task.__class__.__name__
        except Exception as e:
            logger.debug(f"Could not get operator name from task: {e}")

    if hasattr(ti, "priority_weight"):
        attributes["priority_weight"] = str(ti.priority_weight)

    if hasattr(ti, "log_url"):
        attributes["log_url"] = ti.log_url

    return attributes


def get_airflow_compatible_dag_kwargs(**kwargs):  # type: ignore[no-untyped-def]
    """
    Get DAG kwargs that are compatible with current Airflow version.

    Handles differences between Airflow 2.x and 3.x:
    - schedule_interval -> schedule in Airflow 3.0
    - default_view removed in Airflow 3.0
    - start_date handling
    """
    compatible_kwargs = kwargs.copy()

    if IS_AIRFLOW_3_OR_HIGHER:
        # Airflow 3.0 renamed schedule_interval to schedule
        if "schedule_interval" in compatible_kwargs:
            compatible_kwargs["schedule"] = compatible_kwargs.pop("schedule_interval")

        # Airflow 3.0 removed default_view
        if "default_view" in compatible_kwargs:
            del compatible_kwargs["default_view"]

    return compatible_kwargs  # type: ignore[no-any-return]


def days_ago(n: int):  # type: ignore[no-untyped-def]
    """
    Compatibility helper for days_ago which was removed in Airflow 3.0.

    In Airflow 2.x, use airflow.utils.dates.days_ago()
    In Airflow 3.0, use datetime.datetime - datetime.timedelta
    """
    from datetime import datetime, timedelta, timezone

    if IS_AIRFLOW_3_OR_HIGHER:
        # Airflow 3.0: use datetime directly
        return datetime.now(timezone.utc) - timedelta(days=n)
    else:
        # Airflow 2.x: use the official helper
        from airflow.utils.dates import (  # type: ignore[attr-defined]
            days_ago as airflow_days_ago,
        )

        return airflow_days_ago(n)  # type: ignore[no-any-return]


__all__ = [
    "AIRFLOW_VERSION",
    "IS_AIRFLOW_3_OR_HIGHER",
    "get_task_instance_attributes",
    "get_airflow_compatible_dag_kwargs",
    "days_ago",
]
