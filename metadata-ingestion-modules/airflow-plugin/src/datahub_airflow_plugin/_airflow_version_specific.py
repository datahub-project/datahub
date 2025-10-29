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


def _get_duration_attribute(ti: "TaskInstance") -> Dict[str, str]:
    """
    Extract duration attribute, calculating it if necessary.

    Airflow 2.x has duration as a direct attribute.
    Airflow 3.x requires calculation from end_date - start_date.
    """
    if hasattr(ti, "duration"):
        return {"duration": str(ti.duration)}

    if (
        hasattr(ti, "end_date")
        and ti.end_date
        and hasattr(ti, "start_date")
        and ti.start_date
    ):
        try:
            duration_seconds = (ti.end_date - ti.start_date).total_seconds()
            return {"duration": str(duration_seconds)}
        except Exception as e:
            logger.debug(f"Could not calculate duration: {e}")

    return {}


def _get_operator_attribute(ti: "TaskInstance") -> Dict[str, str]:
    """
    Extract operator name in a version-compatible way.

    In Airflow 2.x: Available as database column attribute ti.operator
    In Airflow 3.x (RuntimeTaskInstance): Must extract from ti.task.__class__.__name__
    """
    if hasattr(ti, "operator"):
        operator_from_db = str(ti.operator)
        logger.debug(
            f"Operator from ti.operator (DB): {operator_from_db}, "
            f"hasattr task: {hasattr(ti, 'task')}, "
            f"task class: {ti.task.__class__.__name__ if hasattr(ti, 'task') and ti.task else 'N/A'}"
        )
        return {"operator": operator_from_db}

    if hasattr(ti, "task") and ti.task is not None:
        try:
            return {"operator": ti.task.__class__.__name__}
        except Exception as e:
            logger.debug(f"Could not get operator name from task: {e}")

    return {}


def _get_date_attributes(ti: "TaskInstance") -> Dict[str, str]:
    """
    Extract date-related attributes.

    Handles execution_date -> logical_date rename in Airflow 3.0.
    """
    attributes = {}

    if hasattr(ti, "end_date"):
        attributes["end_date"] = str(ti.end_date)

    if hasattr(ti, "execution_date"):
        attributes["execution_date"] = str(ti.execution_date)
    elif hasattr(ti, "logical_date"):
        attributes["logical_date"] = str(ti.logical_date)

    return attributes


def get_task_instance_attributes(ti: "TaskInstance") -> Dict[str, str]:
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

    # Complex extractions via helper functions
    attributes.update(_get_duration_attribute(ti))
    attributes.update(_get_date_attributes(ti))
    attributes.update(_get_operator_attribute(ti))

    # Optional attributes
    if hasattr(ti, "max_tries"):
        attributes["max_tries"] = str(ti.max_tries)
    if hasattr(ti, "external_executor_id"):
        attributes["external_executor_id"] = str(ti.external_executor_id)
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
