"""
Utilities for extracting attributes from Airflow 3 TaskInstance/RuntimeTaskInstance objects.
"""

import logging
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from airflow.models import TaskInstance

logger = logging.getLogger(__name__)


def _get_duration_attribute(ti: "TaskInstance") -> Dict[str, str]:
    """Compute duration from end_date - start_date (RuntimeTaskInstance has no `duration` attribute)."""
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
    """Operator name lives on `ti.operator` in some paths and on `ti.task.__class__.__name__` in RuntimeTaskInstance."""
    if hasattr(ti, "operator"):
        operator_from_db = str(ti.operator)
        logger.debug(
            f"Operator from ti.operator: {operator_from_db}, "
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
    """Collect end_date and logical_date when present.

    `execution_date` was removed in Airflow 3; the TaskInstance / RuntimeTaskInstance
    only exposes `logical_date` (via association_proxy to dag_run).
    """
    attributes = {}

    if hasattr(ti, "end_date"):
        attributes["end_date"] = str(ti.end_date)

    if hasattr(ti, "logical_date"):
        attributes["logical_date"] = str(ti.logical_date)

    return attributes


def get_task_instance_attributes(ti: "TaskInstance") -> Dict[str, str]:
    """Extract attributes from an Airflow 3 TaskInstance / RuntimeTaskInstance."""
    attributes = {}

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

    attributes.update(_get_duration_attribute(ti))
    attributes.update(_get_date_attributes(ti))
    attributes.update(_get_operator_attribute(ti))

    if hasattr(ti, "max_tries"):
        attributes["max_tries"] = str(ti.max_tries)
    if hasattr(ti, "external_executor_id"):
        attributes["external_executor_id"] = str(ti.external_executor_id)
    if hasattr(ti, "priority_weight"):
        attributes["priority_weight"] = str(ti.priority_weight)
    if hasattr(ti, "log_url"):
        attributes["log_url"] = ti.log_url

    return attributes


__all__ = [
    "get_task_instance_attributes",
]
