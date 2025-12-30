"""Helper functions for tracking action state and lifecycle metrics.

This module provides utilities for tracking:
- Currently running actions (gauge-like)
- Action state transitions (started, stopped, failed)
- Success/error counters per action

These metrics complement the execution duration/total metrics from Phase 1
by providing real-time state visibility.
"""

from opentelemetry import metrics

from datahub_integrations.observability.metrics_constants import (
    ACTIONS_ERROR_TOTAL_BASE,
    ACTIONS_INFO_BASE,
    ACTIONS_RUNNING_COUNT_BASE,
    ACTIONS_SUCCESS_TOTAL_BASE,
)

# Get meter
meter = metrics.get_meter(__name__)

# Phase 2 metrics: Action state tracking
_actions_running_count = meter.create_up_down_counter(
    name=ACTIONS_RUNNING_COUNT_BASE,
    description="Number of actions currently running by stage",
    unit="actions",
)

_actions_info = meter.create_up_down_counter(
    name=ACTIONS_INFO_BASE,
    description="Action metadata and state (info-style metric with labels)",
    unit="1",
)

_actions_success_total = meter.create_counter(
    name=ACTIONS_SUCCESS_TOTAL_BASE,
    description="Total successful action executions by action",
    unit="executions",
)

_actions_error_total = meter.create_counter(
    name=ACTIONS_ERROR_TOTAL_BASE,
    description="Total failed action executions by action",
    unit="executions",
)


def track_action_started(
    action_urn: str,
    action_type: str,
    stage: str,
    executor_id: str,
) -> None:
    """Track when an action starts running.

    Increments:
    - actions_running_count{stage} by 1
    - actions_info{action_urn, action_type, stage, state="running", executor_id} by 1
    """
    # Increment running count for this stage
    _actions_running_count.add(1, attributes={"stage": stage})

    # Add info metric with full metadata
    info_labels = {
        "action_urn": action_urn,
        "action_type": action_type,
        "stage": stage,
        "state": "running",
        "executor_id": executor_id,
    }
    _actions_info.add(1, attributes=info_labels)


def track_action_stopped(
    action_urn: str,
    action_type: str,
    stage: str,
    executor_id: str,
    state: str = "stopped",
) -> None:
    """Track when an action stops running.

    Decrements:
    - actions_running_count{stage} by 1
    - actions_info{..., state="running", ...} by 1

    Increments:
    - actions_info{..., state="stopped" or "failed", ...} by 1

    Args:
        state: Either "stopped" (graceful stop) or "failed" (error termination)
    """
    # Decrement running count for this stage
    _actions_running_count.add(-1, attributes={"stage": stage})

    # Remove old running state from info metric
    running_labels = {
        "action_urn": action_urn,
        "action_type": action_type,
        "stage": stage,
        "state": "running",
        "executor_id": executor_id,
    }
    _actions_info.add(-1, attributes=running_labels)

    # Add new stopped/failed state
    stopped_labels = {**running_labels, "state": state}
    _actions_info.add(1, attributes=stopped_labels)


def track_action_success(
    action_urn: str,
    action_type: str,
    stage: str,
) -> None:
    """Track successful action execution completion.

    Increments:
    - actions_success_total{action_urn, action_type, stage} by 1
    """
    labels = {
        "action_urn": action_urn,
        "action_type": action_type,
        "stage": stage,
    }
    _actions_success_total.add(1, attributes=labels)


def track_action_error(
    action_urn: str,
    action_type: str,
    stage: str,
    error_type: str | None = None,
) -> None:
    """Track failed action execution.

    Increments:
    - actions_error_total{action_urn, action_type, stage, error_type?} by 1

    Args:
        error_type: Optional error classification (e.g., "timeout", "connection_error")
    """
    labels = {
        "action_urn": action_urn,
        "action_type": action_type,
        "stage": stage,
    }
    if error_type:
        labels["error_type"] = error_type
    _actions_error_total.add(1, attributes=labels)
