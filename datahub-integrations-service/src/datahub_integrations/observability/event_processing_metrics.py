"""Helper functions for tracking event and asset processing metrics.

This module provides utilities for tracking:
- Assets processed and impacted per action
- Actions executed (e.g., descriptions written)
- Events processed from Kafka
- Event processing lag (time between event timestamp and processing)

These metrics complement Phase 1 (execution duration) and Phase 2 (state tracking)
by providing visibility into what actions are actually doing during execution.
"""

from datetime import datetime
from typing import Optional

from opentelemetry import metrics

from datahub_integrations.observability.metrics_constants import (
    ACTIONS_ACTIONS_EXECUTED_BASE,
    ACTIONS_ASSETS_IMPACTED_BASE,
    ACTIONS_ASSETS_PROCESSED_BASE,
    ACTIONS_EVENT_LAG_BASE,
    ACTIONS_EVENTS_PROCESSED_BASE,
)

# Get meter
meter = metrics.get_meter(__name__)

# Phase 3 metrics: Event and asset processing

_assets_processed_total = meter.create_counter(
    name=ACTIONS_ASSETS_PROCESSED_BASE,
    description="Total assets processed by action",
    unit="assets",
)

_assets_impacted_total = meter.create_counter(
    name=ACTIONS_ASSETS_IMPACTED_BASE,
    description="Total assets impacted (modified) by action",
    unit="assets",
)

_actions_executed_total = meter.create_counter(
    name=ACTIONS_ACTIONS_EXECUTED_BASE,
    description="Total individual actions executed (e.g., descriptions written)",
    unit="actions",
)

_events_processed_total = meter.create_counter(
    name=ACTIONS_EVENTS_PROCESSED_BASE,
    description="Total Kafka events processed by action",
    unit="events",
)

_event_processing_lag = meter.create_up_down_counter(
    name=ACTIONS_EVENT_LAG_BASE,
    description="Event processing lag in seconds (event timestamp to processing time)",
    unit="seconds",
)


def record_asset_processed(
    action_urn: str,
    action_type: str,
    stage: str,
) -> None:
    """Record that an asset was processed.

    Args:
        action_urn: Unique identifier for the action
        action_type: Type of action (e.g., DocPropagationAction)
        stage: Current stage (BOOTSTRAP, LIVE, ROLLBACK)
    """
    labels = {
        "action_urn": action_urn,
        "action_type": action_type,
        "stage": stage,
    }
    _assets_processed_total.add(1, attributes=labels)


def record_asset_impacted(
    action_urn: str,
    action_type: str,
    stage: str,
) -> None:
    """Record that an asset was impacted (modified).

    Args:
        action_urn: Unique identifier for the action
        action_type: Type of action
        stage: Current stage (BOOTSTRAP, LIVE, ROLLBACK)
    """
    labels = {
        "action_urn": action_urn,
        "action_type": action_type,
        "stage": stage,
    }
    _assets_impacted_total.add(1, attributes=labels)


def record_action_executed(
    action_urn: str,
    action_type: str,
    stage: str,
    count: int = 1,
) -> None:
    """Record that individual action(s) were executed.

    For example, in doc propagation, this tracks individual descriptions written.

    Args:
        action_urn: Unique identifier for the action
        action_type: Type of action
        stage: Current stage (BOOTSTRAP, LIVE, ROLLBACK)
        count: Number of actions executed (default 1)
    """
    labels = {
        "action_urn": action_urn,
        "action_type": action_type,
        "stage": stage,
    }
    _actions_executed_total.add(count, attributes=labels)


def record_event_processed(
    action_urn: str,
    action_type: str,
    stage: str,
    success: bool,
) -> None:
    """Record that a Kafka event was processed.

    Args:
        action_urn: Unique identifier for the action
        action_type: Type of action
        stage: Current stage (BOOTSTRAP, LIVE, ROLLBACK)
        success: Whether processing succeeded
    """
    labels = {
        "action_urn": action_urn,
        "action_type": action_type,
        "stage": stage,
        "success": str(success).lower(),
    }
    _events_processed_total.add(1, attributes=labels)


def record_event_lag(
    action_urn: str,
    action_type: str,
    stage: str,
    event_time: datetime,
    processing_time: Optional[datetime] = None,
) -> None:
    """Record event processing lag.

    Calculates the time between when the event occurred (event timestamp)
    and when it was processed.

    Args:
        action_urn: Unique identifier for the action
        action_type: Type of action
        stage: Current stage (BOOTSTRAP, LIVE, ROLLBACK)
        event_time: When the event occurred (from Kafka)
        processing_time: When the event was processed (defaults to now)
    """
    if processing_time is None:
        processing_time = datetime.now(event_time.tzinfo)

    lag_seconds = (processing_time - event_time).total_seconds()

    labels = {
        "action_urn": action_urn,
        "action_type": action_type,
        "stage": stage,
    }
    _event_processing_lag.add(int(lag_seconds), attributes=labels)
