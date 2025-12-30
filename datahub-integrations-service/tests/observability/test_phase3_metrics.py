"""Test Phase 3 event and asset processing metrics.

These tests verify that the metric recording functions work correctly.
Note: We can't fully test metric output in unit tests since OpenTelemetry's
global MeterProvider is set at import time. These tests verify the functions
execute without errors and the integration with ActionStageReport works.
"""

from datetime import datetime, timezone

from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
)

from datahub_integrations.actions.oss.stats_util import (
    ActionStageReport,
    EventProcessingStats,
)
from datahub_integrations.observability.event_processing_metrics import (
    record_action_executed,
    record_asset_impacted,
    record_asset_processed,
    record_event_lag,
    record_event_processed,
)


def test_record_asset_processed():
    """Test recording asset processed metrics executes without error."""
    # Just verify the function executes without raising an exception
    record_asset_processed(
        action_urn="urn:li:dataHubAction:test-action",
        action_type="TestAction",
        stage="BOOTSTRAP",
    )
    record_asset_processed(
        action_urn="urn:li:dataHubAction:test-action",
        action_type="TestAction",
        stage="BOOTSTRAP",
    )


def test_record_asset_impacted():
    """Test recording asset impacted metrics executes without error."""
    record_asset_impacted(
        action_urn="urn:li:dataHubAction:test-action",
        action_type="TestAction",
        stage="LIVE",
    )


def test_record_action_executed():
    """Test recording action executed metrics executes without error."""
    record_action_executed(
        action_urn="urn:li:dataHubAction:test-action",
        action_type="TestAction",
        stage="LIVE",
        count=5,
    )


def test_record_event_processed():
    """Test recording event processed metrics executes without error."""
    record_event_processed(
        action_urn="urn:li:dataHubAction:test-action",
        action_type="TestAction",
        stage="LIVE",
        success=True,
    )
    record_event_processed(
        action_urn="urn:li:dataHubAction:test-action",
        action_type="TestAction",
        stage="LIVE",
        success=False,
    )


def test_record_event_lag():
    """Test recording event lag metrics executes without error."""
    event_time = datetime.now(tz=timezone.utc)
    processing_time = datetime.now(tz=timezone.utc)

    record_event_lag(
        action_urn="urn:li:dataHubAction:test-action",
        action_type="TestAction",
        stage="LIVE",
        event_time=event_time,
        processing_time=processing_time,
    )


def test_action_stage_report_integration():
    """Test that ActionStageReport correctly records metrics."""
    # Create report with action context
    report = ActionStageReport(
        action_urn="urn:li:dataHubAction:test-action",
        action_type="TestAction",
        stage="BOOTSTRAP",
    )

    # Increment assets
    report.increment_assets_processed("urn:li:dataset:(test,test,PROD)")
    report.increment_assets_impacted("urn:li:dataset:(test,test,PROD)")

    # Verify counters incremented
    assert report.total_assets_processed == 1
    assert report.total_assets_impacted == 1


def test_event_processing_stats_integration():
    """Test that EventProcessingStats records metrics correctly."""
    stats = EventProcessingStats()

    # Create a mock event (simplified - just enough to test the stats)
    from unittest.mock import Mock

    event = Mock()
    event.event_type = ENTITY_CHANGE_EVENT_V1_TYPE
    event.event = Mock()

    # Record event processing
    stats.start(event)
    stats.end(
        event,
        success=True,
        action_urn="urn:li:dataHubAction:test-action",
        action_type="TestAction",
        stage="LIVE",
    )

    # Verify stats updated
    assert stats.num_events_processed == 1
    assert stats.last_event_processed_time_success is not None
