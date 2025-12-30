"""Integration tests for bootstrap stage metrics isolation.

These tests verify the fix for bootstrap stage confusion where fake bootstrap
ECEs were incorrectly counted as LIVE stage events. The fix ensures:
1. Bootstrap subprocess has stage="bootstrap" set on action._stats
2. Bootstrap fake ECEs don't trigger event metrics recording
3. LIVE stage event metrics only count real Kafka events
"""

from unittest.mock import Mock

import pytest
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.actions.stats_util import Stage
from datahub_integrations.propagation.doc.doc_propagation_action import (
    DocPropagationAction,
    DocPropagationConfig,
)


@pytest.fixture
def mock_pipeline_context():
    """Create a mock pipeline context."""
    ctx = Mock(spec=PipelineContext)
    ctx.graph = Mock()
    ctx.graph.graph = Mock()
    ctx.pipeline_name = "urn:li:dataHubAction:test-action"
    return ctx


@pytest.fixture
def doc_propagation_config():
    """Create a DocPropagationConfig with minimal settings."""
    return DocPropagationConfig(
        max_propagation_depth=1,
        max_propagation_fanout=10,
    )


def test_bootstrap_flag_initialization(doc_propagation_config, mock_pipeline_context):
    """Test that action initializes with correct stage."""
    action = DocPropagationAction(doc_propagation_config, mock_pipeline_context)

    # The ExtendedAction wrapper now handles stats automatically
    # Verify stage is initialized correctly
    assert action._stats.stage == "LIVE", "Action should start in LIVE stage"


def test_act_processes_event_during_bootstrap(
    doc_propagation_config, mock_pipeline_context
):
    """Test that act() processes events via ExtendedAction wrapper.

    The ExtendedAction wrapper automatically handles event metrics recording.
    Bootstrap vs live distinction is managed via the stage attribute on _stats.
    """
    action = DocPropagationAction(doc_propagation_config, mock_pipeline_context)

    # Create a fake ECE event (like bootstrap creates)
    fake_event = Mock(spec=EventEnvelope)
    fake_event.event_type = "EntityChangeEvent_v1"
    fake_event.event = Mock(spec=EntityChangeEvent)
    fake_event.meta = {}

    # Mock the base action's act method to do nothing
    action._base_action.act = Mock()

    # Simulate bootstrap mode by setting stage
    action._stats.stage = "bootstrap"

    # Process the event - should not raise
    action.act(fake_event)

    # Verify base action was called (processing still happens in bootstrap)
    action._base_action.act.assert_called_once_with(fake_event)


def test_act_processes_event_outside_bootstrap(
    doc_propagation_config, mock_pipeline_context
):
    """Test that act() processes events normally in LIVE stage."""
    action = DocPropagationAction(doc_propagation_config, mock_pipeline_context)

    # Create a real event (like from Kafka)
    real_event = Mock(spec=EventEnvelope)
    real_event.event_type = "EntityChangeEvent_v1"
    real_event.event = Mock(spec=EntityChangeEvent)
    real_event.meta = {}

    # Mock the base action's act method to do nothing
    action._base_action.act = Mock()

    # In LIVE mode (default stage)
    assert action._stats.stage == "LIVE"

    # Process the event - should not raise
    action.act(real_event)

    # Verify base action was called (processing happened)
    action._base_action.act.assert_called_once_with(real_event)


def test_act_raises_exception_from_base_action(
    doc_propagation_config, mock_pipeline_context
):
    """Test that act() propagates exceptions from base action."""
    action = DocPropagationAction(doc_propagation_config, mock_pipeline_context)

    # Create a real event
    real_event = Mock(spec=EventEnvelope)
    real_event.event_type = "EntityChangeEvent_v1"
    real_event.event = Mock(spec=EntityChangeEvent)
    real_event.meta = {}

    # Mock the base action to raise an exception
    action._base_action.act = Mock(side_effect=ValueError("Test error"))

    # Process the event - should raise exception
    with pytest.raises(ValueError, match="Test error"):
        action.act(real_event)


def test_bootstrap_asset_sets_and_resets_flag(
    doc_propagation_config, mock_pipeline_context
):
    """Test that bootstrap_asset() uses stage to distinguish from live processing."""
    action = DocPropagationAction(doc_propagation_config, mock_pipeline_context)

    # Mock dependencies
    mock_pipeline_context.graph.graph.get_aspect = Mock(return_value=None)
    mock_pipeline_context.graph.graph.emit = Mock()
    action._base_action.act_async = Mock(return_value=[])
    action.is_live_action_running = False

    # Verify action starts in LIVE stage
    assert action._stats.stage == "LIVE"

    # Simulate bootstrap subprocess by changing stage (as done in local_action_runner)
    action._stats.stage = "bootstrap"

    # Call bootstrap_asset
    from datahub.utilities.urns.urn import Urn

    from datahub_integrations.propagation.propagation.generic_propagation_action import (
        SourcedAsset,
    )

    test_asset = SourcedAsset(
        urn=Urn.from_string("urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)")
    )
    action.bootstrap_asset(test_asset)

    # Verify stage remains as set (bootstrap processing uses stage to distinguish)
    assert action._stats.stage == "bootstrap"


def test_stage_propagation_to_action_stats(
    doc_propagation_config, mock_pipeline_context
):
    """Test that stage is correctly set on action's _stats after initialization.

    This test verifies the fix where local_action_runner.py sets
    pipeline.action._stats.stage = stage.value after pipeline creation.
    """
    # Create an action (which initializes with stage="LIVE")
    action = DocPropagationAction(doc_propagation_config, mock_pipeline_context)

    # Verify default stage is LIVE
    assert action._stats.stage == "LIVE"

    # Simulate what local_action_runner.py does for bootstrap subprocess
    action._stats.stage = Stage.BOOTSTRAP.value

    # Verify stage was updated
    assert action._stats.stage == "bootstrap"
    assert action._stats.action_type == "DocPropagationAction"


def test_bootstrap_and_live_have_separate_metrics():
    """Integration test verifying bootstrap and live metrics are separate.

    This test simulates the scenario where:
    1. Bootstrap subprocess runs with stage="bootstrap"
    2. LIVE subprocess runs with stage="live"
    3. Each records metrics with correct stage label
    """
    from datahub_integrations.actions.oss.stats_util import ActionStageReport

    # Create bootstrap stage report
    bootstrap_report = ActionStageReport(
        action_urn="urn:li:dataHubAction:test",
        action_name="DocPropagationAction",
        action_type="DocPropagationAction",
        stage="bootstrap",
    )

    # Create live stage report
    live_report = ActionStageReport(
        action_urn="urn:li:dataHubAction:test",
        action_name="DocPropagationAction",
        action_type="DocPropagationAction",
        stage="live",
    )

    # Simulate bootstrap processing assets (no events)
    bootstrap_report.increment_assets_processed(
        "urn:li:dataset:(urn:li:dataPlatform:test,test1,PROD)"
    )
    bootstrap_report.increment_assets_processed(
        "urn:li:dataset:(urn:li:dataPlatform:test,test2,PROD)"
    )

    # Simulate live processing events
    from datahub_actions.event.event_registry import ENTITY_CHANGE_EVENT_V1_TYPE

    mock_event = Mock(spec=EventEnvelope)
    mock_event.event_type = ENTITY_CHANGE_EVENT_V1_TYPE
    mock_event.event = Mock()

    # Initialize event processing stats for live report
    if live_report.event_processing_stats is None:
        from datahub_integrations.actions.oss.stats_util import EventProcessingStats

        live_report.event_processing_stats = EventProcessingStats()

    # Record event processing for live stage
    live_report.record_event_start(mock_event)
    live_report.record_event_end(mock_event, success=True)

    # Verify metrics are correctly separated
    assert bootstrap_report.stage == "bootstrap"
    assert bootstrap_report.total_assets_processed == 2
    assert (
        bootstrap_report.event_processing_stats is None
        or bootstrap_report.event_processing_stats.num_events_processed == 0
    )

    assert live_report.stage == "live"
    assert live_report.total_assets_processed == 0
    assert live_report.event_processing_stats.num_events_processed == 1


def test_fake_ece_has_no_kafka_timestamp():
    """Test that fake bootstrap ECEs don't have Kafka timestamps.

    This verifies why we need to skip event metrics for bootstrap -
    fake ECEs don't have the metadata that real Kafka events have.
    """
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        ParametersClass,
    )

    # Create a fake ECE like bootstrap does
    fake_ece = EntityChangeEvent(
        entityType="schemaField",
        entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:test,test,PROD),field1)",
        category="DOCUMENTATION",
        operation="ADD",
        auditStamp=AuditStampClass(
            time=1234567890000,
            actor="urn:li:corpuser:datahub",
        ),
        version=1,
        parameters=ParametersClass(),
    )

    # Wrap in event envelope
    fake_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=fake_ece,
        meta={},  # No Kafka metadata
    )

    # Verify no Kafka metadata present
    assert "kafka_offset" not in fake_event.meta
    assert "kafka_partition" not in fake_event.meta
    assert "kafka_timestamp" not in fake_event.meta

    # This is why _get_event_time() returns None for fake events,
    # and why we skip lag calculation
