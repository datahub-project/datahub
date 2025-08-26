# test_unity_catalog_propagator_action.py - Tests for UnityCatalogPropagatorAction methods

from unittest.mock import Mock, patch

import pytest
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event import Event
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.actions.oss.stats_util import EventProcessingStats
from datahub_integrations.propagation.tag.tag_propagation_action import (
    TagPropagationDirective,
)
from datahub_integrations.propagation.unity_catalog.description_sync_action import (
    UnityCatalogDescriptionSyncDirective,
)

# Import the classes under test
from datahub_integrations.propagation.unity_catalog.tag_propagator import (
    UnityCatalogPropagatorAction,
    UnityCatalogTagPropagatorConfig,
)


class TestUnityCatalogPropagatorActionProcessDirective:
    """Test suite for the process_directive method."""

    @pytest.fixture
    def mock_unity_catalog_helper(self) -> Mock:
        """Fixture providing mock Unity Catalog helper."""
        helper = Mock()
        helper.apply_description = Mock()
        helper.apply_tag = Mock()
        helper.remove_tag = Mock()
        return helper

    @pytest.fixture
    def mock_propagator_action(self, mock_unity_catalog_helper: Mock) -> Mock:
        """Fixture providing mock UnityCatalogPropagatorAction."""

        action = Mock(spec=UnityCatalogPropagatorAction)
        action.unity_catalog_helper = mock_unity_catalog_helper

        # Bind the real method to the mock
        action.process_directive = (
            UnityCatalogPropagatorAction.process_directive.__get__(action)
        )

        return action

    @pytest.fixture
    def sample_description_directive(self) -> "UnityCatalogDescriptionSyncDirective":
        """Fixture providing sample description sync directive."""
        from datahub_integrations.propagation.unity_catalog.description_sync_action import (
            UnityCatalogDescriptionSyncDirective,
        )

        return UnityCatalogDescriptionSyncDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)",
            docs="This is a test table description",
            operation="ADD",
            propagate=True,
        )

    @pytest.fixture
    def sample_tag_directive(self) -> TagPropagationDirective:
        """Fixture providing sample tag propagation directive."""
        return TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)",
            tag="urn:li:tag:environment:production",
            operation="ADD",
            propagate=True,
        )

    @pytest.fixture
    def sample_term_directive(self) -> Mock:
        """Fixture providing sample term propagation directive."""
        directive = Mock()
        directive.entity = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )
        directive.term = "urn:li:glossaryTerm:sensitive_data"
        directive.operation = "REMOVE"
        directive.propagate = True
        return directive

    def test_process_directive_description_sync(
        self,
        mock_propagator_action: Mock,
        mock_unity_catalog_helper: Mock,
        sample_description_directive: "UnityCatalogDescriptionSyncDirective",
    ) -> None:
        """Test process_directive with description sync directive."""
        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
        ) as mock_logger:
            mock_propagator_action.process_directive(sample_description_directive)

            # Verify logging
            mock_logger.info.assert_called_once_with(
                "Will add documentation on Unity Catalog urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
            )

            # Verify helper was called
            mock_unity_catalog_helper.apply_description.assert_called_once_with(
                sample_description_directive.entity, sample_description_directive.docs
            )

    def test_process_directive_description_sync_modify_operation(
        self, mock_propagator_action: Mock, mock_unity_catalog_helper: Mock
    ) -> None:
        """Test process_directive with description sync directive for MODIFY operation."""
        from datahub_integrations.propagation.unity_catalog.description_sync_action import (
            UnityCatalogDescriptionSyncDirective,
        )

        directive = UnityCatalogDescriptionSyncDirective(
            entity="urn:li:container:test_catalog",
            docs="Updated catalog description",
            operation="MODIFY",
            propagate=True,
        )

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
        ) as mock_logger:
            mock_propagator_action.process_directive(directive)

            # Verify logging with lowercase operation
            mock_logger.info.assert_called_once_with(
                "Will modify documentation on Unity Catalog urn:li:container:test_catalog"
            )

            # Verify helper was called
            mock_unity_catalog_helper.apply_description.assert_called_once_with(
                directive.entity, directive.docs
            )

    def test_process_directive_tag_add_operation(
        self,
        mock_propagator_action: Mock,
        mock_unity_catalog_helper: Mock,
        sample_tag_directive: Mock,
    ) -> None:
        """Test process_directive with tag propagation ADD directive."""
        # Mock isinstance to return True for TagPropagationDirectivex
        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
        ) as mock_logger:
            mock_propagator_action.process_directive(sample_tag_directive)

            # Verify debug logging
            mock_logger.debug.assert_called_once_with(
                "Will add urn:li:tag:environment:production on Unity Catalog urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
            )

            # Verify apply_tag was called
            mock_unity_catalog_helper.apply_tag.assert_called_once_with(
                sample_tag_directive.entity, sample_tag_directive.tag
            )

            # Verify remove_tag was not called
            mock_unity_catalog_helper.remove_tag.assert_not_called()

    def test_process_directive_tag_remove_operation(
        self, mock_propagator_action: Mock, mock_unity_catalog_helper: Mock
    ) -> None:
        """Test process_directive with tag propagation REMOVE directive."""
        # Create a REMOVE directive
        remove_directive = TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)",
            tag="urn:li:tag:environment:staging",
            operation="REMOVE",
            propagate=True,
        )

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
        ) as mock_logger:
            mock_propagator_action.process_directive(remove_directive)

            # Verify debug logging
            mock_logger.debug.assert_called_once_with(
                "Will remove urn:li:tag:environment:staging on Unity Catalog urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
            )

            # Verify remove_tag was called
            mock_unity_catalog_helper.remove_tag.assert_called_once_with(
                remove_directive.entity, remove_directive.tag
            )

            # Verify apply_tag was not called
            mock_unity_catalog_helper.apply_tag.assert_not_called()

    def test_process_directive_unknown_directive_type(
        self, mock_propagator_action: Mock, mock_unity_catalog_helper: Mock
    ) -> None:
        """Test process_directive with unknown directive type."""
        unknown_directive = Mock()
        unknown_directive.entity = "urn:li:dataset:test"

        # Mock isinstance to return False for all known types
        mock_propagator_action.process_directive(unknown_directive)

        # Verify no helper methods were called
        mock_unity_catalog_helper.apply_description.assert_not_called()
        mock_unity_catalog_helper.apply_tag.assert_not_called()
        mock_unity_catalog_helper.remove_tag.assert_not_called()

    def test_process_directive_with_exception(
        self,
        mock_propagator_action: Mock,
        mock_unity_catalog_helper: Mock,
        sample_description_directive: "UnityCatalogDescriptionSyncDirective",
    ) -> None:
        """Test process_directive handles exceptions gracefully."""
        # Make the helper raise an exception
        mock_unity_catalog_helper.apply_description.side_effect = Exception(
            "Unity Catalog error"
        )

        # Should not suppress the exception
        with pytest.raises(Exception, match="Unity Catalog error"):
            mock_propagator_action.process_directive(sample_description_directive)


class TestUnityCatalogPropagatorActionAct:
    """Test suite for the act method."""

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Fixture providing mock configuration."""

        config = Mock(spec=UnityCatalogTagPropagatorConfig)
        return config

    @pytest.fixture
    def mock_stats(self) -> Mock:
        """Fixture providing mock statistics."""

        stats = Mock()
        stats.event_processing_stats = Mock(spec=EventProcessingStats)
        return stats

    @pytest.fixture
    def mock_ctx(self) -> Mock:
        """Fixture providing mock pipeline context."""
        ctx = Mock(spec=PipelineContext)
        ctx.graph = Mock(spec=AcrylDataHubGraph)
        return ctx

    @pytest.fixture
    def mock_propagator_action(
        self, mock_config: Mock, mock_stats: Mock, mock_ctx: Mock
    ) -> Mock:
        """Fixture providing mock UnityCatalogPropagatorAction."""

        action = Mock(spec=UnityCatalogPropagatorAction)
        action.config = mock_config
        action._stats = mock_stats
        action.ctx = mock_ctx
        action.tag_propagator = None
        action.description_sync = None
        action.process_directive = Mock()

        # Bind the real method to the mock
        action.act = UnityCatalogPropagatorAction.act.__get__(action)

        return action

    @pytest.fixture
    def mock_event_envelope(self) -> Mock:
        """Fixture providing mock event envelope."""
        envelope = Mock(spec=EventEnvelope)
        envelope.event_type = "EntityChangeEvent_v1"

        # Mock the inner event
        mock_event = Mock(spec=EntityChangeEvent)
        mock_event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )
        envelope.event = mock_event

        return envelope

    def test_act_initializes_stats_if_none(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock
    ) -> None:
        """Test act method initializes stats if not present."""

        # Set stats to None initially
        mock_propagator_action._stats.event_processing_stats = None

        with (
            patch(
                "datahub_integrations.propagation.unity_catalog.tag_propagator.EventProcessingStats"
            ) as mock_stats_class,
            patch(
                "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
                return_value=True,
            ),
        ):
            mock_stats_instance = Mock(spec=EventProcessingStats)
            mock_stats_class.return_value = mock_stats_instance

            mock_propagator_action.act(mock_event_envelope)

            # Verify stats were initialized
            assert (
                mock_propagator_action._stats.event_processing_stats
                == mock_stats_instance
            )

            # Verify stats methods were called
            mock_stats_instance.start.assert_called_once_with(mock_event_envelope)
            mock_stats_instance.end.assert_called_once_with(
                mock_event_envelope, success=True
            )

    def test_act_uses_existing_stats(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock, mock_stats: Mock
    ) -> None:
        """Test act method uses existing stats if present."""
        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
            return_value=True,
        ):
            mock_propagator_action.act(mock_event_envelope)

            # Verify existing stats were used
            mock_stats.event_processing_stats.start.assert_called_once_with(
                mock_event_envelope
            )
            mock_stats.event_processing_stats.end.assert_called_once_with(
                mock_event_envelope, success=True
            )

    def test_act_skips_non_entity_change_events(
        self, mock_propagator_action: Mock, mock_stats: Mock
    ) -> None:
        """Test act method skips non-EntityChangeEvent_v1 events."""
        # Create event with different type
        other_event = Mock(spec=EventEnvelope)
        other_event.event_type = "MetadataChangeEvent_v1"

        mock_propagator_action.act(other_event)

        # Verify stats were still called
        mock_stats.event_processing_stats.start.assert_called_once_with(other_event)
        mock_stats.event_processing_stats.end.assert_called_once_with(
            other_event, success=True
        )

        # Verify process_directive was not called
        mock_propagator_action.process_directive.assert_not_called()

    def test_act_skips_disallowed_urns(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock
    ) -> None:
        """Test act method skips disallowed URNs."""
        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
            return_value=False,
        ):
            mock_propagator_action.act(mock_event_envelope)

            # Verify process_directive was not called
            mock_propagator_action.process_directive.assert_not_called()

    def test_act_processes_tag_propagation_directive(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock
    ) -> None:
        """Test act method processes tag propagation directive."""
        # Setup tag propagator
        mock_tag_propagator = Mock()
        mock_directive = Mock()
        mock_directive.propagate = True
        mock_tag_propagator.should_propagate.return_value = mock_directive
        mock_propagator_action.tag_propagator = mock_tag_propagator

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
            return_value=True,
        ):
            mock_propagator_action.act(mock_event_envelope)

            # Verify tag propagator was called
            mock_tag_propagator.should_propagate.assert_called_once_with(
                event=mock_event_envelope
            )

            # Verify directive was processed
            mock_propagator_action.process_directive.assert_called_once_with(
                mock_directive
            )

    def test_act_processes_description_sync_when_no_tag_directive(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock
    ) -> None:
        """Test act method processes description sync when no tag directive."""
        # Setup tag propagator to return None
        mock_tag_propagator = Mock()
        mock_tag_propagator.should_propagate.return_value = None
        mock_propagator_action.tag_propagator = mock_tag_propagator

        # Setup description sync
        mock_description_sync = Mock()
        mock_directive = Mock()
        mock_directive.propagate = True
        mock_description_sync.should_propagate.return_value = mock_directive
        mock_propagator_action.description_sync = mock_description_sync

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
            return_value=True,
        ):
            mock_propagator_action.act(mock_event_envelope)

            # Verify both propagators were called
            mock_tag_propagator.should_propagate.assert_called_once_with(
                event=mock_event_envelope
            )
            mock_description_sync.should_propagate.assert_called_once_with(
                event=mock_event_envelope
            )

            # Verify directive was processed
            mock_propagator_action.process_directive.assert_called_once_with(
                mock_directive
            )

    def test_act_skips_description_sync_when_tag_directive_exists(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock
    ) -> None:
        """Test act method skips description sync when tag directive exists."""
        # Setup tag propagator to return a directive
        mock_tag_propagator = Mock()
        mock_tag_directive = Mock()
        mock_tag_directive.propagate = True
        mock_tag_propagator.should_propagate.return_value = mock_tag_directive
        mock_propagator_action.tag_propagator = mock_tag_propagator

        # Setup description sync
        mock_description_sync = Mock()
        mock_propagator_action.description_sync = mock_description_sync

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
            return_value=True,
        ):
            mock_propagator_action.act(mock_event_envelope)

            # Verify tag propagator was called
            mock_tag_propagator.should_propagate.assert_called_once_with(
                event=mock_event_envelope
            )

            # Verify description sync was NOT called
            mock_description_sync.should_propagate.assert_not_called()

            # Verify tag directive was processed
            mock_propagator_action.process_directive.assert_called_once_with(
                mock_tag_directive
            )

    def test_act_skips_directive_when_propagate_false(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock
    ) -> None:
        """Test act method skips directive when propagate is False."""
        # Setup tag propagator
        mock_tag_propagator = Mock()
        mock_directive = Mock()
        mock_directive.propagate = False  # Set to False
        mock_tag_propagator.should_propagate.return_value = mock_directive
        mock_propagator_action.tag_propagator = mock_tag_propagator

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
            return_value=True,
        ):
            mock_propagator_action.act(mock_event_envelope)

            # Verify tag propagator was called
            mock_tag_propagator.should_propagate.assert_called_once_with(
                event=mock_event_envelope
            )

            # Verify directive was NOT processed
            mock_propagator_action.process_directive.assert_not_called()

    def test_act_handles_exceptions_gracefully(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock, mock_stats: Mock
    ) -> None:
        """Test act method handles exceptions gracefully."""
        # Setup tag propagator to raise exception
        mock_tag_propagator = Mock()
        mock_tag_propagator.should_propagate.side_effect = Exception("Processing error")
        mock_propagator_action.tag_propagator = mock_tag_propagator

        with (
            patch(
                "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
                return_value=True,
            ),
            patch(
                "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
            ) as mock_logger,
        ):
            # Should not raise exception
            mock_propagator_action.act(mock_event_envelope)

            # Verify exception was logged
            mock_logger.exception.assert_called_once_with(
                "Error processing event: Processing error", exc_info=True
            )
            # Verify stats were marked as failed
            mock_stats.event_processing_stats.end.assert_called_once_with(
                mock_event_envelope, success=False
            )

    def test_act_assertion_error_on_invalid_event_type(
        self, mock_propagator_action: Mock
    ) -> None:
        """Test act method assertion error with invalid event type."""

        # Create event with correct type but invalid event object
        class RandomEntityChangeEvent(Event):
            def __init__(self, entityUrn: str):
                self.entityUrn = entityUrn

            def from_json(cls, json_str: str) -> "RandomEntityChangeEvent":
                return RandomEntityChangeEvent(
                    entityUrn="urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
                )

            def as_json(self) -> str:
                return '{"entityUrn": "' + self.entityUrn + '"}'

        invalid_entity_event_type = EventEnvelope(
            event_type="EntityChangeEvent_v1",
            event=RandomEntityChangeEvent(
                "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
            ),
            meta={},
        )

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
        ) as mock_logger:
            mock_propagator_action.act(invalid_entity_event_type)

            assert mock_logger.exception.call_count == 1
            assert (
                "Error processing event: Expected EntityChangeEvent_v1 type"
                in mock_logger.exception.call_args[0][0]
            )

    def test_act_assertion_error_on_missing_graph(
        self, mock_propagator_action: Mock
    ) -> None:
        """Test act method assertion error with invalid event type."""
        entity = EventEnvelope(
            event_type="EntityChangeEvent_v1",
            event=EntityChangeEvent(
                entityUrn="urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)",
                version=0,
                entityType="EntityChangeEvent_v1",
                operation="ADD",
                category="DATASET",
                auditStamp=Mock(),
            ),
            meta={},
        )
        entity.event_type = "EntityChangeEvent_v1"

        mock_propagator_action.ctx.graph = None
        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
        ) as mock_logger:
            mock_propagator_action.act(entity)

            assert mock_logger.exception.call_count == 1
            assert (
                "Error processing event: Graph must be initialized in the context"
                in mock_logger.exception.call_args[0][0]
            )

    def test_act_no_propagators_configured(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock
    ) -> None:
        """Test act method when no propagators are configured."""
        # Ensure both propagators are None
        mock_propagator_action.tag_propagator = None
        mock_propagator_action.description_sync = None

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
            return_value=True,
        ):
            mock_propagator_action.act(mock_event_envelope)

            # Verify process_directive was not called
            mock_propagator_action.process_directive.assert_not_called()

    def test_act_complete_workflow_integration(
        self, mock_propagator_action: Mock, mock_event_envelope: Mock, mock_stats: Mock
    ) -> None:
        """Test complete act workflow with realistic scenario."""
        # Setup realistic tag propagator
        mock_tag_propagator = Mock()
        mock_tag_directive = Mock()
        mock_tag_directive.propagate = True
        mock_tag_directive.entity = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )
        mock_tag_directive.tag = "urn:li:tag:environment:production"
        mock_tag_directive.operation = "ADD"
        mock_tag_propagator.should_propagate.return_value = mock_tag_directive
        mock_propagator_action.tag_propagator = mock_tag_propagator

        # Setup description sync (should not be called due to tag directive)
        mock_description_sync = Mock()
        mock_propagator_action.description_sync = mock_description_sync

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
            return_value=True,
        ):
            mock_propagator_action.act(mock_event_envelope)

            # Verify complete workflow
            mock_stats.event_processing_stats.start.assert_called_once_with(
                mock_event_envelope
            )
            mock_tag_propagator.should_propagate.assert_called_once_with(
                event=mock_event_envelope
            )
            mock_description_sync.should_propagate.assert_not_called()
            mock_propagator_action.process_directive.assert_called_once_with(
                mock_tag_directive
            )
            mock_stats.event_processing_stats.end.assert_called_once_with(
                mock_event_envelope, success=True
            )


class TestUnityCatalogPropagatorActionMethodsIntegration:
    """Integration tests for process_directive and act methods working together."""

    @pytest.fixture
    def full_mock_action(self) -> Mock:
        """Fixture providing fully mocked UnityCatalogPropagatorAction."""
        from datahub_integrations.propagation.unity_catalog.tag_propagator import (
            UnityCatalogPropagatorAction,
            UnityCatalogTagPropagatorConfig,
        )

        action = Mock(spec=UnityCatalogPropagatorAction)

        # Setup configuration
        action.config = Mock(spec=UnityCatalogTagPropagatorConfig)

        # Setup context
        action.ctx = Mock(spec=PipelineContext)
        action.ctx.graph = Mock()

        # Setup stats
        action._stats = Mock()
        action._stats.event_processing_stats = Mock(spec=EventProcessingStats)

        # Setup helper
        action.unity_catalog_helper = Mock()
        action.unity_catalog_helper.apply_description = Mock()
        action.unity_catalog_helper.apply_tag = Mock()
        action.unity_catalog_helper.remove_tag = Mock()

        # Setup propagators
        action.tag_propagator = None
        action.description_sync = None

        # Bind real methods
        action.process_directive = (
            UnityCatalogPropagatorAction.process_directive.__get__(action)
        )
        action.act = UnityCatalogPropagatorAction.act.__get__(action)

        return action

    def test_end_to_end_description_workflow(self, full_mock_action: Mock) -> None:
        """Test end-to-end workflow for description sync."""
        from datahub_integrations.propagation.unity_catalog.description_sync_action import (
            UnityCatalogDescriptionSyncDirective,
        )

        # Setup description sync propagator
        mock_description_sync = Mock()
        directive = UnityCatalogDescriptionSyncDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)",
            docs="Updated description",
            operation="MODIFY",
            propagate=True,
        )
        mock_description_sync.should_propagate.return_value = directive
        full_mock_action.description_sync = mock_description_sync

        # Create event
        event = Mock(spec=EventEnvelope)
        event.event_type = "EntityChangeEvent_v1"
        event.event = Mock(spec=EntityChangeEvent)
        event.event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )

        with (
            patch(
                "datahub_integrations.propagation.unity_catalog.tag_propagator.UrnValidator.is_urn_allowed",
                return_value=True,
            ),
            patch(
                "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
            ),
        ):
            full_mock_action.act(event)

            # Verify complete workflow
            full_mock_action._stats.event_processing_stats.start.assert_called_once_with(
                event
            )
            mock_description_sync.should_propagate.assert_called_once_with(event=event)
            full_mock_action.unity_catalog_helper.apply_description.assert_called_once_with(
                directive.entity, directive.docs
            )
            full_mock_action._stats.event_processing_stats.end.assert_called_once_with(
                event, success=True
            )

    def test_end_to_end_tag_workflow(self, full_mock_action: Mock) -> None:
        """Test end-to-end workflow for tag propagation."""
        # Setup tag propagator
        mock_tag_propagator = Mock()
        directive = TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)",
            tag="urn:li:tag:sensitive",
            operation="ADD",
            propagate=True,
        )

        mock_tag_propagator.should_propagate.return_value = directive
        full_mock_action.tag_propagator = mock_tag_propagator

        # Create event
        event = Mock(spec=EventEnvelope)
        event.event_type = "EntityChangeEvent_v1"
        event.event = Mock(spec=EntityChangeEvent)
        event.event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
        ):
            full_mock_action.act(event)

            # Verify complete workflow
            full_mock_action._stats.event_processing_stats.start.assert_called_once_with(
                event
            )
            mock_tag_propagator.should_propagate.assert_called_once_with(event=event)
            full_mock_action.unity_catalog_helper.apply_tag.assert_called_once_with(
                directive.entity, directive.tag
            )
            full_mock_action._stats.event_processing_stats.end.assert_called_once_with(
                event, success=True
            )

    def test_error_propagation_from_process_directive_to_act(
        self, full_mock_action: Mock
    ) -> None:
        """Test error propagation from process_directive to act method."""
        # Setup tag propagator
        mock_tag_propagator = Mock()
        directive = TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)",
            tag="urn:li:tag:sensitive",
            operation="ADD",
            propagate=True,
        )
        mock_tag_propagator.should_propagate.return_value = directive
        full_mock_action.tag_propagator = mock_tag_propagator

        # Make unity catalog helper raise exception
        full_mock_action.unity_catalog_helper.apply_tag.side_effect = Exception(
            "Unity Catalog connection failed"
        )

        # Create event
        event = Mock(spec=EventEnvelope)
        event.event_type = "EntityChangeEvent_v1"
        event.event = Mock(spec=EntityChangeEvent)
        event.event.entityUrn = (
            "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
        )

        with patch(
            "datahub_integrations.propagation.unity_catalog.tag_propagator.logger"
        ) as mock_logger:
            # Should handle exception gracefully
            full_mock_action.act(event)

            # Verify error handling
            mock_logger.exception.assert_called_once()
            full_mock_action._stats.event_processing_stats.end.assert_called_once_with(
                event, success=False
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
