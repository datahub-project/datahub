from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.propagation.snowflake.description_models import (
    DescriptionPropagationConfig,
    DescriptionPropagationDirective,
)
from datahub_integrations.propagation.snowflake.propagation_dispatcher import (
    PropagationDispatcher,
)
from datahub_integrations.propagation.tag.tag_propagation_action import (
    TagPropagationConfig,
    TagPropagationDirective,
)
from datahub_integrations.propagation.term.term_propagation_action import (
    TermPropagationConfig,
    TermPropagationDirective,
)


class TestPropagationDispatcher:
    """Test PropagationDispatcher class."""

    @pytest.fixture
    def mock_ctx(self):
        """Create a mock PipelineContext."""
        ctx = MagicMock(spec=PipelineContext)
        ctx.graph = MagicMock()
        ctx.pipeline_name = "test_pipeline"
        return ctx

    @pytest.fixture
    def dispatcher(self, mock_ctx):
        """Create a PropagationDispatcher instance."""
        return PropagationDispatcher(mock_ctx)

    def test_initialization(self, mock_ctx):
        """Test dispatcher initialization."""
        dispatcher = PropagationDispatcher(mock_ctx)

        assert dispatcher.ctx == mock_ctx
        assert dispatcher.strategies == []

    def test_add_tag_propagation_enabled(self, dispatcher):
        """Test adding tag propagation strategy when enabled."""
        config = TagPropagationConfig(enabled=True)

        dispatcher.add_tag_propagation(config)

        assert len(dispatcher.strategies) == 1

    def test_add_tag_propagation_disabled(self, dispatcher):
        """Test that disabled tag propagation is not added."""
        config = TagPropagationConfig(enabled=False)

        dispatcher.add_tag_propagation(config)

        assert len(dispatcher.strategies) == 0

    def test_add_term_propagation_enabled(self, dispatcher):
        """Test adding term propagation strategy when enabled."""
        config = TermPropagationConfig(enabled=True)

        dispatcher.add_term_propagation(config)

        assert len(dispatcher.strategies) == 1

    def test_add_term_propagation_disabled(self, dispatcher):
        """Test that disabled term propagation is not added."""
        config = TermPropagationConfig(enabled=False)

        dispatcher.add_term_propagation(config)

        assert len(dispatcher.strategies) == 0

    def test_add_description_propagation_enabled(self, dispatcher):
        """Test adding description propagation strategy when enabled."""
        config = DescriptionPropagationConfig(enabled=True)

        dispatcher.add_description_propagation(config)

        assert len(dispatcher.strategies) == 1

    def test_add_description_propagation_disabled(self, dispatcher):
        """Test that disabled description propagation is not added."""
        config = DescriptionPropagationConfig(enabled=False)

        dispatcher.add_description_propagation(config)

        assert len(dispatcher.strategies) == 0

    def test_add_multiple_strategies(self, dispatcher):
        """Test adding multiple propagation strategies."""
        tag_config = TagPropagationConfig(enabled=True)
        term_config = TermPropagationConfig(enabled=True)
        desc_config = DescriptionPropagationConfig(enabled=True)

        dispatcher.add_tag_propagation(tag_config)
        dispatcher.add_term_propagation(term_config)
        dispatcher.add_description_propagation(desc_config)

        assert len(dispatcher.strategies) == 3

    def test_process_event_with_one_strategy(self, dispatcher):
        """Test processing event with one strategy that returns a directive."""
        # Add a mock strategy
        mock_strategy = Mock()
        mock_directive = TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            tag="test_tag",
            operation="ADD",
            propagate=True,
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        )
        mock_strategy.should_propagate.return_value = mock_directive
        dispatcher.strategies.append(mock_strategy)

        # Create mock event
        mock_event = MagicMock(spec=EntityChangeEvent)
        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=mock_event, meta={}
        )

        directives = dispatcher.process_event(envelope)

        assert len(directives) == 1
        assert directives[0] == mock_directive
        mock_strategy.should_propagate.assert_called_once_with(envelope)

    def test_process_event_with_multiple_strategies(self, dispatcher):
        """Test processing event with multiple strategies."""
        # Add mock strategies
        mock_strategy1 = Mock()
        mock_directive1 = TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            tag="tag1",
            operation="ADD",
            propagate=True,
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        )
        mock_strategy1.should_propagate.return_value = mock_directive1

        mock_strategy2 = Mock()
        mock_directive2 = DescriptionPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            description="Test description",
            operation="ADD",
            propagate=True,
        )
        mock_strategy2.should_propagate.return_value = mock_directive2

        dispatcher.strategies.extend([mock_strategy1, mock_strategy2])

        # Create mock event
        mock_event = MagicMock()
        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=mock_event, meta={}
        )

        directives = dispatcher.process_event(envelope)

        assert len(directives) == 2
        assert mock_directive1 in directives
        assert mock_directive2 in directives

    def test_process_event_strategy_returns_none(self, dispatcher):
        """Test processing event when strategy returns None."""
        mock_strategy = Mock()
        mock_strategy.should_propagate.return_value = None
        dispatcher.strategies.append(mock_strategy)

        mock_event = MagicMock()
        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=mock_event, meta={}
        )

        directives = dispatcher.process_event(envelope)

        assert len(directives) == 0

    def test_process_event_strategy_returns_non_propagate_directive(self, dispatcher):
        """Test that directives with propagate=False are filtered out."""
        mock_strategy = Mock()
        mock_directive = TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            tag="test_tag",
            operation="ADD",
            propagate=False,  # Should be filtered out
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        )
        mock_strategy.should_propagate.return_value = mock_directive
        dispatcher.strategies.append(mock_strategy)

        mock_event = MagicMock()
        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=mock_event, meta={}
        )

        directives = dispatcher.process_event(envelope)

        assert len(directives) == 0

    def test_process_event_strategy_raises_exception(self, dispatcher):
        """Test that exceptions in strategies are caught and logged."""
        mock_strategy = Mock()
        mock_strategy.should_propagate.side_effect = Exception("Strategy error")
        dispatcher.strategies.append(mock_strategy)

        mock_event = MagicMock()
        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=mock_event, meta={}
        )

        # Should not raise, but return empty list
        directives = dispatcher.process_event(envelope)

        assert len(directives) == 0

    def test_process_event_partial_strategy_failure(self, dispatcher):
        """Test that one failing strategy doesn't affect others."""
        # First strategy fails
        mock_strategy1 = Mock()
        mock_strategy1.should_propagate.side_effect = Exception("Error")

        # Second strategy succeeds
        mock_strategy2 = Mock()
        mock_directive = TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            tag="test_tag",
            operation="ADD",
            propagate=True,
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        )
        mock_strategy2.should_propagate.return_value = mock_directive

        dispatcher.strategies.extend([mock_strategy1, mock_strategy2])

        mock_event = MagicMock()
        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=mock_event, meta={}
        )

        directives = dispatcher.process_event(envelope)

        assert len(directives) == 1
        assert directives[0] == mock_directive

    def test_get_bootstrappable_assets_with_term_strategy(self, dispatcher):
        """Test getting bootstrappable assets from term propagation strategy."""
        mock_strategy = Mock()
        mock_strategy.asset_filters.return_value = {"filter1": "value1"}

        # Patch the isinstance check to identify it as TermPropagationAction
        with patch(
            "datahub_integrations.propagation.snowflake.propagation_dispatcher.isinstance"
        ) as mock_isinstance:
            # Make isinstance return True only for TermPropagationAction
            def isinstance_side_effect(obj, class_type):
                from datahub_integrations.propagation.term.term_propagation_action import (
                    TermPropagationAction,
                )

                if class_type == TermPropagationAction:
                    return obj == mock_strategy
                return False

            mock_isinstance.side_effect = isinstance_side_effect
            dispatcher.strategies.append(mock_strategy)

            assets = dispatcher.get_bootstrappable_assets()

            assert assets == {"filter1": "value1"}
            mock_strategy.asset_filters.assert_called_once()

    def test_get_bootstrappable_assets_no_term_strategy(self, dispatcher):
        """Test getting bootstrappable assets when no term strategy is present."""
        # Add a tag strategy (which doesn't support bootstrapping yet)
        mock_strategy = Mock()

        with patch(
            "datahub_integrations.propagation.snowflake.propagation_dispatcher.isinstance"
        ) as mock_isinstance:
            mock_isinstance.return_value = False
            dispatcher.strategies.append(mock_strategy)

            assets = dispatcher.get_bootstrappable_assets()

            assert assets == {}

    def test_process_bootstrap_asset_with_term_strategy(self, dispatcher):
        """Test processing bootstrap asset with term propagation strategy."""
        mock_strategy = Mock()
        mock_directive = TermPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            term="test_term",
            operation="ADD",
            propagate=True,
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        )
        mock_strategy.process_one_asset.return_value = [mock_directive]

        with patch(
            "datahub_integrations.propagation.snowflake.propagation_dispatcher.isinstance"
        ) as mock_isinstance:

            def isinstance_side_effect(obj, class_type):
                from datahub_integrations.propagation.term.term_propagation_action import (
                    TermPropagationAction,
                )

                if class_type == TermPropagationAction:
                    return obj == mock_strategy
                return False

            mock_isinstance.side_effect = isinstance_side_effect
            dispatcher.strategies.append(mock_strategy)

            mock_asset = {"urn": "test_urn"}
            directives = dispatcher.process_bootstrap_asset(mock_asset, "ADD")

            assert len(directives) == 1
            assert directives[0] == mock_directive
            mock_strategy.process_one_asset.assert_called_once_with(mock_asset, "ADD")

    def test_process_bootstrap_asset_with_exception(self, dispatcher):
        """Test that exceptions in bootstrap asset processing are caught."""
        mock_strategy = Mock()
        mock_strategy.process_one_asset.side_effect = Exception("Bootstrap error")

        with patch(
            "datahub_integrations.propagation.snowflake.propagation_dispatcher.isinstance"
        ) as mock_isinstance:

            def isinstance_side_effect(obj, class_type):
                from datahub_integrations.propagation.term.term_propagation_action import (
                    TermPropagationAction,
                )

                if class_type == TermPropagationAction:
                    return obj == mock_strategy
                return False

            mock_isinstance.side_effect = isinstance_side_effect
            dispatcher.strategies.append(mock_strategy)

            mock_asset = {"urn": "test_urn"}
            directives = dispatcher.process_bootstrap_asset(mock_asset, "ADD")

            assert len(directives) == 0

    def test_process_event_no_strategies(self, dispatcher):
        """Test processing event with no strategies registered."""
        mock_event = MagicMock()
        envelope = EventEnvelope(
            event_type="EntityChangeEvent_v1", event=mock_event, meta={}
        )

        directives = dispatcher.process_event(envelope)

        assert len(directives) == 0
