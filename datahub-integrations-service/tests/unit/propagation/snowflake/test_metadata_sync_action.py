"""
Comprehensive unit tests for SnowflakeMetadataSyncAction.

Tests the main action class that orchestrates tag, term, and description propagation to Snowflake.
"""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.propagation.snowflake.config import (
    SnowflakeConnectionConfigPermissive,
)
from datahub_integrations.propagation.snowflake.description_models import (
    DescriptionPropagationConfig,
    DescriptionPropagationDirective,
)
from datahub_integrations.propagation.snowflake.metadata_sync_action import (
    SnowflakeMetadataSyncAction,
    SnowflakeMetadataSyncConfig,
)
from datahub_integrations.propagation.tag.tag_propagation_action import (
    TagPropagationConfig,
    TagPropagationDirective,
)
from datahub_integrations.propagation.term.term_propagation_action import (
    TermPropagationConfig,
    TermPropagationDirective,
)


class TestSnowflakeMetadataSyncAction:
    """Test SnowflakeMetadataSyncAction class."""

    @pytest.fixture
    def mock_ctx(self):
        """Create a mock PipelineContext."""
        ctx = Mock()
        ctx.graph = Mock()
        ctx.pipeline_name = "test_pipeline"
        return ctx

    @pytest.fixture
    def snowflake_config(self):
        """Create a basic Snowflake connection config."""
        return SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",  # type: ignore[arg-type]
            role="test_role",
        )

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_initialization_all_propagation_types(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test initialization with all propagation types enabled."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            tag_propagation=TagPropagationConfig(enabled=True),
            term_propagation=TermPropagationConfig(enabled=True),
            description_sync=DescriptionPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        assert action.config == config
        assert action.ctx == mock_ctx
        assert action.snowflake_helper is not None
        assert action.propagation_dispatcher is not None
        assert len(action.propagation_dispatcher.strategies) == 3

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_initialization_only_tags(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test initialization with only tag propagation enabled."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            tag_propagation=TagPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        assert len(action.propagation_dispatcher.strategies) == 1

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_initialization_only_terms(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test initialization with only term propagation enabled."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            term_propagation=TermPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        assert len(action.propagation_dispatcher.strategies) == 1

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_initialization_only_descriptions(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test initialization with only description propagation enabled."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            description_sync=DescriptionPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        assert len(action.propagation_dispatcher.strategies) == 1

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_initialization_no_propagation(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test initialization with no propagation configured."""
        config = SnowflakeMetadataSyncConfig(snowflake=snowflake_config)

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        assert len(action.propagation_dispatcher.strategies) == 0

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_initialization_disabled_propagation(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test that disabled propagation configs don't add strategies."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            tag_propagation=TagPropagationConfig(enabled=False),
            term_propagation=TermPropagationConfig(enabled=False),
            description_sync=DescriptionPropagationConfig(enabled=False),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        assert len(action.propagation_dispatcher.strategies) == 0

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_process_description_directive(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test processing a description propagation directive."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            description_sync=DescriptionPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        directive = DescriptionPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            description="Test description",
            operation="ADD",
            propagate=True,
        )

        # Mock the snowflake helper
        mock_helper_instance = mock_tag_helper.return_value

        action.process_directive(directive)

        mock_helper_instance.apply_description.assert_called_once_with(
            directive.entity, directive.description, None
        )

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_process_description_directive_with_subtype(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test processing a description directive with subtype."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            description_sync=DescriptionPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        directive = DescriptionPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.view,PROD)",
            description="View description",
            operation="ADD",
            propagate=True,
            subtype="VIEW",
        )

        mock_helper_instance = mock_tag_helper.return_value

        action.process_directive(directive)

        mock_helper_instance.apply_description.assert_called_once_with(
            directive.entity, directive.description, "VIEW"
        )

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_process_tag_directive_add(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test processing a tag ADD directive."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            tag_propagation=TagPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        directive = TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            tag="urn:li:tag:TestTag",
            operation="ADD",
            propagate=True,
        )

        mock_helper_instance = mock_tag_helper.return_value

        action.process_directive(directive)

        mock_helper_instance.apply_tag_or_term.assert_called_once_with(
            directive.entity, directive.tag, mock_ctx.graph
        )

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_process_tag_directive_remove(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test processing a tag REMOVE directive."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            tag_propagation=TagPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        directive = TagPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            tag="urn:li:tag:TestTag",
            operation="REMOVE",
            propagate=True,
        )

        mock_helper_instance = mock_tag_helper.return_value

        action.process_directive(directive)

        mock_helper_instance.remove_tag_or_term.assert_called_once_with(
            directive.entity, directive.tag, mock_ctx.graph
        )

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_process_term_directive_add(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test processing a term ADD directive."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            term_propagation=TermPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        directive = TermPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            term="urn:li:glossaryTerm:TestTerm",
            operation="ADD",
            propagate=True,
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        )

        mock_helper_instance = mock_tag_helper.return_value

        action.process_directive(directive)

        mock_helper_instance.apply_tag_or_term.assert_called_once_with(
            directive.entity, directive.term, mock_ctx.graph
        )

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_process_term_directive_remove(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test processing a term REMOVE directive."""
        config = SnowflakeMetadataSyncConfig(
            snowflake=snowflake_config,
            term_propagation=TermPropagationConfig(enabled=True),
        )

        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        directive = TermPropagationDirective(
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            term="urn:li:glossaryTerm:TestTerm",
            operation="REMOVE",
            propagate=True,
            origin="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        )

        mock_helper_instance = mock_tag_helper.return_value

        action.process_directive(directive)

        mock_helper_instance.remove_tag_or_term.assert_called_once_with(
            directive.entity, directive.term, mock_ctx.graph
        )

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_close(self, mock_tag_helper, snowflake_config, mock_ctx):
        """Test that close properly cleans up resources."""
        config = SnowflakeMetadataSyncConfig(snowflake=snowflake_config)

        action = SnowflakeMetadataSyncAction(config, mock_ctx)
        mock_helper_instance = mock_tag_helper.return_value

        action.close()

        mock_helper_instance.close.assert_called_once()

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_create_factory_method(self, mock_tag_helper, mock_ctx):
        """Test the create factory method."""
        config_dict = {
            "snowflake": {
                "account_id": "test_account",
                "warehouse": "test_warehouse",
                "username": "test_user",
                "password": "test_password",
                "role": "test_role",
            },
            "tag_propagation": {"enabled": True},
        }

        action = SnowflakeMetadataSyncAction.create(config_dict, mock_ctx)

        assert isinstance(action, SnowflakeMetadataSyncAction)
        assert action.config.snowflake.account_id == "test_account"
        assert action.config.tag_propagation is not None
        assert action.config.tag_propagation.enabled is True

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_create_factory_method_empty_config(
        self, mock_tag_helper, snowflake_config, mock_ctx
    ):
        """Test the create factory method with minimal config."""
        config_dict = {
            "snowflake": {
                "account_id": "test_account",
                "warehouse": "test_warehouse",
                "username": "test_user",
                "password": "test_password",
                "role": "test_role",
            }
        }
        action = SnowflakeMetadataSyncAction.create(config_dict, mock_ctx)

        assert isinstance(action, SnowflakeMetadataSyncAction)

    @patch(
        "datahub_integrations.propagation.snowflake.metadata_sync_action.SnowflakeTagHelper"
    )
    def test_name(self, mock_tag_helper, snowflake_config, mock_ctx):
        """Test the name method."""
        config = SnowflakeMetadataSyncConfig(snowflake=snowflake_config)
        action = SnowflakeMetadataSyncAction(config, mock_ctx)

        # The name comes from the parent class, should not be None
        name = action.name()
        assert name is not None
