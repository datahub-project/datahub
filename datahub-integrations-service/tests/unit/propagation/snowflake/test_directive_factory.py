import pytest

from datahub_integrations.propagation.snowflake.description_models import (
    DescriptionPropagationConfig,
)
from datahub_integrations.propagation.snowflake.directive_factory import (
    DirectiveFactory,
)
from datahub_integrations.propagation.snowflake.event_processor import EventData


class TestDirectiveFactory:
    """Test DirectiveFactory class."""

    @pytest.fixture
    def default_config(self):
        """Create default config with all features enabled."""
        return DescriptionPropagationConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
        )

    @pytest.fixture
    def factory(self, default_config):
        """Create DirectiveFactory with default config."""
        return DirectiveFactory(default_config)

    def test_create_directive_for_table_add_operation(self, factory):
        """Test creating directive for table ADD operation."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(
            event_data, "Table description", subtype="TABLE"
        )

        assert directive is not None
        assert (
            directive.entity
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )
        assert directive.description == "Table description"
        assert directive.operation == "ADD"
        assert directive.propagate is True
        assert directive.subtype == "TABLE"

    def test_create_directive_for_table_modify_operation(self, factory):
        """Test creating directive for table MODIFY operation."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="MODIFY",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(
            event_data, "Updated table description"
        )

        assert directive is not None
        assert directive.operation == "MODIFY"
        assert directive.description == "Updated table description"

    def test_create_directive_for_column(self, factory):
        """Test creating directive for column description."""
        event_data = EventData(
            entity_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),column_name)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(
            event_data, "Column description"
        )

        assert directive is not None
        assert "column_name" in directive.entity
        assert directive.description == "Column description"

    def test_create_directive_from_mcl_upsert(self, factory):
        """Test creating directive from MCL UPSERT event."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="MetadataChangeLogEvent_v1",
            operation="UPSERT",
            change_type="UPSERT",
            aspect_name="editableDatasetProperties",
        )

        directive = factory.create_description_directive(
            event_data, "MCL table description"
        )

        assert directive is not None
        assert directive.operation == "MODIFY"  # UPSERT maps to MODIFY
        assert directive.description == "MCL table description"

    def test_create_directive_disabled_globally(self):
        """Test that no directive is created when globally disabled."""
        config = DescriptionPropagationConfig(
            enabled=False,
            table_description_sync_enabled=True,
            column_description_sync_enabled=True,
        )
        factory = DirectiveFactory(config)

        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(event_data, "Description")

        assert directive is None

    def test_create_directive_table_sync_disabled(self):
        """Test that no directive is created when table sync is disabled."""
        config = DescriptionPropagationConfig(
            enabled=True,
            table_description_sync_enabled=False,
            column_description_sync_enabled=True,
        )
        factory = DirectiveFactory(config)

        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(event_data, "Description")

        assert directive is None

    def test_create_directive_column_sync_disabled(self):
        """Test that no directive is created when column sync is disabled."""
        config = DescriptionPropagationConfig(
            enabled=True,
            table_description_sync_enabled=True,
            column_description_sync_enabled=False,
        )
        factory = DirectiveFactory(config)

        event_data = EventData(
            entity_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),column_name)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(
            event_data, "Column description"
        )

        assert directive is None

    def test_create_directive_no_description(self, factory):
        """Test that no directive is created when description is empty."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(event_data, "")

        assert directive is None

    def test_create_directive_none_description(self, factory):
        """Test that no directive is created when description is None."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(event_data, None)

        assert directive is None

    def test_create_directive_invalid_urn(self, factory):
        """Test handling of invalid URN."""
        event_data = EventData(
            entity_urn="invalid:urn:format",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(event_data, "Description")

        assert directive is None

    def test_create_directive_unsupported_entity_type(self, factory):
        """Test handling of unsupported entity types (not Dataset or SchemaField)."""
        event_data = EventData(
            entity_urn="urn:li:chart:(looker,dashboard.chart)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(
            event_data, "Chart description"
        )

        assert directive is None

    def test_create_directive_remove_operation(self, factory):
        """Test that REMOVE operation is not supported."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="REMOVE",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(event_data, "Description")

        assert directive is None

    def test_create_directive_mcl_delete_operation(self, factory):
        """Test that MCL DELETE operation is not supported."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="MetadataChangeLogEvent_v1",
            operation="DELETE",
            change_type="DELETE",
            aspect_name="editableDatasetProperties",
        )

        directive = factory.create_description_directive(event_data, "Description")

        assert directive is None

    def test_create_directive_unsupported_event_type(self, factory):
        """Test handling of unsupported event types."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="UnsupportedEvent_v1",
            operation="ADD",
        )

        directive = factory.create_description_directive(event_data, "Description")

        assert directive is None

    def test_create_directive_with_long_description(self, factory):
        """Test creating directive with very long description."""
        long_description = "A" * 10000
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(event_data, long_description)

        assert directive is not None
        assert directive.description == long_description
        assert len(directive.description) == 10000

    def test_create_directive_with_special_chars(self, factory):
        """Test creating directive with special characters in description."""
        special_description = (
            "Table with 'quotes', \"double quotes\", and\nnewlines\tand tabs"
        )
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(
            event_data, special_description
        )

        assert directive is not None
        assert directive.description == special_description

    def test_create_directive_with_platform_instance(self, factory):
        """Test creating directive for URN with platform instance."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(
            event_data, "Description with platform instance"
        )

        assert directive is not None
        assert "prod.db.schema.table" in directive.entity
        assert directive.description == "Description with platform instance"

    def test_create_directive_column_with_platform_instance(self, factory):
        """Test creating directive for column URN with platform instance."""
        event_data = EventData(
            entity_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD),my_column)",
            event_type="EntityChangeEvent_v1",
            operation="MODIFY",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(
            event_data, "Column description with platform instance"
        )

        assert directive is not None
        assert "my_column" in directive.entity
        assert "prod.db.schema.table" in directive.entity

    def test_create_directive_with_view_subtype(self, factory):
        """Test creating directive with VIEW subtype."""
        event_data = EventData(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.my_view,PROD)",
            event_type="EntityChangeEvent_v1",
            operation="ADD",
            category="DOCUMENTATION",
        )

        directive = factory.create_description_directive(
            event_data, "View description", subtype="VIEW"
        )

        assert directive is not None
        assert directive.subtype == "VIEW"
        assert directive.description == "View description"
