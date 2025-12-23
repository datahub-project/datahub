"""Unit tests for ParsedEventsBuilder."""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.builders.urn_factory import SnowplowURNFactory
from datahub.ingestion.source.snowplow.services.parsed_events_builder import (
    ParsedEventsBuilder,
)
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
)


class TestParsedEventsBuilder:
    """Test parsed events dataset creation and naming logic."""

    @pytest.fixture
    def config(self):
        """Create test config."""
        config = Mock(spec=SnowplowSourceConfig)
        config.platform_instance = None
        config.env = "PROD"
        config.bdp_connection = None
        return config

    @pytest.fixture
    def urn_factory(self, config):
        """Create URN factory."""
        return SnowplowURNFactory(platform="snowplow", config=config)

    @pytest.fixture
    def state(self):
        """Create mock state."""
        state = Mock()
        state.event_spec_id = None
        state.event_spec_name = None
        state.first_event_schema_vendor = None
        state.first_event_schema_name = None
        state.atomic_event_fields = []
        state.extracted_schema_fields = []
        state.parsed_events_urn = None
        return state

    @pytest.fixture
    def builder(self, config, urn_factory, state):
        """Create builder instance."""
        return ParsedEventsBuilder(
            config=config,
            urn_factory=urn_factory,
            state=state,
            platform="snowplow",
        )

    def test_dataset_naming_with_event_spec(self, builder, state):
        """Test dataset naming uses event spec when available."""
        state.event_spec_id = "650986b2-ad4a-453f-a0f1-4a2df337c31d"
        state.event_spec_name = "checkout_started"

        workunits = list(builder.create_parsed_events_dataset())

        # Verify dataset was created with event spec ID in name
        assert len(workunits) > 0
        assert state.parsed_events_urn is not None
        assert "650986b2-ad4a-453f-a0f1-4a2df337c31d_event" in state.parsed_events_urn

        # Find dataset properties to check display name
        dataset_props = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, DatasetPropertiesClass):
                dataset_props = wu.metadata.aspect
                break

        assert dataset_props is not None
        assert dataset_props.name == "checkout_started Event"

    def test_dataset_naming_fallback_to_event_schema(self, builder, state):
        """Test dataset naming falls back to event schema when no event spec."""
        state.event_spec_id = None
        state.event_spec_name = None
        state.first_event_schema_vendor = "com.acme"
        state.first_event_schema_name = "page_view"

        workunits = list(builder.create_parsed_events_dataset())

        # Verify dataset was created with event schema in name
        assert len(workunits) > 0
        assert state.parsed_events_urn is not None
        assert "com.acme.page_view_event" in state.parsed_events_urn

        # Find dataset properties to check display name
        dataset_props = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, DatasetPropertiesClass):
                dataset_props = wu.metadata.aspect
                break

        assert dataset_props is not None
        assert dataset_props.name == "page_view Event"

    def test_dataset_naming_fallback_to_generic(self, builder, state):
        """Test dataset naming uses generic 'event' when no event spec or schema."""
        state.event_spec_id = None
        state.event_spec_name = None
        state.first_event_schema_vendor = None
        state.first_event_schema_name = None

        workunits = list(builder.create_parsed_events_dataset())

        # Verify dataset was created with generic name
        assert len(workunits) > 0
        assert state.parsed_events_urn is not None
        # URN should contain just "event" without any prefix
        assert state.parsed_events_urn.endswith(",event,PROD)")

        # Find dataset properties to check display name
        dataset_props = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, DatasetPropertiesClass):
                dataset_props = wu.metadata.aspect
                break

        assert dataset_props is not None
        assert dataset_props.name == "Event"

    def test_schema_metadata_combines_all_fields(self, builder, state):
        """Test that schema metadata includes atomic event fields and custom schema fields."""
        # Setup atomic event fields
        atomic_field = SchemaFieldClass(
            fieldPath="app_id",
            nativeDataType="STRING",
            type=Mock(),
        )
        state.atomic_event_fields = [atomic_field]

        # Setup custom schema fields (stored as tuples of (urn, field))
        custom_field1 = SchemaFieldClass(
            fieldPath="checkout_total",
            nativeDataType="DOUBLE",
            type=Mock(),
        )
        custom_field2 = SchemaFieldClass(
            fieldPath="user_email",
            nativeDataType="STRING",
            type=Mock(),
        )
        state.extracted_schema_fields = [
            ("urn:li:dataset:1", custom_field1),
            ("urn:li:dataset:2", custom_field2),
        ]

        workunits = list(builder.create_parsed_events_dataset())

        # Find schema metadata
        schema_metadata = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, SchemaMetadataClass):
                schema_metadata = wu.metadata.aspect
                break

        assert schema_metadata is not None
        assert len(schema_metadata.fields) == 3  # 1 atomic + 2 custom
        field_paths = {f.fieldPath for f in schema_metadata.fields}
        assert field_paths == {"app_id", "checkout_total", "user_email"}

    def test_schema_metadata_handles_no_fields(self, builder, state):
        """Test graceful handling when no schema fields available."""
        state.atomic_event_fields = []
        state.extracted_schema_fields = []

        workunits = list(builder.create_parsed_events_dataset())

        # Should still create dataset properties, status, subtypes
        # But no schema metadata should be emitted
        aspect_types = {type(wu.metadata.aspect) for wu in workunits}
        assert DatasetPropertiesClass in aspect_types
        assert StatusClass in aspect_types
        assert SubTypesClass in aspect_types
        # Schema metadata should NOT be present
        assert SchemaMetadataClass not in aspect_types

    def test_dataset_properties_include_metadata_fields(self, builder, state):
        """Test that dataset properties include synthetic dataset metadata."""
        state.event_spec_id = "spec-123"
        state.event_spec_name = "checkout_completed"

        workunits = list(builder.create_parsed_events_dataset())

        # Find dataset properties
        dataset_props = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, DatasetPropertiesClass):
                dataset_props = wu.metadata.aspect
                break

        assert dataset_props is not None
        assert dataset_props.customProperties is not None
        assert dataset_props.customProperties["synthetic"] == "true"
        assert dataset_props.customProperties["purpose"] == "lineage_modeling"
        assert dataset_props.customProperties["stage"] == "post_parse_pre_enrich"
        assert dataset_props.customProperties["event_spec_id"] == "spec-123"
        assert dataset_props.customProperties["event_spec_name"] == "checkout_completed"

    def test_dataset_properties_description_explains_purpose(self, builder, state):
        """Test that dataset description explains its purpose for lineage modeling."""
        workunits = list(builder.create_parsed_events_dataset())

        # Find dataset properties
        dataset_props = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, DatasetPropertiesClass):
                dataset_props = wu.metadata.aspect
                break

        assert dataset_props is not None
        assert dataset_props.description is not None
        # Description should explain the synthetic dataset purpose
        assert "parsed event fields" in dataset_props.description
        assert "BEFORE enrichments" in dataset_props.description
        assert "lineage modeling" in dataset_props.description
        assert "synthetic dataset" in dataset_props.description

    def test_status_aspect_marks_dataset_as_active(self, builder, state):
        """Test that status aspect marks dataset as not removed."""
        workunits = list(builder.create_parsed_events_dataset())

        # Find status aspect
        status = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, StatusClass):
                status = wu.metadata.aspect
                break

        assert status is not None
        assert status.removed is False

    def test_subtypes_aspect_marks_as_event_type(self, builder, state):
        """Test that subtypes aspect marks dataset as event type."""
        workunits = list(builder.create_parsed_events_dataset())

        # Find subtypes aspect
        subtypes = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, SubTypesClass):
                subtypes = wu.metadata.aspect
                break

        assert subtypes is not None
        assert "event" in subtypes.typeNames

    def test_container_aspect_added_when_bdp_connection_configured(
        self, builder, state, config
    ):
        """Test that container aspect is added when BDP connection is configured."""
        from datahub.metadata.schema_classes import ContainerClass

        # Configure BDP connection
        config.bdp_connection = Mock()
        config.bdp_connection.organization_id = "org-123"

        workunits = list(builder.create_parsed_events_dataset())

        # Find container aspect
        container = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, ContainerClass):
                container = wu.metadata.aspect
                break

        assert container is not None
        assert container.container is not None

    def test_no_container_aspect_when_bdp_connection_not_configured(
        self, builder, state, config
    ):
        """Test that no container aspect is added when BDP connection not configured."""
        from datahub.metadata.schema_classes import ContainerClass

        # No BDP connection
        config.bdp_connection = None

        workunits = list(builder.create_parsed_events_dataset())

        # Container aspect should not be present
        aspect_types = {type(wu.metadata.aspect) for wu in workunits}
        assert ContainerClass not in aspect_types

    def test_schema_field_extraction_from_tuples(self, builder, state):
        """Test that custom schema fields are correctly extracted from (urn, field) tuples."""
        # Setup complex schema field tuples
        field1 = SchemaFieldClass(
            fieldPath="nested.field1",
            nativeDataType="STRING",
            type=Mock(),
        )
        field2 = SchemaFieldClass(
            fieldPath="array_field[*]",
            nativeDataType="INTEGER",
            type=Mock(),
        )

        state.extracted_schema_fields = [
            ("urn:li:dataset:(urn:li:dataPlatform:snowplow,schema1,PROD)", field1),
            ("urn:li:dataset:(urn:li:dataPlatform:snowplow,schema2,PROD)", field2),
        ]

        workunits = list(builder.create_parsed_events_dataset())

        # Find schema metadata
        schema_metadata = None
        for wu in workunits:
            if isinstance(wu.metadata.aspect, SchemaMetadataClass):
                schema_metadata = wu.metadata.aspect
                break

        assert schema_metadata is not None
        assert len(schema_metadata.fields) == 2
        field_paths = {f.fieldPath for f in schema_metadata.fields}
        assert field_paths == {"nested.field1", "array_field[*]"}
