"""Unit tests for PropertyManager service."""

from typing import Optional, Type
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowplow.constants import (
    DataClassification,
    StructuredPropertyId,
)
from datahub.ingestion.source.snowplow.services.property_manager import (
    PropertyManager,
    PropertyRegistrationResult,
)
from datahub.ingestion.source.snowplow.snowplow_config import (
    FieldTaggingConfig,
    SnowplowBDPConnectionConfig,
    SnowplowSourceConfig,
)
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import StructuredPropertyDefinitionClass


class TestPropertyManagerInit:
    """Tests for PropertyManager initialization."""

    def test_init_with_config(self):
        """Test that PropertyManager initializes with config."""
        config = SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
        )

        manager = PropertyManager(config=config)

        assert manager.config == config


class TestRegisterStructuredProperties:
    """Tests for register_structured_properties method."""

    @pytest.fixture
    def config_with_structured_properties(self):
        """Create config with structured properties enabled."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
            field_tagging=FieldTaggingConfig(
                use_structured_properties=True,
            ),
        )

    @pytest.fixture
    def config_without_structured_properties(self):
        """Create config with structured properties disabled."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
            field_tagging=FieldTaggingConfig(
                use_structured_properties=False,
            ),
        )

    def test_registers_five_structured_properties_when_enabled(
        self, config_with_structured_properties
    ):
        """Test that 5 structured property definitions are registered."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        assert len(workunits) == 5

    def test_registers_nothing_when_disabled(
        self, config_without_structured_properties
    ):
        """Test that no properties are registered when feature disabled."""
        manager = PropertyManager(config=config_without_structured_properties)

        workunits = list(manager.register_structured_properties())

        assert len(workunits) == 0

    def test_creates_valid_structured_property_urns(
        self, config_with_structured_properties
    ):
        """Test that URNs are correctly formed."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        for wu in workunits:
            assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
            urn = wu.metadata.entityUrn
            assert urn is not None
            assert urn.startswith("urn:li:structuredProperty:")
            assert "io.acryl.snowplow" in urn

    def test_all_expected_property_ids_registered(
        self, config_with_structured_properties
    ):
        """Test that all expected property IDs are registered."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        registered_urns = {
            wu.metadata.entityUrn
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn is not None
        }

        expected_ids = [
            StructuredPropertyId.FIELD_AUTHOR,
            StructuredPropertyId.FIELD_VERSION_ADDED,
            StructuredPropertyId.FIELD_ADDED_TIMESTAMP,
            StructuredPropertyId.FIELD_DATA_CLASS,
            StructuredPropertyId.FIELD_EVENT_TYPE,
        ]

        for prop_id in expected_ids:
            expected_urn = f"urn:li:structuredProperty:{prop_id}"
            assert expected_urn in registered_urns, f"Missing property: {prop_id}"

    def test_emits_structured_property_definition_aspects(
        self, config_with_structured_properties
    ):
        """Test that each workunit contains StructuredPropertyDefinition aspect."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        for wu in workunits:
            assert hasattr(wu.metadata, "aspect")
            assert isinstance(wu.metadata.aspect, StructuredPropertyDefinition)

    def test_field_author_property_has_correct_attributes(
        self, config_with_structured_properties
    ):
        """Test that field author property has correct configuration."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        author_wu = next(
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn is not None
            and StructuredPropertyId.FIELD_AUTHOR in wu.metadata.entityUrn
        )

        assert isinstance(author_wu.metadata, MetadataChangeProposalWrapper)
        assert isinstance(author_wu.metadata.aspect, StructuredPropertyDefinition)
        aspect = author_wu.metadata.aspect
        assert aspect.displayName == "Field Author"
        assert aspect.description is not None
        assert "initiator" in aspect.description.lower()
        assert aspect.cardinality == "SINGLE"
        assert "string" in aspect.valueType

    def test_data_classification_property_has_allowed_values(
        self, config_with_structured_properties
    ):
        """Test that data classification property has restricted allowed values."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        data_class_wu = next(
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn is not None
            and StructuredPropertyId.FIELD_DATA_CLASS in wu.metadata.entityUrn
        )

        assert isinstance(data_class_wu.metadata, MetadataChangeProposalWrapper)
        assert isinstance(data_class_wu.metadata.aspect, StructuredPropertyDefinition)
        aspect = data_class_wu.metadata.aspect
        assert aspect.allowedValues is not None
        assert len(aspect.allowedValues) == 4  # PII, SENSITIVE, PUBLIC, INTERNAL

        allowed_values = {av.value for av in aspect.allowedValues}
        assert DataClassification.PII.value in allowed_values
        assert DataClassification.SENSITIVE.value in allowed_values
        assert DataClassification.PUBLIC.value in allowed_values
        assert DataClassification.INTERNAL.value in allowed_values

    def test_event_type_property_has_allowed_values(
        self, config_with_structured_properties
    ):
        """Test that event type property has restricted allowed values."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        event_type_wu = next(
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn is not None
            and StructuredPropertyId.FIELD_EVENT_TYPE in wu.metadata.entityUrn
        )

        assert isinstance(event_type_wu.metadata, MetadataChangeProposalWrapper)
        assert isinstance(event_type_wu.metadata.aspect, StructuredPropertyDefinition)
        aspect = event_type_wu.metadata.aspect
        assert aspect.allowedValues is not None
        assert len(aspect.allowedValues) == 3  # self_describing, atomic, context

        allowed_values = {av.value for av in aspect.allowedValues}
        assert "self_describing" in allowed_values
        assert "atomic" in allowed_values
        assert "context" in allowed_values

    def test_properties_target_schema_field_entity_type(
        self, config_with_structured_properties
    ):
        """Test that all properties target SchemaField entity type."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        for wu in workunits:
            assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
            assert isinstance(wu.metadata.aspect, StructuredPropertyDefinition)
            aspect = wu.metadata.aspect
            assert aspect.entityTypes is not None
            assert len(aspect.entityTypes) == 1
            # Should target schema fields (case-insensitive check)
            assert "schemafield" in aspect.entityTypes[0].lower()

    def test_version_added_property_description_mentions_format(
        self, config_with_structured_properties
    ):
        """Test that version added property description mentions version format."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        version_wu = next(
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn is not None
            and StructuredPropertyId.FIELD_VERSION_ADDED in wu.metadata.entityUrn
        )

        assert isinstance(version_wu.metadata, MetadataChangeProposalWrapper)
        assert isinstance(version_wu.metadata.aspect, StructuredPropertyDefinition)
        aspect = version_wu.metadata.aspect
        assert aspect.description is not None
        assert (
            "Major-Minor-Patch" in aspect.description or "1-0-2" in aspect.description
        )

    def test_timestamp_property_description_mentions_iso_format(
        self, config_with_structured_properties
    ):
        """Test that timestamp property description mentions ISO 8601 format."""
        manager = PropertyManager(config=config_with_structured_properties)

        workunits = list(manager.register_structured_properties())

        timestamp_wu = next(
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn is not None
            and StructuredPropertyId.FIELD_ADDED_TIMESTAMP in wu.metadata.entityUrn
        )

        assert isinstance(timestamp_wu.metadata, MetadataChangeProposalWrapper)
        assert isinstance(timestamp_wu.metadata.aspect, StructuredPropertyDefinition)
        aspect = timestamp_wu.metadata.aspect
        assert aspect.description is not None
        assert "ISO 8601" in aspect.description or "YYYY-MM-DD" in aspect.description


class TestPropertyRegistrationResult:
    """Tests for PropertyRegistrationResult dataclass."""

    def test_total_processed_counts_all_categories(self):
        """Test that total_processed sums all categories."""
        result = PropertyRegistrationResult(
            created=["prop1", "prop2"],
            already_existed=["prop3"],
            failed=["prop4"],
        )

        assert result.total_processed == 4

    def test_success_true_when_no_failures(self):
        """Test that success is True when failed list is empty."""
        result = PropertyRegistrationResult(
            created=["prop1"],
            already_existed=["prop2"],
            failed=[],
        )

        assert result.success is True

    def test_success_false_when_failures_exist(self):
        """Test that success is False when failed list has items."""
        result = PropertyRegistrationResult(
            created=["prop1"],
            already_existed=[],
            failed=["prop2"],
        )

        assert result.success is False

    def test_empty_result_is_successful(self):
        """Test that empty result is considered successful."""
        result = PropertyRegistrationResult()

        assert result.total_processed == 0
        assert result.success is True


class TestPropertyExistsCheck:
    """Tests for _property_exists method."""

    @pytest.fixture
    def manager(self):
        """Create a PropertyManager instance."""
        config = SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
            field_tagging=FieldTaggingConfig(use_structured_properties=True),
        )
        return PropertyManager(config=config)

    def test_returns_true_when_property_exists(self, manager):
        """Test that _property_exists returns True when property exists."""
        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = StructuredPropertyDefinitionClass(
            qualifiedName="test.prop",
            valueType="urn:li:dataType:datahub.string",
            entityTypes=["urn:li:entityType:datahub.schemaField"],
        )

        result = manager._property_exists(mock_graph, "test.prop")

        assert result is True
        mock_graph.get_aspect.assert_called_once()

    def test_returns_false_when_property_not_exists(self, manager):
        """Test that _property_exists returns False when property doesn't exist."""
        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = None

        result = manager._property_exists(mock_graph, "nonexistent.prop")

        assert result is False

    def test_returns_false_on_exception(self, manager):
        """Test that _property_exists returns False on exception."""
        mock_graph = MagicMock()
        mock_graph.get_aspect.side_effect = Exception("Connection error")

        result = manager._property_exists(mock_graph, "test.prop")

        assert result is False


class TestRegisterStructuredPropertiesSync:
    """Tests for register_structured_properties_sync with auto-create behavior."""

    @pytest.fixture
    def config_with_structured_properties(self):
        """Create config with structured properties enabled."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
            field_tagging=FieldTaggingConfig(use_structured_properties=True),
        )

    @pytest.fixture
    def config_without_structured_properties(self):
        """Create config with structured properties disabled."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
            field_tagging=FieldTaggingConfig(use_structured_properties=False),
        )

    def test_returns_true_when_disabled(self, config_without_structured_properties):
        """Test that sync returns True when structured properties disabled."""
        manager = PropertyManager(config=config_without_structured_properties)
        mock_graph = MagicMock()

        result = manager.register_structured_properties_sync(mock_graph)

        assert result is True
        mock_graph.emit_mcp.assert_not_called()

    def test_returns_false_when_no_graph_client(
        self, config_with_structured_properties
    ):
        """Test that sync returns False when graph client is None."""
        manager = PropertyManager(config=config_with_structured_properties)

        result = manager.register_structured_properties_sync(None)

        assert result is False

    def test_creates_only_missing_properties(self, config_with_structured_properties):
        """Test that only missing properties are created."""
        manager = PropertyManager(config=config_with_structured_properties)
        mock_graph = MagicMock()

        # Simulate: 2 properties exist, 3 don't
        existing_properties = {
            StructuredPropertyId.FIELD_AUTHOR,
            StructuredPropertyId.FIELD_VERSION_ADDED,
        }

        def mock_get_aspect(
            urn: str, aspect_type: Type[StructuredPropertyDefinitionClass]
        ) -> Optional[StructuredPropertyDefinitionClass]:
            for prop_id in existing_properties:
                if prop_id in urn:
                    return StructuredPropertyDefinitionClass(
                        qualifiedName=prop_id,
                        valueType="urn:li:dataType:datahub.string",
                        entityTypes=["urn:li:entityType:datahub.schemaField"],
                    )
            return None

        mock_graph.get_aspect.side_effect = mock_get_aspect

        result = manager.register_structured_properties_sync(mock_graph)

        assert result is True
        # Should only emit MCPs for the 3 missing properties
        assert mock_graph.emit_mcp.call_count == 3

        # Verify the result tracking
        reg_result = manager.last_registration_result
        assert reg_result is not None
        assert len(reg_result.already_existed) == 2
        assert len(reg_result.created) == 3
        assert len(reg_result.failed) == 0

    def test_all_properties_exist_no_creates(self, config_with_structured_properties):
        """Test that no MCPs emitted when all properties already exist."""
        manager = PropertyManager(config=config_with_structured_properties)
        mock_graph = MagicMock()

        # All properties exist
        mock_graph.get_aspect.return_value = StructuredPropertyDefinitionClass(
            qualifiedName="existing",
            valueType="urn:li:dataType:datahub.string",
            entityTypes=["urn:li:entityType:datahub.schemaField"],
        )

        result = manager.register_structured_properties_sync(mock_graph)

        assert result is True
        mock_graph.emit_mcp.assert_not_called()

        reg_result = manager.last_registration_result
        assert reg_result is not None
        assert len(reg_result.already_existed) == 5
        assert len(reg_result.created) == 0

    def test_no_properties_exist_creates_all(self, config_with_structured_properties):
        """Test that all MCPs emitted when no properties exist."""
        manager = PropertyManager(config=config_with_structured_properties)
        mock_graph = MagicMock()

        # No properties exist
        mock_graph.get_aspect.return_value = None

        result = manager.register_structured_properties_sync(mock_graph)

        assert result is True
        assert mock_graph.emit_mcp.call_count == 5

        reg_result = manager.last_registration_result
        assert reg_result is not None
        assert len(reg_result.already_existed) == 0
        assert len(reg_result.created) == 5

    def test_handles_creation_failure(self, config_with_structured_properties):
        """Test that failures are tracked and reported."""
        manager = PropertyManager(config=config_with_structured_properties)
        mock_graph = MagicMock()

        # No properties exist
        mock_graph.get_aspect.return_value = None

        # First 3 succeed, last 2 fail
        call_count = [0]

        def mock_emit_mcp(mcp):
            call_count[0] += 1
            if call_count[0] > 3:
                raise Exception("API Error")

        mock_graph.emit_mcp.side_effect = mock_emit_mcp

        result = manager.register_structured_properties_sync(mock_graph)

        assert result is False  # Failures occurred

        reg_result = manager.last_registration_result
        assert reg_result is not None
        assert len(reg_result.created) == 3
        assert len(reg_result.failed) == 2

    def test_last_registration_result_accessible(
        self, config_with_structured_properties
    ):
        """Test that last_registration_result property works."""
        manager = PropertyManager(config=config_with_structured_properties)

        # Initially None
        assert manager.last_registration_result is None

        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = None

        manager.register_structured_properties_sync(mock_graph)

        # After registration, should be populated
        assert manager.last_registration_result is not None
        assert isinstance(manager.last_registration_result, PropertyRegistrationResult)
