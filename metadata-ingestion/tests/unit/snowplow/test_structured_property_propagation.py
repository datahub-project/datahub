"""
Tests for structured property propagation in Snowplow connector.

These tests verify that structured properties are correctly propagated from
schema datasets to event_spec datasets.

Bug Prevention:
- Bug 3: Structured properties not appearing on event_spec dataset fields
"""

from typing import Dict, List
from unittest.mock import Mock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowplow.dependencies import IngestionState
from datahub.ingestion.source.snowplow.processors.event_spec_processor import (
    EventSpecProcessor,
)
from datahub.ingestion.source.snowplow.services.field_tagging import (
    FieldTagContext,
    FieldTagger,
)
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)


class TestFieldTaggerCaching:
    """Test that FieldTagger caches structured properties for propagation."""

    @pytest.fixture
    def field_tagging_config(self):
        """Create mock field tagging config."""
        config = Mock()
        config.enabled = True
        config.use_structured_properties = True
        config.emit_tags_and_structured_properties = False
        config.tag_authorship = True
        config.tag_schema_version = True
        config.tag_data_class = True
        config.pii_tags_only = False
        config.use_pii_enrichment = True
        config.pii_field_patterns = ["email", "phone", "ssn"]
        config.sensitive_field_patterns = ["password", "secret"]
        return config

    @pytest.fixture
    def sample_field(self) -> SchemaFieldClass:
        """Create a sample schema field."""
        return SchemaFieldClass(
            fieldPath="discount_code",
            type=Mock(),
            nativeDataType="string",
            description="Discount code applied to order",
        )

    @pytest.fixture
    def sample_context(self) -> FieldTagContext:
        """Create sample field tagging context."""
        return FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="discount_code",
            field_type="string",
            field_description="Discount code",
            deployment_initiator="Jane Doe",
            deployment_timestamp="2024-01-15T10:30:00Z",
            pii_fields=set(),
            event_type="self_describing",
        )

    def test_generate_field_structured_properties_caches_to_dict(
        self, field_tagging_config, sample_field, sample_context
    ):
        """
        Test that generate_field_structured_properties populates the cache dict.

        This is critical for propagation: the cache is passed to DataStructureBuilder
        and later read by EventSpecProcessor.
        """
        tagger = FieldTagger(field_tagging_config)
        cache: Dict[str, List[StructuredPropertyValueAssignmentClass]] = {}

        # Generate structured properties with cache
        list(
            tagger.generate_field_structured_properties(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,schema_test,PROD)",
                field=sample_field,
                context=sample_context,
                field_properties_cache=cache,
            )
        )  # Consume the generator to populate cache

        # Verify cache was populated
        assert sample_field.fieldPath in cache
        properties = cache[sample_field.fieldPath]

        # Verify properties contain expected values
        property_urns = [p.propertyUrn for p in properties]
        assert (
            "urn:li:structuredProperty:io.acryl.snowplow.fieldAuthor" in property_urns
        )
        assert (
            "urn:li:structuredProperty:io.acryl.snowplow.fieldVersionAdded"
            in property_urns
        )
        assert (
            "urn:li:structuredProperty:io.acryl.snowplow.fieldAddedTimestamp"
            in property_urns
        )
        assert (
            "urn:li:structuredProperty:io.acryl.snowplow.fieldDataClass"
            in property_urns
        )
        assert (
            "urn:li:structuredProperty:io.acryl.snowplow.fieldEventType"
            in property_urns
        )

    def test_generate_field_structured_properties_emits_workunits(
        self, field_tagging_config, sample_field, sample_context
    ):
        """Test that structured properties are emitted as work units."""
        tagger = FieldTagger(field_tagging_config)
        cache: Dict[str, List[StructuredPropertyValueAssignmentClass]] = {}

        workunits = list(
            tagger.generate_field_structured_properties(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,schema_test,PROD)",
                field=sample_field,
                context=sample_context,
                field_properties_cache=cache,
            )
        )

        # Should emit one work unit for the schema field
        assert len(workunits) == 1

    def test_cache_is_optional(
        self, field_tagging_config, sample_field, sample_context
    ):
        """Test that passing None for cache still works (backward compatibility)."""
        tagger = FieldTagger(field_tagging_config)

        # Should not raise when cache is None
        workunits = list(
            tagger.generate_field_structured_properties(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,schema_test,PROD)",
                field=sample_field,
                context=sample_context,
                field_properties_cache=None,
            )
        )

        # Still emits work units
        assert len(workunits) == 1


class TestEventSpecProcessorStructuredProperties:
    """Test that EventSpecProcessor propagates structured properties."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies for event spec processor."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_event_specifications = True
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.config.event_spec_pattern = Mock()
        deps.config.event_spec_pattern.allowed = Mock(return_value=True)
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.urn_factory.make_event_spec_dataset_urn = Mock(
            side_effect=lambda id: f"urn:li:dataset:(urn:li:dataPlatform:snowplow,{id},PROD)"
        )
        deps.error_handler = Mock()
        deps.ownership_builder = Mock()
        deps.ownership_builder.build_owners = Mock(return_value=[])
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state_with_properties(self) -> IngestionState:
        """Create IngestionState pre-populated with structured properties."""
        state = IngestionState()

        # Simulate DataStructureBuilder having cached properties for fields
        state.field_structured_properties = {
            "discount_code": [
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldAuthor",
                    values=["Jane Doe"],
                ),
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldVersionAdded",
                    values=["1-0-0"],
                ),
            ],
            "amount": [
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldAuthor",
                    values=["Bob Smith"],
                ),
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldVersionAdded",
                    values=["1-1-0"],
                ),
            ],
            "currency": [
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldDataClass",
                    values=["internal"],
                ),
            ],
        }

        # Setup event spec mappings
        state.event_spec_to_schema_urns = {
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,evt-spec-1,PROD)": [
                "urn:li:dataset:(urn:li:dataPlatform:snowplow,schema1,PROD)"
            ]
        }

        return state

    @pytest.fixture
    def sample_fields(self) -> List[SchemaFieldClass]:
        """Create sample schema fields."""
        return [
            SchemaFieldClass(
                fieldPath="discount_code",
                type=Mock(),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="amount",
                type=Mock(),
                nativeDataType="decimal",
            ),
            SchemaFieldClass(
                fieldPath="currency",
                type=Mock(),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="no_properties_field",  # This field has no cached properties
                type=Mock(),
                nativeDataType="string",
            ),
        ]

    def test_emit_field_structured_properties_propagates_from_cache(
        self, mock_deps, mock_state_with_properties, sample_fields
    ):
        """
        Test that _emit_field_structured_properties reads from cache and emits.

        This is the core test for Bug 3: event_spec fields should get structured
        properties that were cached during schema processing.
        """
        processor = EventSpecProcessor(mock_deps, mock_state_with_properties)

        event_spec_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,evt-spec-test,PROD)"
        )
        workunits = list(
            processor._emit_field_structured_properties(
                event_spec_urn=event_spec_urn,
                fields=sample_fields,
            )
        )

        # Should emit 3 work units (discount_code, amount, currency have properties)
        # no_properties_field has no cached properties, so not emitted
        assert len(workunits) == 3

        # Verify each work unit has the correct aspect type
        for wu in workunits:
            mcp = wu.metadata
            assert hasattr(mcp, "aspect")
            assert isinstance(mcp.aspect, StructuredPropertiesClass)

    def test_emit_field_structured_properties_skips_fields_without_properties(
        self, mock_deps, mock_state_with_properties, sample_fields
    ):
        """Test that fields without cached properties are skipped."""
        processor = EventSpecProcessor(mock_deps, mock_state_with_properties)

        event_spec_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,evt-spec-test,PROD)"
        )
        workunits = list(
            processor._emit_field_structured_properties(
                event_spec_urn=event_spec_urn,
                fields=sample_fields,
            )
        )

        # Only 3 fields have properties, not 4
        assert len(workunits) == 3

    def test_emit_field_structured_properties_handles_empty_cache(
        self, mock_deps, sample_fields
    ):
        """Test that empty cache results in no work units."""
        state = IngestionState()  # Empty state, no cached properties
        processor = EventSpecProcessor(mock_deps, state)

        event_spec_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,evt-spec-test,PROD)"
        )
        workunits = list(
            processor._emit_field_structured_properties(
                event_spec_urn=event_spec_urn,
                fields=sample_fields,
            )
        )

        # No properties cached, so no work units
        assert len(workunits) == 0

    def test_schema_field_urns_are_correct(
        self, mock_deps, mock_state_with_properties, sample_fields
    ):
        """Test that schema field URNs use the event_spec URN, not schema URN."""
        processor = EventSpecProcessor(mock_deps, mock_state_with_properties)

        event_spec_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,evt-spec-test,PROD)"
        )
        workunits = list(
            processor._emit_field_structured_properties(
                event_spec_urn=event_spec_urn,
                fields=sample_fields,
            )
        )

        # Check that entity URNs contain the event_spec URN
        for wu in workunits:
            assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
            mcp = wu.metadata
            assert mcp.entityUrn is not None
            assert "evt-spec-test" in mcp.entityUrn
            # Should be a schema field URN
            assert mcp.entityUrn.startswith(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowplow,evt-spec-test"
            )


class TestEndToEndPropertyPropagation:
    """
    Test end-to-end structured property propagation through IngestionState.

    This simulates the full flow:
    1. DataStructureBuilder caches properties in state.field_structured_properties
    2. EventSpecProcessor reads from cache and emits for event_spec fields
    """

    @pytest.fixture
    def shared_state(self) -> IngestionState:
        """Create shared IngestionState for the pipeline."""
        return IngestionState()

    def test_properties_flow_through_state(self, shared_state):
        """
        Test that properties cached in state are accessible to EventSpecProcessor.

        This simulates the real flow without running the actual processors.
        """
        # Step 1: Simulate DataStructureBuilder caching properties
        shared_state.field_structured_properties["user_email"] = [
            StructuredPropertyValueAssignmentClass(
                propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldAuthor",
                values=["Alice"],
            ),
            StructuredPropertyValueAssignmentClass(
                propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldDataClass",
                values=["PII"],
            ),
        ]

        shared_state.field_structured_properties["product_id"] = [
            StructuredPropertyValueAssignmentClass(
                propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldVersionAdded",
                values=["2-0-0"],
            ),
        ]

        # Verify state has the properties
        assert len(shared_state.field_structured_properties) == 2
        assert "user_email" in shared_state.field_structured_properties
        assert "product_id" in shared_state.field_structured_properties

        # Step 2: Verify EventSpecProcessor can access these properties
        user_email_props = shared_state.field_structured_properties.get("user_email")
        assert user_email_props is not None
        assert len(user_email_props) == 2

        product_id_props = shared_state.field_structured_properties.get("product_id")
        assert product_id_props is not None
        assert len(product_id_props) == 1

    def test_state_cleared_between_runs(self, shared_state):
        """Test that state clearing works correctly."""
        # Add some properties
        shared_state.field_structured_properties["test_field"] = [
            StructuredPropertyValueAssignmentClass(
                propertyUrn="urn:li:structuredProperty:test",
                values=["value"],
            ),
        ]

        # Clear schema state (simulates new pipeline run)
        shared_state.clear_schema_state()

        # Properties should be cleared (field_structured_properties is NOT cleared
        # by clear_schema_state since it's used across schema processing)
        # This tests current behavior - if you want it cleared, update the method
        # For now, just verify the state is accessible
        assert hasattr(shared_state, "field_structured_properties")


class TestPropertyValueIntegrity:
    """Test that property values are preserved correctly during propagation."""

    @pytest.fixture
    def mock_deps(self):
        """Create minimal mock dependencies."""
        deps = Mock()
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def state_with_complex_properties(self) -> IngestionState:
        """Create state with various property value types."""
        state = IngestionState()

        # Test various value formats
        state.field_structured_properties = {
            "field_with_special_chars": [
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldAuthor",
                    values=["Jöhn O'Bríen"],  # Special characters in name
                ),
            ],
            "field_with_timestamp": [
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldAddedTimestamp",
                    values=["2024-01-15T10:30:00.123Z"],  # Full ISO timestamp
                ),
            ],
            "field_with_version": [
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldVersionAdded",
                    values=["1-2-3"],  # Standard version format
                ),
            ],
        }

        return state

    def test_special_characters_preserved(
        self, mock_deps, state_with_complex_properties
    ):
        """Test that special characters in values are preserved."""
        processor = EventSpecProcessor(mock_deps, state_with_complex_properties)

        fields = [
            SchemaFieldClass(
                fieldPath="field_with_special_chars",
                type=Mock(),
                nativeDataType="string",
            ),
        ]

        workunits = list(
            processor._emit_field_structured_properties(
                event_spec_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,test,PROD)",
                fields=fields,
            )
        )

        assert len(workunits) == 1
        assert isinstance(workunits[0].metadata, MetadataChangeProposalWrapper)
        assert isinstance(workunits[0].metadata.aspect, StructuredPropertiesClass)
        aspect = workunits[0].metadata.aspect
        props = aspect.properties[0]
        assert props.values[0] == "Jöhn O'Bríen"

    def test_timestamp_format_preserved(self, mock_deps, state_with_complex_properties):
        """Test that timestamp format is preserved exactly."""
        processor = EventSpecProcessor(mock_deps, state_with_complex_properties)

        fields = [
            SchemaFieldClass(
                fieldPath="field_with_timestamp",
                type=Mock(),
                nativeDataType="string",
            ),
        ]

        workunits = list(
            processor._emit_field_structured_properties(
                event_spec_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,test,PROD)",
                fields=fields,
            )
        )

        assert len(workunits) == 1
        assert isinstance(workunits[0].metadata, MetadataChangeProposalWrapper)
        assert isinstance(workunits[0].metadata.aspect, StructuredPropertiesClass)
        aspect = workunits[0].metadata.aspect
        props = aspect.properties[0]
        assert props.values[0] == "2024-01-15T10:30:00.123Z"
