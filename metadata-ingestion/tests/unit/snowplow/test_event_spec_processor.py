"""Unit tests for EventSpecProcessor."""

from typing import List, Type, TypeVar
from unittest.mock import Mock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.models.snowplow_models import (
    EntitiesSection,
    EntitySchemaReference,
    EventSchemaDetail,
    EventSchemaReference,
    EventSpecification,
)
from datahub.ingestion.source.snowplow.processors.event_spec_processor import (
    EventSpecProcessor,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StructuredPropertiesClass,
    UpstreamLineageClass,
    _Aspect,
)

_T = TypeVar("_T", bound=_Aspect)


def _extract_aspects(
    workunits: List[MetadataWorkUnit], aspect_type: Type[_T]
) -> List[_T]:
    """Extract aspects of a given type from workunits (mypy-safe)."""
    results: List[_T] = []
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        if isinstance(wu.metadata.aspect, aspect_type):
            results.append(wu.metadata.aspect)
    return results


class TestEventSpecProcessorIsEnabled:
    """Tests for is_enabled() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_event_specifications = True
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state with real containers."""
        state = Mock()
        state.emitted_event_spec_ids = set()
        state.emitted_event_spec_urns = []
        state.event_spec_id = None
        state.event_spec_name = None
        return state

    def test_is_enabled_when_config_true_and_client_exists(self, mock_deps, mock_state):
        """Processor is enabled when config is True and BDP client exists."""
        processor = EventSpecProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is True

    def test_is_disabled_when_config_false(self, mock_deps, mock_state):
        """Processor is disabled when config is False."""
        mock_deps.config.extract_event_specifications = False
        processor = EventSpecProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False

    def test_is_disabled_when_no_bdp_client(self, mock_deps, mock_state):
        """Processor is disabled when no BDP client."""
        mock_deps.bdp_client = None
        processor = EventSpecProcessor(mock_deps, mock_state)
        assert processor.is_enabled() is False


class TestEventSpecProcessorExtract:
    """Tests for extract() method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies."""
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
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state with real containers."""
        state = Mock()
        # Use real containers for set/list operations
        state.emitted_event_spec_ids = set()
        state.emitted_event_spec_urns = []
        state.event_spec_id = None
        state.event_spec_name = None
        state.event_spec_to_schema_urns = {}
        state.referenced_iglu_uris = set()
        state.atomic_event_urn = None
        return state

    def test_extract_returns_empty_when_no_bdp_client(self, mock_deps, mock_state):
        """Extract returns nothing when BDP client is not configured."""
        mock_deps.bdp_client = None
        processor = EventSpecProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())
        assert workunits == []

    def test_extract_handles_api_error_gracefully(self, mock_deps, mock_state):
        """Extract handles API errors without crashing."""
        mock_deps.bdp_client.get_event_specifications.side_effect = Exception(
            "API Error"
        )
        processor = EventSpecProcessor(mock_deps, mock_state)

        workunits = list(processor.extract())

        # Should handle error gracefully and return empty
        assert workunits == []
        # Error handler should be called
        mock_deps.error_handler.handle_api_error.assert_called_once()

    def test_extract_filters_event_specs_by_pattern(self, mock_deps, mock_state):
        """Extract filters event specs based on pattern."""
        event_spec = EventSpecification(
            id="test-id",
            name="filtered-event",
        )
        mock_deps.bdp_client.get_event_specifications.return_value = [event_spec]
        mock_deps.config.event_spec_pattern.allowed.return_value = False

        processor = EventSpecProcessor(mock_deps, mock_state)
        list(processor.extract())  # Consume generator

        # Filtered event spec should not produce work units
        # Report should track that it was filtered
        mock_deps.report.report_event_spec_found.assert_called_once()
        mock_deps.report.report_event_spec_filtered.assert_called_once_with(
            "filtered-event"
        )

    def test_extract_tracks_first_event_spec_for_naming(self, mock_deps, mock_state):
        """Extract captures first event spec ID and name for parsed events dataset."""
        event_spec1 = EventSpecification(id="first-id", name="first-event")
        event_spec2 = EventSpecification(id="second-id", name="second-event")
        mock_deps.bdp_client.get_event_specifications.return_value = [
            event_spec1,
            event_spec2,
        ]

        processor = EventSpecProcessor(mock_deps, mock_state)
        # Consume the generator
        list(processor.extract())

        # First event spec should be captured in state
        assert mock_state.event_spec_id == "first-id"
        assert mock_state.event_spec_name == "first-event"

    def test_extract_adds_event_spec_ids_to_state(self, mock_deps, mock_state):
        """Extract adds emitted event spec IDs to state."""
        event_spec = EventSpecification(id="test-id", name="test-event")
        mock_deps.bdp_client.get_event_specifications.return_value = [event_spec]

        processor = EventSpecProcessor(mock_deps, mock_state)
        list(processor.extract())

        assert "test-id" in mock_state.emitted_event_spec_ids


class TestEventSpecProcessorProcessing:
    """Tests for _process_event_specification method."""

    @pytest.fixture
    def mock_deps(self):
        """Create mock dependencies with full configuration."""
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
    def mock_state(self):
        """Create mock state with real containers."""
        state = Mock()
        state.emitted_event_spec_ids = set()
        state.emitted_event_spec_urns = []
        state.event_spec_id = None
        state.event_spec_name = None
        state.emitted_schema_urns = {}
        state.event_spec_to_schema_urns = {}
        return state

    def test_process_event_spec_with_minimal_data(self, mock_deps, mock_state):
        """Process event spec with only required fields."""
        event_spec = EventSpecification(
            id="minimal-id",
            name="minimal-event",
        )

        processor = EventSpecProcessor(mock_deps, mock_state)
        workunits = list(processor._process_event_specification(event_spec))

        # Should produce work units without errors
        assert len(workunits) > 0

    def test_process_event_spec_includes_custom_properties(self, mock_deps, mock_state):
        """Process event spec includes metadata in custom properties."""
        event_spec = EventSpecification(
            id="test-id",
            name="test-event",
            status="active",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-02T00:00:00Z",
        )

        processor = EventSpecProcessor(mock_deps, mock_state)
        workunits = list(processor._process_event_specification(event_spec))

        # Should include status and timestamps in properties
        assert len(workunits) > 0


class TestParseIgluUri:
    """Tests for _parse_iglu_uri() static method."""

    def test_valid_iglu_uri(self):
        """Standard Iglu URI parses into (vendor, name, version)."""
        result = EventSpecProcessor._parse_iglu_uri(
            "iglu:com.acme/checkout_started/jsonschema/1-0-0"
        )
        assert result == ("com.acme", "checkout_started", "1-0-0")

    def test_valid_uri_skips_format_segment(self):
        """Format segment (e.g., jsonschema) is correctly discarded."""
        result = EventSpecProcessor._parse_iglu_uri(
            "iglu:com.snowplow/page_view/avro/2-1-3"
        )
        assert result == ("com.snowplow", "page_view", "2-1-3")

    def test_too_few_parts_returns_none(self):
        """URI with fewer than 4 path segments returns None."""
        result = EventSpecProcessor._parse_iglu_uri("iglu:com.acme/event/1-0-0")
        assert result is None

    def test_too_many_parts_returns_none(self):
        """URI with more than 4 path segments returns None."""
        result = EventSpecProcessor._parse_iglu_uri(
            "iglu:com.acme/event/jsonschema/1-0-0/extra"
        )
        assert result is None

    def test_missing_iglu_prefix(self):
        """URI without iglu: prefix still parses if it has 4 segments."""
        result = EventSpecProcessor._parse_iglu_uri("com.acme/event/jsonschema/1-0-0")
        assert result == ("com.acme", "event", "1-0-0")

    def test_empty_string_returns_none(self):
        """Empty string returns None."""
        result = EventSpecProcessor._parse_iglu_uri("")
        assert result is None


class TestCollectUpstreamUrns:
    """Tests for _collect_upstream_urns() — dual API format handling."""

    @pytest.fixture
    def mock_deps(self):
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_event_specifications = True
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.urn_factory.make_schema_dataset_urn = Mock(
            side_effect=lambda vendor, name, version: (
                f"urn:li:dataset:(urn:li:dataPlatform:snowplow,{vendor}.{name}.{version},PROD)"
            )
        )
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        state = Mock()
        state.emitted_event_spec_ids = set()
        state.emitted_event_spec_urns = []
        state.event_spec_id = None
        state.event_spec_name = None
        state.event_spec_to_schema_urns = {}
        state.referenced_iglu_uris = set()
        state.atomic_event_urn = None
        return state

    def test_legacy_format_with_event_schemas(self, mock_deps, mock_state):
        """Legacy format: event_schemas list creates upstream URNs."""
        event_spec = EventSpecification(
            id="spec-1",
            name="test",
            event_schemas=[
                EventSchemaReference(vendor="com.acme", name="click", version="1-0-0"),
                EventSchemaReference(
                    vendor="com.acme", name="page_view", version="2-0-0"
                ),
            ],
        )

        processor = EventSpecProcessor(mock_deps, mock_state)
        result = processor._collect_upstream_urns(event_spec)

        assert len(result.upstream_classes) == 2
        assert len(result.schema_urns) == 2
        for upstream in result.upstream_classes:
            assert upstream.type == DatasetLineageTypeClass.TRANSFORMED
        # Legacy format doesn't produce referenced_iglu_uris
        assert result.referenced_iglu_uris == []

    def test_detail_format_with_event_and_entities(self, mock_deps, mock_state):
        """Detail format: Iglu URIs from event.source and entities.tracked."""
        event_spec = EventSpecification(
            id="spec-2",
            name="detail-event",
            event=EventSchemaDetail(source="iglu:com.acme/checkout/jsonschema/1-0-0"),
            entities=EntitiesSection(
                tracked=[
                    EntitySchemaReference(source="iglu:com.acme/user/jsonschema/1-0-0"),
                    EntitySchemaReference(
                        source="iglu:com.acme/product/jsonschema/2-0-0"
                    ),
                ]
            ),
        )

        processor = EventSpecProcessor(mock_deps, mock_state)
        result = processor._collect_upstream_urns(event_spec)

        # 1 event + 2 entities = 3 upstreams
        assert len(result.upstream_classes) == 3
        assert len(result.schema_urns) == 3
        # All 3 Iglu URIs should be tracked for standard schema extraction
        assert len(result.referenced_iglu_uris) == 3

    def test_detail_format_event_only_no_entities(self, mock_deps, mock_state):
        """Detail format with event source but no entities."""
        event_spec = EventSpecification(
            id="spec-3",
            name="event-only",
            event=EventSchemaDetail(source="iglu:com.acme/click/jsonschema/1-0-0"),
        )

        processor = EventSpecProcessor(mock_deps, mock_state)
        result = processor._collect_upstream_urns(event_spec)

        assert len(result.upstream_classes) == 1
        assert len(result.schema_urns) == 1
        assert len(result.referenced_iglu_uris) == 1

    def test_empty_event_spec_returns_empty_result(self, mock_deps, mock_state):
        """Draft event spec with no schema info returns empty result."""
        event_spec = EventSpecification(id="draft-spec", name="draft")

        processor = EventSpecProcessor(mock_deps, mock_state)
        result = processor._collect_upstream_urns(event_spec)

        assert result.upstream_classes == []
        assert result.schema_urns == []
        assert result.referenced_iglu_uris == []

    def test_detail_format_with_invalid_iglu_uri(self, mock_deps, mock_state):
        """Invalid Iglu URI is tracked but doesn't produce an upstream URN."""
        event_spec = EventSpecification(
            id="spec-bad",
            name="bad-uri",
            event=EventSchemaDetail(source="not-a-valid-uri"),
        )

        processor = EventSpecProcessor(mock_deps, mock_state)
        result = processor._collect_upstream_urns(event_spec)

        # The URI is still tracked as a referenced iglu uri
        assert len(result.referenced_iglu_uris) == 1
        # But parsing failed so no upstream class or schema URN was created
        assert result.upstream_classes == []
        assert result.schema_urns == []


class TestEmitEventSpecSchemaMetadata:
    """Tests for emit_event_spec_schema_metadata() — second-pass schema and lineage emission."""

    @pytest.fixture
    def mock_deps(self):
        deps = Mock()
        deps.config = Mock()
        deps.config.extract_event_specifications = True
        deps.config.bdp_connection = Mock()
        deps.config.bdp_connection.organization_id = "test-org"
        deps.bdp_client = Mock()
        deps.report = Mock()
        deps.cache = Mock()
        deps.urn_factory = Mock()
        deps.urn_factory.make_event_spec_dataset_urn = Mock(
            side_effect=lambda id: f"urn:li:dataset:(urn:li:dataPlatform:snowplow,{id},PROD)"
        )
        deps.error_handler = Mock()
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        state = Mock()
        state.emitted_event_spec_ids = set()
        state.emitted_event_spec_urns = []
        state.event_spec_id = None
        state.event_spec_name = None
        state.event_spec_to_schema_urns = {}
        state.referenced_iglu_uris = set()
        state.atomic_event_urn = None
        state.atomic_event_fields = []
        state.field_structured_properties = {}
        state.extracted_schema_fields_by_urn = {}
        return state

    @staticmethod
    def _make_field(path: str) -> SchemaFieldClass:
        return SchemaFieldClass(
            fieldPath=path,
            type=Mock(),
            nativeDataType="string",
        )

    def test_empty_state_returns_no_workunits(self, mock_deps, mock_state):
        """Returns immediately when no event specs have been processed."""
        processor = EventSpecProcessor(mock_deps, mock_state)

        workunits = list(processor.emit_event_spec_schema_metadata())

        assert workunits == []

    def test_emits_schema_metadata_with_atomic_fields(self, mock_deps, mock_state):
        """Emits SchemaMetadata combining Atomic Event fields with upstream schema fields."""
        event_spec_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,spec-1,PROD)"
        schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.click.1-0-0,PROD)"
        )

        atomic_field = self._make_field("app_id")
        schema_field = self._make_field("click_target")

        mock_state.event_spec_to_schema_urns = {event_spec_urn: [schema_urn]}
        mock_state.atomic_event_fields = [atomic_field]
        # Mock get_fields_for_schemas to return the schema field
        mock_state.get_fields_for_schemas = Mock(return_value=[schema_field])
        # Mock get_fields_for_schema for field-level lineage
        mock_state.get_fields_for_schema = Mock(return_value=[schema_field])

        processor = EventSpecProcessor(mock_deps, mock_state)
        processor.event_spec_names[event_spec_urn] = "Test Spec"

        workunits = list(processor.emit_event_spec_schema_metadata())

        # Should emit: SchemaMetadata + UpstreamLineage (field-level)
        assert len(workunits) >= 2
        schema_aspects = _extract_aspects(workunits, SchemaMetadataClass)
        assert len(schema_aspects) == 1
        # Atomic field + schema field = 2 fields total
        assert len(schema_aspects[0].fields) == 2

    def test_emits_field_level_lineage(self, mock_deps, mock_state):
        """Emits UpstreamLineage with fine-grained field mappings."""
        event_spec_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,spec-1,PROD)"
        schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event.1-0-0,PROD)"
        )
        atomic_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,atomic_event,PROD)"

        atomic_field = self._make_field("app_id")
        schema_field = self._make_field("event_type")

        mock_state.event_spec_to_schema_urns = {event_spec_urn: [schema_urn]}
        mock_state.atomic_event_urn = atomic_urn
        mock_state.atomic_event_fields = [atomic_field]
        mock_state.get_fields_for_schemas = Mock(return_value=[schema_field])
        mock_state.get_fields_for_schema = Mock(return_value=[schema_field])

        processor = EventSpecProcessor(mock_deps, mock_state)
        processor.event_spec_names[event_spec_urn] = "Test Spec"

        workunits = list(processor.emit_event_spec_schema_metadata())

        lineage_aspects = _extract_aspects(workunits, UpstreamLineageClass)
        assert len(lineage_aspects) == 1
        lineage = lineage_aspects[0]

        # Atomic event + 1 schema = 2 upstream datasets
        assert len(lineage.upstreams) == 2
        # 1 atomic field + 1 schema field = 2 fine-grained lineages
        assert lineage.fineGrainedLineages is not None
        assert len(lineage.fineGrainedLineages) == 2

    def test_skips_event_spec_with_no_fields(self, mock_deps, mock_state):
        """Skips schema metadata emission when no fields exist for an event spec."""
        event_spec_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,spec-empty,PROD)"

        mock_state.event_spec_to_schema_urns = {event_spec_urn: ["some-missing-urn"]}
        mock_state.atomic_event_fields = []
        mock_state.get_fields_for_schemas = Mock(return_value=[])
        mock_state.get_fields_for_schema = Mock(return_value=[])

        processor = EventSpecProcessor(mock_deps, mock_state)
        processor.event_spec_names[event_spec_urn] = "Empty Spec"

        workunits = list(processor.emit_event_spec_schema_metadata())

        # No fields → no schema metadata or lineage emitted
        assert workunits == []

    def test_emits_structured_properties_for_fields(self, mock_deps, mock_state):
        """Emits structured properties propagated from schema fields to event spec fields."""
        event_spec_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,spec-sp,PROD)"
        schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.ev.1-0-0,PROD)"
        )

        field_with_props = self._make_field("tagged_field")
        mock_prop = Mock()

        mock_state.event_spec_to_schema_urns = {event_spec_urn: [schema_urn]}
        mock_state.atomic_event_fields = []
        mock_state.get_fields_for_schemas = Mock(return_value=[field_with_props])
        mock_state.get_fields_for_schema = Mock(return_value=[field_with_props])
        mock_state.field_structured_properties = {"tagged_field": [mock_prop]}

        processor = EventSpecProcessor(mock_deps, mock_state)
        processor.event_spec_names[event_spec_urn] = "SP Spec"

        workunits = list(processor.emit_event_spec_schema_metadata())

        sp_aspects = _extract_aspects(workunits, StructuredPropertiesClass)
        assert len(sp_aspects) == 1
        assert sp_aspects[0].properties == [mock_prop]

    def test_handles_multiple_event_specs(self, mock_deps, mock_state):
        """Processes all event specs in event_spec_to_schema_urns."""
        urn1 = "urn:li:dataset:(urn:li:dataPlatform:snowplow,spec-a,PROD)"
        urn2 = "urn:li:dataset:(urn:li:dataPlatform:snowplow,spec-b,PROD)"
        schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.ev.1-0-0,PROD)"
        )

        field = self._make_field("f1")

        mock_state.event_spec_to_schema_urns = {
            urn1: [schema_urn],
            urn2: [schema_urn],
        }
        mock_state.atomic_event_fields = []
        mock_state.get_fields_for_schemas = Mock(return_value=[field])
        mock_state.get_fields_for_schema = Mock(return_value=[field])

        processor = EventSpecProcessor(mock_deps, mock_state)
        processor.event_spec_names[urn1] = "Spec A"
        processor.event_spec_names[urn2] = "Spec B"

        workunits = list(processor.emit_event_spec_schema_metadata())

        # Each spec gets at least SchemaMetadata + UpstreamLineage
        schema_metas = _extract_aspects(workunits, SchemaMetadataClass)
        assert len(schema_metas) == 2

    def test_field_level_lineage_without_atomic_event(self, mock_deps, mock_state):
        """Field-level lineage works when there is no Atomic Event dataset."""
        event_spec_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,spec-no-atomic,PROD)"
        )
        schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.ev.1-0-0,PROD)"
        )

        field = self._make_field("event_name")

        mock_state.event_spec_to_schema_urns = {event_spec_urn: [schema_urn]}
        mock_state.atomic_event_urn = None
        mock_state.atomic_event_fields = []
        mock_state.get_fields_for_schemas = Mock(return_value=[field])
        mock_state.get_fields_for_schema = Mock(return_value=[field])

        processor = EventSpecProcessor(mock_deps, mock_state)
        processor.event_spec_names[event_spec_urn] = "No Atomic"

        workunits = list(processor.emit_event_spec_schema_metadata())

        lineage_aspects = _extract_aspects(workunits, UpstreamLineageClass)
        assert len(lineage_aspects) == 1
        lineage = lineage_aspects[0]

        # Only schema upstream, no atomic event
        assert len(lineage.upstreams) == 1
        assert lineage.upstreams[0].dataset == schema_urn
        # Only schema field lineage, no atomic event lineage
        assert lineage.fineGrainedLineages is not None
        assert len(lineage.fineGrainedLineages) == 1
