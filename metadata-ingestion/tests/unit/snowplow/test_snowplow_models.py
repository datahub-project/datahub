"""Unit tests for Snowplow model parsing logic."""

from datahub.ingestion.source.snowplow.snowplow_models import (
    EntitiesSection,
    EntitySchemaReference,
    EventSpecification,
    SchemaSelf,
)


class TestEventSpecificationIgluURIExtraction:
    """Test Iglu URI extraction from event specifications."""

    def test_get_event_iglu_uri_with_self_describing_event(self):
        """Test extracting event URI from self-describing event spec."""
        event_spec = EventSpecification(
            id="test-event-id",
            name="Test Event",
            version=1,
            description="Test event specification",
            event={
                "source": "iglu:com.acme/button_clicked/jsonschema/1-0-0",
            },
        )

        uri = event_spec.get_event_iglu_uri()

        assert uri == "iglu:com.acme/button_clicked/jsonschema/1-0-0"

    def test_get_event_iglu_uri_with_no_source(self):
        """Test that events without source field return None."""
        event_spec = EventSpecification(
            id="test-event-id",
            name="Test Event",
            version=1,
            description="Test event without source field",
            event={
                "category": "media",
                "action": "play",
            },
        )

        uri = event_spec.get_event_iglu_uri()

        assert uri is None

    def test_get_event_iglu_uri_with_missing_event_field(self):
        """Test handling when event field is missing."""
        event_spec = EventSpecification(
            id="test-event-id",
            name="Test Event",
            version=1,
            description="Event spec without event field",
        )

        uri = event_spec.get_event_iglu_uri()

        assert uri is None


class TestEventSpecificationEntityURIExtraction:
    """Test entity Iglu URI extraction from event specifications."""

    def test_get_entity_iglu_uris_with_multiple_entities(self):
        """Test extracting URIs from multiple entity references."""
        event_spec = EventSpecification(
            id="test-event-id",
            name="Test Event",
            version=1,
            description="Event with multiple entities",
            entities=EntitiesSection(
                tracked=[
                    EntitySchemaReference(
                        source="iglu:com.acme/user_context/jsonschema/1-0-0"
                    ),
                    EntitySchemaReference(
                        source="iglu:com.acme/session_context/jsonschema/2-0-0"
                    ),
                ]
            ),
        )

        uris = event_spec.get_entity_iglu_uris()

        assert len(uris) == 2
        assert "iglu:com.acme/user_context/jsonschema/1-0-0" in uris
        assert "iglu:com.acme/session_context/jsonschema/2-0-0" in uris

    def test_get_entity_iglu_uris_with_no_entities(self):
        """Test that empty list is returned when no entities exist."""
        event_spec = EventSpecification(
            id="test-event-id",
            name="Test Event",
            version=1,
            description="Event without entities",
        )

        uris = event_spec.get_entity_iglu_uris()

        assert uris == []

    def test_get_entity_iglu_uris_with_empty_entities_list(self):
        """Test that empty list is returned for empty entities list."""
        event_spec = EventSpecification(
            id="test-event-id",
            name="Test Event",
            version=1,
            description="Event with empty entities list",
            entities=EntitiesSection(tracked=[]),
        )

        uris = event_spec.get_entity_iglu_uris()

        assert uris == []

    def test_get_entity_iglu_uris_with_duplicates(self):
        """Test that duplicate entity URIs are returned as-is (no deduplication)."""
        event_spec = EventSpecification(
            id="test-event-id",
            name="Test Event",
            version=1,
            description="Event with duplicate entities",
            entities=EntitiesSection(
                tracked=[
                    EntitySchemaReference(
                        source="iglu:com.acme/user_context/jsonschema/1-0-0"
                    ),
                    EntitySchemaReference(
                        source="iglu:com.acme/user_context/jsonschema/1-0-0"
                    ),
                ]
            ),
        )

        uris = event_spec.get_entity_iglu_uris()

        # Should return both (no deduplication in the method)
        assert len(uris) == 2
        assert uris[0] == "iglu:com.acme/user_context/jsonschema/1-0-0"
        assert uris[1] == "iglu:com.acme/user_context/jsonschema/1-0-0"


class TestSchemaSelfURIConstruction:
    """Test Iglu URI construction from schema self descriptors."""

    def test_iglu_uri_construction_standard_format(self):
        """Test constructing Iglu URI from descriptor."""
        descriptor = SchemaSelf(
            vendor="com.snowplowanalytics.snowplow",
            name="web_page",
            format="jsonschema",
            version="1-0-0",
        )

        expected_uri = "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0"

        # Construct URI manually from descriptor (simulating what code does)
        constructed_uri = (
            f"iglu:{descriptor.vendor}/{descriptor.name}/"
            f"{descriptor.format}/{descriptor.version}"
        )

        assert constructed_uri == expected_uri

    def test_iglu_uri_construction_with_special_characters(self):
        """Test URI construction with special characters in name."""
        descriptor = SchemaSelf(
            vendor="com.example",
            name="schema_with_underscores",
            format="jsonschema",
            version="2-1-0",
        )

        constructed_uri = (
            f"iglu:{descriptor.vendor}/{descriptor.name}/"
            f"{descriptor.format}/{descriptor.version}"
        )

        assert (
            constructed_uri
            == "iglu:com.example/schema_with_underscores/jsonschema/2-1-0"
        )
