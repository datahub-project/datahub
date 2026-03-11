"""Unit tests for AtomicEventBuilder service."""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.dependencies import IngestionState
from datahub.ingestion.source.snowplow.services.atomic_event_builder import (
    AtomicEventBuilder,
)
from datahub.ingestion.source.snowplow.snowplow_config import (
    SnowplowBDPConnectionConfig,
    SnowplowSourceConfig,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
)


class TestAtomicEventBuilder:
    """Tests for AtomicEventBuilder service."""

    @pytest.fixture
    def config_with_bdp(self):
        """Create config with BDP connection."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org-123",
                api_key_id="test-key-id",
                api_key="test-secret",
            ),
            platform_instance="test-instance",
            env="PROD",
        )

    @pytest.fixture
    def config_iglu_only(self):
        """Create config with Iglu connection only (no BDP)."""
        from datahub.ingestion.source.snowplow.snowplow_config import (
            IgluConnectionConfig,
        )

        return SnowplowSourceConfig(
            iglu_connection=IgluConnectionConfig(
                iglu_server_url="https://iglu.example.com",
            ),
            platform_instance="test-instance",
            env="PROD",
        )

    @pytest.fixture
    def mock_urn_factory(self):
        """Create mock URN factory."""
        factory = Mock()
        factory.make_organization_urn.return_value = (
            "urn:li:container:test-org-container"
        )
        return factory

    @pytest.fixture
    def state(self):
        """Create fresh ingestion state."""
        return IngestionState()

    def test_atomic_event_dataset_urn_is_created(
        self, config_with_bdp, mock_urn_factory, state
    ):
        """Test that atomic event dataset URN is correctly created."""
        builder = AtomicEventBuilder(
            config=config_with_bdp,
            urn_factory=mock_urn_factory,
            state=state,
            platform="snowplow",
        )

        # Consume workunits
        list(builder.create_atomic_event_dataset())

        # Verify URN was set in state
        assert state.atomic_event_urn is not None
        assert "atomic_event" in state.atomic_event_urn
        assert "snowplow" in state.atomic_event_urn

    def test_atomic_event_fields_are_populated(
        self, config_with_bdp, mock_urn_factory, state
    ):
        """Test that atomic event fields are populated in state."""
        builder = AtomicEventBuilder(
            config=config_with_bdp,
            urn_factory=mock_urn_factory,
            state=state,
            platform="snowplow",
        )

        list(builder.create_atomic_event_dataset())

        # Verify fields were populated
        assert state.atomic_event_fields is not None
        assert len(state.atomic_event_fields) > 0

        # Verify critical fields exist
        field_names = [f.fieldPath for f in state.atomic_event_fields]
        assert "app_id" in field_names
        assert "platform" in field_names
        assert "collector_tstamp" in field_names
        assert "event_id" in field_names
        assert "user_ipaddress" in field_names
        assert "useragent" in field_names
        assert "page_urlquery" in field_names
        assert "page_referrer" in field_names

    def test_atomic_event_excludes_enriched_fields(
        self, config_with_bdp, mock_urn_factory, state
    ):
        """Test that atomic event does NOT include enriched output fields."""
        builder = AtomicEventBuilder(
            config=config_with_bdp,
            urn_factory=mock_urn_factory,
            state=state,
            platform="snowplow",
        )

        list(builder.create_atomic_event_dataset())

        # Verify enriched output fields are NOT present
        field_names = [f.fieldPath for f in state.atomic_event_fields]

        # These are enrichment OUTPUT fields, not inputs
        assert "geo_country" not in field_names
        assert "geo_city" not in field_names
        assert "ip_isp" not in field_names
        assert "mkt_medium" not in field_names
        assert "mkt_source" not in field_names
        assert "br_name" not in field_names
        assert "os_name" not in field_names

    def test_emits_correct_aspects(self, config_with_bdp, mock_urn_factory, state):
        """Test that all required aspects are emitted."""
        builder = AtomicEventBuilder(
            config=config_with_bdp,
            urn_factory=mock_urn_factory,
            state=state,
            platform="snowplow",
        )

        workunits = list(builder.create_atomic_event_dataset())

        # Extract aspect types
        aspect_types = set()
        for wu in workunits:
            if hasattr(wu.metadata, "aspect"):
                aspect_types.add(type(wu.metadata.aspect))

        # Verify all required aspects are present
        assert SchemaMetadataClass in aspect_types
        assert DatasetPropertiesClass in aspect_types
        assert StatusClass in aspect_types
        assert SubTypesClass in aspect_types
        assert ContainerClass in aspect_types  # BDP connection present

    def test_container_linked_when_bdp_connection_exists(
        self, config_with_bdp, mock_urn_factory, state
    ):
        """Test that container is linked when BDP connection exists."""
        builder = AtomicEventBuilder(
            config=config_with_bdp,
            urn_factory=mock_urn_factory,
            state=state,
            platform="snowplow",
        )

        workunits = list(builder.create_atomic_event_dataset())

        # Find container aspect
        container_aspect = None
        for wu in workunits:
            if hasattr(wu.metadata, "aspect") and isinstance(
                wu.metadata.aspect, ContainerClass
            ):
                container_aspect = wu.metadata.aspect
                break

        assert container_aspect is not None
        assert container_aspect.container == "urn:li:container:test-org-container"

        # Verify URN factory was called
        mock_urn_factory.make_organization_urn.assert_called_once_with("test-org-123")

    def test_no_container_when_iglu_only(
        self, config_iglu_only, mock_urn_factory, state
    ):
        """Test that no container aspect is emitted when no BDP connection."""
        builder = AtomicEventBuilder(
            config=config_iglu_only,
            urn_factory=mock_urn_factory,
            state=state,
            platform="snowplow",
        )

        workunits = list(builder.create_atomic_event_dataset())

        # Find container aspect
        container_aspects = [
            wu
            for wu in workunits
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, ContainerClass)
        ]

        assert len(container_aspects) == 0
        mock_urn_factory.make_organization_urn.assert_not_called()

    def test_dataset_properties_have_correct_metadata(
        self, config_with_bdp, mock_urn_factory, state
    ):
        """Test that dataset properties include expected metadata."""
        builder = AtomicEventBuilder(
            config=config_with_bdp,
            urn_factory=mock_urn_factory,
            state=state,
            platform="snowplow",
        )

        workunits = list(builder.create_atomic_event_dataset())

        # Find properties aspect
        properties_aspect = None
        for wu in workunits:
            if hasattr(wu.metadata, "aspect") and isinstance(
                wu.metadata.aspect, DatasetPropertiesClass
            ):
                properties_aspect = wu.metadata.aspect
                break

        assert properties_aspect is not None
        assert properties_aspect.name == "Atomic Event"
        assert properties_aspect.description is not None
        assert "BEFORE enrichments run" in properties_aspect.description
        assert properties_aspect.customProperties is not None
        assert properties_aspect.customProperties["schema_type"] == "atomic_event"
        assert properties_aspect.customProperties["platform"] == "snowplow"
