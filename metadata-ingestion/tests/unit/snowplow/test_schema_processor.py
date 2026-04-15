"""Unit tests for SchemaProcessor."""

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.models.snowplow_models import (
    DataStructure,
    DataStructureDeployment,
    SchemaData,
    SchemaMetadata,
    SchemaSelf,
)
from datahub.ingestion.source.snowplow.processors.schema_processor import (
    SchemaProcessor,
)
from datahub.ingestion.source.snowplow.snowplow_config import (
    FieldTaggingConfig,
    SnowplowSourceConfig,
)


class TestSchemaProcessorFieldVersionMapping:
    """Test field version mapping logic."""

    @pytest.fixture
    def config_with_version_tracking(self):
        """Create config with field version tracking enabled."""
        config = Mock(spec=SnowplowSourceConfig)
        config.field_tagging = FieldTaggingConfig(
            track_field_versions=True, tag_schema_version=True
        )
        config.bdp_connection = Mock()
        config.bdp_connection.organization_id = "test-org"
        return config

    @pytest.fixture
    def processor_with_versioning(
        self, mock_deps_with_bdp, mock_state, config_with_version_tracking
    ):
        """Create processor with version tracking enabled."""
        data_structure_builder = Mock()
        processor = SchemaProcessor(
            mock_deps_with_bdp, mock_state, data_structure_builder
        )
        processor.config = config_with_version_tracking
        processor.cache = Mock()
        processor.report = Mock()
        return processor

    @pytest.fixture
    def mock_deps_with_bdp(self):
        """Create mock dependencies with BDP client."""
        deps = Mock()
        deps.bdp_client = Mock()
        deps.iglu_client = None
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state."""
        state = Mock()
        state.field_version_cache = {}
        return state

    def test_parse_schemaver_valid_versions(self):
        """Test SchemaVer parsing for valid versions."""
        assert SchemaProcessor._parse_schemaver("1-0-0") == (1, 0, 0)
        assert SchemaProcessor._parse_schemaver("2-1-3") == (2, 1, 3)
        assert SchemaProcessor._parse_schemaver("10-5-2") == (10, 5, 2)

    def test_parse_schemaver_invalid_versions(self):
        """Test SchemaVer parsing returns (0,0,0) for invalid versions."""
        assert SchemaProcessor._parse_schemaver("invalid") == (0, 0, 0)
        assert SchemaProcessor._parse_schemaver("1.0.0") == (0, 0, 0)
        assert SchemaProcessor._parse_schemaver("") == (0, 0, 0)

    def test_build_field_version_mapping_tracks_when_fields_added(
        self, processor_with_versioning, mock_deps_with_bdp
    ):
        """Test that field version mapping correctly identifies when fields were introduced."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="checkout",
            deployments=[
                DataStructureDeployment(
                    version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
                ),
                DataStructureDeployment(
                    version="1-1-0", env="PROD", ts="2024-02-01T00:00:00Z"
                ),
                DataStructureDeployment(
                    version="2-0-0", env="PROD", ts="2024-03-01T00:00:00Z"
                ),
            ],
        )

        # Mock schema versions with different fields
        def get_version_side_effect(hash_val, version, env=None):
            if version == "1-0-0":
                return DataStructure(
                    hash=hash_val,
                    vendor="com.acme",
                    name="checkout",
                    data=SchemaData(
                        schema_ref="http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
                        self_descriptor=SchemaSelf(
                            vendor="com.acme",
                            name="checkout",
                            format="jsonschema",
                            version="1-0-0",
                        ),
                        type="object",
                        properties={"field1": {}, "field2": {}},
                    ),
                )
            elif version == "1-1-0":
                return DataStructure(
                    hash=hash_val,
                    vendor="com.acme",
                    name="checkout",
                    data=SchemaData(
                        schema_ref="http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
                        self_descriptor=SchemaSelf(
                            vendor="com.acme",
                            name="checkout",
                            format="jsonschema",
                            version="1-1-0",
                        ),
                        type="object",
                        properties={"field1": {}, "field2": {}, "field3": {}},
                    ),
                )
            elif version == "2-0-0":
                return DataStructure(
                    hash=hash_val,
                    vendor="com.acme",
                    name="checkout",
                    data=SchemaData(
                        schema_ref="http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
                        self_descriptor=SchemaSelf(
                            vendor="com.acme",
                            name="checkout",
                            format="jsonschema",
                            version="2-0-0",
                        ),
                        type="object",
                        properties={
                            "field1": {},
                            "field2": {},
                            "field3": {},
                            "field4": {},
                        },
                    ),
                )

        mock_deps_with_bdp.bdp_client.get_data_structure_version.side_effect = (
            get_version_side_effect
        )

        # Build field version mapping
        result = processor_with_versioning._build_field_version_mapping(data_structure)

        # Verify fields were tracked correctly
        assert result["field1"] == "1-0-0"  # Added in first version
        assert result["field2"] == "1-0-0"  # Added in first version
        assert result["field3"] == "1-1-0"  # Added in second version
        assert result["field4"] == "2-0-0"  # Added in third version

    def test_build_field_version_mapping_handles_missing_deployments(
        self, processor_with_versioning
    ):
        """Test that field version mapping handles schemas without deployments."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="checkout",
        )

        result = processor_with_versioning._build_field_version_mapping(data_structure)

        # Should return empty dict
        assert result == {}

    def test_build_field_version_mapping_handles_api_errors(
        self, processor_with_versioning, mock_deps_with_bdp
    ):
        """Test that field version mapping handles API errors when fetching versions."""
        data_structure = DataStructure(
            hash="test-hash",
            vendor="com.acme",
            name="checkout",
            deployments=[
                DataStructureDeployment(
                    version="1-0-0", env="PROD", ts="2024-01-01T00:00:00Z"
                ),
                DataStructureDeployment(
                    version="1-1-0", env="PROD", ts="2024-02-01T00:00:00Z"
                ),
            ],
        )

        # Mock API errors
        mock_deps_with_bdp.bdp_client.get_data_structure_version.side_effect = (
            Exception("API timeout")
        )

        # Should not raise, just return empty mapping
        result = processor_with_versioning._build_field_version_mapping(data_structure)

        # No fields should be mapped due to errors
        assert result == {}


class TestSchemaProcessorTimestampFiltering:
    """Test timestamp-based deployment filtering."""

    @pytest.fixture
    def config_with_timestamp_filter(self):
        """Create config with timestamp filtering."""
        config = Mock(spec=SnowplowSourceConfig)
        config.deployed_since = "2024-02-01T00:00:00Z"
        config.schema_pattern = Mock()
        config.schema_pattern.allowed = Mock(return_value=True)
        return config

    @pytest.fixture
    def processor_with_filter(
        self, mock_deps_with_bdp, mock_state, config_with_timestamp_filter
    ):
        """Create processor with timestamp filter."""
        data_structure_builder = Mock()
        processor = SchemaProcessor(
            mock_deps_with_bdp, mock_state, data_structure_builder
        )
        processor.config = config_with_timestamp_filter
        processor.report = Mock()
        return processor

    @pytest.fixture
    def mock_deps_with_bdp(self):
        """Create mock dependencies with BDP client."""
        deps = Mock()
        deps.bdp_client = Mock()
        deps.iglu_client = None
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state."""
        state = Mock()
        return state

    def test_is_deployment_recent_with_recent_timestamp(self, processor_with_filter):
        """Test that recent deployments are correctly identified."""
        since_dt = datetime(2024, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        recent_ts = "2024-02-15T10:00:00Z"

        assert processor_with_filter._is_deployment_recent(recent_ts, since_dt) is True

    def test_is_deployment_recent_with_old_timestamp(self, processor_with_filter):
        """Test that old deployments are correctly filtered out."""
        since_dt = datetime(2024, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        old_ts = "2024-01-15T10:00:00Z"

        assert processor_with_filter._is_deployment_recent(old_ts, since_dt) is False

    def test_is_deployment_recent_with_exact_timestamp(self, processor_with_filter):
        """Test edge case where deployment timestamp matches filter exactly."""
        since_dt = datetime(2024, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        exact_ts = "2024-02-01T00:00:00Z"

        assert processor_with_filter._is_deployment_recent(exact_ts, since_dt) is True

    def test_is_deployment_recent_handles_invalid_timestamp(
        self, processor_with_filter
    ):
        """Test that invalid timestamps are handled gracefully."""
        since_dt = datetime(2024, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        invalid_ts = "not-a-timestamp"

        # Should return False and log warning, not raise
        assert (
            processor_with_filter._is_deployment_recent(invalid_ts, since_dt) is False
        )

    def test_filter_by_deployed_since_filters_schemas(self, processor_with_filter):
        """Test that schemas are filtered based on deployment timestamp."""
        data_structures = [
            DataStructure(
                hash="hash1",
                vendor="com.acme",
                name="old_schema",
                deployments=[
                    DataStructureDeployment(
                        version="1-0-0", env="PROD", ts="2024-01-15T00:00:00Z"
                    )
                ],
            ),
            DataStructure(
                hash="hash2",
                vendor="com.acme",
                name="recent_schema",
                deployments=[
                    DataStructureDeployment(
                        version="1-0-0", env="PROD", ts="2024-02-15T00:00:00Z"
                    )
                ],
            ),
        ]

        filtered = processor_with_filter._filter_by_deployed_since(data_structures)

        # Only recent schema should remain
        assert len(filtered) == 1
        assert filtered[0].name == "recent_schema"

    def test_filter_by_deployed_since_handles_invalid_config_timestamp(
        self, processor_with_filter
    ):
        """Test handling of invalid deployed_since configuration."""
        processor_with_filter.config.deployed_since = "invalid-timestamp"

        data_structures = [
            DataStructure(
                hash="hash1",
                vendor="com.acme",
                name="schema1",
                deployments=[
                    DataStructureDeployment(
                        version="1-0-0", env="PROD", ts="2024-01-15T00:00:00Z"
                    )
                ],
            ),
        ]

        # Should return all structures if timestamp is invalid
        filtered = processor_with_filter._filter_by_deployed_since(data_structures)
        assert len(filtered) == 1

    def test_filter_by_deployed_since_skips_when_not_configured(
        self, processor_with_filter
    ):
        """Test that filtering is skipped when deployed_since not configured."""
        processor_with_filter.config.deployed_since = None

        data_structures = [
            DataStructure(
                hash="hash1",
                vendor="com.acme",
                name="schema1",
                deployments=[
                    DataStructureDeployment(
                        version="1-0-0", env="PROD", ts="2024-01-15T00:00:00Z"
                    )
                ],
            ),
        ]

        filtered = processor_with_filter._filter_by_deployed_since(data_structures)

        # All structures should be returned when no filter configured
        assert len(filtered) == 1


class TestSchemaProcessorIgluExtraction:
    """Test Iglu schema extraction logic."""

    @pytest.fixture
    def config_iglu_mode(self):
        """Create config for Iglu-only mode."""
        config = Mock(spec=SnowplowSourceConfig)
        config.schema_pattern = Mock()
        config.schema_pattern.allowed = Mock(return_value=True)
        config.schema_types_to_extract = ["event", "entity"]
        config.include_version_in_urn = False
        config.platform_instance = None
        config.env = "PROD"
        return config

    @pytest.fixture
    def mock_deps_with_iglu(self):
        """Create mock dependencies with Iglu client."""
        deps = Mock()
        deps.bdp_client = None
        deps.iglu_client = Mock()
        deps.platform = "snowplow"
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state."""
        state = Mock()
        state.extracted_schema_urns = []
        return state

    @pytest.fixture
    def processor_iglu_mode(self, mock_deps_with_iglu, mock_state, config_iglu_mode):
        """Create processor for Iglu-only mode."""
        data_structure_builder = Mock()
        processor = SchemaProcessor(
            mock_deps_with_iglu, mock_state, data_structure_builder
        )
        processor.config = config_iglu_mode
        processor.report = Mock()
        return processor

    def test_extract_schemas_from_iglu_handles_empty_results(
        self, processor_iglu_mode, mock_deps_with_iglu
    ):
        """Test graceful handling when Iglu returns no schemas."""
        mock_deps_with_iglu.iglu_client.list_schemas.return_value = []

        # Extract schemas
        workunits = list(processor_iglu_mode._extract_schemas_from_iglu())

        # Should return empty and report failure
        assert len(workunits) == 0
        processor_iglu_mode.report.report_failure.assert_called_once()

    def test_extract_schemas_from_uris_handles_invalid_uri(
        self, processor_iglu_mode, mock_deps_with_iglu
    ):
        """Test that invalid Iglu URIs are skipped."""
        schema_uris = [
            "iglu:com.acme/valid_schema/jsonschema/1-0-0",
            "invalid-uri-format",
        ]

        mock_deps_with_iglu.iglu_client.parse_iglu_uri.side_effect = [
            {
                "vendor": "com.acme",
                "name": "valid_schema",
                "format": "jsonschema",
                "version": "1-0-0",
            },
            None,  # Invalid URI returns None
        ]

        # Extract schemas
        list(processor_iglu_mode._extract_schemas_from_uris(schema_uris))

        # Only valid URI should be processed
        # Invalid URI logged and skipped

    def test_extract_schemas_from_uris_handles_api_errors(
        self, processor_iglu_mode, mock_deps_with_iglu
    ):
        """Test that API errors when fetching schemas are handled gracefully."""
        schema_uris = ["iglu:com.acme/test_schema/jsonschema/1-0-0"]

        mock_deps_with_iglu.iglu_client.parse_iglu_uri.return_value = {
            "vendor": "com.acme",
            "name": "test_schema",
            "format": "jsonschema",
            "version": "1-0-0",
        }

        mock_deps_with_iglu.iglu_client.get_schema.side_effect = Exception(
            "Network timeout"
        )

        # Extract schemas - should not raise
        workunits = list(processor_iglu_mode._extract_schemas_from_uris(schema_uris))

        # No workunits but no exception
        assert len(workunits) == 0
        # Failure should be reported
        processor_iglu_mode.report.report_failure.assert_called()


class TestSchemaProcessorSchemaPatternFiltering:
    """Test schema pattern filtering logic."""

    @pytest.fixture
    def processor_with_pattern(self, mock_deps_with_bdp, mock_state):
        """Create processor with pattern filtering."""
        config = Mock(spec=SnowplowSourceConfig)
        config.schema_pattern = Mock()
        config.schema_types_to_extract = ["event", "entity"]

        data_structure_builder = Mock()
        processor = SchemaProcessor(
            mock_deps_with_bdp, mock_state, data_structure_builder
        )
        processor.config = config
        processor.report = Mock()
        return processor

    @pytest.fixture
    def mock_deps_with_bdp(self):
        """Create mock dependencies."""
        deps = Mock()
        deps.bdp_client = Mock()
        return deps

    @pytest.fixture
    def mock_state(self):
        """Create mock state."""
        return Mock()

    def test_filter_by_schema_pattern_allows_matching_schemas(
        self, processor_with_pattern
    ):
        """Test that schemas matching pattern are included."""
        processor_with_pattern.config.schema_pattern.allowed = Mock(
            side_effect=lambda x: "acme" in x
        )

        data_structures = [
            DataStructure(
                hash="hash1",
                vendor="com.acme",
                name="event1",
                meta=SchemaMetadata(schema_type="event"),
            ),
            DataStructure(
                hash="hash2",
                vendor="com.other",
                name="event2",
                meta=SchemaMetadata(schema_type="event"),
            ),
        ]

        filtered = processor_with_pattern._filter_by_schema_pattern(data_structures)

        # Only acme schema should pass
        assert len(filtered) == 1
        assert filtered[0].vendor == "com.acme"

    def test_filter_by_schema_pattern_handles_missing_vendor_or_name(
        self, processor_with_pattern
    ):
        """Test that schemas with missing vendor/name are skipped."""
        processor_with_pattern.config.schema_pattern.allowed = Mock(return_value=True)

        data_structures = [
            DataStructure(
                hash="hash1",
                vendor=None,
                name="event1",
                meta=SchemaMetadata(schema_type="event"),
            ),
            DataStructure(
                hash="hash2",
                vendor="com.acme",
                name=None,
                meta=SchemaMetadata(schema_type="event"),
            ),
        ]

        filtered = processor_with_pattern._filter_by_schema_pattern(data_structures)

        # Both should be skipped
        assert len(filtered) == 0
