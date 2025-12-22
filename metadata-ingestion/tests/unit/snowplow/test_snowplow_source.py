"""Unit tests for Snowplow source business logic."""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.snowplow.snowplow import SnowplowSource
from datahub.ingestion.source.snowplow.snowplow_config import (
    SnowplowSourceConfig,
)
from datahub.ingestion.source.snowplow.snowplow_models import (
    DataStructure,
)


class TestSnowplowSourceVersionParsing:
    """Test SchemaVer version parsing logic."""

    def test_parse_schemaver_valid_versions(self):
        """Test parsing valid SchemaVer format versions."""
        # Valid formats
        assert SnowplowSource._parse_schemaver("1-0-0") == (1, 0, 0)
        assert SnowplowSource._parse_schemaver("2-1-3") == (2, 1, 3)
        assert SnowplowSource._parse_schemaver("10-5-2") == (10, 5, 2)
        assert SnowplowSource._parse_schemaver("0-0-1") == (0, 0, 1)

    def test_parse_schemaver_invalid_formats(self):
        """Test parsing invalid version formats returns (0,0,0)."""
        # Invalid formats should return (0,0,0)
        assert SnowplowSource._parse_schemaver("invalid") == (0, 0, 0)
        assert SnowplowSource._parse_schemaver("1.0.0") == (0, 0, 0)  # Wrong separator
        assert SnowplowSource._parse_schemaver("1-0") == (0, 0, 0)  # Missing part
        assert SnowplowSource._parse_schemaver("") == (0, 0, 0)  # Empty string
        assert SnowplowSource._parse_schemaver("1-a-0") == (0, 0, 0)  # Non-numeric

    def test_parse_schemaver_sorting(self):
        """Test that parsed versions can be sorted correctly."""
        versions = ["2-0-0", "1-1-0", "1-0-0", "2-1-0", "1-0-1"]
        sorted_versions = sorted(versions, key=SnowplowSource._parse_schemaver)

        # Expected order: 1-0-0, 1-0-1, 1-1-0, 2-0-0, 2-1-0
        assert sorted_versions == ["1-0-0", "1-0-1", "1-1-0", "2-0-0", "2-1-0"]

    def test_parse_schemaver_edge_cases(self):
        """Test edge cases in version parsing."""
        # Large version numbers
        assert SnowplowSource._parse_schemaver("999-999-999") == (999, 999, 999)

        # Extra parts (takes first 3 parts)
        result = SnowplowSource._parse_schemaver("1-0-0-extra")
        # This actually succeeds taking first 3 parts: (1, 0, 0)
        assert result == (1, 0, 0)


class TestSnowplowSourceTimestampFiltering:
    """Test timestamp-based filtering logic."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config for testing."""
        return Mock(spec=SnowplowSourceConfig)

    @pytest.fixture
    def mock_source(self, mock_config):
        """Create a mock source instance."""
        source = Mock(spec=SnowplowSource)
        source.config = mock_config
        source.report = Mock()
        # Bind the actual methods to the mock
        source._is_deployment_recent_main = (
            SnowplowSource._is_deployment_recent_main.__get__(source, SnowplowSource)
        )
        source._filter_by_deployed_since_main = (
            SnowplowSource._filter_by_deployed_since_main.__get__(
                source, SnowplowSource
            )
        )
        return source

    def test_is_deployment_recent_with_recent_timestamp(self, mock_source):
        """Test that recent deployments are correctly identified."""
        since_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # Recent deployment (after since_dt)
        recent_ts = "2024-01-15T10:00:00Z"
        assert mock_source._is_deployment_recent_main(recent_ts, since_dt) is True

        # Exact match (edge case)
        exact_ts = "2024-01-01T00:00:00Z"
        assert mock_source._is_deployment_recent_main(exact_ts, since_dt) is True

    def test_is_deployment_recent_with_old_timestamp(self, mock_source):
        """Test that old deployments are correctly identified."""
        since_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # Old deployment (before since_dt)
        old_ts = "2023-12-31T23:59:59Z"
        assert mock_source._is_deployment_recent_main(old_ts, since_dt) is False

    def test_is_deployment_recent_with_malformed_timestamp(self, mock_source):
        """Test handling of malformed timestamps."""
        since_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # Malformed timestamps should return False and log warning
        assert mock_source._is_deployment_recent_main("invalid", since_dt) is False
        assert mock_source._is_deployment_recent_main("", since_dt) is False
        assert (
            mock_source._is_deployment_recent_main("2024-13-01T00:00:00Z", since_dt)
            is False
        )

    def test_is_deployment_recent_with_timezone_variations(self, mock_source):
        """Test timestamp parsing with different timezone formats."""
        since_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # With Z suffix (UTC)
        assert (
            mock_source._is_deployment_recent_main("2024-01-15T10:00:00Z", since_dt)
            is True
        )

        # With +00:00 suffix
        assert (
            mock_source._is_deployment_recent_main(
                "2024-01-15T10:00:00+00:00", since_dt
            )
            is True
        )

    def test_filter_by_deployed_since_with_no_config(self, mock_source):
        """Test that filtering is skipped when deployed_since is not configured."""
        mock_source.config.deployed_since = None

        data_structures = [
            Mock(spec=DataStructure, vendor="com.test", name="schema1"),
            Mock(spec=DataStructure, vendor="com.test", name="schema2"),
        ]

        result = mock_source._filter_by_deployed_since_main(data_structures)

        # Should return all structures unchanged
        assert result == data_structures

    def test_filter_by_deployed_since_filters_old_deployments(self, mock_source):
        """Test that old deployments are filtered out."""
        mock_source.config.deployed_since = "2024-01-01T00:00:00Z"

        # Create data structures with different deployment timestamps
        recent_ds = Mock(spec=DataStructure)
        recent_ds.vendor = "com.test"
        recent_ds.name = "recent_schema"
        recent_ds.deployments = [
            Mock(ts="2024-01-15T10:00:00Z"),  # Recent
        ]

        old_ds = Mock(spec=DataStructure)
        old_ds.vendor = "com.test"
        old_ds.name = "old_schema"
        old_ds.deployments = [
            Mock(ts="2023-12-31T23:59:59Z"),  # Old
        ]

        data_structures = [recent_ds, old_ds]

        result = mock_source._filter_by_deployed_since_main(data_structures)

        # Should only return recent_ds
        assert len(result) == 1
        assert result[0] == recent_ds

    def test_filter_by_deployed_since_with_mixed_deployments(self, mock_source):
        """Test filtering when a schema has multiple deployments (some old, some new)."""
        mock_source.config.deployed_since = "2024-01-01T00:00:00Z"

        # Schema with both old and recent deployments (should be included)
        mixed_ds = Mock(spec=DataStructure)
        mixed_ds.vendor = "com.test"
        mixed_ds.name = "mixed_schema"
        mixed_ds.deployments = [
            Mock(ts="2023-12-01T00:00:00Z"),  # Old
            Mock(ts="2024-01-15T10:00:00Z"),  # Recent
        ]

        result = mock_source._filter_by_deployed_since_main([mixed_ds])

        # Should include because at least one deployment is recent
        assert len(result) == 1
        assert result[0] == mixed_ds

    def test_filter_by_deployed_since_with_no_deployments(self, mock_source):
        """Test filtering when schemas have no deployment history."""
        mock_source.config.deployed_since = "2024-01-01T00:00:00Z"

        no_deployments_ds = Mock(spec=DataStructure)
        no_deployments_ds.vendor = "com.test"
        no_deployments_ds.name = "no_deploy_schema"
        no_deployments_ds.deployments = []

        result = mock_source._filter_by_deployed_since_main([no_deployments_ds])

        # Should filter out (no deployments = can't verify if recent)
        assert len(result) == 0

    def test_filter_by_deployed_since_with_invalid_config_timestamp(self, mock_source):
        """Test handling of invalid deployed_since config value."""
        mock_source.config.deployed_since = "invalid-timestamp"

        data_structures = [
            Mock(spec=DataStructure, vendor="com.test", name="schema1"),
        ]

        result = mock_source._filter_by_deployed_since_main(data_structures)

        # Should return all structures when config is invalid (logged warning)
        assert result == data_structures


class TestSnowplowSourceSchemaPatternFiltering:
    """Test schema pattern filtering logic."""

    @pytest.fixture
    def mock_source(self):
        """Create a mock source instance."""
        source = Mock(spec=SnowplowSource)
        source.config = Mock()
        source.report = Mock()
        source.report.report_schema_filtered = Mock()
        # Bind the actual method to the mock
        source._filter_by_schema_pattern_main = (
            SnowplowSource._filter_by_schema_pattern_main.__get__(
                source, SnowplowSource
            )
        )
        return source

    def test_filter_by_schema_pattern_no_pattern_configured(self, mock_source):
        """Test that all schemas pass when no pattern is configured."""
        mock_source.config.schema_pattern = None

        data_structures = [
            Mock(
                spec=DataStructure,
                vendor="com.test",
                name="schema1",
                hash="hash1",
                meta=None,
            ),
            Mock(
                spec=DataStructure,
                vendor="com.other",
                name="schema2",
                hash="hash2",
                meta=None,
            ),
        ]

        result = mock_source._filter_by_schema_pattern_main(data_structures)

        assert len(result) == 2
        assert result == data_structures

    def test_filter_by_schema_pattern_with_allow_pattern(self, mock_source):
        """Test filtering with allow pattern."""
        # Mock pattern that only allows com.test schemas
        mock_pattern = Mock()
        mock_pattern.allowed = lambda x: x.startswith("com.test/")
        mock_source.config.schema_pattern = mock_pattern

        # Create mock data structures without spec constraint
        ds1 = Mock()
        ds1.vendor = "com.test"
        ds1.name = "allowed_schema"
        ds1.hash = "hash1"
        ds1.meta = None

        ds2 = Mock()
        ds2.vendor = "com.other"
        ds2.name = "filtered_schema"
        ds2.hash = "hash2"
        ds2.meta = None

        data_structures = [ds1, ds2]

        result = mock_source._filter_by_schema_pattern_main(data_structures)

        # Should only include com.test/allowed_schema
        assert len(result) == 1
        assert result[0].vendor == "com.test"
        assert result[0].name == "allowed_schema"

        # Should report the filtered schema
        mock_source.report.report_schema_filtered.assert_called_once()

    def test_filter_by_schema_pattern_with_missing_vendor_or_name(self, mock_source):
        """Test handling of schemas with missing vendor or name."""
        mock_pattern = Mock()
        mock_pattern.allowed = Mock(return_value=True)
        mock_source.config.schema_pattern = mock_pattern

        # Schema with missing vendor
        no_vendor_ds = Mock()
        no_vendor_ds.vendor = None
        no_vendor_ds.name = "schema1"
        no_vendor_ds.hash = "hash1"

        # Schema with missing name
        no_name_ds = Mock()
        no_name_ds.vendor = "com.test"
        no_name_ds.name = None
        no_name_ds.hash = "hash2"

        result = mock_source._filter_by_schema_pattern_main([no_vendor_ds, no_name_ds])

        # Should skip both (can't construct identifier)
        assert len(result) == 0

    def test_filter_by_schema_pattern_with_schema_type_reporting(self, mock_source):
        """Test that correct schema type is reported when filtering."""
        mock_pattern = Mock()
        mock_pattern.allowed = Mock(return_value=False)
        mock_source.config.schema_pattern = mock_pattern

        # Schema with explicit type in meta
        event_schema = Mock(spec=DataStructure)
        event_schema.vendor = "com.test"
        event_schema.name = "event_schema"
        event_schema.hash = "hash1"
        event_schema.meta = Mock(schema_type="event")

        # Schema with no meta (should default to "event")
        no_meta_schema = Mock(spec=DataStructure)
        no_meta_schema.vendor = "com.test"
        no_meta_schema.name = "no_meta_schema"
        no_meta_schema.hash = "hash2"
        no_meta_schema.meta = None

        mock_source._filter_by_schema_pattern_main([event_schema, no_meta_schema])

        # Should report schema type correctly
        assert mock_source.report.report_schema_filtered.call_count == 2
        calls = mock_source.report.report_schema_filtered.call_args_list

        # First call should have "event" type from meta
        assert calls[0][0][0] == "event"
        # Second call should have "event" type (default)
        assert calls[1][0][0] == "event"


class TestSnowplowSourceSnowflakeColumnMapping:
    """Test Snowflake column name mapping logic."""

    @pytest.fixture
    def mock_source(self):
        """Create a mock source instance."""
        with patch.object(SnowplowSource, "__init__", lambda self, config, ctx: None):
            source = SnowplowSource.__new__(SnowplowSource)
            return source

    def test_map_schema_to_snowflake_column_basic(self, mock_source):
        """Test basic schema to Snowflake column mapping."""
        column = mock_source._map_schema_to_snowflake_column(
            vendor="com.acme", name="checkout_started", version="1-0-0"
        )

        assert column == "contexts_com_acme_checkout_started_1"

    def test_map_schema_to_snowflake_column_with_dots_in_vendor(self, mock_source):
        """Test mapping with multiple dots in vendor name."""
        column = mock_source._map_schema_to_snowflake_column(
            vendor="com.snowplowanalytics.snowplow",
            name="web_page",
            version="1-0-0",
        )

        assert column == "contexts_com_snowplowanalytics_snowplow_web_page_1"

    def test_map_schema_to_snowflake_column_extracts_major_version(self, mock_source):
        """Test that only major version is used in column name."""
        # Version with SchemaVer format
        column1 = mock_source._map_schema_to_snowflake_column(
            vendor="com.test", name="schema", version="2-3-5"
        )
        assert column1 == "contexts_com_test_schema_2"

        # Version with semantic versioning format
        column2 = mock_source._map_schema_to_snowflake_column(
            vendor="com.test", name="schema", version="3.1.0"
        )
        assert column2 == "contexts_com_test_schema_3"

    def test_map_schema_to_snowflake_column_with_special_chars(self, mock_source):
        """Test mapping with special characters that need escaping."""
        # Dots and slashes should be replaced with underscores
        column = mock_source._map_schema_to_snowflake_column(
            vendor="com.example/test", name="schema.name", version="1-0-0"
        )

        assert column == "contexts_com_example_test_schema_name_1"

    def test_map_schema_to_snowflake_column_version_formats(self, mock_source):
        """Test various version format edge cases."""
        # Single digit version
        assert (
            mock_source._map_schema_to_snowflake_column(
                vendor="com.test", name="schema", version="5"
            )
            == "contexts_com_test_schema_5"
        )

        # Version with only major-minor
        assert (
            mock_source._map_schema_to_snowflake_column(
                vendor="com.test", name="schema", version="2-1"
            )
            == "contexts_com_test_schema_2"
        )


class TestSnowplowSourceFieldVersionMapping:
    """Test field version mapping logic."""

    @pytest.fixture
    def mock_bdp_client(self):
        """Create a mock BDP client."""
        return Mock()

    @pytest.fixture
    def mock_source(self, mock_bdp_client):
        """Create a mock source with BDP client."""
        with patch.object(SnowplowSource, "__init__", lambda self, config, ctx: None):
            source = SnowplowSource.__new__(SnowplowSource)
            source.bdp_client = mock_bdp_client
            return source

    def test_build_field_version_mapping_no_deployments(self, mock_source):
        """Test that empty mapping is returned when no deployments exist."""
        data_structure = Mock(spec=DataStructure)
        data_structure.vendor = "com.test"
        data_structure.name = "schema"
        data_structure.hash = "test-hash"
        data_structure.deployments = []

        result = mock_source._build_field_version_mapping(data_structure)

        assert result == {}

    def test_build_field_version_mapping_no_hash(self, mock_source):
        """Test that empty mapping is returned when data structure has no hash."""
        data_structure = Mock(spec=DataStructure)
        data_structure.vendor = "com.test"
        data_structure.name = "schema"
        data_structure.hash = None
        data_structure.deployments = [Mock(version="1-0-0")]

        result = mock_source._build_field_version_mapping(data_structure)

        assert result == {}

    def test_build_field_version_mapping_tracks_field_additions(self, mock_source):
        """Test that field additions across versions are tracked correctly."""
        data_structure = Mock(spec=DataStructure)
        data_structure.vendor = "com.test"
        data_structure.name = "schema"
        data_structure.hash = "test-hash"
        data_structure.deployments = [
            Mock(version="1-0-0"),
            Mock(version="1-1-0"),
            Mock(version="2-0-0"),
        ]

        # Mock version responses
        v1_0_0 = Mock()
        v1_0_0.data = Mock(properties={"field1": {}, "field2": {}})

        v1_1_0 = Mock()
        v1_1_0.data = Mock(properties={"field1": {}, "field2": {}, "field3": {}})

        v2_0_0 = Mock()
        v2_0_0.data = Mock(
            properties={"field1": {}, "field2": {}, "field3": {}, "field4": {}}
        )

        mock_source.bdp_client.get_data_structure_version.side_effect = [
            v1_0_0,
            v1_1_0,
            v2_0_0,
        ]

        result = mock_source._build_field_version_mapping(data_structure)

        # field1 and field2 added in 1-0-0
        assert result["field1"] == "1-0-0"
        assert result["field2"] == "1-0-0"

        # field3 added in 1-1-0
        assert result["field3"] == "1-1-0"

        # field4 added in 2-0-0
        assert result["field4"] == "2-0-0"

    def test_build_field_version_mapping_handles_version_fetch_failure(
        self, mock_source
    ):
        """Test that version fetch failures are handled gracefully."""
        data_structure = Mock(spec=DataStructure)
        data_structure.vendor = "com.test"
        data_structure.name = "schema"
        data_structure.hash = "test-hash"
        data_structure.deployments = [
            Mock(version="1-0-0"),
            Mock(version="1-1-0"),
        ]

        # First version succeeds, second fails
        v1_0_0 = Mock()
        v1_0_0.data = Mock(properties={"field1": {}})

        mock_source.bdp_client.get_data_structure_version.side_effect = [
            v1_0_0,
            Exception("API error"),
        ]

        result = mock_source._build_field_version_mapping(data_structure)

        # Should still track field1 from successful version
        assert result["field1"] == "1-0-0"

        # field from failed version won't be tracked (graceful degradation)
        assert len(result) == 1

    def test_build_field_version_mapping_version_sorting(self, mock_source):
        """Test that versions are processed in correct order."""
        data_structure = Mock(spec=DataStructure)
        data_structure.vendor = "com.test"
        data_structure.name = "schema"
        data_structure.hash = "test-hash"

        # Deployments in random order
        data_structure.deployments = [
            Mock(version="2-0-0"),
            Mock(version="1-0-0"),
            Mock(version="1-1-0"),
        ]

        v1_0_0 = Mock()
        v1_0_0.data = Mock(properties={"field1": {}})

        v1_1_0 = Mock()
        v1_1_0.data = Mock(properties={"field1": {}, "field2": {}})

        v2_0_0 = Mock()
        v2_0_0.data = Mock(properties={"field1": {}, "field2": {}, "field3": {}})

        # Mock should be called in sorted order: 1-0-0, 1-1-0, 2-0-0
        mock_source.bdp_client.get_data_structure_version.side_effect = [
            v1_0_0,
            v1_1_0,
            v2_0_0,
        ]

        result = mock_source._build_field_version_mapping(data_structure)

        # Verify fields mapped to correct versions
        assert result["field1"] == "1-0-0"
        assert result["field2"] == "1-1-0"
        assert result["field3"] == "2-0-0"


class TestSnowplowSourcePIIFieldExtraction:
    """Test PII field extraction from enrichment configuration."""

    @pytest.fixture
    def mock_bdp_client(self):
        """Create a mock BDP client."""
        return Mock()

    @pytest.fixture
    def mock_source_with_pii_enabled(self, mock_bdp_client):
        """Create a mock source with PII enrichment enabled."""
        with patch.object(SnowplowSource, "__init__", lambda self, config, ctx: None):
            source = SnowplowSource.__new__(SnowplowSource)
            source.bdp_client = mock_bdp_client
            source.config = Mock()
            source.config.field_tagging = Mock()
            source.config.field_tagging.use_pii_enrichment = True
            source.state = Mock()
            source.state.pii_fields_cache = None
            return source

    def test_extract_pii_fields_returns_cached_value(
        self, mock_source_with_pii_enabled
    ):
        """Test that cached PII fields are returned without API calls."""
        # Set cached value
        cached_fields = {"email", "phone"}
        mock_source_with_pii_enabled.state.pii_fields_cache = cached_fields

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should return cached value without calling API
        assert result == cached_fields
        mock_source_with_pii_enabled.bdp_client.get_pipelines.assert_not_called()

    def test_extract_pii_fields_disabled(self, mock_bdp_client):
        """Test that empty set is returned when PII enrichment is disabled."""
        with patch.object(SnowplowSource, "__init__", lambda self, config, ctx: None):
            source = SnowplowSource.__new__(SnowplowSource)
            source.bdp_client = mock_bdp_client
            source.config = Mock()
            source.config.field_tagging = Mock()
            source.config.field_tagging.use_pii_enrichment = False
            source.state = Mock()
            source.state.pii_fields_cache = None

            result = source._extract_pii_fields()

            assert result == set()
            mock_bdp_client.get_pipelines.assert_not_called()

    def test_extract_pii_fields_no_bdp_client(self):
        """Test that empty set is returned when BDP client is not available."""
        with patch.object(SnowplowSource, "__init__", lambda self, config, ctx: None):
            source = SnowplowSource.__new__(SnowplowSource)
            source.bdp_client = None
            source.config = Mock()
            source.config.field_tagging = Mock()
            source.config.field_tagging.use_pii_enrichment = True
            source.state = Mock()
            source.state.pii_fields_cache = None

            result = source._extract_pii_fields()

            assert result == set()

    def test_extract_pii_fields_from_enrichment_list_format(
        self, mock_source_with_pii_enabled
    ):
        """Test extraction of PII fields from enrichment with list format."""
        # Mock pipeline and enrichment
        pipeline = Mock(id="pipeline-1")
        enrichment = Mock()
        enrichment.schema_ref = "iglu:com.snowplowanalytics.snowplow.enrichments/pii_pseudonymization_enrichment/jsonschema/2-0-0"
        enrichment.content = Mock()
        enrichment.content.data = Mock()
        enrichment.content.data.parameters = {
            "pii": [
                {"fieldName": "email"},
                {"fieldName": "phone"},
                {"fieldName": "ssn"},
            ]
        }

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [pipeline]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.return_value = [
            enrichment
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        assert result == {"email", "phone", "ssn"}

    def test_extract_pii_fields_from_enrichment_dict_format(
        self, mock_source_with_pii_enabled
    ):
        """Test extraction of PII fields from enrichment with dict format."""
        pipeline = Mock(id="pipeline-1")
        enrichment = Mock()
        enrichment.schema_ref = "iglu:com.snowplowanalytics.snowplow.enrichments/pii_pseudonymization_enrichment/jsonschema/2-0-0"
        enrichment.content = Mock()
        enrichment.content.data = Mock()
        enrichment.content.data.parameters = {
            "pii": {"fieldNames": ["email", "credit_card"]}
        }

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [pipeline]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.return_value = [
            enrichment
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        assert result == {"email", "credit_card"}

    def test_extract_pii_fields_from_multiple_pipelines(
        self, mock_source_with_pii_enabled
    ):
        """Test extraction from multiple pipelines."""
        pipeline1 = Mock(id="pipeline-1")
        pipeline2 = Mock(id="pipeline-2")

        enrichment1 = Mock()
        enrichment1.schema_ref = "pii_pseudonymization"
        enrichment1.content = Mock()
        enrichment1.content.data = Mock()
        enrichment1.content.data.parameters = {"pii": [{"fieldName": "email"}]}

        enrichment2 = Mock()
        enrichment2.schema_ref = "pii_pseudonymization"
        enrichment2.content = Mock()
        enrichment2.content.data = Mock()
        enrichment2.content.data.parameters = {"pii": [{"fieldName": "phone"}]}

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [
            pipeline1,
            pipeline2,
        ]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.side_effect = [
            [enrichment1],
            [enrichment2],
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should combine fields from both pipelines
        assert result == {"email", "phone"}

    def test_extract_pii_fields_skips_non_pii_enrichments(
        self, mock_source_with_pii_enabled
    ):
        """Test that non-PII enrichments are skipped."""
        pipeline = Mock(id="pipeline-1")

        pii_enrichment = Mock()
        pii_enrichment.schema_ref = "pii_pseudonymization"
        pii_enrichment.content = Mock()
        pii_enrichment.content.data = Mock()
        pii_enrichment.content.data.parameters = {"pii": [{"fieldName": "email"}]}

        other_enrichment = Mock()
        other_enrichment.schema_ref = "ip_lookups"
        other_enrichment.content = Mock()
        other_enrichment.content.data = Mock()
        other_enrichment.content.data.parameters = {"database": "GeoIP2-City.mmdb"}

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [pipeline]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.return_value = [
            pii_enrichment,
            other_enrichment,
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should only extract from PII enrichment
        assert result == {"email"}

    def test_extract_pii_fields_handles_missing_schema_ref(
        self, mock_source_with_pii_enabled
    ):
        """Test handling of enrichments with missing schema_ref."""
        pipeline = Mock(id="pipeline-1")

        enrichment_no_ref = Mock()
        enrichment_no_ref.schema_ref = None

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [pipeline]
        mock_source_with_pii_enabled.bdp_client.get_enrichments.return_value = [
            enrichment_no_ref
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should return empty set (no valid enrichments)
        assert result == set()

    def test_extract_pii_fields_handles_api_failures(
        self, mock_source_with_pii_enabled
    ):
        """Test that API failures are handled gracefully."""
        mock_source_with_pii_enabled.bdp_client.get_pipelines.side_effect = Exception(
            "API error"
        )

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should return empty set and log warning
        assert result == set()

        # Should cache the empty result
        assert mock_source_with_pii_enabled.state.pii_fields_cache == set()

    def test_extract_pii_fields_handles_enrichment_fetch_failure(
        self, mock_source_with_pii_enabled
    ):
        """Test handling when enrichment fetch fails for a specific pipeline."""
        pipeline1 = Mock(id="pipeline-1")
        pipeline2 = Mock(id="pipeline-2")

        enrichment = Mock()
        enrichment.schema_ref = "pii_pseudonymization"
        enrichment.content = Mock()
        enrichment.content.data = Mock()
        enrichment.content.data.parameters = {"pii": [{"fieldName": "email"}]}

        mock_source_with_pii_enabled.bdp_client.get_pipelines.return_value = [
            pipeline1,
            pipeline2,
        ]

        # First pipeline succeeds, second fails
        mock_source_with_pii_enabled.bdp_client.get_enrichments.side_effect = [
            [enrichment],
            Exception("Fetch failed"),
        ]

        result = mock_source_with_pii_enabled._extract_pii_fields()

        # Should still return fields from successful pipeline
        assert result == {"email"}
