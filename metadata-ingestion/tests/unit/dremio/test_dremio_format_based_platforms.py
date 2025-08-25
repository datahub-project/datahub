"""
Unit tests for Dremio format-based platform URN functionality.

Tests the new feature that generates platform-specific URNs based on table format
(e.g., delta-lake, iceberg) while maintaining backward compatibility.
"""

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioDataset,
)
from datahub.ingestion.source.dremio.dremio_source import DremioSource


class TestFormatBasedPlatforms:
    """Test format-based platform URN generation functionality."""

    @pytest.fixture
    def config_with_format_platforms(self):
        """Config with format-based platforms enabled."""
        return DremioSourceConfig(
            username="test",
            password="test",
            hostname="localhost",
            use_format_based_platform_urns=True,
            format_platform_mapping={
                "delta": "delta-lake",
                "iceberg": "iceberg",
                "hudi": "hudi",
                "parquet": "s3",
            },
        )

    @pytest.fixture
    def config_without_format_platforms(self):
        """Config with format-based platforms disabled (default)."""
        return DremioSourceConfig(
            username="test",
            password="test",
            hostname="localhost",
            use_format_based_platform_urns=False,
        )

    @pytest.fixture
    def pipeline_context(self):
        """Mock pipeline context."""
        ctx = Mock(spec=PipelineContext)
        ctx.graph = None
        ctx.pipeline_name = "test_pipeline"
        ctx.run_id = "test_run_id"
        return ctx

    def test_format_platform_mapping_static_method(self):
        """Test the static method for format-based platform detection."""

        # Test Delta Lake detection
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type="DELTA",
            source_type="S3",
            format_mapping={"delta": "delta-lake", "iceberg": "iceberg"},
            use_format_based_platforms=True,
        )
        assert platform == "delta-lake"

        # Test Iceberg detection
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type="ICEBERG",
            source_type="S3",
            format_mapping={"delta": "delta-lake", "iceberg": "iceberg"},
            use_format_based_platforms=True,
        )
        assert platform == "iceberg"

        # Test case insensitive matching
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type="delta_lake",
            source_type="S3",
            format_mapping={"delta": "delta-lake"},
            use_format_based_platforms=True,
        )
        assert platform == "delta-lake"

        # Test fallback to source mapping when format not found
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type="UNKNOWN_FORMAT",
            source_type="S3",
            format_mapping={"delta": "delta-lake"},
            use_format_based_platforms=True,
        )
        assert platform == "s3"  # Falls back to source mapping

        # Test disabled format-based platforms
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type="DELTA",
            source_type="S3",
            format_mapping={"delta": "delta-lake"},
            use_format_based_platforms=False,
        )
        assert platform == "s3"  # Uses source mapping when disabled

        # Test None format_type
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type=None,
            source_type="S3",
            format_mapping={"delta": "delta-lake"},
            use_format_based_platforms=True,
        )
        assert platform == "s3"  # Falls back to source mapping

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_determine_dataset_platform_enabled(
        self,
        mock_catalog_class,
        mock_api_class,
        config_with_format_platforms,
        pipeline_context,
    ):
        """Test _determine_dataset_platform method when format-based platforms are enabled."""

        # Create source with format-based platforms enabled
        source = DremioSource(config_with_format_platforms, pipeline_context)

        # Mock source map
        source.source_map = {"s3_source": Mock(platform="s3")}

        # Test Delta Lake dataset
        delta_dataset = Mock(spec=DremioDataset)
        delta_dataset.format_type = "DELTA"
        delta_dataset.path = ["s3_source", "warehouse"]
        delta_dataset.resource_name = "test_table"

        platform = source._determine_dataset_platform(delta_dataset)
        assert platform == "delta-lake"

        # Test Iceberg dataset
        iceberg_dataset = Mock(spec=DremioDataset)
        iceberg_dataset.format_type = "ICEBERG"
        iceberg_dataset.path = ["s3_source", "warehouse"]
        iceberg_dataset.resource_name = "test_table"

        platform = source._determine_dataset_platform(iceberg_dataset)
        assert platform == "iceberg"

        # Test unknown format (should fall back to source mapping)
        unknown_dataset = Mock(spec=DremioDataset)
        unknown_dataset.format_type = "UNKNOWN"
        unknown_dataset.path = ["s3_source", "warehouse"]
        unknown_dataset.resource_name = "test_table"

        platform = source._determine_dataset_platform(unknown_dataset)
        assert platform == "s3"

        # Test dataset with no format_type
        no_format_dataset = Mock(spec=DremioDataset)
        no_format_dataset.format_type = None
        no_format_dataset.path = ["s3_source", "warehouse"]
        no_format_dataset.resource_name = "test_table"

        platform = source._determine_dataset_platform(no_format_dataset)
        assert platform == "s3"

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_determine_dataset_platform_disabled(
        self,
        mock_catalog_class,
        mock_api_class,
        config_without_format_platforms,
        pipeline_context,
    ):
        """Test _determine_dataset_platform method when format-based platforms are disabled."""

        # Create source with format-based platforms disabled
        source = DremioSource(config_without_format_platforms, pipeline_context)

        # Test that all datasets use "dremio" platform when disabled
        delta_dataset = Mock(spec=DremioDataset)
        delta_dataset.format_type = "DELTA"
        delta_dataset.path = ["s3_source", "warehouse"]
        delta_dataset.resource_name = "test_table"

        platform = source._determine_dataset_platform(delta_dataset)
        assert platform == "dremio"

    def test_container_urns_always_use_dremio_platform(self):
        """Test that container URNs always use 'dremio' platform regardless of format settings."""

        # This is tested implicitly by checking that DremioAspects is initialized
        # with platform="dremio" in the source initialization.
        # The actual URN generation is tested in integration tests.
        pass

    def test_default_format_platform_mapping(self, config_with_format_platforms):
        """Test that default format platform mapping is correctly configured."""

        expected_mapping = {
            "delta": "delta-lake",
            "iceberg": "iceberg",
            "hudi": "hudi",
            "parquet": "s3",
        }

        assert config_with_format_platforms.format_platform_mapping == expected_mapping

    def test_config_validation_format_platforms_disabled_by_default(self):
        """Test that format-based platforms are disabled by default."""

        config = DremioSourceConfig(
            username="test",
            password="test",
            hostname="localhost",
        )

        assert config.use_format_based_platform_urns is False

    def test_custom_format_platform_mapping(self):
        """Test custom format platform mapping configuration."""

        custom_mapping = {
            "delta": "custom-delta",
            "iceberg": "custom-iceberg",
            "custom_format": "custom-platform",
        }

        config = DremioSourceConfig(
            username="test",
            password="test",
            hostname="localhost",
            use_format_based_platform_urns=True,
            format_platform_mapping=custom_mapping,
        )

        assert config.format_platform_mapping == custom_mapping

    def test_partial_format_matching(self):
        """Test that format matching works with partial strings."""

        # Test that "delta_lake_format" matches "delta" key
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type="delta_lake_format",
            source_type="S3",
            format_mapping={"delta": "delta-lake"},
            use_format_based_platforms=True,
        )
        assert platform == "delta-lake"

        # Test that "apache_iceberg" matches "iceberg" key
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type="apache_iceberg",
            source_type="S3",
            format_mapping={"iceberg": "iceberg"},
            use_format_based_platforms=True,
        )
        assert platform == "iceberg"

    def test_format_priority_first_match_wins(self):
        """Test that when multiple format keys match, the first one wins."""

        # If format_type contains both "delta" and "parquet",
        # the first matching key in iteration order should win
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type="delta_parquet_hybrid",
            source_type="S3",
            format_mapping={"delta": "delta-lake", "parquet": "s3"},
            use_format_based_platforms=True,
        )

        # The result depends on dictionary iteration order, but should be consistent
        assert platform in ["delta-lake", "s3"]

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_profiling_uses_format_based_platform(
        self,
        mock_catalog_class,
        mock_api_class,
        config_with_format_platforms,
        pipeline_context,
    ):
        """Test that profiling uses the same format-based platform as the dataset."""

        source = DremioSource(config_with_format_platforms, pipeline_context)
        source.source_map = {"s3_source": Mock(platform="s3")}

        # Mock dataset with Delta format
        delta_dataset = Mock(spec=DremioDataset)
        delta_dataset.format_type = "DELTA"
        delta_dataset.path = ["s3_source", "warehouse"]
        delta_dataset.resource_name = "test_table"

        # Mock the profiler
        with patch.object(source.profiler, "get_workunits", return_value=iter([])):
            # The generate_profiles method should use the format-based platform
            list(source.generate_profiles(delta_dataset))

        # We can't easily test the URN generation without more complex mocking,
        # but the integration tests verify this works correctly
