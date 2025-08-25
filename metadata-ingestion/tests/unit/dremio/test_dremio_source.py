"""
Comprehensive unit tests for the main DremioSource functionality.

This test file covers the core ingestion workflow and main functionality
of the Dremio connector, including:
- Basic ingestion workflow
- Container, dataset, and glossary term processing
- Configuration handling
- Report generation
- Success scenarios and normal operation

This complements the focused test files that cover specific aspects
like error handling, streaming, and schema filtering.
"""

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dremio.dremio_api import DremioEdition
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioDataset,
    DremioDatasetType,
    DremioFolder,
    DremioSourceContainer,
    DremioSpace,
)
from datahub.ingestion.source.dremio.dremio_source import DremioSource


class TestDremioSource:
    """Comprehensive tests for DremioSource main functionality."""

    @pytest.fixture
    def pipeline_context(self):
        """Create a pipeline context for testing."""
        return PipelineContext(run_id="test_run")

    @pytest.fixture
    def basic_config(self):
        """Create a basic test configuration."""
        return DremioSourceConfig(
            hostname="test-dremio",
            port=9047,
            tls=False,
            authentication_method="password",
            username="test-user",
            password="test-password",
            schema_pattern={"allow": [".*"], "deny": []},
        )

    @pytest.fixture
    def config_with_profiling(self):
        """Create a configuration with profiling enabled."""
        config = DremioSourceConfig(
            hostname="test-dremio",
            port=9047,
            tls=False,
            authentication_method="password",
            username="test-user",
            password="test-password",
            schema_pattern={"allow": [".*"], "deny": []},
        )
        config.profiling.enabled = True
        return config

    @pytest.fixture
    def mock_containers(self):
        """Create mock container data."""
        return [
            {
                "container_type": "SOURCE",
                "name": "s3_source",
                "id": "source_id_1",
                "source_type": "S3",
                "root_path": "/data",
                "database_name": None,
            },
            {
                "container_type": "SPACE",
                "name": "analytics_space",
                "id": "space_id_1",
            },
            {
                "container_type": "FOLDER",
                "name": "processed",
                "id": "folder_id_1",
                "path": ["analytics_space"],
            },
        ]

    @pytest.fixture
    def mock_datasets(self):
        """Create mock dataset data."""
        return [
            {
                "TABLE_NAME": "customers",
                "TABLE_SCHEMA": "[s3_source]",
                "RESOURCE_ID": "table_id_1",
                "LOCATION_ID": "source_id_1",
                "COLUMNS": [
                    {
                        "COLUMN_NAME": "id",
                        "DATA_TYPE": "INTEGER",
                        "IS_NULLABLE": "NO",
                        "COLUMN_SIZE": None,
                    },
                    {
                        "COLUMN_NAME": "name",
                        "DATA_TYPE": "VARCHAR",
                        "IS_NULLABLE": "YES",
                        "COLUMN_SIZE": 255,
                    },
                ],
                "VIEW_DEFINITION": None,
            },
            {
                "TABLE_NAME": "customer_summary",
                "TABLE_SCHEMA": "[analytics_space]",
                "RESOURCE_ID": "view_id_1",
                "LOCATION_ID": "space_id_1",
                "COLUMNS": [
                    {
                        "COLUMN_NAME": "customer_count",
                        "DATA_TYPE": "BIGINT",
                        "IS_NULLABLE": "NO",
                        "COLUMN_SIZE": None,
                    },
                ],
                "VIEW_DEFINITION": "SELECT COUNT(*) as customer_count FROM s3_source.customers",
            },
        ]

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_basic_ingestion_workflow(
        self,
        mock_catalog_class,
        mock_api_class,
        basic_config,
        pipeline_context,
        mock_containers,
    ):
        """Test the basic ingestion workflow produces workunits."""

        # Setup mocks
        mock_api = Mock()
        mock_api.get_all_containers.return_value = mock_containers
        mock_api.get_description_for_resource.return_value = "Test description"
        mock_api.get_tags_for_resource.return_value = []

        mock_catalog = Mock()

        # Create mock container objects
        mock_source = Mock(spec=DremioSourceContainer)
        mock_source.container_name = "s3_source"
        mock_source.path = []
        mock_source.subclass = "Dremio Source"
        mock_source.dremio_source_type = "S3"
        mock_source.root_path = "/data"
        mock_source.database_name = None

        mock_space = Mock(spec=DremioSpace)
        mock_space.container_name = "analytics_space"
        mock_space.path = []
        mock_space.subclass = "Dremio Space"

        mock_folder = Mock(spec=DremioFolder)
        mock_folder.container_name = "processed"
        mock_folder.path = ["analytics_space"]
        mock_folder.subclass = "Dremio Folder"

        mock_catalog.get_containers.return_value = iter(
            [mock_source, mock_space, mock_folder]
        )
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([mock_source])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = mock_api

        # Create source
        source = DremioSource(basic_config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Mock aspect methods
        mock_container_mcp = Mock(spec=MetadataWorkUnit)

        with (
            patch.object(
                source.dremio_aspects,
                "populate_container_mcp",
                return_value=iter([mock_container_mcp]),
            ),
            patch.object(
                source.dremio_aspects, "populate_dataset_mcp", return_value=iter([])
            ),
            patch.object(
                source.dremio_aspects,
                "populate_glossary_term_mcp",
                return_value=iter([]),
            ),
            patch.object(
                source.sql_parsing_aggregator, "gen_metadata", return_value=[]
            ),
        ):
            # Execute ingestion
            workunits = list(source.get_workunits_internal())

        # Verify workunits were generated
        assert len(workunits) >= 1  # At least container workunits

        # Verify all generators were called
        mock_catalog.get_containers.assert_called_once()
        mock_catalog.get_datasets.assert_called_once()
        mock_catalog.get_glossary_terms.assert_called_once()

        # Verify no failures
        assert source.report.num_containers_failed == 0
        assert source.report.num_datasets_failed == 0

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_dataset_processing(
        self,
        mock_catalog_class,
        mock_api_class,
        basic_config,
        pipeline_context,
        mock_datasets,
    ):
        """Test dataset processing generates appropriate workunits."""

        # Setup mocks
        mock_api = Mock()
        mock_catalog = Mock()

        # Create mock dataset objects
        table_dataset = Mock(spec=DremioDataset)
        table_dataset.resource_name = "customers"
        table_dataset.path = ["s3_source"]
        table_dataset.dataset_type = DremioDatasetType.TABLE
        table_dataset.sql_definition = None
        table_dataset.parents = None
        table_dataset.columns = [Mock(name="id"), Mock(name="name")]

        view_dataset = Mock(spec=DremioDataset)
        view_dataset.resource_name = "customer_summary"
        view_dataset.path = ["analytics_space"]
        view_dataset.dataset_type = DremioDatasetType.VIEW
        view_dataset.sql_definition = (
            "SELECT COUNT(*) as customer_count FROM s3_source.customers"
        )
        view_dataset.parents = ["s3_source.customers"]
        view_dataset.columns = [Mock(name="customer_count")]

        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.return_value = iter([table_dataset, view_dataset])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])
        mock_catalog.edition = DremioEdition.ENTERPRISE  # Enable view lineage

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = mock_api

        # Create source
        source = DremioSource(basic_config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Add missing attributes to mock datasets
        mock_dataset_mcp = Mock(spec=MetadataWorkUnit)
        mock_dataset_mcp.metadata = Mock()
        mock_lineage_mcp = Mock(spec=MetadataWorkUnit)

        # Add default_schema to view dataset
        view_dataset.default_schema = "test_schema"

        with (
            patch.object(
                source.dremio_aspects, "populate_container_mcp", return_value=iter([])
            ),
            patch.object(
                source.dremio_aspects,
                "populate_dataset_mcp",
                return_value=iter([mock_dataset_mcp]),
            ) as mock_populate_dataset,
            patch.object(
                source.dremio_aspects,
                "populate_glossary_term_mcp",
                return_value=iter([]),
            ),
            patch.object(
                source, "generate_view_lineage", return_value=iter([mock_lineage_mcp])
            ) as mock_generate_lineage,
            patch.object(
                source.sql_parsing_aggregator, "gen_metadata", return_value=[]
            ),
        ):
            # Execute ingestion
            workunits = list(source.get_workunits_internal())

        # Verify dataset processing occurred
        mock_populate_dataset.assert_called()

        # Verify lineage was generated for view
        mock_generate_lineage.assert_called()

        # Should have dataset and lineage workunits
        assert len(workunits) >= 2

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_profiling_integration(
        self,
        mock_catalog_class,
        mock_api_class,
        config_with_profiling,
        pipeline_context,
    ):
        """Test that profiling is properly integrated when enabled."""

        # Setup mocks
        mock_api = Mock()
        mock_catalog = Mock()

        # Create mock dataset for profiling
        mock_dataset = Mock(spec=DremioDataset)
        mock_dataset.resource_name = "customers"
        mock_dataset.path = ["s3_source"]
        mock_dataset.dataset_type = DremioDatasetType.TABLE
        mock_dataset.columns = [Mock(name="id"), Mock(name="name")]

        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.side_effect = [
            iter([mock_dataset]),  # For main processing
            iter([mock_dataset]),  # For profiling
        ]
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = mock_api

        # Create source
        source = DremioSource(config_with_profiling, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Mock profiling
        mock_profile_workunit = Mock(spec=MetadataWorkUnit)

        with (
            patch.object(
                source.dremio_aspects, "populate_container_mcp", return_value=iter([])
            ),
            patch.object(
                source.dremio_aspects, "populate_dataset_mcp", return_value=iter([])
            ),
            patch.object(
                source.dremio_aspects,
                "populate_glossary_term_mcp",
                return_value=iter([]),
            ),
            patch.object(
                source.profiler,
                "get_workunits",
                return_value=iter([mock_profile_workunit]),
            ) as mock_profiler_get_workunits,
            patch.object(
                source.sql_parsing_aggregator, "gen_metadata", return_value=[]
            ),
        ):
            # Execute ingestion
            workunits = list(source.get_workunits_internal())

        # Verify profiling was called
        mock_profiler_get_workunits.assert_called()

        # Should include profile workunit
        assert mock_profile_workunit in workunits

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_source_map_building(
        self, mock_catalog_class, mock_api_class, basic_config, pipeline_context
    ):
        """Test that source map is built correctly from Dremio sources."""

        # Setup mocks
        mock_api = Mock()
        mock_catalog = Mock()

        # Create mock source containers
        s3_source = Mock(spec=DremioSourceContainer)
        s3_source.container_name = "s3_data"
        s3_source.dremio_source_type = "S3"
        s3_source.root_path = "/bucket/data"
        s3_source.database_name = None

        postgres_source = Mock(spec=DremioSourceContainer)
        postgres_source.container_name = "pg_db"
        postgres_source.dremio_source_type = "POSTGRES"
        postgres_source.root_path = None
        postgres_source.database_name = "mydb"

        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([s3_source, postgres_source])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = mock_api

        # Create source
        source = DremioSource(basic_config, pipeline_context)
        source.dremio_catalog = mock_catalog

        with (
            patch.object(
                source.dremio_aspects, "populate_container_mcp", return_value=iter([])
            ),
            patch.object(
                source.dremio_aspects, "populate_dataset_mcp", return_value=iter([])
            ),
            patch.object(
                source.dremio_aspects,
                "populate_glossary_term_mcp",
                return_value=iter([]),
            ),
            patch.object(
                source.sql_parsing_aggregator, "gen_metadata", return_value=[]
            ),
        ):
            # Execute ingestion to trigger source map building
            list(source.get_workunits_internal())

        # Verify source map was built (this happens in _build_source_map)
        mock_catalog.get_sources.assert_called()

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_configuration_validation(
        self, mock_catalog_class, mock_api_class, pipeline_context
    ):
        """Test that various configuration options are handled correctly."""

        # Test with custom schema patterns
        config = DremioSourceConfig(
            hostname="test-dremio",
            port=9047,
            tls=True,
            authentication_method="password",
            username="test-user",
            password="test-password",
            schema_pattern={"allow": ["prod.*"], "deny": ["prod.temp.*"]},
            max_view_definition_length=500000,
            truncate_large_view_definitions=False,
        )

        mock_api = Mock()
        mock_catalog = Mock()
        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = mock_api

        # Should create source without errors
        source = DremioSource(config, pipeline_context)

        # Verify configuration is accessible
        assert source.config.max_view_definition_length == 500000
        assert source.config.truncate_large_view_definitions is False
        assert source.config.tls is True

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_report_generation(
        self, mock_catalog_class, mock_api_class, basic_config, pipeline_context
    ):
        """Test that the source report is properly populated."""

        mock_api = Mock()
        mock_catalog = Mock()
        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = mock_api

        # Create source
        source = DremioSource(basic_config, pipeline_context)
        source.dremio_catalog = mock_catalog

        with (
            patch.object(
                source.dremio_aspects, "populate_container_mcp", return_value=iter([])
            ),
            patch.object(
                source.dremio_aspects, "populate_dataset_mcp", return_value=iter([])
            ),
            patch.object(
                source.dremio_aspects,
                "populate_glossary_term_mcp",
                return_value=iter([]),
            ),
            patch.object(
                source.sql_parsing_aggregator, "gen_metadata", return_value=[]
            ),
        ):
            # Execute ingestion
            list(source.get_workunits_internal())

        # Verify report exists and has expected structure
        report = source.get_report()
        assert report is not None
        assert hasattr(report, "num_containers_failed")
        assert hasattr(report, "num_datasets_failed")
        assert hasattr(report, "containers_scanned")
        assert hasattr(report, "containers_filtered")

    def test_workunit_processors_include_auto_reporter(
        self, basic_config, pipeline_context
    ):
        """Test that auto workunit reporter is included in processors."""

        with (
            patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations"),
            patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog"),
        ):
            source = DremioSource(basic_config, pipeline_context)

            processors = source.get_workunit_processors()

            # Should have at least one processor
            assert len(processors) >= 1

            # Should include auto workunit reporter
            from datahub.ingestion.api.source_helpers import auto_workunit_reporter

            auto_reporter_found = False
            for processor in processors:
                if processor is not None and hasattr(processor, "func"):
                    if processor.func == auto_workunit_reporter:
                        auto_reporter_found = True
                        break

            assert auto_reporter_found, "auto_workunit_reporter not found in processors"
