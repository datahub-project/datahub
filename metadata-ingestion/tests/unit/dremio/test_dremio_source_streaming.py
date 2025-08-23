from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_source import DremioSource


class TestDremioSourceStreaming:
    @pytest.fixture
    def pipeline_context(self):
        """Create a mock pipeline context"""
        return PipelineContext(run_id="test_run")

    @pytest.fixture
    def config(self):
        """Create a test configuration"""
        return DremioSourceConfig(
            hostname="dummy-host",
            port=9047,
            tls=False,
            authentication_method="password",
            username="dummy-user",
            password="dummy-password",
        )

    @pytest.fixture
    def mock_catalog(self):
        """Create a mock DremioCatalog"""
        catalog = Mock()

        # Mock containers generator
        mock_containers = [
            Mock(
                container_name="source1",
                path=[],
                subclass="Dremio Source",
            ),
            Mock(
                container_name="space1",
                path=[],
                subclass="Dremio Space",
            ),
        ]
        catalog.get_containers.return_value = iter(mock_containers)

        # Mock datasets generator
        mock_datasets = [
            Mock(
                resource_name="table1",
                path=["source1"],
                dataset_type=Mock(value="Table"),
                sql_definition=None,
                parents=None,
            ),
            Mock(
                resource_name="view1",
                path=["space1"],
                dataset_type=Mock(value="View"),
                sql_definition="SELECT * FROM table1",
                parents=["source1.table1"],
                default_schema="space1",
            ),
        ]
        catalog.get_datasets.return_value = iter(mock_datasets)

        # Mock glossary terms generator
        mock_terms = [
            Mock(glossary_term="term1", urn="urn:li:glossaryTerm:term1"),
        ]
        catalog.get_glossary_terms.return_value = iter(mock_terms)

        # Mock sources for source mapping
        mock_sources = [
            Mock(
                container_name="source1",
                dremio_source_type="S3",
                root_path="/data",
                database_name=None,
            )
        ]
        catalog.get_sources.return_value = iter(mock_sources)

        return catalog

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_source_uses_streaming_processing(
        self, mock_catalog_class, mock_api_class, config, pipeline_context, mock_catalog
    ):
        """Test that the source processes entities one at a time using generators"""

        # Setup mocks
        mock_catalog_class.return_value = mock_catalog
        mock_api_instance = Mock()
        mock_api_class.return_value = mock_api_instance

        # Create source
        source = DremioSource(config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Mock the aspect methods to return empty generators
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
            # Get all workunits
            list(source.get_workunits_internal())

        # Verify that generators were called (not stored collections)
        mock_catalog.get_containers.assert_called_once()
        mock_catalog.get_datasets.assert_called_once()
        mock_catalog.get_glossary_terms.assert_called_once()

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_source_handles_container_processing_errors(
        self, mock_catalog_class, mock_api_class, config, pipeline_context
    ):
        """Test that container processing errors don't stop the entire ingestion"""

        # Create a mock catalog with containers that will cause errors
        mock_catalog = Mock()

        # First container will succeed, second will fail
        mock_containers = [
            Mock(container_name="good_container", path=[], subclass="Dremio Source"),
            Mock(container_name="bad_container", path=[], subclass="Dremio Source"),
        ]
        mock_catalog.get_containers.return_value = iter(mock_containers)
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = Mock()

        # Create source
        source = DremioSource(config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Mock process_container to fail for second container
        def mock_process_container(container):
            if container.container_name == "bad_container":
                raise Exception("Container processing failed")
            return []

        # Mock other methods
        with (
            patch.object(
                source, "process_container", side_effect=mock_process_container
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
            # Should not raise exception despite container error
            list(source.get_workunits_internal())

        # Verify error was tracked
        assert source.report.num_containers_failed == 1

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_source_handles_dataset_processing_errors(
        self, mock_catalog_class, mock_api_class, config, pipeline_context
    ):
        """Test that dataset processing errors don't stop the entire ingestion"""

        mock_catalog = Mock()
        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        # Mock datasets with one that will cause error
        mock_datasets = [
            Mock(
                resource_name="good_table",
                path=["source1"],
                dataset_type=Mock(value="Table"),
            ),
            Mock(
                resource_name="bad_table",
                path=["source1"],
                dataset_type=Mock(value="Table"),
            ),
        ]
        mock_catalog.get_datasets.return_value = iter(mock_datasets)

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = Mock()

        # Create source
        source = DremioSource(config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Mock process_dataset to fail for bad_table
        def mock_process_dataset(dataset):
            if dataset.resource_name == "bad_table":
                raise Exception("Dataset processing failed")
            return []

        # Mock other methods
        with (
            patch.object(source, "process_dataset", side_effect=mock_process_dataset),
            patch.object(
                source.dremio_aspects,
                "populate_glossary_term_mcp",
                return_value=iter([]),
            ),
            patch.object(
                source.sql_parsing_aggregator, "gen_metadata", return_value=[]
            ),
        ):
            # Should not raise exception despite dataset error
            list(source.get_workunits_internal())

        # Verify error was tracked
        assert source.report.num_datasets_failed == 1

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_profiling_uses_streaming_approach(
        self, mock_catalog_class, mock_api_class, config, pipeline_context
    ):
        """Test that profiling processes datasets one at a time when enabled"""

        # Enable profiling
        config.profiling.enabled = True

        mock_catalog = Mock()
        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        # Mock datasets for profiling
        mock_datasets = [
            Mock(resource_name="table1", path=["source1"]),
            Mock(resource_name="table2", path=["source1"]),
        ]

        # Return datasets twice - once for main processing, once for profiling
        mock_catalog.get_datasets.side_effect = [
            iter(mock_datasets),  # For main processing
            iter(mock_datasets),  # For profiling
        ]

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = Mock()

        # Create source
        source = DremioSource(config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Mock profiling method
        profile_call_count = 0

        def mock_generate_profiles(dataset):
            nonlocal profile_call_count
            profile_call_count += 1
            return []

        # Mock other methods
        with (
            patch.object(
                source, "generate_profiles", side_effect=mock_generate_profiles
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
            # Process all workunits
            list(source.get_workunits_internal())

        # Verify profiling was called for each dataset
        assert profile_call_count == 2

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_source_map_building_with_generators(
        self, mock_catalog_class, mock_api_class, config, pipeline_context
    ):
        """Test that source map building works with generator-based sources"""

        mock_catalog = Mock()
        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])

        # Mock sources generator
        mock_sources = [
            Mock(
                container_name="s3_source",
                dremio_source_type="S3",
                root_path="/data",
                database_name=None,
            ),
            Mock(
                container_name="pg_source",
                dremio_source_type="POSTGRES",
                root_path=None,
                database_name="mydb",
            ),
        ]
        mock_catalog.get_sources.return_value = iter(mock_sources)

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = Mock()

        # Create source
        source = DremioSource(config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Build source map
        source_map = source._build_source_map()

        # Verify source map was built correctly
        assert len(source_map) == 2
        assert "s3_source" in source_map
        assert "pg_source" in source_map
        assert source_map["s3_source"].platform == "s3"
        assert source_map["pg_source"].platform == "postgres"

    def test_memory_efficiency_no_large_collections_stored(
        self, config, pipeline_context
    ):
        """Test that the source doesn't store large collections in memory"""

        with patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations"):
            # Create source
            source = DremioSource(config, pipeline_context)

            # Verify that source doesn't have large collection attributes
            assert not hasattr(source, "all_containers")
            assert not hasattr(source, "all_datasets")
            assert not hasattr(source, "all_queries")

            # Verify catalog is a real DremioCatalog instance (not mocked)
            from datahub.ingestion.source.dremio.dremio_entities import DremioCatalog

            catalog = source.dremio_catalog
            assert isinstance(catalog, DremioCatalog)

            # Verify catalog uses generators
            assert hasattr(catalog, "get_containers")
            assert hasattr(catalog, "get_datasets")
            assert hasattr(catalog, "get_sources")
            assert hasattr(catalog, "get_glossary_terms")
            assert hasattr(catalog, "get_queries")

            # Verify catalog doesn't store collections (only caches containers)
            assert not hasattr(catalog, "datasets")
            assert not hasattr(catalog, "sources")
            assert not hasattr(catalog, "spaces")
            assert not hasattr(catalog, "folders")
            assert not hasattr(catalog, "queries")

            # Verify it only has the containers cache
            assert hasattr(catalog, "_containers_cache")
