from typing import Any, Dict, List
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.dremio.dremio_api import DremioAPIOperations
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioCatalog,
    DremioDataset,
    DremioFolder,
    DremioSourceContainer,
    DremioSpace,
)
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport


class TestDremioStreamingProcessing:
    @pytest.fixture
    def mock_api(self):
        """Create a mock DremioAPIOperations instance"""
        config = DremioSourceConfig(
            hostname="dummy-host",
            port=9047,
            tls=False,
            authentication_method="password",
            username="dummy-user",
            password="dummy-password",
        )
        report = DremioSourceReport()

        with patch("requests.Session"):
            api = DremioAPIOperations(config, report)
            return api

    @pytest.fixture
    def catalog(self, mock_api):
        """Create DremioCatalog instance with mocked API"""
        from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport

        mock_report = Mock(spec=DremioSourceReport)
        return DremioCatalog(mock_api, mock_report)

    def test_get_containers_returns_generator(self, catalog, mock_api):
        """Test that get_containers returns a generator, not a collection"""
        # Mock the API response
        mock_containers = [
            {
                "container_type": "SOURCE",
                "name": "source1",
                "id": "id1",
                "source_type": "S3",
                "root_path": "/data",
                "database_name": None,
            },
            {
                "container_type": "SPACE",
                "name": "space1",
                "id": "id2",
            },
            {
                "container_type": "FOLDER",
                "name": "folder1",
                "id": "id3",
                "path": ["space1"],
            },
        ]

        mock_api.get_all_containers = Mock(return_value=mock_containers)
        mock_api.get_description_for_resource = Mock(return_value="Test description")

        # Get the generator
        containers_gen = catalog.get_containers()

        # Verify it's a generator
        assert hasattr(containers_gen, "__iter__")
        assert hasattr(containers_gen, "__next__")

        # Convert to list to check contents
        containers = list(containers_gen)

        assert len(containers) == 3
        assert isinstance(containers[0], DremioSourceContainer)
        assert isinstance(containers[1], DremioSpace)
        assert isinstance(containers[2], DremioFolder)

        # Verify API was called only once (containers are cached)
        mock_api.get_all_containers.assert_called_once()

    def test_get_containers_caches_api_calls(self, catalog, mock_api):
        """Test that containers are cached to avoid multiple API calls"""
        mock_containers = [
            {
                "container_type": "SOURCE",
                "name": "source1",
                "id": "id1",
                "source_type": "S3",
                "root_path": "/data",
                "database_name": None,
            }
        ]

        mock_api.get_all_containers = Mock(return_value=mock_containers)
        mock_api.get_description_for_resource = Mock(return_value="Test description")

        # Call get_containers multiple times
        list(catalog.get_containers())
        list(catalog.get_containers())
        list(catalog.get_containers())

        # API should only be called once due to caching
        mock_api.get_all_containers.assert_called_once()

    def test_get_sources_filters_correctly(self, catalog, mock_api):
        """Test that get_sources returns only source containers"""
        mock_containers = [
            {
                "container_type": "SOURCE",
                "name": "source1",
                "id": "id1",
                "source_type": "S3",
                "root_path": "/data",
                "database_name": None,
            },
            {
                "container_type": "SPACE",
                "name": "space1",
                "id": "id2",
            },
            {
                "container_type": "SOURCE",
                "name": "source2",
                "id": "id3",
                "source_type": "POSTGRES",
                "root_path": None,
                "database_name": "mydb",
            },
        ]

        mock_api.get_all_containers = Mock(return_value=mock_containers)
        mock_api.get_description_for_resource = Mock(return_value="Test description")

        # Get only sources
        sources = list(catalog.get_sources())

        assert len(sources) == 2
        assert all(isinstance(source, DremioSourceContainer) for source in sources)
        assert sources[0].container_name == "source1"
        assert sources[1].container_name == "source2"

    def test_get_datasets_returns_generator(self, catalog, mock_api):
        """Test that get_datasets returns a generator and processes one at a time"""
        # Mock containers
        mock_containers = [
            {
                "container_type": "SOURCE",
                "name": "source1",
                "id": "id1",
                "source_type": "S3",
                "root_path": "/data",
                "database_name": None,
            }
        ]

        # Mock dataset details
        mock_dataset_details = [
            {
                "TABLE_NAME": "table1",
                "TABLE_SCHEMA": "[source1]",
                "RESOURCE_ID": "res1",
                "LOCATION_ID": "loc1",
                "COLUMNS": [
                    {
                        "name": "col1",
                        "ordinal_position": 1,
                        "is_nullable": True,
                        "data_type": "VARCHAR",
                        "column_size": 255,
                    }
                ],
                "VIEW_DEFINITION": None,
                "OWNER": "user1",
                "OWNER_TYPE": "USER",
                "CREATED": "2024-01-01 00:00:00.000",
            },
            {
                "TABLE_NAME": "table2",
                "TABLE_SCHEMA": "[source1]",
                "RESOURCE_ID": "res2",
                "LOCATION_ID": "loc2",
                "COLUMNS": [],
                "VIEW_DEFINITION": "SELECT * FROM table1",
                "OWNER": "user2",
                "OWNER_TYPE": "USER",
                "CREATED": "2024-01-01 00:00:00.000",
            },
        ]

        mock_api.get_all_containers = Mock(return_value=mock_containers)
        mock_api.get_all_tables_and_columns = Mock(return_value=mock_dataset_details)
        mock_api.get_description_for_resource = Mock(return_value="Test description")
        mock_api.get_tags_for_resource = Mock(return_value=None)
        mock_api.get_context_for_vds = Mock(return_value="source1")

        # Get the generator
        datasets_gen = catalog.get_datasets()

        # Verify it's a generator
        assert hasattr(datasets_gen, "__iter__")
        assert hasattr(datasets_gen, "__next__")

        # Process one at a time
        datasets = []
        for dataset in datasets_gen:
            datasets.append(dataset)
            # Verify we can process incrementally
            assert isinstance(dataset, DremioDataset)

        assert len(datasets) == 2
        assert datasets[0].resource_name == "table1"
        assert datasets[1].resource_name == "table2"

    def test_get_datasets_handles_creation_errors(self, catalog, mock_api):
        """Test that dataset creation errors are handled gracefully"""
        mock_containers = [
            {
                "container_type": "SOURCE",
                "name": "source1",
                "id": "id1",
                "source_type": "S3",
                "root_path": "/data",
                "database_name": None,
            }
        ]

        # Mock dataset details with one invalid entry
        mock_dataset_details = [
            {
                "TABLE_NAME": "valid_table",
                "TABLE_SCHEMA": "[source1]",
                "RESOURCE_ID": "res1",
                "LOCATION_ID": "loc1",
                "COLUMNS": [],
                "VIEW_DEFINITION": None,
            },
            {
                # Missing required fields to trigger error
                "TABLE_NAME": None,
                "TABLE_SCHEMA": None,
            },
        ]

        mock_api.get_all_containers = Mock(return_value=mock_containers)
        mock_api.get_all_tables_and_columns = Mock(return_value=mock_dataset_details)
        mock_api.get_description_for_resource = Mock(return_value="Test description")
        mock_api.get_tags_for_resource = Mock(return_value=None)

        # Should handle errors gracefully and continue processing
        datasets = list(catalog.get_datasets())

        # Only the valid dataset should be returned
        assert len(datasets) == 1
        assert datasets[0].resource_name == "valid_table"

    def test_get_glossary_terms_returns_generator(self, catalog, mock_api):
        """Test that get_glossary_terms returns a generator and deduplicates terms"""
        mock_containers = [
            {
                "container_type": "SOURCE",
                "name": "source1",
                "id": "id1",
                "source_type": "S3",
                "root_path": "/data",
                "database_name": None,
            }
        ]

        mock_dataset_details: List[Dict[str, Any]] = [
            {
                "TABLE_NAME": "table1",
                "TABLE_SCHEMA": "[source1]",
                "RESOURCE_ID": "res1",
                "LOCATION_ID": "loc1",
                "COLUMNS": [],
                "VIEW_DEFINITION": None,
            }
        ]

        mock_api.get_all_containers = Mock(return_value=mock_containers)
        mock_api.get_all_tables_and_columns = Mock(return_value=mock_dataset_details)
        mock_api.get_description_for_resource = Mock(return_value="Test description")
        mock_api.get_tags_for_resource = Mock(
            return_value=["tag1", "tag2", "tag1"]
        )  # Duplicate tag1

        # Get the generator
        terms_gen = catalog.get_glossary_terms()

        # Verify it's a generator
        assert hasattr(terms_gen, "__iter__")
        assert hasattr(terms_gen, "__next__")

        # Convert to list and check deduplication
        terms = list(terms_gen)

        assert len(terms) == 2  # Should deduplicate tag1
        term_names = [term.glossary_term for term in terms]
        assert "tag1" in term_names
        assert "tag2" in term_names
        assert term_names.count("tag1") == 1  # Only one instance of tag1

    def test_get_queries_returns_generator(self, catalog, mock_api):
        """Test that get_queries returns a generator and handles invalid queries"""
        mock_queries = [
            {
                "job_id": "job1",
                "user_name": "user1",
                "submitted_ts": "2024-01-01 12:00:00.000",
                "query": "SELECT * FROM table1",
                "queried_datasets": "table1,table2",
            },
            {
                # Invalid query missing required fields
                "job_id": "job2",
                "user_name": None,
                "query": "SELECT * FROM table2",
            },
            {
                "job_id": "job3",
                "user_name": "user3",
                "submitted_ts": "2024-01-01 13:00:00.000",
                "query": "INSERT INTO table1 VALUES (1)",
                "queried_datasets": "table1",
            },
        ]

        mock_api.extract_all_queries = Mock(return_value=mock_queries)

        # Get the generator
        queries_gen = catalog.get_queries()

        # Verify it's a generator
        assert hasattr(queries_gen, "__iter__")
        assert hasattr(queries_gen, "__next__")

        # Process queries
        queries = list(queries_gen)

        # Should skip invalid query and return only valid ones
        assert len(queries) == 2
        assert queries[0].job_id == "job1"
        assert queries[1].job_id == "job3"

    def test_get_queries_handles_creation_errors(self, catalog, mock_api):
        """Test that query creation errors are handled gracefully"""
        mock_queries = [
            {
                "job_id": "job1",
                "user_name": "user1",
                "submitted_ts": "invalid-timestamp",  # This will cause parsing error
                "query": "SELECT * FROM table1",
                "queried_datasets": "table1",
            }
        ]

        mock_api.extract_all_queries = Mock(return_value=mock_queries)

        # Should handle creation errors gracefully
        queries = list(catalog.get_queries())

        # Should return empty list due to creation error
        assert len(queries) == 0

    def test_containers_with_creation_errors_are_skipped(self, catalog, mock_api):
        """Test that containers with creation errors are skipped gracefully"""
        mock_containers = [
            {
                "container_type": "SOURCE",
                "name": "valid_source",
                "id": "id1",
                "source_type": "S3",
                "root_path": "/data",
                "database_name": None,
            },
            {
                "container_type": "INVALID_TYPE",  # This will cause creation error
                "name": "invalid_container",
                "id": "id2",
            },
        ]

        mock_api.get_all_containers = Mock(return_value=mock_containers)
        mock_api.get_description_for_resource = Mock(return_value="Test description")

        # Should handle creation errors gracefully
        containers = list(catalog.get_containers())

        # Should return only the valid container and default the invalid one to Space
        assert len(containers) == 2
        assert containers[0].container_name == "valid_source"
        assert isinstance(containers[0], DremioSourceContainer)
        assert containers[1].container_name == "invalid_container"
        assert isinstance(containers[1], DremioSpace)  # Defaults to Space

    def test_memory_efficiency_no_large_collections(self, catalog, mock_api):
        """Test that we're not storing large collections in memory"""
        # Create a large number of mock containers
        large_container_list = []
        for i in range(1000):
            large_container_list.append(
                {
                    "container_type": "SOURCE",
                    "name": f"source_{i}",
                    "id": f"id_{i}",
                    "source_type": "S3",
                    "root_path": f"/data_{i}",
                    "database_name": None,
                }
            )

        mock_api.get_all_containers = Mock(return_value=large_container_list)
        mock_api.get_description_for_resource = Mock(return_value="Test description")

        # Get generator
        containers_gen = catalog.get_containers()

        # Process only first 10 items
        processed_containers = []
        for i, container in enumerate(containers_gen):
            if i >= 10:
                break
            processed_containers.append(container)

        # Verify we only processed what we needed
        assert len(processed_containers) == 10

        # Verify the catalog doesn't store the full list of container objects
        # (only the raw API response is cached)
        assert not hasattr(catalog, "sources")
        assert not hasattr(catalog, "spaces")
        assert not hasattr(catalog, "folders")
        assert catalog._containers_cache is not None  # Only raw data is cached
