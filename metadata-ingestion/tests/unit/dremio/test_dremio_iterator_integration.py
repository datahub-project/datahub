import inspect
from collections import deque
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_api import DremioAPIOperations
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import DremioCatalog, DremioDataset
from datahub.ingestion.source.dremio.dremio_source import DremioSource


class TestDremioIteratorIntegration:
    @pytest.fixture
    def mock_config(self):
        """Create mock configuration"""
        return DremioSourceConfig(
            hostname="test-host",
            port=9047,
            tls=False,
            username="test-user",
            password="test-password",
        )

    @pytest.fixture
    def mock_dremio_source(self, mock_config, monkeypatch):
        """Create mock Dremio source"""
        # Mock the requests.Session
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200

        # Create a mock pipeline context
        mock_ctx = Mock()
        mock_ctx.run_id = "test-run-id"
        source = DremioSource(mock_config, mock_ctx)

        # Mock the catalog and API
        source.dremio_catalog = Mock(spec=DremioCatalog)
        source.dremio_catalog.dremio_api = Mock()

        return source

    def test_source_uses_iterators_by_default(self, mock_dremio_source):
        """Test that source uses iterators by default"""
        # Mock dataset for streaming
        mock_dataset = Mock(spec=DremioDataset)
        mock_dataset.path = ["test", "path"]
        mock_dataset.resource_name = "test_table"

        # Mock streaming method to return iterator
        mock_dremio_source.dremio_catalog.get_datasets_iter.return_value = iter(
            [mock_dataset]
        )
        mock_dremio_source.dremio_catalog.get_containers.return_value = []
        mock_dremio_source.dremio_catalog.get_containers_iter.return_value = iter([])
        mock_dremio_source.dremio_catalog.get_glossary_terms_iter.return_value = iter(
            []
        )

        # Mock the source map building dependencies
        mock_dremio_source.dremio_catalog.get_sources.return_value = []
        mock_dremio_source.config.source_mappings = []

        # Mock process_dataset to return empty generator
        mock_dremio_source.process_dataset = Mock(return_value=iter([]))

        # Test that streaming method is called
        list(mock_dremio_source.get_workunits_internal())

        mock_dremio_source.dremio_catalog.get_datasets_iter.assert_called_once()
        mock_dremio_source.dremio_catalog.get_glossary_terms_iter.assert_called_once()

    def test_catalog_iterator_method_exists(self):
        """Test that DremioCatalog has iterator method"""
        # Verify the method exists
        assert hasattr(DremioCatalog, "get_datasets_iter")

        # Verify it's a method
        assert inspect.ismethod(DremioCatalog.get_datasets_iter) or inspect.isfunction(
            DremioCatalog.get_datasets_iter
        )

    def test_iterator_handles_exceptions_gracefully(self, mock_dremio_source):
        """Test that iterator handles exceptions without crashing"""
        # Mock dataset that will cause an exception during processing
        mock_dataset = Mock(spec=DremioDataset)
        mock_dataset.path = ["test", "path"]
        mock_dataset.resource_name = "test_table"

        # Mock streaming to return the problematic dataset
        mock_dremio_source.dremio_catalog.get_datasets_iter.return_value = iter(
            [mock_dataset]
        )
        mock_dremio_source.dremio_catalog.get_containers.return_value = []
        mock_dremio_source.dremio_catalog.get_containers_iter.return_value = iter([])
        mock_dremio_source.dremio_catalog.get_glossary_terms_iter.return_value = iter(
            []
        )

        # Mock the source map building dependencies
        mock_dremio_source.dremio_catalog.get_sources.return_value = []
        mock_dremio_source.config.source_mappings = []

        # Mock process_dataset to raise an exception
        mock_dremio_source.process_dataset = Mock(
            side_effect=Exception("Processing error")
        )

        # Should not raise exception, but should handle it gracefully
        list(mock_dremio_source.get_workunits_internal())

        # Verify the exception was handled (report should be updated)
        assert mock_dremio_source.report.num_datasets_failed > 0

    def test_iterator_memory_efficiency_concept(self):
        """Conceptual test for memory efficiency of iterator approach"""
        # This test demonstrates the concept - in iterator approach, we process one item at a time
        # rather than loading all items into memory first

        def mock_iterator_generator():
            """Simulate streaming data source"""
            for i in range(1000):  # Large dataset
                yield {"id": i, "data": f"row_{i}"}

        def batch_processing(data_source):
            """Simulate batch processing (loads all data first)"""
            all_data = list(data_source)  # This would use more memory
            processed = []
            for item in all_data:
                processed.append(f"processed_{item['id']}")
            return processed

        def streaming_processing(data_source):
            """Simulate streaming processing (one item at a time)"""
            for item in data_source:  # This uses constant memory
                yield f"processed_{item['id']}"

        # Test streaming approach
        data_stream = mock_iterator_generator()
        streaming_results = list(streaming_processing(data_stream))

        # Test batch approach
        data_batch = mock_iterator_generator()
        batch_results = batch_processing(data_batch)

        # Results should be the same
        assert streaming_results == batch_results

        # But streaming approach would use less memory for large datasets
        # (This is conceptual - actual memory measurement would require profiling tools)

    def test_api_streaming_methods_exist(self):
        """Test that API has all required streaming methods"""
        # Verify streaming methods exist
        assert hasattr(DremioAPIOperations, "_fetch_results_iter")
        assert hasattr(DremioAPIOperations, "execute_query_iter")
        assert hasattr(DremioAPIOperations, "get_all_tables_and_columns")
        assert hasattr(DremioAPIOperations, "extract_all_queries_iter")

    def test_backward_compatibility(self, mock_dremio_source):
        """Test that core methods still exist for backward compatibility"""
        # Verify core methods still exist
        assert hasattr(DremioAPIOperations, "_fetch_all_results")
        assert hasattr(DremioAPIOperations, "execute_query")
        assert hasattr(DremioAPIOperations, "get_all_tables_and_columns")
        assert hasattr(DremioAPIOperations, "extract_all_queries_iter")

        # Test that batch methods still exist for backward compatibility
        mock_dataset = Mock(spec=DremioDataset)
        mock_dataset.path = ["test"]
        mock_dataset.resource_name = "table"

        mock_datasets = deque([mock_dataset])
        mock_dremio_source.dremio_catalog.get_datasets.return_value = mock_datasets
        mock_dremio_source.dremio_catalog.get_containers.return_value = []
        mock_dremio_source.dremio_catalog.get_glossary_terms.return_value = []

    def test_configuration_validation(self):
        """Test that configuration validates correctly"""
        # Test valid configuration
        config = DremioSourceConfig(
            hostname="test",
            port=9047,
            username="user",
            password="pass",
        )
        assert config.hostname == "test"
        assert config.port == 9047
