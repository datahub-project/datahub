from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import DremioCatalog, DremioDataset
from datahub.ingestion.source.dremio.dremio_source import DremioSource


class TestDremioIteratorIntegration:
    @pytest.fixture
    def mock_config(self):
        return DremioSourceConfig(
            hostname="test-host",
            port=9047,
            tls=False,
            username="test-user",
            password="test-password",
        )

    @pytest.fixture
    def mock_dremio_source(self, mock_config, monkeypatch):
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
        mock_dataset = Mock(spec=DremioDataset)
        mock_dataset.path = ["test", "path"]
        mock_dataset.resource_name = "test_table"

        # Mock streaming method to return iterator
        mock_dremio_source.dremio_catalog.get_datasets_iter.return_value = iter(
            [mock_dataset]
        )
        mock_dremio_source.dremio_catalog.get_containers.return_value = []
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

    def test_iterator_handles_exceptions_gracefully(self, mock_dremio_source):
        """Test that iterator handles exceptions without crashing"""
        mock_dataset = Mock(spec=DremioDataset)
        mock_dataset.path = ["test", "path"]
        mock_dataset.resource_name = "test_table"

        # Mock streaming to return the problematic dataset
        mock_dremio_source.dremio_catalog.get_datasets_iter.return_value = iter(
            [mock_dataset]
        )
        mock_dremio_source.dremio_catalog.get_containers.return_value = []
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
