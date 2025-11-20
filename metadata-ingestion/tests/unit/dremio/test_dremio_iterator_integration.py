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
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200

        mock_ctx = Mock()
        mock_ctx.run_id = "test-run-id"
        source = DremioSource(mock_config, mock_ctx)

        source.dremio_catalog = Mock(spec=DremioCatalog)
        source.dremio_catalog.dremio_api = Mock()

        return source

    def test_source_uses_iterators_by_default(self, mock_dremio_source):
        """Test that source uses iterators by default"""
        mock_dataset = Mock(spec=DremioDataset)
        mock_dataset.path = ["test", "path"]
        mock_dataset.resource_name = "test_table"

        mock_dremio_source.dremio_catalog.get_datasets.return_value = iter(
            [mock_dataset]
        )
        mock_dremio_source.dremio_catalog.get_containers.return_value = []
        mock_dremio_source.dremio_catalog.get_glossary_terms.return_value = iter([])
        mock_dremio_source.dremio_catalog.get_sources.return_value = []
        mock_dremio_source.config.source_mappings = []
        mock_dremio_source.process_dataset = Mock(return_value=iter([]))
        list(mock_dremio_source.get_workunits_internal())

        mock_dremio_source.dremio_catalog.get_datasets.assert_called_once()
        mock_dremio_source.dremio_catalog.get_glossary_terms.assert_called_once()

    def test_iterator_handles_exceptions_gracefully(self, mock_dremio_source):
        """Test that iterator handles exceptions without crashing"""
        mock_dataset = Mock(spec=DremioDataset)
        mock_dataset.path = ["test", "path"]
        mock_dataset.resource_name = "test_table"

        mock_dremio_source.dremio_catalog.get_datasets.return_value = iter(
            [mock_dataset]
        )
        mock_dremio_source.dremio_catalog.get_containers.return_value = []
        mock_dremio_source.dremio_catalog.get_glossary_terms.return_value = iter([])
        mock_dremio_source.dremio_catalog.get_sources.return_value = []
        mock_dremio_source.config.source_mappings = []
        mock_dremio_source.process_dataset = Mock(
            side_effect=Exception("Processing error")
        )
        list(mock_dremio_source.get_workunits_internal())

        assert mock_dremio_source.report.num_datasets_failed > 0
