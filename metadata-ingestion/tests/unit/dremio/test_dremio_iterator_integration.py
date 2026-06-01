from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioCatalog,
    DremioDataset,
    DremioGlossaryTerm,
)
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

    def test_iterator_handles_exceptions_gracefully(self, mock_dremio_source):
        """Test that iterator handles exceptions without crashing"""
        mock_dataset = Mock(spec=DremioDataset)
        mock_dataset.path = ["test", "path"]
        mock_dataset.resource_name = "test_table"
        mock_dataset.glossary_terms = []

        mock_dremio_source.dremio_catalog.get_datasets.return_value = iter(
            [mock_dataset]
        )
        mock_dremio_source.dremio_catalog.get_containers.return_value = []
        mock_dremio_source.dremio_catalog.get_sources.return_value = []
        mock_dremio_source.config.source_mappings = []
        mock_dremio_source.process_dataset = Mock(
            side_effect=Exception("Processing error")
        )
        list(mock_dremio_source.get_workunits_internal())

        assert mock_dremio_source.report.num_datasets_failed > 0

    def test_glossary_terms_harvested_from_dataset_pass(self, mock_dremio_source):
        # Regression: ``get_glossary_terms()`` used to be a separate iterator
        # that called ``self.get_datasets()`` again — i.e. a third full
        # catalog scan after the main dataset pass and (when enabled) the
        # profiling pass. The dataset loop now harvests unique terms inline
        # from ``dataset.glossary_terms`` (already populated in
        # ``DremioDataset.__init__``), so ``get_glossary_terms`` must not be
        # invoked from the workunit pipeline.
        term_a = DremioGlossaryTerm(glossary_term="finance")
        term_a_dup = DremioGlossaryTerm(glossary_term="finance")
        term_b = DremioGlossaryTerm(glossary_term="pii")

        ds1 = Mock(spec=DremioDataset)
        ds1.path = ["space", "schema"]
        ds1.resource_name = "t1"
        ds1.glossary_terms = [term_a, term_b]
        ds2 = Mock(spec=DremioDataset)
        ds2.path = ["space", "schema"]
        ds2.resource_name = "t2"
        ds2.glossary_terms = [term_a_dup]  # same string, different object

        mock_dremio_source.dremio_catalog.get_datasets.return_value = iter([ds1, ds2])
        mock_dremio_source.dremio_catalog.get_containers.return_value = []
        mock_dremio_source.dremio_catalog.get_sources.return_value = []
        mock_dremio_source.config.source_mappings = []
        mock_dremio_source.process_dataset = Mock(return_value=iter([]))

        processed = []
        mock_dremio_source.process_glossary_term = Mock(
            side_effect=lambda gt: processed.append(gt.glossary_term) or iter([])
        )

        list(mock_dremio_source.get_workunits_internal())

        # Catalog-level iterator must not be touched.
        mock_dremio_source.dremio_catalog.get_glossary_terms.assert_not_called()
        # Per-dataset terms are deduped by string, not by object identity.
        assert sorted(processed) == ["finance", "pii"]
