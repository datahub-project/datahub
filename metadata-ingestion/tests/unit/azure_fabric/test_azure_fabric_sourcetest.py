import unittest
from unittest.mock import MagicMock, patch

from datahub_azure_fabric.source import AzureFabricSource, AzureFabricSourceConfig


class TestAzureFabricSource(unittest.TestCase):
    def setUp(self):
        self.config = AzureFabricSourceConfig(
            tenant_id="test-tenant",
            workspace_url="https://test-workspace.com",
            workspace_name="test-workspace",
        )
        self.ctx = MagicMock()

    @patch("datahub_azure_fabric.source.AzureFabricAuthenticator")
    def test_source_initialization(self, mock_auth):
        source = AzureFabricSource(self.config, self.ctx)
        self.assertIsNotNone(source)
        self.assertEqual(source.config, self.config)
        mock_auth.assert_called_once()

    @patch("datahub_azure_fabric.source.DatasetExtractor")
    def test_dataset_extraction(self, mock_extractor):
        source = AzureFabricSource(self.config, self.ctx)
        list(source.get_workunits())
        mock_extractor.return_value.get_workunits.assert_called_once()

    @patch("datahub_azure_fabric.source.LineageExtractor")
    def test_lineage_extraction(self, mock_extractor):
        source = AzureFabricSource(self.config, self.ctx)
        list(source.get_workunits())
        mock_extractor.return_value.get_workunits.assert_called_once()
