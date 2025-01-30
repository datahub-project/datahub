from unittest.mock import MagicMock, patch

from datahub_azure_fabric.extractors.dataflow_extractor import DataFlowExtractor
from datahub_azure_fabric.extractors.dataset_extractor import DatasetExtractor
from datahub_azure_fabric.extractors.job_extractor import JobExtractor
from datahub_azure_fabric.extractors.lineage_extractor import LineageExtractor
from datahub_azure_fabric.extractors.usage_extractor import UsageExtractor


def test_dataset_extractor(mock_config, mock_auth_client, mock_response):
    with patch("requests.Session") as mock_session:
        mock_session.return_value.get.return_value = mock_response
        extractor = DatasetExtractor(mock_auth_client, mock_config, MagicMock())
        workunits = list(extractor.get_workunits())
        assert len(workunits) == 0


def test_lineage_extractor(mock_config, mock_auth_client, mock_response):
    with patch("requests.Session") as mock_session:
        mock_session.return_value.get.return_value = mock_response
        extractor = LineageExtractor(mock_auth_client, mock_config, MagicMock())
        workunits = list(extractor.get_workunits())
        assert len(workunits) == 0


def test_usage_extractor(mock_config, mock_auth_client, mock_response):
    with patch("requests.Session") as mock_session:
        mock_session.return_value.get.return_value = mock_response
        extractor = UsageExtractor(mock_auth_client, mock_config, MagicMock())
        workunits = list(extractor.get_workunits())
        assert len(workunits) == 0


def test_job_extractor(mock_config, mock_auth_client, mock_response):
    with patch("requests.Session") as mock_session:
        mock_session.return_value.get.return_value = mock_response
        extractor = JobExtractor(mock_auth_client, mock_config, MagicMock())
        workunits = list(extractor.get_workunits())
        assert len(workunits) == 0


def test_dataflow_extractor(mock_config, mock_auth_client, mock_response):
    with patch("requests.Session") as mock_session:
        mock_session.return_value.get.return_value = mock_response
        extractor = DataFlowExtractor(mock_auth_client, mock_config, MagicMock())
        workunits = list(extractor.get_workunits())
        assert len(workunits) == 0
