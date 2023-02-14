from unittest.mock import Mock, patch

from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import CorpUserEditableInfoClass


@patch("datahub.ingestion.graph.client.telemetry_enabled", False)
@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_get_aspect(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DataHubGraphConfig())
    user_urn = "urn:li:corpuser:foo"
    with patch("requests.Session.get") as mock_get:
        mock_response = Mock()
        mock_response.json = Mock(
            return_value={
                "version": 0,
                "aspect": {"com.linkedin.identity.CorpUserEditableInfo": {}},
            }
        )
        mock_get.return_value = mock_response
        editable = graph.get_aspect(user_urn, CorpUserEditableInfoClass)
        assert editable is not None
