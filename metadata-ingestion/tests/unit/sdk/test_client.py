from unittest.mock import Mock, patch

from datahub.ingestion.graph.client import (
    DatahubClientConfig,
    DataHubGraph,
    _graphql_entity_type,
)
from datahub.metadata.schema_classes import CorpUserEditableInfoClass


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_get_aspect(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))
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


def test_graphql_entity_types():
    # FIXME: This is a subset of all the types, but it's enough to get us ok coverage.

    assert _graphql_entity_type("domain") == "DOMAIN"
    assert _graphql_entity_type("dataset") == "DATASET"
    assert _graphql_entity_type("dashboard") == "DASHBOARD"
    assert _graphql_entity_type("chart") == "CHART"

    assert _graphql_entity_type("corpuser") == "CORP_USER"
    assert _graphql_entity_type("corpGroup") == "CORP_GROUP"

    assert _graphql_entity_type("dataFlow") == "DATA_FLOW"
    assert _graphql_entity_type("dataJob") == "DATA_JOB"
    assert _graphql_entity_type("glossaryNode") == "GLOSSARY_NODE"
    assert _graphql_entity_type("glossaryTerm") == "GLOSSARY_TERM"

    assert _graphql_entity_type("dataHubExecutionRequest") == "EXECUTION_REQUEST"
