from typing import Any
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.graph.client import (
    DatahubClientConfig,
    DataHubGraph,
    entity_type_to_graphql,
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


def test_graphql_entity_types() -> None:
    # FIXME: This is a subset of all the types, but it's enough to get us ok coverage.

    known_mappings = {
        "domain": "DOMAIN",
        "dataset": "DATASET",
        "dashboard": "DASHBOARD",
        "chart": "CHART",
        "corpuser": "CORP_USER",
        "corpGroup": "CORP_GROUP",
        "dataFlow": "DATA_FLOW",
        "dataJob": "DATA_JOB",
        "glossaryNode": "GLOSSARY_NODE",
        "glossaryTerm": "GLOSSARY_TERM",
        "dataProduct": "DATA_PRODUCT",
        "dataHubExecutionRequest": "EXECUTION_REQUEST",
        "document": "DOCUMENT",
    }

    for entity_type, graphql_type in known_mappings.items():
        assert entity_type_to_graphql(entity_type) == graphql_type


def _scroll_response(
    urn: str = "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)",
) -> dict[str, Any]:
    return {
        "scrollAcrossEntities": {
            "nextScrollId": None,
            "searchResults": [{"entity": {"urn": urn}}],
        }
    }


def test_get_urns_by_filter_omits_optional_lifecycle_flags_by_default() -> None:
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(
        graph, "execute_graphql", return_value=_scroll_response()
    ) as mock_execute_graphql:
        urns = list(graph.get_urns_by_filter(entity_types=["dataset"]))

    assert urns == ["urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)"]
    assert mock_execute_graphql.call_count == 1
    query = mock_execute_graphql.call_args.args[0]
    variables = mock_execute_graphql.call_args.kwargs["variables"]
    assert "includeDraft" not in query
    assert "includeHiddenLifecycleStages" not in query
    assert "includeDraft" not in variables
    assert "includeHiddenLifecycleStages" not in variables


def test_get_urns_by_filter_includes_draft_when_server_supports_flag() -> None:
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(
        graph,
        "execute_graphql",
        side_effect=[
            {"__type": {"inputFields": [{"name": "includeDraft"}]}},
            _scroll_response(),
        ],
    ) as mock_execute_graphql:
        urns = list(
            graph.get_urns_by_filter(entity_types=["dataset"], include_draft=True)
        )

    assert urns == ["urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)"]
    assert mock_execute_graphql.call_count == 2
    query = mock_execute_graphql.call_args.args[0]
    variables = mock_execute_graphql.call_args.kwargs["variables"]
    assert "includeDraft: $includeDraft" in query
    assert variables["includeDraft"] is True


def test_get_urns_by_filter_rejects_draft_when_server_does_not_support_flag() -> None:
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(
        graph,
        "execute_graphql",
        return_value={"__type": {"inputFields": [{"name": "includeSoftDeleted"}]}},
    ) as mock_execute_graphql:
        with pytest.raises(ValueError, match="SearchFlags.includeDraft"):
            list(
                graph.get_urns_by_filter(
                    entity_types=["dataset"],
                    include_draft=True,
                )
            )

    assert mock_execute_graphql.call_count == 1


_LOWERCASED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.events,PROD)"
_STORED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.EVENTS,PROD)"


def _queried_fields(mock_get_urns: Mock) -> dict:
    """The values each filter clause asked for, by the field it asked under."""
    rules = [
        group["and"][0] for group in mock_get_urns.call_args.kwargs["extra_or_filters"]
    ]
    return {rule["field"]: rule["values"] for rule in rules}


def test_get_dataset_urns_ignoring_case_asks_under_both_fields() -> None:
    # GMS skips the alias for a dataset already equal to its lowercased form, so that
    # one is findable only by `urn`. Both clauses are OR'd into a single query.
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(graph, "get_urns_by_filter", return_value=iter([_STORED])) as m:
        assert graph.get_dataset_urns_ignoring_case(_LOWERCASED) == [_STORED]

    assert _queried_fields(m) == {
        "lowercasedUrn": [_LOWERCASED],
        "urn": [_LOWERCASED],
    }
    assert m.call_args.kwargs["entity_types"] == ["dataset"]


def test_get_dataset_urns_ignoring_case_dedupes() -> None:
    # A urn matching under both fields would otherwise read as two stored casings.
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(
        graph, "get_urns_by_filter", return_value=iter([_STORED, _STORED])
    ):
        assert graph.get_dataset_urns_ignoring_case(_LOWERCASED) == [_STORED]
