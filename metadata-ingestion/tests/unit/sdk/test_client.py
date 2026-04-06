import re
from pathlib import Path
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


def _parse_entity_type_enum() -> set:
    """Parse the valid EntityType enum values from the GraphQL schema file.

    Returns an empty set if entity.graphql is not present (e.g. when only the
    Python package is installed without the full repo checkout).
    """
    entity_graphql = (
        Path(__file__).parents[4]
        / "datahub-graphql-core/src/main/resources/entity.graphql"
    )
    if not entity_graphql.exists():
        return set()
    schema = entity_graphql.read_text()
    m = re.search(r"enum EntityType \{([^}]+)\}", schema, re.DOTALL)
    if not m:
        return set()
    return {
        line.strip()
        for line in m.group(1).splitlines()
        if re.match(r"^\s+[A-Z][A-Z0-9_]+\s*$", line)
    }


def test_entity_type_to_graphql_produces_valid_enum_values() -> None:
    """Every camelCase entity type name the Python client may pass through
    entity_type_to_graphql() must produce a value that exists in the GraphQL
    EntityType enum defined in entity.graphql.

    This is a regression test for the class of bug where the mechanical
    camelCase -> UPPER_SNAKE_CASE conversion produces a string that was never
    added to the enum (e.g. 'dataProcess' -> 'DATA_PROCESS', which does not
    exist; only 'DATA_PROCESS_INSTANCE' does).
    """
    valid_values = _parse_entity_type_enum()
    if not valid_values:
        pytest.skip("entity.graphql not found - skipping schema validation")

    # Comprehensive list of camelCase entity type names the Python client may
    # encounter, covering both active and deprecated types in the URN registry.
    entity_types = [
        "dataset",
        "dashboard",
        "chart",
        "domain",
        "container",
        "corpuser",
        "corpGroup",
        "dataFlow",
        "dataJob",
        "glossaryNode",
        "glossaryTerm",
        "dataProduct",
        "dataHubExecutionRequest",
        "document",
        "mlModel",
        "mlModelGroup",
        "mlFeatureTable",
        "mlFeature",
        "mlPrimaryKey",
        "dataProcessInstance",
        # Deprecated (PDL: @deprecated = "Use DataJob instead.") but still
        # present in the URN registry.  Must produce a valid enum value rather
        # than the non-existent DATA_PROCESS.
        "dataProcess",
    ]
    for entity_type in entity_types:
        result = entity_type_to_graphql(entity_type)
        assert result in valid_values, (
            f"entity_type_to_graphql({entity_type!r}) returned {result!r}, "
            f"which is not a valid GraphQL EntityType enum value. "
            f"Add it to the special_cases dict in entity_type_to_graphql() "
            f"or add {result!r} to the EntityType enum in entity.graphql."
        )


def test_data_process_maps_to_data_process_instance() -> None:
    # "dataProcess" is a deprecated entity type (the PDL model is annotated
    # @deprecated = "Use DataJob instead."). It was never added to the GraphQL
    # EntityType enum in entity.graphql -- DATA_PROCESS does not exist as an
    # enum value, only DATA_PROCESS_INSTANCE does.
    #
    # entity_type_to_graphql() converts camelCase mechanically, producing
    # "DATA_PROCESS" for "dataProcess". Passing that to searchAcrossEntities
    # causes GMS to return a GraphQL ValidationError:
    #
    #   Variable 'types' has an invalid value: Invalid input for enum
    #   'EntityType'. No value found for name 'DATA_PROCESS'
    #
    # This is triggered in practice when mcp-server-datahub's search tool
    # receives a filter like entity_type=dataProcess from an LLM agent
    # querying for pipeline run entities. The mapping to DATA_PROCESS_INSTANCE
    # is intentional: LLM agents using the deprecated name are almost certainly
    # looking for run instances (traces/spans), not workflow definitions
    # (DataJob). See entity.graphql enum EntityType for the authoritative list.
    assert entity_type_to_graphql("dataProcess") == "DATA_PROCESS_INSTANCE"
