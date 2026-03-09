from unittest.mock import patch

import pytest

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sql_parsing.schema_resolver import GraphQLSchemaMetadata

# A minimal GraphQL schema payload yielded by _bulk_fetch_schema_info_by_filter.
_FAKE_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
_FAKE_SCHEMA: GraphQLSchemaMetadata = {
    "fields": [{"fieldPath": "id", "nativeDataType": "INT64"}]
}


@pytest.fixture(autouse=True)
def clear_lru_cache():
    # _make_schema_resolver is lru_cache'd at the class level — clear between tests
    # to prevent cross-test contamination.
    DataHubGraph._make_schema_resolver.cache_clear()
    yield
    DataHubGraph._make_schema_resolver.cache_clear()


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_bulk_fetch_runs_once_per_platform(mock_test_connection):
    """Calling initialize_schema_resolver_from_datahub twice for the same platform
    should only bulk-fetch from GMS once."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_FAKE_URN, _FAKE_SCHEMA)]),
    ) as mock_fetch:
        graph.initialize_schema_resolver_from_datahub(
            platform="bigquery", platform_instance=None, env="PROD"
        )
        graph.initialize_schema_resolver_from_datahub(
            platform="bigquery", platform_instance=None, env="PROD"
        )

    assert mock_fetch.call_count == 1


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_bulk_fetch_runs_per_distinct_platform(mock_test_connection):
    """Different platforms must each get their own bulk fetch."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_FAKE_URN, _FAKE_SCHEMA)]),
    ) as mock_fetch:
        graph.initialize_schema_resolver_from_datahub(
            platform="bigquery", platform_instance=None, env="PROD"
        )
        graph.initialize_schema_resolver_from_datahub(
            platform="mongodb", platform_instance=None, env="PROD"
        )

    assert mock_fetch.call_count == 2


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_returned_resolver_is_same_object(mock_test_connection):
    """Both calls should return the same SchemaResolver instance."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_FAKE_URN, _FAKE_SCHEMA)]),
    ):
        resolver1 = graph.initialize_schema_resolver_from_datahub(
            platform="bigquery", platform_instance=None, env="PROD"
        )
        resolver2 = graph.initialize_schema_resolver_from_datahub(
            platform="bigquery", platform_instance=None, env="PROD"
        )

    assert resolver1 is resolver2
