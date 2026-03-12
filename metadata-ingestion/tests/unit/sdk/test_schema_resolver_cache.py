from unittest.mock import patch

import pytest

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sql_parsing.schema_resolver import GraphQLSchemaMetadata
from datahub.sql_parsing.schema_resolver_provider import (
    SchemaResolverProvider,
    provide_schema_resolver,
)

# A minimal GraphQL schema payload yielded by _bulk_fetch_schema_info_by_filter.
_FAKE_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
_FAKE_SCHEMA: GraphQLSchemaMetadata = {
    "fields": [{"fieldPath": "id", "nativeDataType": "INT64"}]
}


@pytest.fixture(autouse=True)
def clear_module_level_cache():
    # provide_schema_resolver is module-level lru_cache — must be cleared between
    # tests to prevent cross-test contamination.
    provide_schema_resolver.cache_clear()
    yield
    provide_schema_resolver.cache_clear()


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_bulk_fetch_runs_once_per_platform(mock_test_connection):
    """Calling provider.get() twice for the same platform should only bulk-fetch once."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))
    provider = SchemaResolverProvider(graph=graph)

    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_FAKE_URN, _FAKE_SCHEMA)]),
    ) as mock_fetch:
        provider.get(platform="bigquery", platform_instance=None, env="PROD")
        provider.get(platform="bigquery", platform_instance=None, env="PROD")

    assert mock_fetch.call_count == 1


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_bulk_fetch_runs_per_distinct_platform(mock_test_connection):
    """Different platforms must each get their own bulk fetch."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))
    provider = SchemaResolverProvider(graph=graph)

    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_FAKE_URN, _FAKE_SCHEMA)]),
    ) as mock_fetch:
        provider.get(platform="bigquery", platform_instance=None, env="PROD")
        provider.get(platform="mongodb", platform_instance=None, env="PROD")

    assert mock_fetch.call_count == 2


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_returned_resolver_is_same_object(mock_test_connection):
    """Both calls should return the same SchemaResolver instance."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))
    provider = SchemaResolverProvider(graph=graph)

    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_FAKE_URN, _FAKE_SCHEMA)]),
    ):
        resolver1 = provider.get(
            platform="bigquery", platform_instance=None, env="PROD"
        )
        resolver2 = provider.get(
            platform="bigquery", platform_instance=None, env="PROD"
        )

    assert resolver1 is resolver2


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_provide_schema_resolver_deduplicates_across_instances(mock_test_connection):
    """provide_schema_resolver must bulk-fetch only once even when called from
    different SchemaResolverProvider instances with the same graph/platform/env.
    This is the cross-instance deduplication guarantee that the per-instance
    lru_cache on SchemaResolverProvider.get() cannot provide alone."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_FAKE_URN, _FAKE_SCHEMA)]),
    ) as mock_fetch:
        resolver1 = provide_schema_resolver(
            graph=graph, platform="bigquery", platform_instance=None, env="PROD"
        )
        # Simulate a second call site (e.g. BigQuery and sql_parsing_aggregator
        # both resolving the same platform in the same process).
        resolver2 = provide_schema_resolver(
            graph=graph, platform="bigquery", platform_instance=None, env="PROD"
        )

    assert mock_fetch.call_count == 1
    assert resolver1 is resolver2
