from unittest.mock import patch

import pytest

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubUpgradeResultClass,
    DataHubUpgradeStateClass,
)
from datahub.sql_parsing.schema_resolver import GraphQLSchemaMetadata
from datahub.sql_parsing.schema_resolver_provider import (
    SchemaResolverProvider,
    provide_schema_resolver,
)

_PROVIDER_LOGGER = "datahub.sql_parsing.schema_resolver_provider"

# A minimal GraphQL schema payload yielded by _bulk_fetch_schema_info_by_filter.
_FAKE_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
_FAKE_SCHEMA: GraphQLSchemaMetadata = {
    "fields": [{"fieldPath": "id", "nativeDataType": "INT64"}]
}


@pytest.fixture(autouse=True)
def server_registers_aliases():
    """Pin the server as one whose dataset aliases backfill completed, so no test here
    sends the marker read to the fake host; every other aspect read goes through
    untouched."""
    marker = DataHubUpgradeResultClass(
        timestampMs=0, state=DataHubUpgradeStateClass.SUCCEEDED
    )
    read_aspect = DataHubGraph.get_aspect

    def get_aspect(self, entity_urn, aspect_type, version=0):
        if aspect_type is DataHubUpgradeResultClass:
            return marker
        return read_aspect(self, entity_urn, aspect_type, version)

    with patch.object(DataHubGraph, "get_aspect", get_aspect):
        yield


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


# The same dataset names in a different casing, plus a dataset with no schemaMetadata.
_FAKE_URN_UPPERCASED = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,PROJECT.DATASET.TABLE,PROD)"
)
_SCHEMALESS_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.no_schema,PROD)"
)
_SCHEMALESS_URN_UPPERCASED = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,PROJECT.DATASET.NO_SCHEMA,PROD)"
)
# Inside the scrolled slice (same platform and env) but never returned by it.
_MISSING_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.absent,PROD)"
)
_OTHER_PLATFORM_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD)"
)
_OTHER_PLATFORM_URN_UPPERCASED = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.EVENTS,PROD)"
)


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_bulk_fetch_yields_datasets_that_have_no_schema(mock_test_connection):
    """A dataset without schemaMetadata must still be reported, with a None schema."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    with patch.object(
        graph,
        "_scroll_across_entities",
        return_value=iter(
            [
                {"urn": _FAKE_URN, "schemaMetadata": _FAKE_SCHEMA},
                {"urn": _SCHEMALESS_URN, "schemaMetadata": None},
            ]
        ),
    ):
        results = list(
            graph._bulk_fetch_schema_info_by_filter(platform="bigquery", env="PROD")
        )

    assert results == [(_FAKE_URN, _FAKE_SCHEMA), (_SCHEMALESS_URN, None)]
