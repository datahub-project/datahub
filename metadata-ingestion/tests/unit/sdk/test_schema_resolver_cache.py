import logging
from unittest.mock import patch

import pytest

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.sql_parsing.schema_resolver import GraphQLSchemaMetadata, SchemaResolver
from datahub.sql_parsing.schema_resolver_provider import (
    SchemaResolverProvider,
    provide_schema_resolver,
)
from datahub.utilities.urn_alias_resolver import set_urn_alias_loading

_PROVIDER_LOGGER = "datahub.sql_parsing.schema_resolver_provider"

# A minimal GraphQL schema payload yielded by _bulk_fetch_schema_info_by_filter.
_FAKE_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
_FAKE_SCHEMA: GraphQLSchemaMetadata = {
    "fields": [{"fieldPath": "id", "nativeDataType": "INT64"}]
}


@pytest.fixture(autouse=True)
def reset_urn_alias_loading():
    set_urn_alias_loading(False)
    yield
    set_urn_alias_loading(False)


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


def _load(graph: DataHubGraph) -> SchemaResolver:
    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_FAKE_URN, _FAKE_SCHEMA), (_SCHEMALESS_URN, None)]),
    ):
        return SchemaResolverProvider(graph=graph).get(
            platform="bigquery", platform_instance=None, env="PROD"
        )


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_provider_indexes_every_urn_including_schemaless(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    set_urn_alias_loading(True)
    resolver = _load(graph)

    # Both URNs are resolvable by casing, whether or not DataHub knows their columns.
    assert resolver.urn_aliases.find_match(_FAKE_URN_UPPERCASED) == [_FAKE_URN]
    assert resolver.urn_aliases.find_match(_SCHEMALESS_URN_UPPERCASED) == [
        _SCHEMALESS_URN
    ]
    # ...but only the one with a schema is in the schema cache.
    assert resolver.schema_count() == 1


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_provider_skips_the_urn_index_when_no_consumer_needs_it(mock_test_connection):
    """The index is a whole platform's URNs in memory; nobody pays unless it is wanted."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))

    resolver = _load(graph)

    assert resolver.urn_aliases.cached_urn_count() == 0
    # Schemas are loaded either way.
    assert resolver.schema_count() == 1


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_warns_when_datasets_are_found_but_none_have_schemas(
    mock_test_connection, caplog
):
    """URNs without schemas resolve by name but cannot support column-level lineage."""
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))
    provider = SchemaResolverProvider(graph=graph)

    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_SCHEMALESS_URN, None)]),
    ):
        with caplog.at_level(logging.WARNING, logger=_PROVIDER_LOGGER):
            provider.get(platform="bigquery", platform_instance=None, env="PROD")

    assert [r for r in caplog.records if r.levelno == logging.WARNING]


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_does_not_warn_when_schemas_were_loaded(mock_test_connection, caplog):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))
    provider = SchemaResolverProvider(graph=graph)

    with patch.object(
        graph,
        "_bulk_fetch_schema_info_by_filter",
        return_value=iter([(_FAKE_URN, _FAKE_SCHEMA), (_SCHEMALESS_URN, None)]),
    ):
        with caplog.at_level(logging.WARNING, logger=_PROVIDER_LOGGER):
            provider.get(platform="bigquery", platform_instance=None, env="PROD")

    assert not [r for r in caplog.records if r.levelno == logging.WARNING]
