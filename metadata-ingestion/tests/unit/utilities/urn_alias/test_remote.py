from typing import Dict, List
from unittest import mock

import pytest

from datahub.utilities.server_config_util import RestServiceConfig
from datahub.utilities.urn_alias.index import UrnAliasIndex
from datahub.utilities.urn_alias.remote import gms_maintains_urn_aliases
from datahub.utilities.urn_alias.resolver import (
    UrnAliasResolver,
)

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Db.My_Schema.Events,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
_OTHER_UPPER = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.ORDERS,PROD)"
)


def _server(graph: mock.MagicMock, version: str, cloud: bool = False) -> mock.MagicMock:
    """Make `graph` report itself as DataHub `version`."""
    graph.server_config = RestServiceConfig(
        raw_config={
            "versions": {"acryldata/datahub": {"version": version}},
            "datahub": {"serverEnv": "prod" if cloud else ""},
        }
    )
    return graph


# --- alias search --------------------------------------------------------------------


def _search_graph(*matches: str) -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.get_urns_by_filter.return_value = iter(matches)
    return _server(graph, "v1.8.0")


def _queried(graph: mock.MagicMock) -> Dict[str, List[str]]:
    """The keys the last search asked for, by the field they were asked under."""
    _, kwargs = graph.get_urns_by_filter.call_args
    rules = [group["and"][0] for group in kwargs["extra_or_filters"]]
    return {rule["field"]: rule["values"] for rule in rules}


def _searching(graph: mock.MagicMock) -> UrnAliasResolver:
    return UrnAliasResolver(graph, query_on_demand=True)


def test_the_search_asks_under_the_key_gms_indexes() -> None:
    graph = _search_graph(_UPPER)

    assert _searching(graph).resolve(_UPPER) == _UPPER
    # The name lowercased, platform and env untouched. `_LOWER` is exactly that form of
    # `_UPPER`, so it doubles as the expected key.
    assert _queried(graph)["lowercasedUrn"] == [_LOWER]


def test_the_search_asks_under_the_urn_field_too() -> None:
    # GMS writes aliases asynchronously, so a dataset whose alias has not landed yet is
    # findable only under `urn`.
    graph = _search_graph(_LOWER)

    assert _searching(graph).resolve(_UPPER) == _LOWER
    # One search, both fields, the same keys — the two clauses are OR'd.
    assert _queried(graph) == {"lowercasedUrn": [_LOWER], "urn": [_LOWER]}
    graph.get_urns_by_filter.assert_called_once()


def test_the_search_batches_references_into_one_query() -> None:
    graph = _search_graph(_LOWER, _OTHER)
    resolver = _searching(graph)

    resolver.prefetch([_UPPER, _OTHER])

    assert graph.get_urns_by_filter.call_count == 1
    assert sorted(_queried(graph)["lowercasedUrn"]) == sorted([_LOWER, _OTHER])
    # Each reference is answered from the one round trip, with no further calls.
    assert resolver.resolve(_UPPER) == _LOWER
    assert resolver.resolve(_OTHER) == _OTHER
    assert graph.get_urns_by_filter.call_count == 1


def test_the_search_records_an_absence_so_it_is_asked_once() -> None:
    # The search is exhaustive, so "not found" is a fact worth keeping: an absent entity
    # costs one call, not one per reference to it.
    graph = _search_graph()
    resolver = _searching(graph)

    assert resolver.resolve(_OTHER) is None
    assert resolver.resolve(_OTHER) is None

    assert graph.get_urns_by_filter.call_count == 1


def test_a_failed_search_records_nothing() -> None:
    graph = mock.MagicMock()
    graph.get_urns_by_filter.side_effect = Exception("boom")
    resolver = _searching(graph)

    # Raised, not answered None: the caller must not read a failure as an absence.
    for _ in range(2):
        with pytest.raises(Exception, match="boom"):
            resolver.resolve(_LOWER)

    # A transient failure recorded as "known absent" would decline every later reference
    # to a real entity for the rest of the run.
    assert graph.get_urns_by_filter.call_count == 2


def test_a_bulk_loaded_name_is_never_asked_about() -> None:
    # A load wrote the same table the search fills, so its rows answer outright.
    graph = _search_graph()
    index = UrnAliasIndex()
    index.add(_LOWER)
    resolver = UrnAliasResolver(graph, [index], query_on_demand=True)

    assert resolver.resolve(_UPPER) == _LOWER

    graph.get_urns_by_filter.assert_not_called()


def test_a_non_dataset_reference_is_never_asked_about() -> None:
    graph = _search_graph()

    assert _searching(graph).resolve("urn:li:corpuser:alice") is None

    graph.get_urns_by_filter.assert_not_called()


# --- the gate on the whole feature ----------------------------------------------------


@pytest.mark.parametrize(
    ("version", "cloud", "supported"),
    [
        ("v1.8.0", False, True),
        ("v1.9.2", False, True),
        ("v1.7.0", False, False),
        ("v2.2.0", True, True),
        ("v2.1.0", True, False),
    ],
)
def test_the_feature_needs_a_server_that_maintains_aliases(
    version: str, cloud: bool, supported: bool
) -> None:
    # Cloud and OSS number separately, so each has its own floor.
    graph = _server(mock.MagicMock(), version, cloud=cloud)

    assert gms_maintains_urn_aliases(graph) is supported
