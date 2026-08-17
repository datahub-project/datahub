from typing import Dict, List
from unittest import mock

from datahub.utilities.urn_alias.index import UrnAliasIndex
from datahub.utilities.urn_alias.remote import AliasSearchLookup, CasingProbeLookup
from datahub.utilities.urn_alias.resolver import UrnAliasResolver

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
# `_UPPER` with only the platform instance left alone, the casing connectors produce
# when they lowercase the table name but not the instance.
_PLATFORM_INSTANCE = "MY_DB"
_INSTANCE_KEPT = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.my_schema.events,PROD)"
)


def _search_graph(*matches: str) -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.get_urns_by_filter.return_value = iter(matches)
    return graph


def _queried(graph: mock.MagicMock) -> Dict[str, List[str]]:
    """The keys the last search asked for, by the field they were asked under."""
    _, kwargs = graph.get_urns_by_filter.call_args
    rules = [group["and"][0] for group in kwargs["extra_or_filters"]]
    return {rule["field"]: rule["values"] for rule in rules}


def _searching(graph: mock.MagicMock) -> UrnAliasResolver:
    index = UrnAliasIndex()
    return UrnAliasResolver(index, AliasSearchLookup(graph, index))


# --- alias search --------------------------------------------------------------------


def test_the_search_asks_under_the_key_gms_indexes() -> None:
    graph = _search_graph(_UPPER)

    assert _searching(graph).resolve(_UPPER) == _UPPER
    # The name lowercased, platform and env untouched. `_LOWER` is exactly that form of
    # `_UPPER`, so it doubles as the expected key.
    assert _queried(graph)["lowercasedUrn"] == [_LOWER]


def test_the_search_asks_under_the_urn_field_too() -> None:
    # A dataset predating the `aliases` aspect has no alias until the backfill reaches
    # it, leaving it findable only under `urn`.
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

    assert resolver.resolve(_LOWER) is None
    assert resolver.resolve(_LOWER) is None

    # A transient failure recorded as "known absent" would decline every later reference
    # to a real entity for the rest of the run.
    assert graph.get_urns_by_filter.call_count == 2


def test_a_non_dataset_reference_is_never_asked_about() -> None:
    graph = _search_graph()

    assert _searching(graph).resolve("urn:li:corpuser:alice") is None

    graph.get_urns_by_filter.assert_not_called()


# --- casing probe --------------------------------------------------------------------


def _probe_graph(*existing: str) -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.get_entities.side_effect = lambda **kwargs: {
        urn: {} for urn in kwargs["urns"] if urn in existing
    }
    return graph


def _probing(graph: mock.MagicMock, *platform_instances: str) -> UrnAliasResolver:
    index = UrnAliasIndex()
    return UrnAliasResolver(index, CasingProbeLookup(graph, index, platform_instances))


def test_the_probe_finds_a_guessable_casing() -> None:
    graph = _probe_graph(_LOWER)

    assert _probing(graph).resolve(_UPPER) == _LOWER


def test_the_probe_guesses_the_instance_kept_casing_it_is_told_about() -> None:
    # A platform instance cannot be recovered from a URN, only tested against one, so
    # this casing is only reachable when the instance is known.
    graph = _probe_graph(_INSTANCE_KEPT)

    assert _probing(graph, _PLATFORM_INSTANCE).resolve(_UPPER) == _INSTANCE_KEPT
    assert _probing(graph).resolve(_UPPER) is None


def test_the_probe_records_no_absence_but_still_asks_only_once() -> None:
    graph = _probe_graph()
    resolver = _probing(graph)

    assert resolver.resolve(_OTHER) is None
    assert resolver.resolve(_OTHER) is None

    # A guess that came back empty proves nothing about the name — the probe only finds
    # casings it thought of — so it is kept out of the shared index entirely. Not asking
    # twice is tracked by the probe itself.
    assert graph.get_entities.call_count == 1


def test_a_failed_probe_records_nothing() -> None:
    graph = mock.MagicMock()
    graph.get_entities.side_effect = Exception("boom")
    resolver = _probing(graph)

    assert resolver.resolve(_LOWER) is None
    assert resolver.resolve(_LOWER) is None

    assert graph.get_entities.call_count == 2
