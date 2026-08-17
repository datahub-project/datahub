from typing import Dict, List
from unittest import mock

from datahub.utilities.urn_alias.index import UrnAliasIndex
from datahub.utilities.urn_alias.remote import (
    AliasIndexLookup,
    CasingProbeLookup,
    select_lookup,
)
from datahub.utilities.urn_alias.resolver import (
    UrnAliasResolver,
    get_urn_alias_resolver,
)

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Db.My_Schema.Events,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
_OTHER_UPPER = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.ORDERS,PROD)"
)
# `_UPPER` with only the platform instance left alone, the casing connectors produce
# when they lowercase the table name but not the instance.
_PLATFORM_INSTANCE = "MY_DB"
_INSTANCE_KEPT = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.my_schema.events,PROD)"
)

# What `select_lookup` reads the capability from. Patched to reach the fallback, which this
# client's own model can never select on its own — the aspect is right there in it.
_ALIASES_SUPPORTED = "datahub.utilities.urn_alias.remote._aliases_supported"


# --- alias search --------------------------------------------------------------------


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
    return UrnAliasResolver(index, AliasIndexLookup(index, graph))


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


def test_a_bulk_loaded_name_is_never_asked_about() -> None:
    # A load wrote the same table the search fills, so its rows answer outright.
    graph = _search_graph()
    index = UrnAliasIndex()
    resolver = UrnAliasResolver(index, AliasIndexLookup(index, graph))
    resolver.add(_LOWER)

    assert resolver.resolve(_UPPER) == _LOWER

    graph.get_urns_by_filter.assert_not_called()


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
    return UrnAliasResolver(index, CasingProbeLookup(index, graph, platform_instances))


def test_the_probe_finds_a_guessable_casing() -> None:
    graph = _probe_graph(_LOWER)

    assert _probing(graph).resolve(_UPPER) == _LOWER


def test_the_probe_guesses_the_instance_kept_casing_it_is_told_about() -> None:
    # A platform instance cannot be recovered from a URN, only tested against one, so
    # this casing is only reachable when the instance is known.
    graph = _probe_graph(_INSTANCE_KEPT)

    assert _probing(graph, _PLATFORM_INSTANCE).resolve(_UPPER) == _INSTANCE_KEPT
    assert _probing(graph).resolve(_UPPER) is None


def test_the_probe_asks_about_a_casing_only_once() -> None:
    graph = _probe_graph()
    resolver = _probing(graph)

    assert resolver.resolve(_OTHER) is None
    assert resolver.resolve(_OTHER) is None

    # A guess that came back empty is still worth keeping, so an entity nothing holds
    # costs one question for the run rather than one per reference to it.
    assert graph.get_entities.call_count == 1


def test_a_casing_that_was_found_settles_only_itself() -> None:
    # DataHub holds two casings, and each reference reaches only the casings guessed for
    # it. Recording the first answer under the *name* would make the name look settled,
    # so the second reference — naming a casing the first never guessed — would resolve
    # to the wrong entity, and would not even ask.
    graph = _probe_graph(_LOWER, _MIXED)
    resolver = _probing(graph)

    assert resolver.resolve(_UPPER, prefer_lowercased=True) == _LOWER
    assert resolver.resolve(_MIXED, prefer_lowercased=True) == _MIXED


def test_a_casing_that_was_missing_settles_only_itself() -> None:
    # The same in the other direction: "this exact spelling is not here" must not read as
    # "nothing by this name is here", or the casing that does exist stays invisible.
    graph = _probe_graph(_OTHER_UPPER)
    resolver = _probing(graph)

    assert resolver.resolve(_OTHER) is None
    assert resolver.resolve(_OTHER_UPPER) == _OTHER_UPPER


def test_what_the_probe_asked_is_shared_by_every_consumer_of_the_index() -> None:
    # It lives in the index shared per DataHub instance rather than on the lookup that
    # asked, so a second consumer of the same graph does not re-ask the same questions.
    graph = _probe_graph(_LOWER)
    index = UrnAliasIndex()
    first = UrnAliasResolver(index, CasingProbeLookup(index, graph))
    second = UrnAliasResolver(index, CasingProbeLookup(index, graph))

    assert first.resolve(_UPPER) == _LOWER
    assert second.resolve(_UPPER) == _LOWER

    assert graph.get_entities.call_count == 1


def _probing_a_load(graph: mock.MagicMock, *loaded: str) -> UrnAliasResolver:
    index = UrnAliasIndex()
    resolver = UrnAliasResolver(index, CasingProbeLookup(index, graph))
    for urn in loaded:
        resolver.add(urn)
    return resolver


def test_the_probe_reads_a_bulk_loaded_casing_it_would_guess() -> None:
    resolver = _probing_a_load(_probe_graph(), _LOWER)

    assert resolver.resolve(_UPPER) == _LOWER


def test_preloading_does_not_widen_what_the_probe_can_reach() -> None:
    # A load's rows are read under the casings the probe would try, and nothing suggests
    # `My_Db.My_Schema.Events`. Reading them by name instead would resolve this reference
    # where the operator preloaded and leave it UNRESOLVED where they did not, which makes
    # the verdict a property of the recipe rather than of what DataHub holds. Preloading is
    # a cost knob. Reaching an unguessable casing is the `aliases` aspect's job.
    resolver = _probing_a_load(_probe_graph(), _MIXED)

    assert resolver.resolve(_UPPER) is None
    # Identical outcome with nothing preloaded at all: the same question, the same answer.
    assert _probing(_probe_graph(_MIXED)).resolve(_UPPER) is None


def test_a_failed_probe_records_nothing() -> None:
    graph = mock.MagicMock()
    graph.get_entities.side_effect = Exception("boom")
    resolver = _probing(graph)

    assert resolver.resolve(_LOWER) is None
    assert resolver.resolve(_LOWER) is None

    assert graph.get_entities.call_count == 2


# --- picking the one way to ask -------------------------------------------------------


def test_the_search_is_used_where_the_aliases_aspect_exists() -> None:
    assert isinstance(
        select_lookup(UrnAliasIndex(), mock.MagicMock()), AliasIndexLookup
    )


def test_the_probe_is_used_where_it_does_not() -> None:
    # Nothing indexes an alias there, so the search would answer every reference with zero
    # hits — and record each as a settled absence.
    with mock.patch(_ALIASES_SUPPORTED, return_value=False):
        lookup = select_lookup(UrnAliasIndex(), mock.MagicMock())

    assert isinstance(lookup, CasingProbeLookup)


def test_choosing_asks_the_server_nothing() -> None:
    # It is answered from the metadata model this client is built on.
    graph = mock.MagicMock()

    select_lookup(UrnAliasIndex(), graph)

    graph.execute_graphql.assert_not_called()
    graph.get_urns_by_filter.assert_not_called()


def test_a_consumer_that_may_not_query_never_reaches_the_server() -> None:
    # A bulk load keys by the lowercased URN whatever the server maintains, so its rows
    # resolve any casing on their own.
    graph = _search_graph(_LOWER)
    resolver = get_urn_alias_resolver(graph)
    resolver.add(_LOWER)

    assert resolver.resolve(_UPPER) == _LOWER

    graph.get_urns_by_filter.assert_not_called()
