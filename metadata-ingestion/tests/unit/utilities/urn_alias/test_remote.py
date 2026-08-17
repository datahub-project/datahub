from typing import Dict, List, Optional, Set
from unittest import mock

from datahub.ingestion.graph.entity_aspect_specs import EntityAspectSpecs
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


def _specs(*dataset_aspects: str) -> EntityAspectSpecs:
    """The server's entity registry, registering `dataset_aspects` on datasets."""
    aspects: Set[str] = {"datasetKey", *dataset_aspects}
    return EntityAspectSpecs(entity_aspects={"dataset": aspects})


def _reporting(
    graph: mock.MagicMock, specs: Optional[EntityAspectSpecs]
) -> mock.MagicMock:
    """Make `graph` answer the capability check with `specs`; None if unfetchable."""
    graph.get_entity_aspect_specs.return_value = specs
    return graph


# --- alias search --------------------------------------------------------------------


def _search_graph(*matches: str) -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.get_urns_by_filter.return_value = iter(matches)
    return _reporting(graph, _specs("aliases"))


def _queried(graph: mock.MagicMock) -> Dict[str, List[str]]:
    """The keys the last search asked for, by the field they were asked under."""
    _, kwargs = graph.get_urns_by_filter.call_args
    rules = [group["and"][0] for group in kwargs["extra_or_filters"]]
    return {rule["field"]: rule["values"] for rule in rules}


def _searching(graph: mock.MagicMock) -> UrnAliasResolver:
    index = UrnAliasIndex()
    return UrnAliasResolver(index, AliasIndexLookup(index, graph), query_on_demand=True)


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
    resolver = UrnAliasResolver(
        index, AliasIndexLookup(index, graph), query_on_demand=True
    )
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
    return _reporting(graph, _specs())


def _probing(graph: mock.MagicMock, *platform_instances: str) -> UrnAliasResolver:
    index = UrnAliasIndex()
    return UrnAliasResolver(
        index, CasingProbeLookup(index, graph, platform_instances), query_on_demand=True
    )


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
    first = UrnAliasResolver(
        index, CasingProbeLookup(index, graph), query_on_demand=True
    )
    second = UrnAliasResolver(
        index, CasingProbeLookup(index, graph), query_on_demand=True
    )

    assert first.resolve(_UPPER) == _LOWER
    assert second.resolve(_UPPER) == _LOWER

    assert graph.get_entities.call_count == 1


def _probing_a_load(graph: mock.MagicMock, *loaded: str) -> UrnAliasResolver:
    index = UrnAliasIndex()
    resolver = UrnAliasResolver(
        index, CasingProbeLookup(index, graph), query_on_demand=True
    )
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


def test_the_search_is_used_where_the_server_registers_the_aspect() -> None:
    graph = _reporting(mock.MagicMock(), _specs("aliases"))

    assert isinstance(select_lookup(UrnAliasIndex(), graph), AliasIndexLookup)


def test_the_probe_is_used_where_it_does_not() -> None:
    # Nothing there computes an alias, so the search would answer every reference with zero
    # hits — and record each as a settled absence.
    graph = _reporting(mock.MagicMock(), _specs())

    assert isinstance(select_lookup(UrnAliasIndex(), graph), CasingProbeLookup)


def test_the_probe_is_used_when_the_server_cannot_be_asked() -> None:
    graph = _reporting(mock.MagicMock(), None)

    assert isinstance(select_lookup(UrnAliasIndex(), graph), CasingProbeLookup)


def test_the_probe_is_used_when_the_specs_are_unusable() -> None:
    # A payload registering no `dataset` entity type at all is malformed, not a verdict, and
    # must not take the pipeline down with it.
    graph = _reporting(mock.MagicMock(), EntityAspectSpecs(entity_aspects={}))

    assert isinstance(select_lookup(UrnAliasIndex(), graph), CasingProbeLookup)


def test_every_consumer_of_a_server_gets_the_same_lookup() -> None:
    # They share one index, so they must key it the same way. Whether a consumer may query
    # is its own business and must not change the choice.
    graph = _reporting(mock.MagicMock(), _specs())

    bulk_only = get_urn_alias_resolver(graph)
    querying = get_urn_alias_resolver(graph, query_on_demand=True)

    assert type(bulk_only._lookup) is type(querying._lookup) is CasingProbeLookup


def test_a_consumer_that_may_not_query_issues_no_lookup() -> None:
    # It resolves from the rows a bulk load left, and never asks about anything else.
    graph = _search_graph(_LOWER)
    resolver = get_urn_alias_resolver(graph)
    resolver.add(_LOWER)

    assert resolver.resolve(_UPPER) == _LOWER
    assert resolver.resolve(_OTHER) is None

    graph.get_urns_by_filter.assert_not_called()
