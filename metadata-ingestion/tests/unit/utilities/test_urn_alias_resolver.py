import pathlib
from typing import Dict, List
from unittest import mock

from datahub.utilities.file_backed_collections import ConnectionWrapper
from datahub.utilities.urn_alias_resolver import UrnAliasCache, UrnAliasResolver

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Db.My_Schema.Events,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
_TABLE = "test_cache"
# `_UPPER` with only the platform instance left alone, the casing connectors produce when
# they lowercase the table name but not the instance.
_PLATFORM_INSTANCE = "MY_DB"
_INSTANCE_KEPT = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.my_schema.events,PROD)"
)


def _loaded(*urns: str) -> UrnAliasResolver:
    resolver = UrnAliasResolver()
    for urn in urns:
        resolver.add(urn)
    return resolver


# --- cache ------------------------------------------------------------------------


def test_cache_distinguishes_unknown_from_known_absent() -> None:
    cache = UrnAliasCache(_TABLE)
    cache.add(_LOWER, _MIXED)

    # None means unknown; an empty list would mean known not to exist.
    assert cache.get(_OTHER) is None


def test_cache_records_the_same_urn_once() -> None:
    cache = UrnAliasCache(_TABLE)
    cache.add(_LOWER, _MIXED)
    cache.add(_LOWER, _MIXED)

    assert cache.get(_LOWER) == [_MIXED]


def test_cache_reopened_on_the_same_file_keeps_what_it_held(
    tmp_path: pathlib.Path,
) -> None:
    # A lost table reads as "nothing matched", so the failure would be silent.
    path = tmp_path / "aliases.db"
    conn = ConnectionWrapper(filename=path)
    UrnAliasCache(_TABLE, conn).add(_LOWER, _MIXED)
    conn.close()

    reopened = ConnectionWrapper(filename=path)
    cache = UrnAliasCache(_TABLE, reopened)

    assert cache.get(_LOWER) == [_MIXED]
    reopened.close()


def test_caches_on_different_tables_do_not_see_each_other(
    tmp_path: pathlib.Path,
) -> None:
    # The two lookups key their rows differently, so the index's "lowercases to K" row
    # must never be read by the probe as "K exists".
    with ConnectionWrapper(filename=tmp_path / "shared.db") as conn:
        index = UrnAliasCache("urn_aliases", conn)
        probe = UrnAliasCache("urn_casing_probe", conn)

        index.add(_LOWER, _UPPER)

        assert probe.get(_LOWER) is None


def test_cache_does_not_hand_out_its_stored_list() -> None:
    cache = UrnAliasCache(_TABLE)
    cache.add(_LOWER, _MIXED)

    entry = cache.get(_LOWER)
    assert entry is not None
    entry.append(_OTHER)

    assert cache.get(_LOWER) == [_MIXED]


# --- resolver ---------------------------------------------------------------------


def test_lookup_matches_a_different_casing() -> None:
    resolver = _loaded(_LOWER)

    assert resolver.find_match(_UPPER) == [_LOWER]
    assert resolver.find_match(_MIXED) == [_LOWER]


def test_lookup_returns_the_stored_urn_for_an_exact_match() -> None:
    resolver = _loaded(_MIXED)

    assert resolver.find_match(_MIXED) == [_MIXED]


def test_lookup_returns_every_urn_of_a_case_collision() -> None:
    # Two real entities differing only by case: both are returned so the caller can
    # see the ambiguity.
    resolver = _loaded(_LOWER, _UPPER)

    assert resolver.find_match(_MIXED) == [_LOWER, _UPPER]


def test_lookup_flattens_unknown_and_absent_to_empty() -> None:
    resolver = _loaded(_LOWER)

    # Callers get one "no match" answer and never have to interpret None.
    assert resolver.find_match(_OTHER) == []


def test_resolve_returns_the_stored_urn_for_a_different_casing() -> None:
    resolver = _loaded(_LOWER)

    assert resolver.resolve(_UPPER) == _LOWER


def test_resolve_returns_an_exact_match_even_when_another_casing_exists() -> None:
    # An exact hit is never ambiguous: the reference names a real entity, so it stands
    # regardless of other casings of the same name.
    resolver = _loaded(_LOWER, _UPPER)

    assert resolver.resolve(_UPPER) == _UPPER


def test_resolve_returns_none_when_casings_collide() -> None:
    resolver = _loaded(_LOWER, _UPPER)

    assert resolver.resolve(_MIXED) is None


def test_resolve_returns_none_when_nothing_matches() -> None:
    resolver = _loaded(_LOWER)

    assert resolver.resolve(_OTHER) is None


def test_resolve_prefers_the_lowercased_urn_on_a_collision() -> None:
    resolver = _loaded(_LOWER, _UPPER)

    assert resolver.resolve(_MIXED, prefer_lowercased=True) == _LOWER


def test_resolve_prefers_lowercased_still_declines_when_none_is_lowercased() -> None:
    # Both stored casings are non-lowercase, so the preference has nothing to pick.
    resolver = _loaded(_MIXED, _UPPER)

    assert resolver.resolve(_LOWER, prefer_lowercased=True) is None


def test_resolve_prefers_lowercased_does_not_override_an_exact_match() -> None:
    resolver = _loaded(_LOWER, _UPPER)

    assert resolver.resolve(_UPPER, prefer_lowercased=True) == _UPPER


# --- on-demand lookup -------------------------------------------------------------


def _graph(*matches: str) -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.get_urns_by_filter.return_value = iter(matches)
    return graph


def _queried(graph: mock.MagicMock) -> Dict[str, List[str]]:
    """The keys the last search asked for, by the field they were asked under."""
    _, kwargs = graph.get_urns_by_filter.call_args
    rules = [group["and"][0] for group in kwargs["extra_or_filters"]]
    return {rule["field"]: rule["values"] for rule in rules}


def test_on_demand_lookup_resolves_a_urn_the_index_never_loaded() -> None:
    graph = _graph(_UPPER)

    assert UrnAliasResolver(graph=graph).resolve(_UPPER) == _UPPER
    # Filtered on the key GMS indexes: the name lowercased, platform and env untouched.
    # `_LOWER` is exactly that form of `_UPPER`, so it doubles as the expected key.
    assert _queried(graph)["lowercasedUrn"] == [_LOWER]


def test_on_demand_lookup_asks_under_the_urn_field_too() -> None:
    # A dataset predating the `aliases` aspect has no alias until the backfill reaches it,
    # leaving it findable only under `urn`.
    graph = _graph(_LOWER)

    assert UrnAliasResolver(graph=graph).resolve(_UPPER) == _LOWER
    # One search, both fields, the same keys — the two clauses are OR'd.
    assert _queried(graph) == {"lowercasedUrn": [_LOWER], "urn": [_LOWER]}
    graph.get_urns_by_filter.assert_called_once()


def test_on_demand_lookup_batches_references_into_one_query() -> None:
    graph = _graph(_LOWER, _OTHER)
    resolver = UrnAliasResolver(graph=graph)

    resolver.prefetch([_UPPER, _OTHER])

    assert graph.get_urns_by_filter.call_count == 1
    assert sorted(_queried(graph)["lowercasedUrn"]) == sorted([_LOWER, _OTHER])
    # Each reference is answered from the one round trip, with no further calls.
    assert resolver.resolve(_UPPER) == _LOWER
    assert resolver.resolve(_OTHER) == _OTHER
    assert graph.get_urns_by_filter.call_count == 1


def test_on_demand_lookup_queries_only_the_references_still_unknown() -> None:
    graph = _graph(_OTHER)
    resolver = UrnAliasResolver(graph=graph)
    resolver.add(_LOWER)

    resolver.prefetch([_UPPER, _OTHER])

    # Only the unknown reference is queried; the loaded one is not re-fetched.
    assert _queried(graph)["lowercasedUrn"] == [_OTHER]


def test_on_demand_lookup_ignores_a_non_dataset_reference() -> None:
    graph = _graph()

    assert UrnAliasResolver(graph=graph).resolve("urn:li:corpuser:alice") is None
    # No dataset key can be derived for it, so there is nothing to ask the backend.
    graph.get_urns_by_filter.assert_not_called()


def test_on_demand_lookup_is_skipped_when_the_index_already_answers() -> None:
    graph = _graph(_UPPER)
    resolver = UrnAliasResolver(graph=graph)
    resolver.add(_LOWER)

    assert resolver.resolve(_LOWER) == _LOWER
    graph.get_urns_by_filter.assert_not_called()


def test_on_demand_lookup_caches_a_negative_answer() -> None:
    graph = _graph()
    resolver = UrnAliasResolver(graph=graph)

    assert resolver.resolve(_OTHER) is None
    assert resolver.resolve(_OTHER) is None
    # The second reference must not re-query: an absent entity costs one call, not one
    # per reference.
    assert graph.get_urns_by_filter.call_count == 1


def test_on_demand_lookup_failure_is_not_cached() -> None:
    graph = mock.MagicMock()
    graph.get_urns_by_filter.side_effect = Exception("boom")
    resolver = UrnAliasResolver(graph=graph)

    assert resolver.resolve(_LOWER) is None
    assert resolver.resolve(_LOWER) is None
    # A transient failure must not be recorded as "known absent" for the rest of the run.
    assert graph.get_urns_by_filter.call_count == 2


def test_without_a_graph_an_unknown_urn_is_simply_unresolved() -> None:
    assert UrnAliasResolver().resolve(_LOWER) is None


# --- cacheless resolution ----------------------------------------------------------


def _graph_answering_every_call(*matches: str) -> mock.MagicMock:
    """A graph that answers every search with `matches`, not only the first.

    `_graph` hands back one iterator, which a second search would find exhausted — fine
    for a cached resolver, which only ever queries once per key.
    """
    graph = mock.MagicMock()
    graph.get_urns_by_filter.side_effect = lambda **kwargs: iter(matches)
    return graph


def test_cacheless_resolves_what_a_cached_resolver_resolves() -> None:
    graph = _graph(_LOWER)

    assert UrnAliasResolver(graph=graph, cached=False).resolve(_UPPER) == _LOWER


def test_cacheless_queries_per_reference_and_retains_nothing() -> None:
    graph = _graph_answering_every_call(_LOWER)
    resolver = UrnAliasResolver(graph=graph, cached=False)

    assert resolver.resolve(_UPPER) == _LOWER
    assert resolver.resolve(_UPPER) == _LOWER

    # The point of the mode: nothing is held, so a repeated reference is paid for again.
    assert graph.get_urns_by_filter.call_count == 2


def test_cacheless_add_does_not_accumulate() -> None:
    graph = _graph(_LOWER)
    resolver = UrnAliasResolver(graph=graph, cached=False)

    resolver.add(_LOWER)

    # A bulk load has nowhere to land, so the reference is still resolved by querying.
    assert resolver.resolve(_UPPER) == _LOWER
    graph.get_urns_by_filter.assert_called_once()


def test_cacheless_prefetch_neither_queries_nor_spares_a_later_lookup() -> None:
    graph = _graph_answering_every_call(_LOWER)
    resolver = UrnAliasResolver(graph=graph, cached=False)

    resolver.prefetch([_UPPER])
    graph.get_urns_by_filter.assert_not_called()

    assert resolver.resolve(_UPPER) == _LOWER
    assert graph.get_urns_by_filter.call_count == 1


def test_cacheless_query_failure_is_simply_unresolved() -> None:
    graph = mock.MagicMock()
    graph.get_urns_by_filter.side_effect = Exception("boom")

    assert UrnAliasResolver(graph=graph, cached=False).resolve(_LOWER) is None


def test_cacheless_without_a_graph_resolves_nothing() -> None:
    # Nothing retained and nothing to query: the resolver is inert rather than wrong.
    assert UrnAliasResolver(cached=False).resolve(_LOWER) is None


# --- casing probe, for a server without the aliases aspect -------------------------


def _existing(*urns: str) -> mock.MagicMock:
    graph = mock.MagicMock()
    # get_entities drops entities that have none of the requested aspects, so the
    # response holding a urn is what makes it "exists".
    graph.get_entities.return_value = {
        urn: {"datasetKey": (mock.Mock(), None)} for urn in urns
    }
    return graph


def _unsupported() -> mock._patch:
    return mock.patch.object(UrnAliasResolver, "_aliases_supported", return_value=False)


def test_casing_probe_finds_a_lowercased_stored_urn() -> None:
    graph = _existing(_LOWER)

    with _unsupported():
        assert UrnAliasResolver(graph=graph).resolve(_UPPER) == _LOWER

    # The alias search is never issued: the server would answer it with zero hits.
    graph.get_urns_by_filter.assert_not_called()
    # No platform instance configured, so there is no third casing to guess — the same two
    # `resolve_table` tries.
    _, kwargs = graph.get_entities.call_args
    assert kwargs["urns"] == [_UPPER, _LOWER]


def test_casing_probe_cannot_find_an_uppercased_stored_urn() -> None:
    # The inherent limit of guessing: lowercasing a reference finds a lowercased entity,
    # but nothing derives `_UPPER` from `_LOWER`. Only the alias index resolves this.
    graph = _existing(_UPPER)

    with _unsupported():
        assert UrnAliasResolver(graph=graph).resolve(_LOWER) is None


def test_casing_probe_caches_a_negative_answer() -> None:
    graph = _existing()

    with _unsupported():
        resolver = UrnAliasResolver(graph=graph)
        assert resolver.resolve(_OTHER) is None
        assert resolver.resolve(_OTHER) is None

    assert graph.get_entities.call_count == 1


def test_casing_probe_failure_is_not_cached() -> None:
    graph = mock.MagicMock()
    graph.get_entities.side_effect = Exception("boom")

    with _unsupported():
        resolver = UrnAliasResolver(graph=graph)
        assert resolver.resolve(_LOWER) is None
        assert resolver.resolve(_LOWER) is None

    assert graph.get_entities.call_count == 2


def test_casing_probe_does_not_answer_for_a_casing_it_never_probed() -> None:
    # Both casings are real. Probing `_UPPER` never guesses `_MIXED`, so its answer must
    # not be reused for `_MIXED` — sharing one entry would resolve `_MIXED` to `_UPPER`.
    graph = _existing(_UPPER, _MIXED)

    with _unsupported():
        resolver = UrnAliasResolver(graph=graph)
        assert resolver.resolve(_UPPER) == _UPPER
        assert resolver.resolve(_MIXED) == _MIXED

    assert graph.get_entities.call_count == 2


def test_casing_probe_asks_about_a_shared_candidate_once() -> None:
    # `_LOWER` is a guess for `_UPPER` as well as its own only guess, and whether it exists
    # does not depend on who asked — so the second reference needs no query.
    graph = _existing(_LOWER)

    with _unsupported():
        resolver = UrnAliasResolver(graph=graph)
        assert resolver.resolve(_UPPER) == _LOWER
        assert resolver.resolve(_LOWER) == _LOWER

    assert graph.get_entities.call_count == 1


def test_casing_probe_negative_answer_covers_only_the_urn_probed() -> None:
    # Finding nothing records that this URN's guesses missed, not that the name is absent:
    # a stored casing the guesses never reached is still found when it is asked about.
    graph = _existing(_MIXED)

    with _unsupported():
        resolver = UrnAliasResolver(graph=graph)
        assert resolver.resolve(_UPPER) is None
        assert resolver.resolve(_MIXED) == _MIXED


def test_casing_probe_finds_an_instance_kept_stored_urn() -> None:
    # Neither the reference as written nor its fully lowercased form matches; only the
    # casing that preserves the configured platform instance does.
    graph = _existing(_INSTANCE_KEPT)

    with _unsupported():
        resolver = UrnAliasResolver(graph=graph, platform_instance=_PLATFORM_INSTANCE)
        assert resolver.resolve(_UPPER) == _INSTANCE_KEPT

    _, kwargs = graph.get_entities.call_args
    assert kwargs["urns"] == [_UPPER, _LOWER, _INSTANCE_KEPT]


def test_casing_probe_keeps_the_instance_however_the_reference_spells_it() -> None:
    # The configured instance is matched case-insensitively, and kept as the reference
    # wrote it — that is the casing DataHub was given.
    graph = _existing(_INSTANCE_KEPT)

    with _unsupported():
        resolver = UrnAliasResolver(
            graph=graph, platform_instance=_PLATFORM_INSTANCE.lower()
        )
        assert resolver.resolve(_UPPER) == _INSTANCE_KEPT


def test_cacheless_casing_probe_resolves_and_retains_nothing() -> None:
    graph = _existing(_LOWER)

    with _unsupported():
        resolver = UrnAliasResolver(graph=graph, cached=False)
        assert resolver.resolve(_UPPER) == _LOWER
        assert resolver.resolve(_UPPER) == _LOWER

    assert graph.get_entities.call_count == 2


def test_cacheless_casing_probe_ignores_a_non_dataset_reference() -> None:
    graph = _existing()

    with _unsupported():
        assert (
            UrnAliasResolver(graph=graph, cached=False).resolve("urn:li:corpuser:alice")
            is None
        )

    # There is no dataset casing to guess, so it must not be probed as a dataset.
    graph.get_entities.assert_not_called()


def test_casing_probe_ignores_an_instance_the_reference_does_not_carry() -> None:
    # The reference belongs to some other namespace, so there is no instance to preserve
    # and guessing one would only waste a slot in the batch.
    graph = _existing()

    with _unsupported():
        UrnAliasResolver(graph=graph, platform_instance="other_instance").resolve(
            _UPPER
        )

    _, kwargs = graph.get_entities.call_args
    assert kwargs["urns"] == [_UPPER, _LOWER]
