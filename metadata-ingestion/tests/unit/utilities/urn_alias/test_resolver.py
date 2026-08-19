from typing import List
from unittest import mock

from datahub.utilities.urn_alias.index import CatalogSlice, UrnAliasIndex
from datahub.utilities.urn_alias.resolver import UrnAliasResolver

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Db.My_Schema.Events,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
_REDSHIFT = "urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.my_schema.events,PROD)"

_SNOWFLAKE_PROD = CatalogSlice(platform="snowflake", platform_instance=None, env="PROD")


def _loaded(*urns: str) -> UrnAliasResolver:
    """A resolver over a completed load of `urns`, not allowed to ask the server."""
    index = UrnAliasIndex(_SNOWFLAKE_PROD)
    for urn in urns:
        index.add(urn)
    return UrnAliasResolver(mock.MagicMock(), [index])


def _server(*stored: str) -> mock.MagicMock:
    """A graph answering every search with `stored`, recording what it was asked."""
    graph = mock.MagicMock()
    graph.get_urns_by_filter.return_value = list(stored)
    return graph


def _asked(graph: mock.MagicMock) -> List[List[str]]:
    """The keys each search asked about, in order."""
    return [
        call.kwargs["extra_or_filters"][0]["and"][0]["values"]
        for call in graph.get_urns_by_filter.call_args_list
    ]


def _asking(*stored: str) -> UrnAliasResolver:
    """A resolver with nothing loaded, allowed to ask the server."""
    return UrnAliasResolver(_server(*stored), query_on_demand=True)


# --- choosing a URN out of the matches ---------------------------------------------


def test_resolve_returns_the_stored_urn_for_a_different_casing() -> None:
    assert _loaded(_LOWER).resolve(_UPPER) == _LOWER


def test_resolve_returns_an_exact_match_even_when_another_casing_exists() -> None:
    # An exact hit is never ambiguous: the reference names a real entity, so it stands
    # regardless of other casings of the same name.
    assert _loaded(_LOWER, _UPPER).resolve(_UPPER) == _UPPER


def test_resolve_returns_none_when_casings_collide() -> None:
    assert _loaded(_LOWER, _UPPER).resolve(_MIXED) is None


def test_resolve_returns_none_when_nothing_matches() -> None:
    assert _loaded(_LOWER).resolve(_OTHER) is None


def test_resolve_prefers_the_lowercased_urn_on_a_collision() -> None:
    assert _loaded(_LOWER, _UPPER).resolve(_MIXED, prefer_lowercased=True) == _LOWER


def test_resolve_prefers_lowercased_still_declines_when_none_is_lowercased() -> None:
    # Both stored casings are non-lowercase, so the preference has nothing to pick.
    assert _loaded(_MIXED, _UPPER).resolve(_LOWER, prefer_lowercased=True) is None


def test_resolve_prefers_lowercased_does_not_override_an_exact_match() -> None:
    assert _loaded(_LOWER, _UPPER).resolve(_UPPER, prefer_lowercased=True) == _UPPER


def test_find_match_is_empty_when_nothing_matches() -> None:
    assert _loaded(_LOWER).find_match(_OTHER) == []


def test_with_nothing_loaded_and_nothing_to_ask_a_urn_is_unresolved() -> None:
    assert _loaded().resolve(_LOWER) is None


# --- when the server is asked --------------------------------------------------------


def test_a_miss_inside_a_loaded_slice_is_answered_without_asking() -> None:
    # A miss here is a fact, not a gap, so it must not cost a round trip.
    graph = _server()
    resolver = UrnAliasResolver(
        graph, [UrnAliasIndex(_SNOWFLAKE_PROD)], query_on_demand=True
    )

    assert resolver.resolve(_OTHER) is None
    assert _asked(graph) == []


def test_a_miss_outside_every_loaded_slice_is_asked_about() -> None:
    graph = _server(_LOWER)
    resolver = UrnAliasResolver(graph, query_on_demand=True)

    assert resolver.resolve(_UPPER) == _LOWER
    # Asked under the key, not the reference's own casing — one key covers every casing.
    assert _asked(graph) == [[_LOWER]]


def test_prefetch_asks_only_about_references_no_loaded_slice_answers() -> None:
    graph = _server()
    resolver = UrnAliasResolver(
        graph, [UrnAliasIndex(_SNOWFLAKE_PROD)], query_on_demand=True
    )

    resolver.prefetch([_UPPER, _REDSHIFT])

    # Nothing here has ever looked at redshift, so that one is still a question.
    assert _asked(graph) == [[_REDSHIFT]]


def test_references_sharing_a_key_are_one_question() -> None:
    graph = _server(_LOWER)
    resolver = UrnAliasResolver(graph, query_on_demand=True)

    resolver.prefetch([_UPPER, _MIXED, _LOWER])

    assert _asked(graph) == [[_LOWER]]


def test_an_answer_from_a_query_does_not_make_the_next_miss_look_settled() -> None:
    # Per-URN answers land in the scratch index, which covers nothing.
    graph = _server(_LOWER)
    resolver = UrnAliasResolver(graph, query_on_demand=True)

    assert resolver.resolve(_UPPER) == _LOWER
    assert resolver.covered(_OTHER) is False

    resolver.resolve(_OTHER)
    assert _asked(graph) == [[_LOWER], [_OTHER]]


def test_a_recorded_absence_is_not_asked_about_twice() -> None:
    graph = _server()
    resolver = UrnAliasResolver(graph, query_on_demand=True)

    assert resolver.resolve(_UPPER) is None
    assert resolver.resolve(_MIXED) is None

    assert _asked(graph) == [[_LOWER]]


def test_matches_are_read_across_every_loaded_index() -> None:
    snowflake = UrnAliasIndex(_SNOWFLAKE_PROD)
    snowflake.add(_LOWER)
    redshift = UrnAliasIndex(
        CatalogSlice(platform="redshift", platform_instance=None, env="PROD")
    )
    redshift.add(_REDSHIFT)
    resolver = UrnAliasResolver(mock.MagicMock(), [snowflake, redshift])

    assert resolver.resolve(_UPPER) == _LOWER
    assert resolver.find_match(_REDSHIFT) == [_REDSHIFT]
