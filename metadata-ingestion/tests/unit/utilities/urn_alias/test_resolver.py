from typing import List
from unittest import mock

from datahub.utilities.urn_alias.index import (
    CatalogSlice,
    UrnAliasIndex,
    lowercased_urn,
)
from datahub.utilities.urn_alias.remote import AliasIndexLookup
from datahub.utilities.urn_alias.resolver import UrnAliasResolver

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Db.My_Schema.Events,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
_REDSHIFT = "urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.my_schema.events,PROD)"

_SNOWFLAKE_PROD = CatalogSlice(platform="snowflake", platform_instance=None, env="PROD")


def _loaded(*urns: str) -> UrnAliasResolver:
    """A resolver over a bulk load of `urns`, not allowed to ask the server."""
    index = UrnAliasIndex()
    resolver = UrnAliasResolver(index, AliasIndexLookup(index, mock.MagicMock()))
    for urn in urns:
        resolver.add(urn)
    return resolver


class _RecordingLookup:
    """A lookup that records what it was asked, and answers from a fixed catalog."""

    def __init__(self, index: UrnAliasIndex, *stored: str) -> None:
        self._index = index
        self._stored = stored
        self.asked: List[List[str]] = []

    def add(self, urn: str) -> None:
        key = lowercased_urn(urn)
        if key is not None:
            self._index.add(key, urn)

    def prefetch(self, urns: List[str]) -> None:
        self.asked.append(list(urns))
        for urn in self._stored:
            self.add(urn)

    def matches(self, urn: str) -> List[str]:
        key = lowercased_urn(urn)
        if key is None:
            return []
        return self._index.get(key) or []


def _asking(*stored: str) -> UrnAliasResolver:
    index = UrnAliasIndex()
    return UrnAliasResolver(
        index, _RecordingLookup(index, *stored), query_on_demand=True
    )


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
    # The point of recording coverage: a miss here is a fact, not a gap, so it must not
    # cost a round trip.
    index = UrnAliasIndex()
    lookup = _RecordingLookup(index)
    resolver = UrnAliasResolver(index, lookup, query_on_demand=True)
    resolver.record_slice_loaded(_SNOWFLAKE_PROD)

    assert resolver.resolve(_OTHER) is None
    assert lookup.asked == []


def test_a_miss_outside_every_loaded_slice_is_asked_about() -> None:
    index = UrnAliasIndex()
    lookup = _RecordingLookup(index, _LOWER)
    resolver = UrnAliasResolver(index, lookup, query_on_demand=True)

    assert resolver.resolve(_UPPER) == _LOWER
    assert lookup.asked == [[_UPPER]]


def test_prefetch_asks_only_about_references_no_loaded_slice_answers() -> None:
    index = UrnAliasIndex()
    lookup = _RecordingLookup(index)
    resolver = UrnAliasResolver(index, lookup, query_on_demand=True)
    resolver.record_slice_loaded(_SNOWFLAKE_PROD)

    resolver.prefetch([_UPPER, _REDSHIFT])

    # Nothing here has ever looked at redshift, so that one is still a question.
    assert lookup.asked == [[_REDSHIFT]]


def test_coverage_is_recorded_once_per_slice() -> None:
    resolver = _asking()

    resolver.record_slice_loaded(_SNOWFLAKE_PROD)
    resolver.record_slice_loaded(_SNOWFLAKE_PROD)

    assert resolver._index.loaded_slices == [_SNOWFLAKE_PROD]


# --- confining an answer to slices the caller loaded --------------------------------


_OTHER_INSTANCE = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,other_inst.my_db.events,PROD)"
)
_OTHER_INSTANCE_UPPER = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,OTHER_INST.MY_DB.EVENTS,PROD)"
)


def test_within_declines_a_match_outside_the_callers_slices() -> None:
    # The index is shared, so it holds entities other consumers loaded. A caller that
    # also needs the entity's columns must not be handed one it never fetched.
    resolver = _loaded(_OTHER_INSTANCE)

    mine = [CatalogSlice(platform="snowflake", platform_instance="my_inst", env="PROD")]

    assert resolver.resolve(_OTHER_INSTANCE_UPPER) == _OTHER_INSTANCE
    assert resolver.resolve(_OTHER_INSTANCE_UPPER, within=mine) is None


def test_within_accepts_a_match_inside_the_callers_slices() -> None:
    assert _loaded(_LOWER).resolve(_UPPER, within=[_SNOWFLAKE_PROD]) == _LOWER


def test_within_does_not_disturb_collision_handling() -> None:
    resolver = _loaded(_LOWER, _UPPER)

    assert (
        resolver.resolve(_MIXED, prefer_lowercased=True, within=[_SNOWFLAKE_PROD])
        == _LOWER
    )
