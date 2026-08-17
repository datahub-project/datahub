from typing import List

from datahub.utilities.urn_alias.index import CatalogSlice, UrnAliasIndex
from datahub.utilities.urn_alias.resolver import UrnAliasResolver

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Db.My_Schema.Events,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"

_SNOWFLAKE_PROD = CatalogSlice(platform="snowflake", platform_instance=None, env="PROD")


class _RecordingRemote:
    """A remote that records what it was asked and answers from a fixed catalog."""

    def __init__(self, *stored: str) -> None:
        self._stored = stored
        self.asked: List[List[str]] = []

    def fetch(self, urns: List[str]) -> None:
        self.asked.append(list(urns))


class _AnsweringRemote(_RecordingRemote):
    def __init__(self, index: UrnAliasIndex, *stored: str) -> None:
        super().__init__(*stored)
        self._index = index

    def fetch(self, urns: List[str]) -> None:
        super().fetch(urns)
        for urn in self._stored:
            self._index.add(urn)


def _loaded(*urns: str) -> UrnAliasResolver:
    index = UrnAliasIndex()
    for urn in urns:
        index.add(urn)
    return UrnAliasResolver(index)


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


def test_find_match_flattens_unknown_and_absent_to_empty() -> None:
    # Callers choosing a URN get one "no match" answer and never interpret None.
    assert _loaded(_LOWER).find_match(_OTHER) == []


# --- when the remote is consulted ---------------------------------------------------


def test_with_no_remote_an_unknown_urn_is_simply_unresolved() -> None:
    assert UrnAliasResolver(UrnAliasIndex()).resolve(_LOWER) is None


def test_a_stored_match_is_answered_without_asking() -> None:
    index = UrnAliasIndex()
    index.add(_LOWER)
    remote = _RecordingRemote()

    assert UrnAliasResolver(index, remote).resolve(_UPPER) == _LOWER
    assert remote.asked == []


def test_a_miss_inside_a_loaded_slice_is_answered_without_asking() -> None:
    # The point of recording coverage: a miss here is a fact, not a gap, so it must not
    # cost a round trip.
    index = UrnAliasIndex()
    index.record_slice_loaded(_SNOWFLAKE_PROD)
    remote = _RecordingRemote()

    assert UrnAliasResolver(index, remote).resolve(_OTHER) is None
    assert remote.asked == []


def test_a_miss_outside_every_loaded_slice_is_asked_about() -> None:
    index = UrnAliasIndex()
    remote = _AnsweringRemote(index, _LOWER)

    assert UrnAliasResolver(index, remote).resolve(_UPPER) == _LOWER
    assert remote.asked == [[_UPPER]]


def test_prefetch_asks_only_about_references_still_unknown() -> None:
    index = UrnAliasIndex()
    index.add(_LOWER)
    remote = _RecordingRemote()

    UrnAliasResolver(index, remote).prefetch([_UPPER, _OTHER])

    assert remote.asked == [[_OTHER]]


def test_prefetch_asks_nothing_when_the_index_answers_everything() -> None:
    index = UrnAliasIndex()
    index.add(_LOWER)
    index.record_slice_loaded(_SNOWFLAKE_PROD)
    remote = _RecordingRemote()

    UrnAliasResolver(index, remote).prefetch([_UPPER, _OTHER])

    assert remote.asked == []


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
    index = UrnAliasIndex()
    index.add(_OTHER_INSTANCE)
    resolver = UrnAliasResolver(index)

    mine = [CatalogSlice(platform="snowflake", platform_instance="my_inst", env="PROD")]

    assert resolver.resolve(_OTHER_INSTANCE_UPPER) == _OTHER_INSTANCE
    assert resolver.resolve(_OTHER_INSTANCE_UPPER, within=mine) is None


def test_within_accepts_a_match_inside_the_callers_slices() -> None:
    index = UrnAliasIndex()
    index.add(_LOWER)
    resolver = UrnAliasResolver(index)

    assert resolver.resolve(_UPPER, within=[_SNOWFLAKE_PROD]) == _LOWER


def test_within_does_not_disturb_collision_handling() -> None:
    index = UrnAliasIndex()
    index.add(_LOWER)
    index.add(_UPPER)
    resolver = UrnAliasResolver(index)

    assert (
        resolver.resolve(_MIXED, prefer_lowercased=True, within=[_SNOWFLAKE_PROD])
        == _LOWER
    )
