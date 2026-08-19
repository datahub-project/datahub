from typing import List, Optional
from unittest import mock

import pytest

from datahub.utilities.dataset_aliases.provider import (
    graph_urn_alias_resolver,
    provide_urn_alias_resolver,
)
from datahub.utilities.dataset_aliases.resolver import (
    UrnAliasResolver,
    lowercased_urn,
    maintains_dataset_aliases,
)
from datahub.utilities.server_config_util import RestServiceConfig

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Db.My_Schema.Events,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"


def _loaded(*urns: str) -> UrnAliasResolver:
    """A resolver over a completed bulk load, which answers its own misses."""
    resolver = UrnAliasResolver()
    for urn in urns:
        resolver.add(urn)
    return resolver


def _server(*stored: str, fails: bool = False) -> mock.MagicMock:
    """A graph holding `stored`, whose bulk scroll optionally dies part way through."""
    graph = mock.MagicMock()

    def scroll(**kwargs: object) -> object:
        yield from stored
        if fails:
            raise RuntimeError("boom")

    graph.get_urns_by_filter.side_effect = scroll
    graph.get_dataset_urns_by_lowercased_urn.side_effect = lambda key: [
        urn for urn in stored if lowercased_urn(urn) == key
    ]
    return graph


def _asked(graph: mock.MagicMock) -> List[str]:
    """The key each per-URN search asked about, in order."""
    return [
        call.args[0] for call in graph.get_dataset_urns_by_lowercased_urn.call_args_list
    ]


def _load(graph: mock.MagicMock, instance: str = "") -> Optional[UrnAliasResolver]:
    return provide_urn_alias_resolver(
        graph=graph,
        platform="snowflake",
        platform_instance=instance or None,
        env="PROD",
    )


# --- the key ---------------------------------------------------------------------------


def test_only_the_dataset_name_is_lowercased() -> None:
    # Platform and env keep their casing; GMS lowercases the name alone.
    assert lowercased_urn(_MIXED) == _LOWER


def test_a_non_dataset_urn_has_no_key() -> None:
    assert lowercased_urn("urn:li:corpuser:alice") is None


def test_a_non_dataset_urn_is_neither_stored_nor_matched() -> None:
    assert _loaded("urn:li:corpuser:alice").find_match("urn:li:corpuser:alice") == []


# --- choosing a URN out of the matches -------------------------------------------------


def test_every_casing_of_a_name_answers_under_one_key() -> None:
    # Two datasets differing only by case can both exist, and a caller has to see the
    # ambiguity rather than be handed an arbitrary winner.
    assert _loaded(_LOWER, _UPPER).find_match(_MIXED) == [_LOWER, _UPPER]


def test_the_same_urn_is_stored_once() -> None:
    assert _loaded(_LOWER, _LOWER).find_match(_UPPER) == [_LOWER]


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


# --- bulk-loaded answers its own misses; graph-backed asks -----------------------------


def test_a_bulk_loaded_resolver_never_asks() -> None:
    # Its load ran to completion, so a miss is a fact rather than a gap.
    graph = _server(_LOWER)
    resolver = _load(graph)
    assert resolver is not None
    graph.get_urns_by_filter.reset_mock()

    assert resolver.resolve(_UPPER) == _LOWER
    assert resolver.resolve(_OTHER) is None
    graph.get_dataset_urns_by_lowercased_urn.assert_not_called()


def test_a_graph_backed_resolver_asks_under_the_key() -> None:
    graph = _server(_LOWER)

    assert UrnAliasResolver(graph).resolve(_UPPER) == _LOWER
    # Asked under the key, not the reference's own casing.
    assert _asked(graph) == [_LOWER]


def test_references_sharing_a_key_are_one_question() -> None:
    graph = _server(_LOWER)
    resolver = UrnAliasResolver(graph)

    resolver.resolve(_UPPER)
    resolver.resolve(_MIXED)

    assert _asked(graph) == [_LOWER]


def test_an_absence_is_recorded_so_it_is_asked_once() -> None:
    graph = _server()
    resolver = UrnAliasResolver(graph)

    assert resolver.resolve(_UPPER) is None
    assert resolver.resolve(_MIXED) is None

    assert _asked(graph) == [_LOWER]


def test_a_failed_search_records_nothing() -> None:
    # A transient failure recorded as "known absent" would decline every later reference
    # to a real entity for the rest of the run.
    graph = mock.MagicMock()
    graph.get_dataset_urns_by_lowercased_urn.side_effect = Exception("search failed")
    resolver = UrnAliasResolver(graph)

    with pytest.raises(Exception, match="search failed"):
        resolver.resolve(_UPPER)

    graph.get_dataset_urns_by_lowercased_urn.side_effect = None
    graph.get_dataset_urns_by_lowercased_urn.return_value = [_LOWER]
    assert resolver.resolve(_UPPER) == _LOWER


def test_the_graph_backed_resolver_is_shared_per_server() -> None:
    graph = mock.MagicMock()

    assert graph_urn_alias_resolver(graph) is graph_urn_alias_resolver(graph)


# --- the bulk load ---------------------------------------------------------------------


def test_a_load_that_fails_part_way_yields_no_resolver() -> None:
    # Its rows are real, but their keys would claim to hold every casing of the name, and
    # a partial answer resolves a later reference to the wrong entity.
    assert _load(_server(_LOWER, fails=True)) is None


def test_one_resolver_is_shared_by_every_consumer_of_a_region() -> None:
    graph = _server(_LOWER)

    assert _load(graph) is _load(graph)
    assert graph.get_urns_by_filter.call_count == 1


def test_an_empty_region_still_loads_and_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The known hole while platform_instance is still a scroll filter — see
    # docs/dev_guides/lineage_urn_casing.md.
    resolver = _load(_server(), instance="prod_wh")

    assert resolver is not None
    assert any("Loaded 0 URNs" in r.message for r in caplog.records)


# --- the gate on the whole feature -----------------------------------------------------


def _versioned_server(version: str, cloud: bool = False) -> mock.MagicMock:
    """A graph reporting itself as DataHub `version`."""
    graph = mock.MagicMock()
    graph.server_config = RestServiceConfig(
        raw_config={
            "versions": {"acryldata/datahub": {"version": version}},
            "datahub": {"serverEnv": "prod" if cloud else ""},
        }
    )
    return graph


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
    assert maintains_dataset_aliases(_versioned_server(version, cloud)) is supported


def test_a_server_that_reports_no_version_is_not_assumed_new() -> None:
    graph = mock.MagicMock()
    graph.server_config = RestServiceConfig(raw_config={})
    assert maintains_dataset_aliases(graph) is False
