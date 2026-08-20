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
_DEV = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,DEV)"
_BIGQUERY = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_db.my_schema.events,PROD)"
# The same table under two platform instances. The instance is a prefix of the dataset
# name, so it is part of the URN and of every comparison made on one.
_IN_A = "urn:li:dataset:(urn:li:dataPlatform:snowflake,inst_a.my_db.events,PROD)"
_IN_B = "urn:li:dataset:(urn:li:dataPlatform:snowflake,inst_b.my_db.events,PROD)"
_IN_A_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,inst_a.MY_DB.EVENTS,PROD)"
_IN_B_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,inst_b.MY_DB.EVENTS,PROD)"


def _server(*stored: str, fails: bool = False) -> mock.MagicMock:
    """A graph holding `stored`, whose bulk scroll optionally dies part way through.

    The scroll honours the `platform_instance` filter, as DataHub's search does, so a
    narrowed read yields only its own slice rather than the whole store.
    """
    graph = mock.MagicMock()

    def scroll(**kwargs: object) -> object:
        instance = kwargs.get("platform_instance")
        for urn in stored:
            if instance and f",{instance}.".lower() not in urn.lower():
                continue
            yield urn
        if fails:
            raise RuntimeError("boom")

    graph.get_urns_by_filter.side_effect = scroll
    graph.get_dataset_urns_ignoring_case.side_effect = lambda key: [
        urn for urn in stored if lowercased_urn(urn) == key
    ]
    return graph


def _loaded(*urns: str) -> UrnAliasResolver:
    """A graph-less resolver holding `urns`, as a completed bulk load would."""
    resolver = UrnAliasResolver()
    for urn in urns:
        resolver.add(urn)
    return resolver


def _asked(graph: mock.MagicMock) -> List[str]:
    """The key each per-URN search asked about, in order."""
    return [
        call.args[0] for call in graph.get_dataset_urns_ignoring_case.call_args_list
    ]


def _load(graph: mock.MagicMock, instance: str = "") -> Optional[UrnAliasResolver]:
    """One bulk-loaded region, or None when its scroll did not finish."""
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


# --- a bulk load caches; the graph answers ---------------------------------------------


def test_a_bulk_loaded_hit_needs_no_question() -> None:
    graph = _server(_LOWER)
    resolver = _load(graph)
    assert resolver is not None

    assert resolver.resolve(_UPPER) == _LOWER
    graph.get_dataset_urns_ignoring_case.assert_not_called()


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
    graph.get_dataset_urns_ignoring_case.side_effect = Exception("search failed")
    resolver = UrnAliasResolver(graph)

    with pytest.raises(Exception, match="search failed"):
        resolver.resolve(_UPPER)

    graph.get_dataset_urns_ignoring_case.side_effect = None
    graph.get_dataset_urns_ignoring_case.return_value = [_LOWER]
    assert resolver.resolve(_UPPER) == _LOWER


def test_a_graph_less_resolver_answers_a_miss_with_nothing() -> None:
    # A bulk load covers one region, so its miss is not the last word — the caller asks.
    assert _loaded(_LOWER).find_match(_OTHER) == []


def test_the_graph_backed_resolver_is_shared_per_server() -> None:
    graph = mock.MagicMock()

    assert graph_urn_alias_resolver(graph) is graph_urn_alias_resolver(graph)


# --- the bulk load ---------------------------------------------------------------------


def test_a_load_that_fails_part_way_yields_no_resolver() -> None:
    # A partial row is a hit with an incomplete list of casings, which heals a reference to
    # the wrong entity.
    assert _load(_server(_LOWER, fails=True)) is None


def test_a_partial_load_cannot_answer_a_collision_wrongly() -> None:
    # Both casings exist but the scroll reached only the uppercase one. Kept, that row
    # heals a mixed-case reference to the uppercase entity and rewrites an exact uppercase
    # one — wrong table either way.
    graph = _server(_UPPER, _LOWER, fails=True)

    assert _load(graph) is None
    fetching = UrnAliasResolver(graph)
    assert fetching.resolve(_MIXED, prefer_lowercased=True) == _LOWER
    assert fetching.resolve(_UPPER, prefer_lowercased=True) == _UPPER


def test_one_resolver_is_shared_by_every_consumer_of_a_region() -> None:
    graph = _server(_LOWER)

    assert _load(graph) is _load(graph)
    assert graph.get_urns_by_filter.call_count == 1


def test_a_load_narrowed_to_an_instance_holds_that_instance_alone() -> None:
    # So its miss is not an absence: inst_b exists and this resolver never saw it.
    resolver = _load(_server(_IN_A, _IN_B), instance="inst_a")

    assert resolver is not None
    assert resolver.resolve(_IN_A_UPPER, prefer_lowercased=True) == _IN_A
    assert resolver.find_match(_IN_B_UPPER) == []


def test_an_unfiltered_load_holds_every_instance() -> None:
    resolver = _load(_server(_IN_A, _IN_B))

    assert resolver is not None
    assert resolver.resolve(_IN_B_UPPER, prefer_lowercased=True) == _IN_B


def test_an_empty_instance_filtered_load_still_loads_and_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The instance filter matches the dataPlatformInstance aspect a connector may never
    # emit. Every reference then misses and is asked, which is correct but worth a log.
    resolver = _load(_server(), instance="inst_a")

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


def test_a_server_whose_version_does_not_parse_disables_the_feature() -> None:
    # A build off a git SHA reports an unparseable version. The gate is read outside its
    # caller's try/except, so raising here would abort the run rather than turn the feature
    # off.
    assert maintains_dataset_aliases(_versioned_server("a1b2c3d")) is False
