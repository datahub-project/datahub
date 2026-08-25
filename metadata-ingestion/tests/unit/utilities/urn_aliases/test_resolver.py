from typing import List
from unittest import mock

import pytest

from datahub.metadata.schema_classes import (
    DataHubUpgradeResultClass,
    DataHubUpgradeStateClass,
)
from datahub.utilities.urn_aliases.provider import (
    graph_urn_alias_resolver,
    provide_urn_alias_resolver,
)
from datahub.utilities.urn_aliases.resolver import (
    UrnAliasResolver,
    dataset_aliases_backfilled,
    lowercased_urn,
)

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


def _load(graph: mock.MagicMock, instance: str = "") -> UrnAliasResolver:
    """One bulk-loaded region. Raises when its scroll did not finish."""
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


# --- the matches under a key -----------------------------------------------------------


def test_every_casing_of_a_name_answers_under_one_key() -> None:
    # Two datasets differing only by case can both exist, and a caller has to see the
    # ambiguity rather than be handed an arbitrary winner.
    assert _loaded(_LOWER, _UPPER).find_match(_MIXED) == [_LOWER, _UPPER]


def test_the_same_urn_is_stored_once() -> None:
    assert _loaded(_LOWER, _LOWER).find_match(_UPPER) == [_LOWER]


def test_a_stored_urn_matches_a_different_casing_of_it() -> None:
    assert _loaded(_LOWER).find_match(_UPPER) == [_LOWER]


# --- a bulk load caches; the graph answers ---------------------------------------------


def test_a_bulk_loaded_hit_needs_no_question() -> None:
    graph = _server(_LOWER)
    resolver = _load(graph)

    assert resolver.find_match(_UPPER) == [_LOWER]
    graph.get_dataset_urns_ignoring_case.assert_not_called()


def test_a_graph_backed_resolver_asks_under_the_key() -> None:
    graph = _server(_LOWER)

    assert UrnAliasResolver(graph).find_match(_UPPER) == [_LOWER]
    # Asked under the key, not the reference's own casing.
    assert _asked(graph) == [_LOWER]


def test_references_sharing_a_key_are_one_question() -> None:
    graph = _server(_LOWER)
    resolver = UrnAliasResolver(graph)

    resolver.find_match(_UPPER)
    resolver.find_match(_MIXED)

    assert _asked(graph) == [_LOWER]


def test_an_absence_is_recorded_so_it_is_asked_once() -> None:
    graph = _server()
    resolver = UrnAliasResolver(graph)

    assert resolver.find_match(_UPPER) == []
    assert resolver.find_match(_MIXED) == []

    assert _asked(graph) == [_LOWER]


def test_a_failed_search_records_nothing() -> None:
    # A transient failure recorded as "known absent" would decline every later reference
    # to a real entity for the rest of the run.
    graph = mock.MagicMock()
    graph.get_dataset_urns_ignoring_case.side_effect = Exception("search failed")
    resolver = UrnAliasResolver(graph)

    with pytest.raises(Exception, match="search failed"):
        resolver.find_match(_UPPER)

    graph.get_dataset_urns_ignoring_case.side_effect = None
    graph.get_dataset_urns_ignoring_case.return_value = [_LOWER]
    assert resolver.find_match(_UPPER) == [_LOWER]


def test_a_graph_less_resolver_answers_a_miss_with_nothing() -> None:
    # A bulk load covers one region, so its miss is not the last word — the caller asks.
    assert _loaded(_LOWER).find_match(_OTHER) == []


def test_the_graph_backed_resolver_is_shared_per_server() -> None:
    graph = mock.MagicMock()

    assert graph_urn_alias_resolver(graph) is graph_urn_alias_resolver(graph)


# --- the bulk load ---------------------------------------------------------------------


def test_a_load_that_fails_part_way_yields_no_resolver() -> None:
    # A partial row is a hit with an incomplete list of casings, which heals a reference to
    # the wrong entity. Raised rather than swallowed, so the cause reaches the report.
    with pytest.raises(RuntimeError):
        _load(_server(_LOWER, fails=True))


def test_a_partial_load_cannot_answer_a_collision_wrongly() -> None:
    # Both casings exist but the scroll reached only the uppercase one. Kept, that row
    # would offer the uppercase entity as the only match for the name, and a caller would
    # heal a mixed-case reference to it. Discarded, the graph search still sees both.
    graph = _server(_UPPER, _LOWER, fails=True)

    with pytest.raises(RuntimeError):
        _load(graph)
    assert UrnAliasResolver(graph).find_match(_MIXED) == [_UPPER, _LOWER]


def test_one_resolver_is_shared_by_every_consumer_of_a_region() -> None:
    graph = _server(_LOWER)

    assert _load(graph) is _load(graph)
    assert graph.get_urns_by_filter.call_count == 1


def test_a_load_narrowed_to_an_instance_holds_that_instance_alone() -> None:
    # So its miss is not an absence: inst_b exists and this resolver never saw it.
    resolver = _load(_server(_IN_A, _IN_B), instance="inst_a")

    assert resolver.find_match(_IN_A_UPPER) == [_IN_A]
    assert resolver.find_match(_IN_B_UPPER) == []


def test_an_unfiltered_load_holds_every_instance() -> None:
    resolver = _load(_server(_IN_A, _IN_B))

    assert resolver.find_match(_IN_B_UPPER) == [_IN_B]


def test_an_empty_instance_filtered_load_still_loads() -> None:
    # The instance filter matches the dataPlatformInstance aspect a connector may never
    # emit. An empty load is a load, not a failure; the caller reports the emptiness.
    resolver = _load(_server(), instance="inst_a")

    assert resolver.urn_count() == 0


# --- the gate on the whole feature -----------------------------------------------------


def _marked_server(marker: object) -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.get_aspect.return_value = marker
    return graph


def _result(state: str) -> DataHubUpgradeResultClass:
    return DataHubUpgradeResultClass(timestampMs=0, state=state)


@pytest.mark.parametrize(
    ("state", "backfilled"),
    [
        (DataHubUpgradeStateClass.SUCCEEDED, True),
        (DataHubUpgradeStateClass.IN_PROGRESS, False),
        (DataHubUpgradeStateClass.FAILED, False),
        (DataHubUpgradeStateClass.ABORTED, False),
    ],
)
def test_only_a_succeeded_backfill_opens_the_gate(state: str, backfilled: bool) -> None:
    graph = _marked_server(_result(state))
    assert dataset_aliases_backfilled(graph) is backfilled
    # Read off the upgrade entity the backfill stamps, not off anything inferred.
    assert (
        graph.get_aspect.call_args.args[0] == "urn:li:dataHubUpgrade:dataset-aliases-v1"
    )


def test_a_backfill_that_never_ran_keeps_the_gate_shut() -> None:
    # GMS answers 404 for the marker, which get_aspect reports as None.
    assert dataset_aliases_backfilled(_marked_server(None)) is False


def test_a_failed_read_is_left_to_the_caller() -> None:
    # Not a verdict on the backfill, so it is not turned into one here.
    graph = mock.MagicMock()
    graph.get_aspect.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        dataset_aliases_backfilled(graph)
