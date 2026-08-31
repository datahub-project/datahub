"""Unit tests for the bookkeeping in m_query/shared_expressions.py.

These cover the parts that need no parse tree. `SharedExpressions.parse` is an
injected callable, so the cache, the failure classification and the precedence
between stop reasons can be driven with a stub -- no V8 bridge, and no
hand-written NodeIdMap standing in for what the real parser emits. Walking an
actual parse tree is covered by tests/integration/powerbi/test_m_parser.py
against real M text.

The precedence rules and the re-record-on-cache-hit behaviour are here because
both have been wrong: a route-dependent stop masked a real parse failure, and a
cached failure warned only the first table that paid for the parse.
"""

from typing import Callable, Dict, Optional

import pytest

from datahub.ingestion.source.powerbi.m_query._bridge import (
    MQueryBridgeError,
    MQueryParseError,
)
from datahub.ingestion.source.powerbi.m_query.ast_utils import NodeIdMap
from datahub.ingestion.source.powerbi.m_query.shared_expressions import (
    MAX_REFERENCE_DEPTH,
    PARAMETER_QUERY_MARKER,
    ExpressionCache,
    SharedExpressions,
    StopReason,
)
from datahub.utilities.threading_timeout import TimeoutException

_A_LET = "let x = 1 in x"


def _parsed_ok(text: str) -> NodeIdMap:
    return {0: {"kind": "LetExpression"}}


def _shared(
    texts: Optional[Dict[str, str]] = None,
    parse: Optional[Callable[[str], NodeIdMap]] = None,
    cache: Optional[ExpressionCache] = None,
) -> SharedExpressions:
    return SharedExpressions(
        texts=texts or {},
        parse=parse or _parsed_ok,
        cache=cache if cache is not None else ExpressionCache(),
    )


# --- the enum's own shape, which the precedence and counters read -------------


def test_no_stop_reason_is_an_alias_of_another() -> None:
    """Two members declared with the same value make the second an alias.

    An alias is absent from iteration but present in __members__, and the code
    reads reasons by member, so the duplicate would silently never be reported
    under its own title.
    """
    assert len(StopReason) == len(StopReason.__members__)


def test_only_cycle_and_depth_are_route_dependent() -> None:
    """`stopped` lets a real failure replace one of these and not the reverse."""
    assert {r.name for r in StopReason if r.route_dependent} == {"CYCLE", "TOO_DEEP"}


def test_only_parse_bridge_and_timeout_count_as_failures() -> None:
    """`is_failure` picks which of the two report counters is charged."""
    assert {r.name for r in StopReason if r.is_failure} == {
        "PARSE_ERROR",
        "BRIDGE_ERROR",
        "TIMEOUT",
    }


# --- precedence between reasons ----------------------------------------------


def test_a_real_failure_replaces_a_route_dependent_stop() -> None:
    """Hitting the cap down one branch says nothing about the query itself."""
    shared = _shared()
    shared.stopped("q", StopReason.TOO_DEEP, "a -> b -> q")
    shared.stopped("q", StopReason.PARSE_ERROR, "unexpected token")

    assert shared.stops["q"] == (StopReason.PARSE_ERROR, "unexpected token")


def test_a_route_dependent_stop_does_not_overwrite_a_real_failure() -> None:
    shared = _shared()
    shared.stopped("q", StopReason.PARSE_ERROR, "unexpected token")
    shared.stopped("q", StopReason.CYCLE, "q -> a -> q")

    assert shared.stops["q"] == (StopReason.PARSE_ERROR, "unexpected token")


def test_one_route_dependent_stop_does_not_replace_another() -> None:
    shared = _shared()
    shared.stopped("q", StopReason.CYCLE, "first")
    shared.stopped("q", StopReason.TOO_DEEP, "second")

    assert shared.stops["q"] == (StopReason.CYCLE, "first")


def test_the_first_reason_that_is_a_property_of_the_text_stands() -> None:
    """Not route-dependent means identical however the walk arrives."""
    shared = _shared()
    shared.stopped("q", StopReason.NO_LET)
    shared.stopped("q", StopReason.PARSE_ERROR, "later")

    assert shared.stops["q"][0] is StopReason.NO_LET


def test_a_query_stopped_under_two_casings_is_one_entry() -> None:
    shared = _shared()
    shared.stopped("Base Rows", StopReason.NO_LET)
    shared.stopped("base rows", StopReason.NO_LET)

    assert len(shared.stops) == 1


# --- lookup ------------------------------------------------------------------


def test_lookup_resolves_a_quoted_reference_whatever_its_case() -> None:
    shared = _shared(texts={"base rows": _A_LET})

    assert shared.lookup('#"Base Rows"') == _A_LET


def test_a_parameter_query_is_not_followed() -> None:
    """Following one reaches a literal, never a data source."""
    shared = _shared(
        texts={"Server Hostname": f'"a-host" meta [{PARAMETER_QUERY_MARKER}]'}
    )

    assert shared.lookup("Server Hostname") is None


def test_a_name_this_dataset_does_not_define_is_not_ours_to_explain() -> None:
    shared = _shared(texts={"a": _A_LET})

    assert shared.lookup("b") is None


# --- the cache ---------------------------------------------------------------


def test_a_query_is_parsed_once_however_many_routes_reach_it() -> None:
    calls = []

    def parse(text: str) -> NodeIdMap:
        calls.append(text)
        return _parsed_ok(text)

    cache = ExpressionCache()
    one_route = _shared(parse=parse, cache=cache)
    another_route = _shared(parse=parse, cache=cache)

    one_route.parsed("q", _A_LET)
    another_route.parsed("q", _A_LET)

    assert len(calls) == 1


def test_a_failure_is_recorded_again_on_every_walk_that_hits_the_cache() -> None:
    """The parse is worth caching; the warning is not.

    Otherwise only the first table to pay for the parse learns its lineage is
    short, and the rest are silently missing an upstream.
    """

    def parse(text: str) -> NodeIdMap:
        raise MQueryParseError("unexpected token")

    cache = ExpressionCache()
    first_table = _shared(parse=parse, cache=cache)
    assert first_table.parsed("q", "let (((") is None
    assert first_table.stops["q"][0] is StopReason.PARSE_ERROR

    second_table = _shared(parse=parse, cache=cache)
    assert second_table.parsed("q", "let (((") is None
    assert second_table.stops["q"][0] is StopReason.PARSE_ERROR


def test_a_failed_query_is_not_retried_per_route() -> None:
    """With a timeout, each retry would cost the full timeout again."""
    calls = []

    def parse(text: str) -> NodeIdMap:
        calls.append(text)
        raise TimeoutException("timed out")

    cache = ExpressionCache()
    _shared(parse=parse, cache=cache).parsed("q", _A_LET)
    _shared(parse=parse, cache=cache).parsed("q", _A_LET)

    assert len(calls) == 1


@pytest.mark.parametrize(
    "error,expected",
    [
        (MQueryParseError("unexpected token"), StopReason.PARSE_ERROR),
        (MQueryBridgeError("v8 context gone"), StopReason.BRIDGE_ERROR),
        (TimeoutException("timed out"), StopReason.TIMEOUT),
    ],
    ids=["parse", "bridge", "timeout"],
)
def test_each_known_parse_failure_maps_to_its_own_reason(
    error: Exception, expected: StopReason
) -> None:
    def parse(text: str) -> NodeIdMap:
        raise error

    shared = _shared(parse=parse)

    assert shared.parsed("q", _A_LET) is None
    assert shared.stops["q"][0] is expected


def test_an_unexpected_error_is_left_to_propagate() -> None:
    """Anything else is a defect rather than bad input, and must not be
    reported as a query the operator should go and fix."""

    def parse(text: str) -> NodeIdMap:
        raise ValueError("a defect")

    with pytest.raises(ValueError):
        _shared(parse=parse).parsed("q", _A_LET)


# --- the reference chain -----------------------------------------------------


def test_two_siblings_reaching_one_query_is_not_a_cycle() -> None:
    """A diamond is a legitimate model shape, unlike a repeat within one path."""
    shared = _shared()

    assert not shared.entered("rows kept").would_repeat("base rows")
    assert not shared.entered("rows dropped").would_repeat("base rows")


def test_a_repeat_within_one_chain_is_a_cycle_whatever_its_case() -> None:
    shared = _shared().entered("a").entered("b")

    assert shared.would_repeat("A")


def test_the_chain_is_exhausted_only_at_the_cap() -> None:
    shared = _shared()
    for hop in range(MAX_REFERENCE_DEPTH):
        assert not shared.exhausted()
        shared = shared.entered(f"q{hop}")

    assert shared.exhausted()


def test_entering_a_query_keeps_the_caches_shared() -> None:
    """The chain forks per path; the cache must not fork with it."""
    cache = ExpressionCache()
    shared = _shared(cache=cache)

    assert shared.entered("q").cache is cache
