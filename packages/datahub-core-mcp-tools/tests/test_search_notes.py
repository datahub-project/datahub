"""The notes search adds to an empty result: what each part matches on its own."""

from typing import Any, Optional, cast
from unittest.mock import MagicMock

import pytest

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.read_backend import JsonObject, ReadBackendBinding
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.search import (
    _top_level_and_clauses,
    explain_zero_results,
    search,
)
from datahub_core_mcp_tools.view_preference import NoView

PLATFORM = ("platform.keyword", ("urn:li:dataPlatform:snowflake",))
DATASET = ("_entityType", ("DATASET",))

# (query, the positive filter rules sent) -> total
Totals = dict[tuple[str, frozenset], int]


def _positive_rules(variables: dict[str, Any]) -> frozenset:
    rules = {
        (rule["field"], tuple(rule["values"]))
        for clause in variables.get("orFilters") or []
        for rule in clause["and"]
        if not rule.get("negated")
    }
    rules |= {("_entityType", (t,)) for t in variables.get("types") or []}
    return frozenset(rules)


class CountingBackend:
    """A ReadBackend that answers each search with a total looked up in ``totals``."""

    def __init__(self, totals: Totals, fail_after: Optional[int] = None) -> None:
        self.totals = totals
        self.fail_after = fail_after
        self.sent: list[dict[str, Any]] = []

    def exists(self, urn: str) -> bool:
        return True

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        return None

    def entity(self, urn: str, *, include_contract: bool = False) -> None:
        return None

    def related_documents(self, urn: str, start: int, count: int) -> dict:
        return {}

    def search_documents(self, variables: JsonObject, *, semantic: bool) -> JsonObject:
        return {}

    def search_entities(self, variables: JsonObject) -> JsonObject:
        if self.fail_after is not None and len(self.sent) >= self.fail_after:
            raise RuntimeError("Failed to execute search")
        self.sent.append(dict(variables))
        key = (str(variables["query"]), _positive_rules(variables))
        total = self.totals.get(key, 0)
        return cast(JsonObject, {"total": total, "searchResults": [], "facets": []})


def _search(backend: CountingBackend, query: str, filter: Optional[str] = None) -> dict:
    context = ToolContext([NoView(), ReadBackendBinding(backend)])
    with with_datahub_client(MagicMock(), tool_context=context):
        return search(query=query, filter=filter)


def test_words_that_never_share_a_field_are_explained() -> None:
    backend = CountingBackend(
        {("/q sales", frozenset()): 5, ("/q customer_id", frozenset()): 9}
    )
    response = _search(backend, "/q sales+customer_id")
    assert response["search_notes"] == [
        "Each word matches on its own (sales: 5, customer_id: 9), but never all"
        " together in one field."
    ]
    # Count queries skip the facets; the search itself doesn't.
    flags = [v["searchFlags"].get("skipAggregates") for v in backend.sent]
    assert flags[0] is None and all(flags[1:])


def test_the_filter_condition_that_matches_nothing_is_named() -> None:
    backend = CountingBackend(
        {("/q orders", frozenset()): 12, ("*", frozenset({PLATFORM})): 100}
    )
    response = _search(
        backend,
        "/q orders",
        "platform = snowflake AND container = urn:li:container:abc",
    )
    (note,) = response["search_notes"]
    assert note.startswith("12 results match without the filter")
    assert note.endswith(
        "Matches nothing on its own: `container = urn:li:container:abc`."
    )


def test_filter_conditions_that_all_match_are_not_blamed() -> None:
    backend = CountingBackend(
        {
            ("/q cards", frozenset()): 42,
            ("*", frozenset({DATASET})): 400,
            ("*", frozenset({PLATFORM})): 900,
        }
    )
    response = _search(
        backend, "/q cards", "entity_type = dataset AND platform = snowflake"
    )
    assert response["search_notes"] == [
        "42 results match without the filter, so the filter removes all of them."
    ]


def test_a_filter_only_search_names_the_empty_condition() -> None:
    backend = CountingBackend({("*", frozenset({DATASET})): 50})
    response = _search(backend, "*", "entity_type = dataset AND platform = snowflake")
    assert response["search_notes"] == [
        "Matches nothing on its own: `platform = snowflake`."
    ]


def test_the_short_word_note_comes_before_the_empty_result_note() -> None:
    response = _search(CountingBackend({}), "/q os+version")
    assert response["search_notes"][0].startswith("No results for /q os+version.")
    assert response["search_notes"][1] == "Matches nothing on its own: version."


def test_searches_with_results_or_past_the_first_page_run_no_extra_queries() -> None:
    backend = CountingBackend({("/q orders", frozenset()): 3})
    assert "search_notes" not in _search(backend, "/q orders")
    assert len(backend.sent) == 1

    backend = CountingBackend({})
    context = ToolContext([NoView(), ReadBackendBinding(backend)])
    with with_datahub_client(MagicMock(), tool_context=context):
        assert "search_notes" not in search(query="/q sales+customer_id", offset=10)
    assert len(backend.sent) == 1


def test_a_failing_explanation_keeps_the_empty_result() -> None:
    response = _search(CountingBackend({}, fail_after=1), "/q sales+customer_id")
    assert response["total"] == 0
    (note,) = response["search_notes"]
    assert note.startswith("Couldn't check why the search is empty:")


def test_a_failing_retry_is_not_explained() -> None:
    # The rewritten query was never measured, so nothing explains it.
    backend = CountingBackend({}, fail_after=1)
    (note,) = _search(backend, "/q os+version")["search_notes"]
    assert note.startswith("No results for /q os+version. Couldn't search again")


def _explain(
    query: str, totals: dict[str, int], filter: Optional[str] = None
) -> tuple[list[str], list[str]]:
    sent: list[str] = []

    def count(
        query: str, filter: Optional[str], num_results: int, **kwargs: object
    ) -> dict:
        sent.append(query)
        return {"total": totals.get(query, 0)}

    return explain_zero_results(query, filter, search_fn=count), sent


@pytest.mark.parametrize(
    ("query", "totals", "expected"),
    [
        (
            "/q alpha beta gamma delta missing",
            {"/q alpha": 1, "/q beta": 1, "/q gamma": 1, "/q delta": 1},
            "Each of these words matches on its own: alpha: 1, beta: 1, gamma: 1,"
            " delta: 1. 1 more word wasn't checked.",
        ),
        (
            '/q "os version"',
            {"/q os version": 7},
            "Nothing matches the quoted phrase exactly; without the quotes, 7 results"
            " match.",
        ),
    ],
)
def test_word_notes_conclude_only_what_the_counts_prove(
    query: str, totals: dict[str, int], expected: str
) -> None:
    assert _explain(query, totals)[0] == [expected]


def test_word_notes_say_when_the_counts_include_the_filter() -> None:
    notes, _ = _explain(
        "/q sales+customer_id", {"/q sales": 5}, filter="platform = snowflake"
    )
    assert notes == ["Matches nothing on its own with the filter: customer_id."]


@pytest.mark.parametrize(
    "query",
    [
        "/q alpha OR beta",
        "/q sales+finance AND -archived",
        '/q orders "NOT" version',
        '/q "absent!"',
        "/q os+++missing",
    ],
)
def test_queries_outside_the_simple_subset_get_no_word_checks(query: str) -> None:
    assert _explain(query, {}) == ([], [])


def test_filter_only_notes_conclude_only_when_every_condition_was_checked() -> None:
    filter = " AND ".join(f"platform = p{i}" for i in range(5))
    assert _explain("*", {"*": 1}, filter)[0] == [
        "Each of these filter conditions matches on its own: `platform = p0`: 1,"
        " `platform = p1`: 1, `platform = p2`: 1, `platform = p3`: 1."
        " 1 more condition wasn't checked."
    ]


@pytest.mark.parametrize(
    ("filter", "expected"),
    [
        (
            "tag = 'urn:li:tag:Sales AND Marketing' AND platform = snowflake",
            ["tag = 'urn:li:tag:Sales AND Marketing'", "platform = snowflake"],
        ),
        ('x = "a \\" AND b" AND y = 1', ['x = "a \\" AND b"', "y = 1"]),
        ("(a = 1 OR b = 2) AND c = 3", ["(a = 1 OR b = 2)", "c = 3"]),
        (
            "domain WITHIN urn:li:domain:a and NOT env = DEV",
            ["domain WITHIN urn:li:domain:a", "NOT env = DEV"],
        ),
        ("tag = AND AND platform = x", ["tag = AND AND platform = x"]),
        # A top-level OR: not a list of conditions an entity must all match.
        ("a = 1 OR b = 2 AND c = 3", ["a = 1 OR b = 2 AND c = 3"]),
    ],
)
def test_filter_conditions_follow_the_filter_parser(
    filter: str, expected: list[str]
) -> None:
    assert _top_level_and_clauses(filter) == expected
