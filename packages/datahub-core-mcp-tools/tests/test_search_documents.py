"""search_documents: what it sends to DataHub and what it returns."""

from typing import Any, Optional, Sequence, cast
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.openapi_client import RawVersionedBatch, VersionedAspectPair
from datahub_core_mcp_tools.read_backend import JsonObject, ReadBackendBinding
from datahub_core_mcp_tools.result_policy import (
    ANNOTATION_KEY,
    DEFAULT_POLICY_DOCUMENT_URN,
    ResultPolicyBinding,
)
from datahub_core_mcp_tools.search_flags import SearchFlagsOverride
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.documents import (
    SearchResponse,
    _looks_like_natural_language,
    _merge_search_results,
    search_documents,
)
from datahub_core_mcp_tools.view_preference import CustomView, NoView

# 2026-05-20T13:45:00Z
MAY_20_MS = 1779284700000


def _doc(urn: str, **extra: Any) -> dict:
    return {"entity": {"urn": urn, "info": {"title": urn.rsplit(":", 1)[-1]}}, **extra}


class FakeBackend:
    """A ReadBackend whose document searches return fixed keyword/semantic hits."""

    def __init__(
        self,
        keyword: Optional[list[dict]] = None,
        semantic: Optional[list[dict]] = None,
        semantic_error: Optional[Exception] = None,
    ) -> None:
        self.keyword = keyword or []
        self.semantic = semantic or []
        self.semantic_error = semantic_error
        # Typed loosely so tests can index into what was sent.
        self.sent: list[tuple[bool, dict[str, Any]]] = []

    def exists(self, urn: str) -> bool:
        return True

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        return None

    def entity(self, urn: str, *, include_contract: bool = False) -> None:
        return None

    def related_documents(self, urn: str, start: int, count: int) -> dict:
        return {}

    def search_entities(self, variables: JsonObject) -> JsonObject:
        return {}

    def document_contents(self, urns: list[str]) -> list[JsonObject | None]:
        return []

    def dataset_usage(self, urn: str, time_range: str) -> JsonObject | None:
        return None

    def aspect_versions(self, pairs: list[VersionedAspectPair]) -> RawVersionedBatch:
        return RawVersionedBatch(aspects={}, errors={}, http_calls=0)

    def glossary_term_version(self, urn: str) -> JsonObject | None:
        return None

    def version_set_history(
        self, urn: str, start: int, count: int
    ) -> JsonObject | None:
        return None

    def glossary_terms(self, urns: list[str]) -> list[JsonObject | None]:
        return []

    def tags_named(self, name: str, count: int) -> list[JsonObject]:
        return []

    def ownership_types_named(self, name: str, count: int) -> list[JsonObject]:
        return []

    def domains_named(
        self, name: str, parent_domain: str | None, count: int
    ) -> list[JsonObject]:
        return []

    def list_queries(self, variables: JsonObject) -> JsonObject:
        return {}

    def lineage_paths(self, variables: JsonObject) -> JsonObject:
        return {}

    def search_lineage(self, variables: JsonObject) -> JsonObject:
        return {}

    def search_documents(self, variables: JsonObject, *, semantic: bool) -> JsonObject:
        self.sent.append((semantic, dict(variables)))
        if semantic and self.semantic_error is not None:
            raise self.semantic_error
        hits = self.semantic if semantic else self.keyword
        count = cast(int, variables["count"])
        return cast(
            JsonObject,
            {
                "start": 0,
                "count": min(count, len(hits)),
                "total": len(hits),
                "searchResults": hits[:count],
                "facets": [{"field": "platform", "aggregations": []}],
            },
        )

    def variables(self, semantic: bool = False) -> dict[str, Any]:
        return next(v for s, v in self.sent if s == semantic)


def _run(backend: FakeBackend, *items: object, **kwargs: Any) -> dict:
    context = ToolContext([NoView(), ReadBackendBinding(backend), *items])
    with with_datahub_client(MagicMock(), tool_context=context):
        return search_documents(**kwargs)


def _rules(variables: dict[str, Any], field: str) -> list[dict]:
    return [
        rule
        for clause in variables["orFilters"]
        for rule in clause["and"]
        if rule["field"] == field
    ]


@pytest.fixture
def semantic_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEMANTIC_SEARCH_ENABLED", "true")


def test_keyword_search_returns_metadata_with_dates() -> None:
    hit = _doc("urn:li:document:runbook")
    hit["entity"]["info"]["lastModified"] = {"time": MAY_20_MS}
    backend = FakeBackend(keyword=[hit])

    result = _run(backend, query="/q deployment")

    semantic, variables = backend.sent[0]
    assert not semantic and len(backend.sent) == 1
    assert variables["query"] == "/q deployment"
    assert variables["start"] == 0
    assert variables["viewUrn"] is None
    assert variables["searchFlags"] == {
        "skipHighlighting": True,
        "maxAggValues": 10,
        "skipAggregates": True,
    }
    assert result["searchResults"][0]["entity"]["info"] == {
        "title": "runbook",
        "lastModified": "2026-05-20",
    }


def test_facets_are_requested_only_for_discovery() -> None:
    backend = FakeBackend(keyword=[_doc("urn:li:document:a")])

    _run(backend, query="*")
    facets_only = _run(backend, query="/q deployment", num_results=0)

    assert all("skipAggregates" not in v["searchFlags"] for _, v in backend.sent)
    assert "searchResults" not in facets_only
    assert facets_only["facets"]


def test_page_size_is_capped() -> None:
    backend = FakeBackend()

    _run(backend, query="/q runbook", num_results=500, offset=20)

    assert backend.variables()["count"] == 50
    assert backend.variables()["start"] == 20


def test_filters_compile_and_exclude_system_and_bridge_documents() -> None:
    backend = FakeBackend()

    _run(backend, query="*", filter="platform = notion OR platform = confluence")

    variables = backend.variables()
    assert len(variables["orFilters"]) == 2
    # Every OR branch carries the default exclusions, not only the last one.
    for clause in variables["orFilters"]:
        fields = {
            (rule["field"], tuple(rule["values"]), rule.get("negated", False))
            for rule in clause["and"]
        }
        assert ("sourceType", ("SYSTEM",), True) in fields
        assert ("typeNames", ("Bridge Document",), True) in fields
    platforms = {rule["values"][0] for rule in _rules(variables, "platform.keyword")}
    assert platforms == {
        "urn:li:dataPlatform:notion",
        "urn:li:dataPlatform:confluence",
    }


def test_a_filter_naming_system_or_bridge_documents_opts_in() -> None:
    backend = FakeBackend()

    _run(backend, query="*", filter="sourceType = SYSTEM")
    _run(backend, query="*", filter='subtype = "Bridge Document"')

    system_rules = _rules(backend.sent[0][1], "sourceType")
    assert [r.get("negated", False) for r in system_rules] == [False]
    bridge_rules = [
        r
        for r in _rules(backend.sent[1][1], "typeNames")
        if r["values"] == ["Bridge Document"]
    ]
    assert [r.get("negated", False) for r in bridge_rules] == [False]


def test_invalid_filter_is_a_tool_error() -> None:
    with pytest.raises(ToolError, match="Invalid filter"):
        _run(FakeBackend(), query="*", filter="platform = (notion")


def test_view_and_search_flag_overrides_apply() -> None:
    backend = FakeBackend()

    context = ToolContext(
        [
            CustomView(urn="urn:li:dataHubView:mine"),
            ReadBackendBinding(backend),
            SearchFlagsOverride({"skipCache": True}),
        ]
    )
    with with_datahub_client(MagicMock(), tool_context=context):
        search_documents(query="/q runbook")

    variables = backend.variables()
    assert variables["viewUrn"] == "urn:li:dataHubView:mine"
    assert variables["searchFlags"]["skipCache"] is True
    assert variables["searchFlags"]["skipHighlighting"] is True


def test_semantic_leg_is_off_by_default() -> None:
    backend = FakeBackend(keyword=[_doc("urn:li:document:a")])

    _run(backend, query="how do I deploy", semantic_query="deploying to production")

    assert [semantic for semantic, _ in backend.sent] == [False]


@pytest.mark.usefixtures("semantic_on")
def test_hybrid_search_merges_and_dedupes() -> None:
    backend = FakeBackend(
        keyword=[
            _doc(
                "urn:li:document:both",
                matchedFields=[{"name": "title", "value": "deploy"}],
            ),
            _doc("urn:li:document:kw"),
        ],
        semantic=[
            _doc("urn:li:document:sem"),
            _doc(
                "urn:li:document:both",
                matchedFields=[{"name": "chunk", "value": "kubectl apply"}],
            ),
        ],
    )

    result = _run(backend, query="/q deploy", semantic_query="how to deploy")

    assert [(r["entity"]["urn"], r["searchType"]) for r in result["searchResults"]] == [
        ("urn:li:document:both", "both"),
        ("urn:li:document:sem", "semantic"),
        ("urn:li:document:kw", "keyword"),
    ]
    assert result["searchResults"][0]["matchedFields"] == [
        {"name": "title", "value": "deploy"},
        {"name": "chunk", "value": "kubectl apply"},
    ]
    assert backend.variables(semantic=True)["query"] == "how to deploy"
    assert "start" not in backend.variables(semantic=True)
    assert result["total"] == 3


@pytest.mark.usefixtures("semantic_on")
def test_hybrid_pages_over_the_merged_pool() -> None:
    backend = FakeBackend(
        keyword=[_doc(f"urn:li:document:kw{i}") for i in range(5)],
        semantic=[_doc(f"urn:li:document:sem{i}") for i in range(5)],
    )

    result = _run(
        backend, query="/q deploy", semantic_query="deploying", num_results=3, offset=3
    )

    # Both legs fetch offset + num_results from the start, then the merge is sliced.
    for _, variables in backend.sent:
        assert variables["count"] == 6
        assert variables.get("start", 0) == 0
    assert result["start"] == 3
    assert result["count"] == 3
    assert [r["entity"]["urn"] for r in result["searchResults"]] == [
        "urn:li:document:sem1",
        "urn:li:document:kw2",
        "urn:li:document:sem2",
    ]


@pytest.mark.usefixtures("semantic_on")
def test_failing_semantic_leg_falls_back_to_keyword() -> None:
    backend = FakeBackend(
        keyword=[_doc("urn:li:document:a")],
        semantic_error=ToolError("Semantic search is disabled in this environment"),
    )

    result = _run(backend, query="/q deploy", semantic_query="how to deploy")

    assert [r["searchType"] for r in result["searchResults"]] == ["keyword"]


@pytest.mark.usefixtures("semantic_on")
def test_natural_language_query_runs_the_semantic_leg_too() -> None:
    backend = FakeBackend()

    _run(backend, query="how do I deploy to production")
    _run(backend, query="/q deploy+production")

    assert [semantic for semantic, _ in backend.sent] == [False, True, False]
    assert backend.variables(semantic=True)["query"] == "how do I deploy to production"


@pytest.mark.usefixtures("semantic_on")
@pytest.mark.parametrize("query", ["*", " ", ""])
def test_match_all_cannot_anchor_a_hybrid_search(query: str) -> None:
    backend = FakeBackend()

    with pytest.raises(ToolError, match="match-all"):
        _run(backend, query=query, semantic_query="how to deploy")

    assert backend.sent == []


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("how do I deploy to production", True),
        ("revenue recognition", True),
        ("deployment", False),
        ("/q deploy OR release", False),
        ("urn:li:document:abc def", False),
        ("*", False),
    ],
)
def test_looks_like_natural_language(query: str, expected: bool) -> None:
    assert _looks_like_natural_language(query) is expected


def test_empty_semantic_results_keep_keyword_results() -> None:
    keyword: SearchResponse = {"searchResults": [_doc("urn:li:document:a")]}  # type: ignore[list-item]

    merged = _merge_search_results(keyword, {"searchResults": [], "total": 0})

    assert [r["searchType"] for r in merged["searchResults"]] == ["keyword"]


def test_results_are_trust_labeled_by_the_host_policy() -> None:
    class Decisions:
        def annotation(self, urn: str, *, explain: bool = False) -> dict:
            return {"grade": "authored"}

        def header(self, urns: Sequence[str]) -> dict:
            return {"count": len(urns)}

        def is_configuration(self, urn: str) -> bool:
            return False

    class Policy:
        def decide(self, urns: Sequence[str]) -> Decisions:
            return Decisions()

    backend = FakeBackend(
        keyword=[_doc("urn:li:document:a"), _doc(DEFAULT_POLICY_DOCUMENT_URN)]
    )

    result = _run(backend, ResultPolicyBinding(Policy()), query="/q a")

    assert [r["entity"]["urn"] for r in result["searchResults"]] == [
        "urn:li:document:a"
    ]
    assert result["searchResults"][0][ANNOTATION_KEY] == {"grade": "authored"}


def test_docs_explain_within() -> None:
    doc = search_documents.__doc__ or ""
    assert "parent_document WITHIN" in doc
    assert "$DOCUMENT_FILTER_DOCS" not in doc


@patch("datahub_core_mcp_tools.graphql_helpers.execute_graphql")
async def test_registered_tool_requires_a_query_and_reads_graphql(
    execute: MagicMock,
) -> None:
    execute.return_value = {
        "searchAcrossEntities": {
            "total": 1,
            "searchResults": [_doc("urn:li:document:a")],
        }
    }
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    with with_datahub_client(MagicMock(), tool_context=ToolContext([NoView()])):
        async with Client(mcp) as client:
            tool = next(
                t for t in await client.list_tools() if t.name == "search_documents"
            )
            result = await client.call_tool(
                "search_documents", {"query": "/q runbook", "limit": 5}
            )

    assert tool.input_schema["required"] == ["query"]
    assert tool.annotations is not None and tool.annotations.read_only_hint is True
    assert execute.call_args.kwargs["operation_name"] == "documentSearch"
    assert execute.call_args.kwargs["variables"]["count"] == 5
    assert result.structured_content is not None
    (hit,) = result.structured_content["searchResults"]
    assert hit["entity"]["urn"] == "urn:li:document:a"
