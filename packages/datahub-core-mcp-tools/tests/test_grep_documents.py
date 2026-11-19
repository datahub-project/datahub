"""grep_documents: regex excerpts from document bodies, within one output budget."""

from typing import Any, Optional, Sequence, cast
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import (
    GREP_OUTPUT_CHAR_BUDGET,
    with_datahub_client,
)
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.read_backend import JsonObject, ReadBackendBinding
from datahub_core_mcp_tools.result_policy import (
    ANNOTATION_KEY,
    DEFAULT_POLICY_DOCUMENT_URN,
    ResultPolicyBinding,
)
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.documents import clamp_repetitions, grep_documents

DOC1 = "urn:li:document:doc1"
DOC2 = "urn:li:document:doc2"
DEPLOY_GUIDE = (
    "This guide explains how to deploy applications to production. "
    "First, ensure you have kubectl installed. Then run kubectl apply -f "
    "deployment.yaml. After deployment, verify the pods are running with "
    "kubectl get pods."
)
TROUBLESHOOTING = (
    "Common errors and solutions. Error: Connection refused - check network. "
    "Error: Timeout - increase timeout value. Warning: Deprecated API - update client."
)


class FakeBackend:
    """A ReadBackend serving fixed document bodies; records what was read."""

    def __init__(self, texts: dict[str, str], *, stubs: Sequence[str] = ()) -> None:
        self.texts = texts
        self.stubs = stubs
        self.reads: list[list[str]] = []

    def exists(self, urn: str) -> bool:
        return urn in self.texts

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        return None

    def entity(self, urn: str, *, include_contract: bool = False) -> None:
        return None

    def related_documents(self, urn: str, start: int, count: int) -> dict:
        return {}

    def search_entities(self, variables: JsonObject) -> JsonObject:
        return {}

    def search_documents(self, variables: JsonObject, *, semantic: bool) -> JsonObject:
        return {}

    def document_contents(self, urns: list[str]) -> list[JsonObject | None]:
        self.reads.append(list(urns))
        found: list[JsonObject | None] = [
            {
                "urn": urn,
                "info": {"title": "Doc", "contents": {"text": self.texts[urn]}},
            }
            for urn in urns
            if urn in self.texts
        ]
        return found + [None for urn in urns if urn in self.stubs]

    def search_lineage(self, variables: JsonObject) -> JsonObject:
        return {}


def _grep(backend: FakeBackend, *items: object, **kwargs: Any) -> dict:
    context = ToolContext([ReadBackendBinding(backend), *items])
    with with_datahub_client(MagicMock(), tool_context=context):
        return grep_documents(**kwargs)


def _shown_text(doc: dict) -> str:
    return "".join(
        m["excerpt"].removeprefix("...").removesuffix("...") for m in doc["matches"]
    )


def test_finds_excerpts_with_absolute_positions() -> None:
    backend = FakeBackend({DOC1: DEPLOY_GUIDE, DOC2: TROUBLESHOOTING})

    result = _grep(backend, urns=[DOC1, DOC2], pattern="kubectl", context_chars=10)

    assert result["documents_with_matches"] == 1
    assert result["total_matches"] == 3
    (doc,) = result["results"]
    assert doc["urn"] == DOC1
    assert doc["title"] == "Doc"
    first = doc["matches"][0]
    assert first["position"] == DEPLOY_GUIDE.index("kubectl")
    assert first["excerpt"].startswith("...") and "kubectl" in first["excerpt"]
    assert "next_offset" not in doc


def test_case_insensitive_alternation_counts_every_match() -> None:
    backend = FakeBackend({DOC2: TROUBLESHOOTING})

    result = _grep(backend, urns=[DOC2], pattern="(?i)error:|warning:")

    assert result["total_matches"] == 3


def test_max_matches_per_doc_limits_excerpts_and_says_where_to_continue() -> None:
    backend = FakeBackend({DOC1: "word " * 10})

    result = _grep(
        backend, urns=[DOC1], pattern="word", context_chars=0, max_matches_per_doc=3
    )

    (doc,) = result["results"]
    assert doc["total_matches"] == 10
    assert len(doc["matches"]) == 3
    assert doc["next_offset"] == 14


def test_start_offset_skips_the_beginning_and_reports_the_length() -> None:
    text = "A" * 100 + "MATCH" + "B" * 100 + "MATCH"
    backend = FakeBackend({DOC1: text, DOC2: "short MATCH"})

    result = _grep(
        backend, urns=[DOC1, DOC2], pattern="MATCH", context_chars=5, start_offset=150
    )

    # DOC2 is shorter than the offset, so it is skipped.
    (doc,) = result["results"]
    assert doc["urn"] == DOC1
    assert [m["position"] for m in doc["matches"]] == [205]
    assert doc["content_length"] == len(text)


def test_bad_and_unknown_urns_are_skipped_not_fatal() -> None:
    backend = FakeBackend({DOC1: DEPLOY_GUIDE}, stubs=["urn:li:document:deleted"])

    result = _grep(
        backend,
        urns=[
            DOC1,
            "deployment guide",
            "urn:li:document:missing",
            "urn:li:document:deleted",
        ],
        pattern="kubectl",
    )

    assert result["total_matches"] == 3
    skipped = {entry["urn"]: entry["error"] for entry in result["skipped_urns"]}
    assert set(skipped) == {
        "deployment guide",
        "urn:li:document:missing",
        "urn:li:document:deleted",
    }
    assert "search_documents" in skipped["deployment guide"]
    assert skipped["urn:li:document:missing"] == "Document not found or inaccessible."
    assert backend.reads == [
        [DOC1, "urn:li:document:missing", "urn:li:document:deleted"]
    ]


def test_no_usable_urns_is_a_tool_error() -> None:
    with pytest.raises(ToolError, match="none of the 2 values"):
        _grep(FakeBackend({}), urns=["runbook", ""], pattern="x")


def test_empty_urns_read_nothing() -> None:
    backend = FakeBackend({})

    result = _grep(backend, urns=[], pattern="x")

    assert result == {"results": [], "total_matches": 0, "documents_with_matches": 0}
    assert backend.reads == []


@pytest.mark.parametrize("pattern", ["[invalid", "a{2000}("])
def test_a_pattern_that_cannot_compile_is_a_tool_error(pattern: str) -> None:
    backend = FakeBackend({DOC1: DEPLOY_GUIDE})

    with pytest.raises(ToolError) as error:
        _grep(backend, urns=[DOC1], pattern=pattern)

    message = str(error.value)
    assert "Invalid regex pattern" in message
    assert "context_chars" in message
    # RE2 reports the reason as bytes; it should reach the agent as text.
    assert "b'" not in message
    assert backend.reads == []


def test_an_oversized_repetition_is_clamped_with_a_note() -> None:
    backend = FakeBackend({DOC1: "kubectl " + "x" * 50})

    result = _grep(backend, urns=[DOC1], pattern="kubectl.{0,3000}")

    assert result["total_matches"] == 1
    assert "context_chars" in result["pattern_note"]


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("a{0,3000}", "a{0,1000}"),
        ("a{5000}", "a{1000}"),
        ("a{2000,}", "a{1000,}"),
        ("a{2,5}", "a{2,5}"),
    ],
)
def test_clamp_repetitions(pattern: str, expected: str) -> None:
    assert clamp_repetitions(pattern) == expected


def test_reading_a_short_document_returns_it_once() -> None:
    text = "\n".join(f"Line {i}: some guidance text." for i in range(20))

    result = _grep(
        FakeBackend({DOC1: text}), urns=[DOC1], pattern=".*", context_chars=3000
    )

    (doc,) = result["results"]
    # Every line matches, but the first window already holds the whole body.
    assert doc["total_matches"] >= 20
    assert [m["excerpt"] for m in doc["matches"]] == [text]
    assert "next_offset" not in doc


def test_reading_returns_consecutive_windows_and_where_to_continue() -> None:
    text = "\n".join(f"Line {i:03d}: some guidance text." for i in range(200))
    backend = FakeBackend({DOC1: text})

    first = _grep(
        backend, urns=[DOC1], pattern=".*", context_chars=500, max_matches_per_doc=3
    )["results"][0]
    rest = _grep(
        backend,
        urns=[DOC1],
        pattern=".*",
        context_chars=500,
        start_offset=first["next_offset"],
    )["results"][0]

    # Three excerpts that continue one another, with no text repeated.
    assert len(first["matches"]) == 3
    assert _shown_text(first) == text[: first["next_offset"]]
    assert text[first["next_offset"] :].startswith(
        rest["matches"][0]["excerpt"].removeprefix("...").removesuffix("...")
    )


def test_a_body_without_newlines_pages_within_the_budget() -> None:
    # Text extracted from HTML or PDF often has no newlines, so ".*" matches the
    # whole body at once.
    text = "lorem ipsum dolor sit amet " * (GREP_OUTPUT_CHAR_BUDGET // 10)
    backend = FakeBackend({DOC1: text})

    pages = []
    offset = 0
    while True:
        result = _grep(
            backend, urns=[DOC1], pattern=".*", context_chars=8000, start_offset=offset
        )
        doc = result["results"][0]
        pages.append(_shown_text(doc))
        assert len(pages[-1]) <= GREP_OUTPUT_CHAR_BUDGET
        if "next_offset" not in doc:
            assert "truncated" not in result
            break
        assert result["truncated"] is True
        assert doc["matches"][-1]["truncated"] is True
        assert doc["content_length"] == len(text)
        offset = doc["next_offset"]

    assert len(pages) == 3
    # Consecutive pages continue each other exactly: no gaps, no repeats.
    assert "".join(pages) == text


@pytest.mark.parametrize("pattern", [r"(?i)start[\s\S]*end", r"(?is)start.*end"])
def test_a_match_spanning_many_lines_is_cut_at_the_budget(pattern: str) -> None:
    body = "\n".join(
        f"line {i}: some text" for i in range(GREP_OUTPUT_CHAR_BUDGET // 10)
    )
    text = f"start\n{body}\nend"

    result = _grep(FakeBackend({DOC1: text}), urns=[DOC1], pattern=pattern)

    doc = result["results"][0]
    assert result["truncated"] is True
    assert [m["truncated"] for m in doc["matches"]] == [True]
    assert _shown_text(doc) == text[: doc["next_offset"]]
    assert doc["next_offset"] == GREP_OUTPUT_CHAR_BUDGET


def test_documents_past_the_budget_report_where_to_continue() -> None:
    size = GREP_OUTPUT_CHAR_BUDGET * 3 // 5
    texts = {
        f"urn:li:document:doc{i}": f"doc{i} " + "x" * (size - 5) for i in (1, 2, 3)
    }
    backend = FakeBackend(texts)

    result = _grep(backend, urns=list(texts), pattern=".*", context_chars=0)

    assert result["truncated"] is True
    assert result["documents_with_matches"] == 3
    doc1, doc2, doc3 = result["results"]
    # The first document fits, the second is cut where the budget ends, and the
    # third is listed with no excerpts and the offset to start it from.
    assert _shown_text(doc1) == texts[doc1["urn"]]
    assert "next_offset" not in doc1
    cut = GREP_OUTPUT_CHAR_BUDGET - size
    assert _shown_text(doc2) == texts[doc2["urn"]][:cut]
    assert doc2["next_offset"] == cut
    assert doc3["matches"] == []
    assert doc3["next_offset"] == 0

    rest = _grep(
        backend, urns=[doc2["urn"]], pattern=".*", context_chars=0, start_offset=cut
    )
    assert _shown_text(rest["results"][0]) == texts[doc2["urn"]][cut:]


def test_reading_markdown_stays_under_the_budget() -> None:
    # A typical ".*" read of a long markdown document returns max_matches_per_doc
    # windows of about context_chars each. The budget must leave these alone.
    text = "\n".join(
        f"- item {i}: a line of markdown guidance text" for i in range(5000)
    )

    result = _grep(
        FakeBackend({DOC1: text}), urns=[DOC1], pattern=".*", context_chars=8000
    )

    doc = result["results"][0]
    assert "truncated" not in result
    assert len(doc["matches"]) == 5
    assert not any(m.get("truncated") for m in doc["matches"])
    assert _shown_text(doc) == text[: doc["next_offset"]]


class _Decisions:
    def annotation(self, urn: str, *, explain: bool = False) -> Optional[dict]:
        return {"grade": "certified"} if urn == DOC1 else None

    def header(self, urns: Sequence[str]) -> dict:
        return {"count": len(urns)}

    def is_configuration(self, urn: str) -> bool:
        return False


class _Policy:
    def decide(self, urns: Sequence[str]) -> _Decisions:
        return _Decisions()


def test_configuration_documents_are_never_read_and_results_are_labeled() -> None:
    backend = FakeBackend({DOC1: DEPLOY_GUIDE, DEFAULT_POLICY_DOCUMENT_URN: "kubectl"})

    result = _grep(
        backend,
        ResultPolicyBinding(_Policy()),
        urns=[DOC1, DEFAULT_POLICY_DOCUMENT_URN],
        pattern="kubectl",
    )

    assert backend.reads == [[DOC1]]
    assert [entry["urn"] for entry in result["skipped_urns"]] == [
        DEFAULT_POLICY_DOCUMENT_URN
    ]
    assert result["results"][0][ANNOTATION_KEY] == {"grade": "certified"}
    assert result["trust_policy"] == {"count": 1}


@patch("datahub_core_mcp_tools.graphql_helpers.execute_graphql")
async def test_registered_tool_reads_graphql_and_reports_bad_patterns(
    execute: MagicMock,
) -> None:
    # entities(urns:) returns a hard-deleted document as a stub with info null.
    execute.return_value = {
        "entities": [
            {
                "urn": DOC1,
                "info": {"title": "Guide", "contents": {"text": DEPLOY_GUIDE}},
            },
            {"urn": DOC2, "info": None},
        ]
    }
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    with with_datahub_client(MagicMock()):
        async with Client(mcp) as client:
            found = await client.call_tool(
                "grep_documents",
                {"documentUrns": f'["{DOC1}", "{DOC2}"]', "pattern": "kubectl"},
            )
            bad = await client.call_tool(
                "grep_documents",
                {"urns": [DOC1], "pattern": "[invalid"},
                raise_on_error=False,
            )

    assert execute.call_args.kwargs["operation_name"] == "documentContent"
    data = cast(dict, found.structured_content)
    assert data["total_matches"] == 3
    assert data["skipped_urns"] == [
        {"urn": DOC2, "error": "Document not found or inaccessible."}
    ]
    # A client that checks isError must not read a bad pattern as zero matches.
    assert bad.is_error
