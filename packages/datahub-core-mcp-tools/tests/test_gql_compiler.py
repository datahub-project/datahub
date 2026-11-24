from unittest.mock import MagicMock

import pytest
from graphql import parse, print_ast

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.gql_compiler import GqlCompileError, compile_gql
from datahub_core_mcp_tools.graphql_helpers import execute_graphql, with_datahub_client
from datahub_core_mcp_tools.tool_context import ToolContext


def oss(version: str) -> GmsServerInfo:
    return GmsServerInfo(version=version, server_env="core")


def cloud(version: str) -> GmsServerInfo:
    return GmsServerInfo(version=version, server_env="cloud")


def same(actual: str, expected: str) -> bool:
    return print_ast(parse(actual)) == print_ast(parse(expected))


DOC = """
query getDataset($urn: String!, $count: Int!) {
  dataset(urn: $urn) {
    urn
    newField #[since oss=1.8.0 cloud=0.3.16]
    usage(count: $count) { #[since oss=1.9.0 cloud=0.3.17]
      ...UsageFields
    }
  }
}

fragment UsageFields on UsageAggregation {
  ...Nested
}

fragment Nested on UsageAggregation {
  bucket
}
"""


@pytest.mark.parametrize(
    ("info", "expected"),
    [
        (
            oss("v1.7.0"),
            "query getDataset($urn: String!) { dataset(urn: $urn) { urn } }",
        ),
        (
            oss("v1.8.0"),
            "query getDataset($urn: String!) { dataset(urn: $urn) { urn newField } }",
        ),
        (cloud("v0.3.16.4"), None),
        (oss("v1.9.0"), "full"),
        (cloud("v0.3.17"), "full"),
    ],
)
def test_version_boundaries(info: GmsServerInfo, expected: str | None) -> None:
    compiled = compile_gql(DOC, info)

    assert not compiled.strip_unsupported_fields
    if expected == "full":
        assert same(compiled.query, DOC)
    elif expected is None:
        assert "newField" in compiled.query
        assert "usage" not in compiled.query
        assert "$count" not in compiled.query
    else:
        # The dropped field was the only user of $count and of both fragments.
        assert same(compiled.query, expected)
        assert compiled.dropped_variables == {"count"}


def test_unknown_version_keeps_version_gates_and_strips_at_runtime() -> None:
    compiled = compile_gql(DOC, oss("v0.0.0-dev"))

    assert compiled.strip_unsupported_fields
    assert same(compiled.query, DOC)


def test_side_left_out_of_a_marker_never_runs_there() -> None:
    doc = "query q {\n  me {\n    urn\n    cloudOnly #[since cloud=0.3.16]\n  }\n}\n"

    assert "cloudOnly" not in compile_gql(doc, oss("v0.0.0-dev")).query
    assert "cloudOnly" in compile_gql(doc, cloud("v0.3.16")).query


def test_nothing_known_keeps_everything() -> None:
    compiled = compile_gql(DOC, None)

    assert compiled.strip_unsupported_fields
    assert same(compiled.query, DOC)


def test_unmarked_document_is_returned_unchanged() -> None:
    doc = "query q { me { corpUser { urn } } }"

    assert compile_gql(doc, oss("v1.7.0")).query == doc


def test_marker_on_a_multiline_field_drops_the_whole_field() -> None:
    doc = """
query q($start: Int!) {
  me {
    urn
    groups: relationships( #[since oss=1.9.0]
      input: { start: $start }
    ) {
      total
    }
  }
}
"""
    compiled = compile_gql(doc, oss("v1.8.0"))

    assert same(compiled.query, "query q { me { urn } }")


def test_emptied_selection_set_is_an_error() -> None:
    doc = """
query q {
  me {
    onlyField #[since oss=1.9.0]
  }
}
"""
    with pytest.raises(GqlCompileError, match="every selection"):
        compile_gql(doc, oss("v1.8.0"))


def test_fragment_emptied_with_its_only_spread_is_dropped() -> None:
    doc = """
query q {
  me {
    urn
    ...Newer #[since oss=1.9.0]
  }
}

fragment Newer on AuthenticatedUser {
  onlyField #[since oss=1.9.0]
}
"""
    compiled = compile_gql(doc, oss("v1.8.0"))

    assert same(compiled.query, "query q { me { urn } }")


def test_fragment_emptied_but_still_spread_is_an_error() -> None:
    doc = """
query q {
  me {
    urn
    ...Newer
  }
}

fragment Newer on AuthenticatedUser {
  onlyField #[since oss=1.9.0]
}
"""
    with pytest.raises(GqlCompileError, match="still used"):
        compile_gql(doc, oss("v1.8.0"))


@pytest.mark.parametrize(
    "line",
    [
        "a #[since]",
        "a #[since oss=1.9]",
        "a #[since enterprise=1.9.0]",
        "a #[since oss=1.9.0 oss=1.10.0]",
        "a #[since oss=1.9.0",
        "a b #[since oss=1.9.0]",
    ],
)
def test_malformed_markers(line: str) -> None:
    with pytest.raises(GqlCompileError):
        compile_gql(
            f"query q {{\n  me {{\n    urn\n    {line}\n  }}\n}}\n", oss("1.7.0")
        )


def test_marker_without_a_selection_is_an_error() -> None:
    doc = "query q {\n  me { urn }\n  #[since oss=1.9.0]\n}\n"

    with pytest.raises(GqlCompileError, match="exactly one"):
        compile_gql(doc, oss("v1.7.0"))


def test_invalid_graphql() -> None:
    with pytest.raises(GqlCompileError, match="Invalid GraphQL"):
        compile_gql("query q { me { urn #[since oss=1.9.0]\n", oss("v1.7.0"))


def test_compiled_once_per_version() -> None:
    assert compile_gql(DOC, oss("v1.8.0")) is compile_gql(DOC, oss("v1.8.0"))


def _graph_returning_query() -> MagicMock:
    graph = MagicMock()
    graph.execute_graphql.side_effect = lambda **kwargs: kwargs
    return graph


def test_execute_graphql_compiles_for_the_requests_gms() -> None:
    doc = "query q($n: Int) {\n  me {\n    urn\n    newer(n: $n) #[since oss=1.9.0]\n  }\n}\n"

    with with_datahub_client(MagicMock(), ToolContext([oss("v1.8.0")])):
        sent = execute_graphql(_graph_returning_query(), query=doc, variables={"n": 1})
    assert "newer" not in sent["query"]
    assert sent["variables"] == {}
    assert not sent["strip_unsupported_fields"]

    with with_datahub_client(MagicMock()):
        sent = execute_graphql(_graph_returning_query(), query=doc)
    assert "newer" in sent["query"]
    assert sent["strip_unsupported_fields"]
