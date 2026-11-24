import importlib
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest
import requests
from datahub.configuration.common import GraphError, OperationalError
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools import graphql_helpers
from datahub_core_mcp_tools.arg_validation import (
    OptionalStrList,
    StrList,
    StrListOrStr,
    coerce_keyword_list,
)
from datahub_core_mcp_tools.graphql_helpers import (
    execute_graphql,
    truncate_with_ellipsis,
)


@pytest.mark.parametrize(
    ("text", "limit", "kwargs", "expected"),
    [
        ("short", 10, {}, "short"),
        ("abcdefghij", 6, {}, "abc..."),
        ("abcdefghij", 2, {}, "ab"),  # limit shorter than the suffix
        ("the quick brown fox", 15, {"word_boundary": True}, "the quick..."),
        ("the quick brown fox", 9, {"dropped_count": True}, "the quick… [+10 chars]"),
    ],
)
def test_truncate_with_ellipsis(
    text: str, limit: int, kwargs: dict[str, Any], expected: str
) -> None:
    assert truncate_with_ellipsis(text, limit, **kwargs) == expected


def test_limits_come_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIELD_DESCRIPTION_LENGTH_LIMIT", "300")
    monkeypatch.setenv("QUERY_LENGTH_LIMIT", "100")
    try:
        reloaded = importlib.reload(graphql_helpers)
        assert reloaded.SCHEMA_FIELD_TEXT_LENGTH_LIMIT == 300
        assert reloaded.QUERY_LENGTH_HARD_LIMIT == 100

        monkeypatch.setenv("QUERY_LENGTH_LIMIT", "lots")
        with pytest.raises(ValueError, match="QUERY_LENGTH_LIMIT"):
            importlib.reload(graphql_helpers)
    finally:
        monkeypatch.delenv("FIELD_DESCRIPTION_LENGTH_LIMIT")
        monkeypatch.delenv("QUERY_LENGTH_LIMIT")
        importlib.reload(graphql_helpers)


def _status_error(status: int) -> Exception:
    response = requests.Response()
    response.status_code = status
    try:
        raise OperationalError("Unable to get metadata from DataHub") from (
            requests.HTTPError(response=response)
        )
    except OperationalError as e:
        return e


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (_status_error(401), "HTTP 401"),
        (_status_error(503), "could not be reached"),
        (requests.ConnectionError("refused"), "could not be reached"),
        (GraphError("Unknown field 'nope'"), "Unknown field 'nope'"),
    ],
)
def test_failures_become_tool_errors(error: Exception, message: str) -> None:
    graph = MagicMock()
    graph.execute_graphql.side_effect = error

    with pytest.raises(ToolError, match=message):
        execute_graphql(graph, query="query q { me { corpUser { urn } } }")


def test_raw_errors_for_callers_that_handle_them() -> None:
    graph = MagicMock()
    graph.execute_graphql.side_effect = GraphError([{"message": "x"}])

    with pytest.raises(GraphError):
        execute_graphql(graph, query="query q { me { urn } }", format_exception=False)


def echo_tags(tags: StrList, columns: OptionalStrList = None) -> Any:
    return {"tags": tags, "columns": columns}


def echo_urns(urns: StrListOrStr) -> str:
    return urns if isinstance(urns, str) else f"list:{','.join(urns)}"


async def _schema_and_call(fn: Any, **arguments: Any) -> tuple[dict, Any]:
    mcp = FastMCP("t")
    mcp.tool(fn)
    async with Client(mcp) as client:
        tool = next(t for t in await client.list_tools() if t.name == fn.__name__)
        return tool.input_schema, (await client.call_tool(fn.__name__, arguments)).data


@pytest.mark.parametrize(
    ("tags", "columns", "expected"),
    [
        (["a", "b"], None, {"tags": ["a", "b"], "columns": None}),
        ('["a", "b"]', "null", {"tags": ["a", "b"], "columns": None}),
        ("a", '["c"]', {"tags": ["a"], "columns": ["c"]}),
        ("not [json", "", {"tags": ["not [json"], "columns": None}),
    ],
)
async def test_str_list_accepts_stringified_lists(
    tags: Any, columns: Any, expected: dict
) -> None:
    schema, result = await _schema_and_call(echo_tags, tags=tags, columns=columns)

    assert result == expected
    # One declared type: strict function-calling APIs reject anyOf unions.
    assert schema["properties"]["tags"] == {
        "type": "array",
        "items": {"type": "string"},
    }


async def test_str_list_or_str_publishes_an_array_but_passes_strings() -> None:
    schema, result = await _schema_and_call(echo_urns, urns="urn:li:corpuser:a")

    assert result == "urn:li:corpuser:a"
    assert schema["properties"]["urns"]["type"] == "array"
    assert "anyOf" not in schema["properties"]["urns"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("pii, finance", ["pii", "finance"]),
        ('["a,b", "c"]', ["a", "b", "c"]),
        (None, None),
        ("null", None),
    ],
)
def test_coerce_keyword_list(value: Optional[str], expected: Any) -> None:
    assert coerce_keyword_list(value) == expected
