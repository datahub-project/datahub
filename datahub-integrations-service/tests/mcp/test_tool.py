import re
from typing import Optional

import pytest
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter
from fastmcp import FastMCP
from pydantic import BaseModel

from datahub_integrations.mcp.tool import ToolRunError, ToolWrapper, tools_from_fastmcp


def test_from_function() -> None:
    def fn(a: int, b: int) -> int:
        return a + b

    tool = ToolWrapper.from_function(
        fn=fn,
        name="test",
        description="Test tool",
    )
    assert tool.name == "test"
    assert tool.run({"a": 1, "b": 2}) == {"result": 3}


def test_tool_complex_return_type() -> None:
    class ComplexReturn(BaseModel):
        a: int
        b: int

    def fn(a: int, b: int) -> ComplexReturn:
        return ComplexReturn(a=a, b=b)

    tool = ToolWrapper.from_function(fn, name="test", description="Test tool")
    assert tool.run({"a": 1, "b": 2}) == {"a": 1, "b": 2}


def test_tool_no_return_type() -> None:
    def fn(a, b):  # type: ignore[no-untyped-def]
        return [a, b, a + b]

    tool = ToolWrapper.from_function(fn, name="test", description="Test tool")
    assert tool.run({"a": 1, "b": 2}) == "[1,2,3]"


def test_tools_from_fastmcp() -> None:
    fake_mcp = FastMCP[None](name="fake_mcp")

    @fake_mcp.tool(description="Compile filters")
    def get_entity_filter(
        query: str, filter: Optional[Filter], num_results: int = 10
    ) -> list[str] | None:
        if not filter:
            raise ValueError("missing filter")
        return compile_filters(filter)[0]

    tools = tools_from_fastmcp(fake_mcp)
    tools_map = {tool.name: tool for tool in tools}
    assert set(tools_map.keys()) == {"fake_mcp__get_entity_filter"}

    search_tool = tools_map["fake_mcp__get_entity_filter"]

    result = search_tool.run(
        {
            "query": "test",
            "filter": {
                "and": [
                    {"platform": ["snowflake", "bigquery", "redshift"]},
                    {"entity_type": ["dataset"]},
                ]
            },
        }
    )
    assert result == {"result": ["DATASET"]}

    with pytest.raises(ToolRunError, match="missing filter"):
        search_tool.run({"query": "test", "filter": None})

    with pytest.raises(
        ToolRunError,
        match=re.compile(r"filter[\S\s]*Missing required argument", re.MULTILINE),
    ):
        search_tool.run({"query": "test"})
