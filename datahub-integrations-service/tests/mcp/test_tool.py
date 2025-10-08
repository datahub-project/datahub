import re
from typing import Optional

import pytest
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter
from fastmcp import FastMCP
from pydantic import BaseModel

from datahub_integrations.mcp.tool import (
    TOOL_RESPONSE_TOKEN_LIMIT,
    ToolRunError,
    ToolWrapper,
    tools_from_fastmcp,
)


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

    with pytest.raises(
        ToolRunError,
        match=re.compile(r"validation error for call[\S\s]*filter", re.MULTILINE),
    ):
        search_tool.run({"query": "test", "filter": []})


def test_tool_response_token_limit_within_limit() -> None:
    """Test that tool responses within token limit are returned normally."""

    def fn() -> str:
        # Create a response that's well within the token limit
        return "This is a short response that should be well within the token limit."

    tool = ToolWrapper.from_function(fn, name="test", description="Test tool")
    result = tool.run({})
    assert result == {
        "result": "This is a short response that should be well within the token limit."
    }


def test_tool_response_token_limit_exceeded() -> None:
    """Test that tool responses exceeding token limit raise ToolRunError."""

    def fn() -> str:
        # Create a response that exceeds the token limit
        # TOOL_RESPONSE_TOKEN_LIMIT is 80000 tokens
        # We need approximately 250,000 characters to exceed the limit
        return "x" * 250000  # This should definitely exceed the limit

    tool = ToolWrapper.from_function(fn, name="test", description="Test tool")

    with pytest.raises(ToolRunError) as exc_info:
        tool.run({})

    error_message = str(exc_info.value)
    assert "Error executing tool test:" in error_message
    assert "Tool response too large" in error_message
    assert f"limit: {TOOL_RESPONSE_TOKEN_LIMIT:,}" in error_message
    assert "Please try with more specific parameters" in error_message


def test_tool_response_token_limit_dict_response() -> None:
    """Test that dict responses are also validated for token count."""

    def fn() -> dict:
        # Create a large dict response that exceeds the token limit
        return {"data": "x" * 250000, "metadata": {"size": "large"}}

    tool = ToolWrapper.from_function(fn, name="test", description="Test tool")

    with pytest.raises(ToolRunError) as exc_info:
        tool.run({})

    error_message = str(exc_info.value)
    assert "Error executing tool test:" in error_message
    assert "Tool response too large" in error_message
    assert f"limit: {TOOL_RESPONSE_TOKEN_LIMIT:,}" in error_message
