import re
from typing import Optional

import pytest
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter
from mcp.server.fastmcp import FastMCP

from datahub_integrations.mcp.tool import ToolRunError, tools_from_fastmcp

fake_mcp = FastMCP(name="fake_mcp")


@fake_mcp.tool(description="Search for stuff")
def search(query: str, filter: Optional[Filter], num_results: int = 10) -> Filter:
    if not filter:
        raise ValueError("missing filter")
    return filter


def test_tool_call() -> None:
    tools = tools_from_fastmcp(fake_mcp)
    tools_map = {tool.name: tool for tool in tools}
    assert set(tools_map.keys()) == {"fake_mcp__search"}

    search_tool = tools_map["fake_mcp__search"]

    result = search_tool.run(
        {
            "query": "test",
            "filter": {
                "and": [
                    {"env": ["PROD"]},
                    {"platform": ["snowflake", "bigquery", "redshift"]},
                ]
            },
        }
    )
    assert result is not None
    _ = compile_filters(result)

    with pytest.raises(ToolRunError, match="missing filter"):
        search_tool.run({"query": "test", "filter": None})

    with pytest.raises(
        ToolRunError,
        match=re.compile(r"filter[\S\s]*Field required", re.MULTILINE),
    ):
        search_tool.run({"query": "test"})
