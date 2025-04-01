import re
from typing import Optional

import pytest
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter

from datahub_integrations.chat.tool import ToolRegistry, ToolRunError

fake_mcp = ToolRegistry()


@fake_mcp.tool(description="Search for stuff")
def search(query: str, filter: Optional[Filter], num_results: int = 10) -> Filter:
    if not filter:
        raise ValueError("missing filter")
    return filter


def test_tool_call() -> None:
    assert set(fake_mcp.tools.keys()) == {"search"}

    search_tool = fake_mcp.tools["search"]

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
    assert compile_filters(result)

    with pytest.raises(ToolRunError, match="missing filter"):
        search_tool.run({"query": "test", "filter": None})

    with pytest.raises(
        ToolRunError, match=re.compile(r"filter[\S\s]*field required", re.MULTILINE)
    ):
        search_tool.run({"query": "test"})
