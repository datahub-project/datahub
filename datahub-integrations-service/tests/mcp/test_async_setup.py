import inspect
import time

import anyio
import fastmcp.tools.tool
import pytest

from datahub_integrations.mcp.mcp_server import async_background, mcp


@pytest.mark.anyio
async def test_async_background() -> None:
    @async_background
    def my_sleep(sec: float) -> None:
        time.sleep(sec)

    start_time = time.time()

    async with anyio.create_task_group() as tg:
        tg.start_soon(my_sleep, 0.5)
        tg.start_soon(my_sleep, 0.6)
        tg.start_soon(my_sleep, 0.7)

    end_time = time.time()
    duration = end_time - start_time
    # The calls should not be serialized, so the duration should be less than the sum of the durations.
    assert 0.5 <= duration < 1.8


def test_all_tools_are_async() -> None:
    # If any tools are sync, the tool execution will block the main event loop.
    for tool in mcp._tool_manager._tools.values():
        assert isinstance(tool, fastmcp.tools.tool.FunctionTool)
        assert inspect.iscoroutinefunction(tool.fn)
