"""Test MCP server connectivity.

Standalone helper for testing that an MCP server is reachable and responds
to tool discovery. Used by the admin "Test Connection" flow and potentially
by other validation flows.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import asyncer
from fastmcp import Client
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from loguru import logger


@dataclass
class McpTestResult:
    """Result of testing an MCP server connection."""

    success: bool
    tool_count: int = 0
    tool_names: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    error: Optional[str] = None
    error_type: Optional[str] = None
    status_code: Optional[int] = None


async def check_mcp_connection(
    *,
    url: str,
    transport: str = "HTTP",
    connection_timeout: float = 30.0,
    headers: Optional[Dict[str, str]] = None,
) -> McpTestResult:
    """Test connectivity to an MCP server by attempting tool discovery.

    Creates a temporary MCP client, connects, calls list_tools(), and reports
    the result. No persistent side effects.

    Args:
        url: The MCP server URL.
        transport: Transport protocol ("HTTP" or "SSE").
        connection_timeout: Connection timeout in seconds.
        headers: Headers to send (auth + custom headers merged by caller).

    Returns:
        McpTestResult with success/failure details and discovered tool names.
    """
    all_headers = dict(headers or {})
    start_time = time.perf_counter()

    try:
        mcp_transport: SSETransport | StreamableHttpTransport
        if transport.upper() == "SSE":
            mcp_transport = SSETransport(url=url, headers=all_headers)
        else:
            mcp_transport = StreamableHttpTransport(url=url, headers=all_headers)

        client = Client(mcp_transport, timeout=connection_timeout)

        async with client:
            tools = await client.list_tools()
            duration = time.perf_counter() - start_time

            tool_names = [t.name for t in tools]
            logger.bind(
                url=url,
                transport=transport,
                tool_count=len(tools),
            ).info(f"MCP test connection succeeded: {len(tools)} tools discovered")

            return McpTestResult(
                success=True,
                tool_count=len(tools),
                tool_names=tool_names,
                duration_seconds=duration,
            )

    except Exception as e:
        duration = time.perf_counter() - start_time

        # Try to extract HTTP status code
        status_code = None
        import httpx

        if isinstance(e, httpx.HTTPStatusError):
            status_code = e.response.status_code
        elif e.__cause__ and isinstance(e.__cause__, httpx.HTTPStatusError):
            status_code = e.__cause__.response.status_code

        logger.bind(
            url=url,
            transport=transport,
            error_type=type(e).__name__,
            status_code=status_code,
        ).opt(exception=True).warning(f"MCP test connection failed: {e}")

        return McpTestResult(
            success=False,
            duration_seconds=duration,
            error=str(e),
            error_type=type(e).__name__,
            status_code=status_code,
        )


def check_mcp_connection_sync(
    *,
    url: str,
    transport: str = "HTTP",
    connection_timeout: float = 30.0,
    headers: Optional[Dict[str, str]] = None,
) -> McpTestResult:
    """Synchronous wrapper for check_mcp_connection."""
    return asyncer.syncify(check_mcp_connection, raise_sync_error=False)(
        url=url,
        transport=transport,
        connection_timeout=connection_timeout,
        headers=headers,
    )
