#!/usr/bin/env python3

import sys
from functools import wraps
from typing import Awaitable, Callable, Optional, ParamSpec, TypeVar
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import anyio
import typer
from fastmcp import Client
from fastmcp.client.transports import SSETransport, StreamableHttpTransport

# Asyncio decorator for typer commands
P = ParamSpec("P")
T = TypeVar("T")


def coro(f: Callable[P, Awaitable[T]]) -> Callable[P, T]:
    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        async def _run() -> T:
            return await f(*args, **kwargs)

        return anyio.run(_run)

    return wrapper


def create_client(
    url: str,
    transport_type: str,
    token: Optional[str] = None,
    token_mode: str = "header",
    timeout: float = 30.0,
) -> Client:
    """Create an MCP client with the specified transport and authentication configuration."""

    # Parse URL to validate
    parsed_url = urlparse(url)
    if not parsed_url.scheme or not parsed_url.netloc:
        raise ValueError(f"Invalid URL: {url}")

    # SSE transport requires header authentication due to multiple requests
    if (
        transport_type.lower() in ["sse", "server-sent-events"]
        and token_mode == "param"
    ):
        raise ValueError(
            "SSE transport does not support query parameter authentication. "
            "Use --token-mode header instead."
        )

    # Prepare authentication
    transport_headers = {}
    final_url = url

    if token:
        if token_mode == "header":
            transport_headers["Authorization"] = f"Bearer {token}"
        elif token_mode == "param":
            # Add token as query parameter
            parsed = urlparse(url)
            query_dict = parse_qs(parsed.query) if parsed.query else {}
            query_dict["token"] = [token]
            new_query = urlencode(query_dict, doseq=True)
            final_url = urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    parsed.params,
                    new_query,
                    parsed.fragment,
                )
            )
        else:
            raise ValueError(
                f"Invalid token mode: {token_mode}. Must be 'header' or 'param'"
            )

    # Create transport based on type
    if transport_type.lower() in ["sse", "server-sent-events"]:
        transport = SSETransport(url=final_url, headers=transport_headers)
    elif transport_type.lower() in ["http", "streamable-http", "http-stream"]:
        transport = StreamableHttpTransport(url=final_url, headers=transport_headers)
    else:
        raise ValueError(f"Unsupported transport type: {transport_type}")

    # Create client with transport
    client = Client(transport, timeout=timeout)
    return client


async def validate_server(client: Client) -> bool:
    """
    Run validation sequence on the MCP server.
    Returns True if all tests pass, False otherwise.
    """

    try:
        typer.echo("🔗 Connecting to server...", nl=False)

        async with client:
            typer.echo(" ✅")

            # Test basic ping
            typer.echo("🏓 Testing ping...", nl=False)

            await client.ping()
            typer.echo(" ✅")

            # List tools (count only)
            typer.echo("🛠️  Listing tools...", nl=False)

            try:
                tools = await client.list_tools()
                tool_count = len(tools)
                tool_names = [tool.name for tool in tools]

                typer.echo(f" ✅ ({tool_count} tools)")
                if tool_names:
                    for name in tool_names:
                        typer.echo(f"    - {name}")
            except Exception as e:
                typer.echo(f" ❌ ({e})")
                return False

            return True

    except Exception as e:
        typer.echo(" ❌")
        typer.echo(f"❌ Connection failed: {e}")
        return False


@coro
async def main(
    url: str = typer.Argument(..., help="MCP server URL"),
    transport: str = typer.Option(
        "http",
        "--transport",
        "-t",
        help="Transport type: 'sse' or 'http' (streamable-http)",
    ),
    token: Optional[str] = typer.Option(
        None,
        "--token",
        help="Authentication token for the MCP server",
    ),
    token_mode: str = typer.Option(
        "param",
        "--token-mode",
        help="How to send the token: 'header' (Authorization: Bearer) or 'param' (query parameter)",
    ),
):
    """
    Test MCP server connectivity, authentication, and transport.
    
    This script validates basic MCP server connectivity, authentication, and transport
    functionality. It performs a simple validation sequence to ensure the server is
    working correctly.

    Authentication Notes:
        - SSE transport requires header authentication (--token-mode header)
        - HTTP transport supports both header and query parameter authentication

    Examples:
        # Test server with SSE transport (must use header auth)
        python scripts/mcp_tester.py http://localhost:9003/public/ai-legacy/sse/ \\
            --transport sse --token YOUR_JWT_TOKEN --token-mode header

        # Test server with HTTP transport and query param auth
        python scripts/mcp_tester.py http://localhost:9003/public/ai-legacy/mcp/ \\
            --transport http --token YOUR_JWT_TOKEN --token-mode param
    """

    # Validate token mode
    if token_mode not in ["header", "param"]:
        typer.echo(
            f"❌ Invalid token mode: {token_mode} (expected 'header' or 'param')",
            err=True,
        )
        sys.exit(1)

    # Display test configuration
    typer.echo(f"🌐 Testing MCP Server: {url}")
    typer.echo(f"🚀 Transport: {transport}")
    if token:
        typer.echo(f"🔐 Token: *** (via {token_mode})")
    else:
        typer.echo("🔐 Token: Not provided")
    typer.echo("---")

    # Create client
    try:
        client = create_client(url, transport, token, token_mode, timeout=30.0)
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        sys.exit(1)

    # Run validation sequence
    success = await validate_server(client)

    # Summary
    typer.echo("---")
    if success:
        typer.echo("✅ All tests passed! Server is working correctly.")
        sys.exit(0)
    else:
        typer.echo("❌ Some tests failed. Check server configuration.")
        sys.exit(1)


if __name__ == "__main__":
    typer.run(main)
