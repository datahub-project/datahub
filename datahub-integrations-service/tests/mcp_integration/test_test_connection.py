"""Tests for the MCP test connection helper."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datahub_integrations.mcp_integration.connection_tester import (
    McpTestResult,
    check_mcp_connection,
)


class TestMcpTestResult:
    """Tests for the McpTestResult dataclass."""

    def test_success_result(self) -> None:
        result = McpTestResult(
            success=True,
            tool_count=3,
            tool_names=["search", "get_entity", "list"],
            duration_seconds=1.5,
        )
        assert result.success is True
        assert result.tool_count == 3
        assert len(result.tool_names) == 3
        assert result.error is None

    def test_failure_result(self) -> None:
        result = McpTestResult(
            success=False,
            error="Connection refused",
            error_type="ConnectionError",
            duration_seconds=5.0,
        )
        assert result.success is False
        assert result.tool_count == 0
        assert result.error == "Connection refused"

    def test_defaults(self) -> None:
        result = McpTestResult(success=True)
        assert result.tool_count == 0
        assert result.tool_names == []
        assert result.duration_seconds == 0.0
        assert result.error is None
        assert result.status_code is None


class TestTestMcpConnection:
    """Tests for the check_mcp_connection async function."""

    @pytest.mark.asyncio
    async def test_successful_connection(self) -> None:
        """Test successful MCP connection returns tool names."""
        mock_tool_1 = MagicMock()
        mock_tool_1.name = "search"
        mock_tool_2 = MagicMock()
        mock_tool_2.name = "get_entity"

        mock_client = AsyncMock()
        mock_client.list_tools.return_value = [mock_tool_1, mock_tool_2]
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "datahub_integrations.mcp_integration.connection_tester.Client",
            return_value=mock_client,
        ):
            result = await check_mcp_connection(
                url="http://localhost:3001",
                transport="HTTP",
            )

        assert result.success is True
        assert result.tool_count == 2
        assert result.tool_names == ["search", "get_entity"]
        assert result.duration_seconds > 0

    @pytest.mark.asyncio
    async def test_connection_error(self) -> None:
        """Test connection error returns failure result."""
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(
            side_effect=ConnectionError("Connection refused")
        )

        with patch(
            "datahub_integrations.mcp_integration.connection_tester.Client",
            return_value=mock_client,
        ):
            result = await check_mcp_connection(
                url="http://unreachable:3001",
            )

        assert result.success is False
        assert "Connection refused" in (result.error or "")
        assert result.error_type == "ConnectionError"

    @pytest.mark.asyncio
    async def test_http_error_captures_status_code(self) -> None:
        """Test HTTP error captures status code."""
        import httpx

        mock_response = MagicMock()
        mock_response.status_code = 401

        http_error = httpx.HTTPStatusError(
            "Unauthorized", request=MagicMock(), response=mock_response
        )

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(side_effect=http_error)

        with patch(
            "datahub_integrations.mcp_integration.connection_tester.Client",
            return_value=mock_client,
        ):
            result = await check_mcp_connection(
                url="http://localhost:3001",
            )

        assert result.success is False
        assert result.status_code == 401

    @pytest.mark.asyncio
    async def test_sse_transport_used_for_sse(self) -> None:
        """Test SSE transport is selected for SSE transport type."""

        mock_client = AsyncMock()
        mock_client.list_tools.return_value = []
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch(
                "datahub_integrations.mcp_integration.connection_tester.Client",
                return_value=mock_client,
            ),
            patch(
                "datahub_integrations.mcp_integration.connection_tester.SSETransport",
            ) as mock_sse,
        ):
            await check_mcp_connection(
                url="http://localhost:3001",
                transport="SSE",
            )
            mock_sse.assert_called_once()

    @pytest.mark.asyncio
    async def test_custom_headers_passed(self) -> None:
        """Test custom headers are passed to transport."""
        mock_client = AsyncMock()
        mock_client.list_tools.return_value = []
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch(
                "datahub_integrations.mcp_integration.connection_tester.Client",
                return_value=mock_client,
            ),
            patch(
                "datahub_integrations.mcp_integration.connection_tester.StreamableHttpTransport",
            ) as mock_transport,
        ):
            await check_mcp_connection(
                url="http://localhost:3001",
                headers={"Authorization": "Bearer token123", "X-Custom": "value"},
            )
            call_kwargs = mock_transport.call_args.kwargs
            assert call_kwargs["headers"]["Authorization"] == "Bearer token123"
            assert call_kwargs["headers"]["X-Custom"] == "value"
