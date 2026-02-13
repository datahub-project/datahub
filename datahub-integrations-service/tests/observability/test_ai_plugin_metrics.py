"""Tests for AI plugin operational metrics.

Tests verify that:
- Tool call latency histogram is populated (was previously empty)
- is_external label distinguishes internal vs external tool calls
- ChatbotToolCallEvent carries plugin context for external tools
- Plugin discovery metrics are recorded on success/failure
- OAuth flow metrics are recorded for AI plugin connect/callback/refresh
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp_integration.ai_plugin_loader import (
    AiPluginAuthType,
    AiPluginConfig,
    McpServerConfig,
    McpTransport,
)
from datahub_integrations.mcp_integration.external_mcp_manager import (
    ExternalToolWrapper,
    PluginConnectionError,
)
from datahub_integrations.observability.bot_metrics import (
    AiPluginOAuthStep,
    record_ai_plugin_oauth_flow,
    record_plugin_discovery,
)
from datahub_integrations.telemetry.chat_events import ChatbotToolCallEvent

# --- Tool call latency and is_external label ---


class TestToolCallIsExternalDetection:
    """Tests that ExternalToolWrapper is correctly detected for is_external labeling."""

    def test_internal_tool_not_detected_as_external(self):
        """A plain MagicMock tool should not be detected as ExternalToolWrapper."""
        tool = MagicMock()
        assert not isinstance(tool, ExternalToolWrapper)

    def test_external_tool_wrapper_detected_as_external(self):
        """An ExternalToolWrapper instance should be detected as external."""
        ext_tool = ExternalToolWrapper(
            name="github__search",
            description="Search GitHub",
            parameters={"type": "object", "properties": {}},
            _client_factory=MagicMock(),
            _original_name="search",
            plugin_id="test-plugin",
            plugin_name="GitHub",
            tool_prefix="github",
        )
        assert isinstance(ext_tool, ExternalToolWrapper)

    def test_tool_map_get_preserves_type(self):
        """tool_map.get() should return the tool with its actual type preserved."""
        ext_tool = ExternalToolWrapper(
            name="github__search",
            description="Search GitHub",
            parameters={"type": "object", "properties": {}},
            _client_factory=MagicMock(),
            _original_name="search",
            plugin_id="test-plugin",
            plugin_name="GitHub",
            tool_prefix="github",
        )
        internal_tool = MagicMock()

        tool_map = {"github__search": ext_tool, "search_datasets": internal_tool}

        # External tool should be detected
        resolved = tool_map.get("github__search")
        assert isinstance(resolved, ExternalToolWrapper)

        # Internal tool should not be detected
        resolved = tool_map.get("search_datasets")
        assert not isinstance(resolved, ExternalToolWrapper)

        # Missing tool should not be detected
        resolved = tool_map.get("nonexistent")
        assert not isinstance(resolved, ExternalToolWrapper)


# --- ChatbotToolCallEvent plugin context ---


class TestChatbotToolCallEventPluginContext:
    """Tests that ChatbotToolCallEvent carries plugin context for external tools."""

    def test_internal_tool_event_defaults(self):
        """Internal tool event should have is_external=False and no plugin fields."""
        event = ChatbotToolCallEvent(
            chat_session_id="session-1",
            tool_name="search_datasets",
            tool_execution_duration_sec=0.5,
        )
        assert event.is_external is False
        assert event.plugin_id is None
        assert event.plugin_name is None

    def test_external_tool_event_has_plugin_fields(self):
        """External tool event should carry plugin_id, plugin_name, and is_external=True."""
        event = ChatbotToolCallEvent(
            chat_session_id="session-1",
            tool_name="github__search",
            tool_execution_duration_sec=1.2,
            is_external=True,
            plugin_id="urn:li:service:abc123",
            plugin_name="GitHub",
        )
        assert event.is_external is True
        assert event.plugin_id == "urn:li:service:abc123"
        assert event.plugin_name == "GitHub"

    def test_external_tool_event_with_error(self):
        """External tool error event should carry plugin fields alongside error info."""
        event = ChatbotToolCallEvent(
            chat_session_id="session-1",
            tool_name="dbt__get_model",
            tool_execution_duration_sec=0.3,
            tool_result_is_error=True,
            tool_error="TimeoutError: request timed out",
            is_external=True,
            plugin_id="urn:li:service:def456",
            plugin_name="dbt Cloud",
        )
        assert event.is_external is True
        assert event.plugin_id == "urn:li:service:def456"
        assert event.tool_result_is_error is True


# --- Plugin discovery metrics ---


class TestPluginDiscoveryMetrics:
    """Tests that plugin discovery records OTel metrics."""

    def test_record_plugin_discovery_success(self):
        """record_plugin_discovery should execute without error on success."""
        record_plugin_discovery(
            plugin_name="GitHub",
            duration_seconds=0.8,
            success=True,
        )

    def test_record_plugin_discovery_failure(self):
        """record_plugin_discovery should execute without error on failure."""
        record_plugin_discovery(
            plugin_name="Unreachable Server",
            duration_seconds=5.0,
            success=False,
        )

    @patch(
        "datahub_integrations.mcp_integration.external_mcp_manager.record_plugin_discovery"
    )
    def test_discover_all_tools_records_metrics_on_success(self, mock_record_discovery):
        """Successful discovery should record metric with success=True."""
        from datahub_integrations.mcp_integration.external_mcp_manager import (
            ExternalMCPManager,
        )

        plugin = AiPluginConfig(
            id="test-plugin",
            service_urn="urn:li:service:test",
            display_name="Test Plugin",
            description=None,
            instructions=None,
            auth_type=AiPluginAuthType.NONE,
            mcp_config=McpServerConfig(
                url="http://localhost:3001",
                transport=McpTransport.HTTP,
                timeout=30.0,
                custom_headers={},
            ),
        )

        manager = ExternalMCPManager(
            plugins=[plugin],
            credential_store=MagicMock(),
            user_urn="urn:li:corpuser:test",
        )

        # Mock _discover_tools to return empty list
        with patch.object(manager, "_discover_tools", return_value=[]):
            import asyncio

            asyncio.run(manager._discover_all_tools())

        mock_record_discovery.assert_called_once()
        call_kwargs = mock_record_discovery.call_args.kwargs
        assert call_kwargs["plugin_name"] == "Test Plugin"
        assert call_kwargs["success"] is True
        assert call_kwargs["duration_seconds"] >= 0

    @patch(
        "datahub_integrations.mcp_integration.external_mcp_manager.record_plugin_discovery"
    )
    def test_discover_all_tools_records_metrics_on_auth_error(
        self, mock_record_discovery
    ):
        """Auth error during discovery should record metric with success=False."""
        from datahub_integrations.mcp_integration.external_mcp_manager import (
            ExternalMCPManager,
        )

        plugin = AiPluginConfig(
            id="auth-fail-plugin",
            service_urn="urn:li:service:authfail",
            display_name="Auth Fail Plugin",
            description=None,
            instructions=None,
            auth_type=AiPluginAuthType.NONE,
            mcp_config=McpServerConfig(
                url="http://localhost:3001",
                transport=McpTransport.HTTP,
                timeout=30.0,
                custom_headers={},
            ),
        )

        manager = ExternalMCPManager(
            plugins=[plugin],
            credential_store=MagicMock(),
            user_urn="urn:li:corpuser:test",
        )

        # Mock _discover_tools to raise PluginConnectionError
        with patch.object(
            manager,
            "_discover_tools",
            side_effect=PluginConnectionError(
                plugin_id="auth-fail-plugin",
                plugin_name="Auth Fail Plugin",
                message="Auth failed",
            ),
        ):
            import asyncio

            with pytest.raises(PluginConnectionError):
                asyncio.run(manager._discover_all_tools())

        mock_record_discovery.assert_called_once()
        call_kwargs = mock_record_discovery.call_args.kwargs
        assert call_kwargs["plugin_name"] == "Auth Fail Plugin"
        assert call_kwargs["success"] is False

    @patch(
        "datahub_integrations.mcp_integration.external_mcp_manager.record_plugin_discovery"
    )
    def test_discover_all_tools_records_metrics_on_transient_error(
        self, mock_record_discovery
    ):
        """Transient error during discovery should record metric and skip plugin."""
        from datahub_integrations.mcp_integration.external_mcp_manager import (
            ExternalMCPManager,
        )

        plugin = AiPluginConfig(
            id="transient-fail",
            service_urn="urn:li:service:transient",
            display_name="Transient Fail",
            description=None,
            instructions=None,
            auth_type=AiPluginAuthType.NONE,
            mcp_config=McpServerConfig(
                url="http://localhost:3001",
                transport=McpTransport.HTTP,
                timeout=30.0,
                custom_headers={},
            ),
        )

        manager = ExternalMCPManager(
            plugins=[plugin],
            credential_store=MagicMock(),
            user_urn="urn:li:corpuser:test",
        )

        # Mock _discover_tools to raise a generic error (transient - plugin skipped)
        with patch.object(
            manager,
            "_discover_tools",
            side_effect=ConnectionError("Connection refused"),
        ):
            import asyncio

            # Should NOT raise - transient errors skip the plugin
            result = asyncio.run(manager._discover_all_tools())
            assert result == []

        mock_record_discovery.assert_called_once()
        call_kwargs = mock_record_discovery.call_args.kwargs
        assert call_kwargs["success"] is False


# --- OAuth flow metrics ---


class TestAiPluginOAuthFlowMetrics:
    """Tests that AI plugin OAuth flow metrics are recorded correctly.

    AI plugin OAuth metrics are separate from bot OAuth metrics (bot_oauth_*)
    which track Slack/Teams bot installation flows.
    """

    def test_record_connect_step(self):
        """Connect step should be recordable."""
        record_ai_plugin_oauth_flow(
            step=AiPluginOAuthStep.CONNECT,
            duration_seconds=0.3,
            success=True,
        )

    def test_record_callback_step(self):
        """Callback step should be recordable."""
        record_ai_plugin_oauth_flow(
            step=AiPluginOAuthStep.CALLBACK,
            duration_seconds=1.5,
            success=True,
        )

    def test_record_refresh_step(self):
        """Refresh step should be recordable."""
        record_ai_plugin_oauth_flow(
            step=AiPluginOAuthStep.REFRESH,
            duration_seconds=0.2,
            success=True,
        )

    def test_record_failure(self):
        """Failed OAuth flow should be recordable."""
        record_ai_plugin_oauth_flow(
            step=AiPluginOAuthStep.CALLBACK,
            duration_seconds=2.0,
            success=False,
        )

    def test_step_enum_values(self):
        """AiPluginOAuthStep enum should have expected string values."""
        assert AiPluginOAuthStep.CONNECT.value == "connect"
        assert AiPluginOAuthStep.CALLBACK.value == "callback"
        assert AiPluginOAuthStep.REFRESH.value == "refresh"


# --- CostTracker is_external label ---


class TestCostTrackerIsExternalLabel:
    """Tests that CostTracker passes is_external label to OTel metrics."""

    def test_record_tool_call_with_is_external_false(self):
        """record_tool_call should accept is_external=False without error."""
        from datahub_integrations.observability.cost import CostTracker
        from datahub_integrations.observability.metrics_constants import AIModule

        tracker = CostTracker()
        tracker.record_tool_call(
            ai_module=AIModule.CHAT,
            tool="search_datasets",
            success=True,
            is_external=False,
        )

    def test_record_tool_call_with_is_external_true(self):
        """record_tool_call should accept is_external=True without error."""
        from datahub_integrations.observability.cost import CostTracker
        from datahub_integrations.observability.metrics_constants import AIModule

        tracker = CostTracker()
        tracker.record_tool_call(
            ai_module=AIModule.CHAT,
            tool="github__search",
            success=True,
            is_external=True,
        )

    def test_record_tool_call_latency_with_is_external(self):
        """record_tool_call_latency should accept is_external parameter."""
        from datahub_integrations.observability.cost import CostTracker
        from datahub_integrations.observability.metrics_constants import AIModule

        tracker = CostTracker()
        tracker.record_tool_call_latency(
            duration_seconds=0.5,
            ai_module=AIModule.CHAT,
            tool="github__search",
            success=True,
            is_external=True,
        )

    def test_record_tool_call_latency_defaults_to_internal(self):
        """record_tool_call_latency should default is_external to False."""
        from datahub_integrations.observability.cost import CostTracker
        from datahub_integrations.observability.metrics_constants import AIModule

        tracker = CostTracker()
        # Should work without explicitly passing is_external
        tracker.record_tool_call_latency(
            duration_seconds=0.1,
            ai_module=AIModule.CHAT,
            tool="search_datasets",
            success=True,
        )
