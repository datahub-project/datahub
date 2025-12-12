"""Unit tests for ingestion troubleshooting agent factory."""

from unittest.mock import Mock, patch

from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.agents.ingestion_troubleshooting_agent import (
    create_ingestion_troubleshooting_agent,
)


class TestIngestionTroubleshootingAgentFactory:
    """Test the agent factory with troubleshoot tool registration."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mock_client = Mock(spec=DataHubClient)
        # Mock the _graph attribute that is accessed during agent creation
        mock_graph = Mock()
        mock_graph.frontend_base_url = "http://localhost:9002"
        self.mock_client._graph = mock_graph

    def test_troubleshoot_tool_registered_when_provider_available(self) -> None:
        """Troubleshoot tool is registered when provider is configured."""
        with patch.dict(
            "os.environ",
            {
                "RUNLLM_API_KEY": "test-api-key",
                "RUNLLM_ASSISTANT_ID": "test-assistant-id",
            },
        ):
            # Patch MCP registration to avoid side effects
            with patch(
                "datahub_integrations.chat.agents.ingestion_troubleshooting_agent.register_all_tools"
            ):
                agent = create_ingestion_troubleshooting_agent(self.mock_client)

                # Check that troubleshoot tool is in the agent's tools
                tool_names = [tool.name for tool in agent.tools]
                assert "troubleshoot" in tool_names

    def test_troubleshoot_tool_not_registered_when_provider_unavailable(self) -> None:
        """Troubleshoot tool is not registered when provider is not configured."""
        with patch.dict("os.environ", {}, clear=True):
            # Patch MCP registration to avoid side effects
            with patch(
                "datahub_integrations.chat.agents.ingestion_troubleshooting_agent.register_all_tools"
            ):
                agent = create_ingestion_troubleshooting_agent(self.mock_client)

                # Check that troubleshoot tool is not in the agent's tools
                tool_names = [tool.name for tool in agent.tools]
                assert "troubleshoot" not in tool_names

    def test_agent_creates_successfully_without_troubleshoot_tool(self) -> None:
        """Agent can be created successfully even when troubleshoot tool is unavailable."""
        with patch.dict("os.environ", {}, clear=True):
            # Patch MCP registration to avoid side effects
            with patch(
                "datahub_integrations.chat.agents.ingestion_troubleshooting_agent.register_all_tools"
            ):
                agent = create_ingestion_troubleshooting_agent(self.mock_client)

                # Agent should be created successfully
                assert agent is not None
                assert agent.client == self.mock_client
                # Should still have other tools
                assert len(agent.tools) > 0

    def test_ingestion_tools_always_registered(self) -> None:
        """Ingestion tools are always registered for ingestion troubleshooting agent."""
        with patch.dict("os.environ", {}, clear=True):
            # Patch MCP registration to avoid side effects
            with patch(
                "datahub_integrations.chat.agents.ingestion_troubleshooting_agent.register_all_tools"
            ):
                agent = create_ingestion_troubleshooting_agent(self.mock_client)

                # Check that all three ingestion tools are in the agent's tools
                tool_names = [tool.name for tool in agent.tools]
                assert "get_ingestion_source" in tool_names
                assert "get_ingestion_execution_request" in tool_names
                assert "get_ingestion_execution_logs" in tool_names

                # Also verify they're in plannable_tools (subset used for planning)
                plannable_tool_names = [tool.name for tool in agent.plannable_tools]
                assert "get_ingestion_source" in plannable_tool_names
                assert "get_ingestion_execution_request" in plannable_tool_names
                assert "get_ingestion_execution_logs" in plannable_tool_names
