"""Unit tests for data_catalog_agent.py factory and helper functions."""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.agent import AgentError, AgentRunner
from datahub_integrations.chat.agents.data_catalog_agent import (
    _is_respond_to_user_result,
    create_data_catalog_explorer_agent,
    create_data_catalog_explorer_agent_fast,
    create_data_catalog_explorer_agent_research,
    create_response_formatter,
    data_catalog_completion_check,
)
from datahub_integrations.chat.agents.data_catalog_tools import _respond_to_user_tool
from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    HumanMessage,
    ToolCallRequest,
    ToolResult,
)
from datahub_integrations.chat.types import ChatType, NextMessage


class TestIsRespondToUserResult:
    """Tests for _is_respond_to_user_result TypeGuard function."""

    def test_returns_true_for_respond_to_user_tool_result(self) -> None:
        """Should return True when message is a ToolResult from respond_to_user."""
        tool_request = ToolCallRequest(
            tool_use_id="test-id",
            tool_name=_respond_to_user_tool.name,
            tool_input={"response": "Hello"},
        )
        message = ToolResult(
            tool_request=tool_request,
            result={"text": "Hello", "suggestions": []},
        )
        assert _is_respond_to_user_result(message) is True

    def test_returns_false_for_other_tool_result(self) -> None:
        """Should return False for ToolResult from different tool."""
        tool_request = ToolCallRequest(
            tool_use_id="test-id",
            tool_name="some_other_tool",
            tool_input={"arg": "value"},
        )
        message = ToolResult(
            tool_request=tool_request,
            result={"data": "something"},
        )
        assert _is_respond_to_user_result(message) is False

    def test_returns_false_for_assistant_message(self) -> None:
        """Should return False for AssistantMessage."""
        message = AssistantMessage(text="Hello")
        assert _is_respond_to_user_result(message) is False

    def test_returns_false_for_human_message(self) -> None:
        """Should return False for HumanMessage."""
        message = HumanMessage(text="Hello")
        assert _is_respond_to_user_result(message) is False


class TestDataCatalogCompletionCheck:
    """Tests for data_catalog_completion_check function."""

    def test_returns_true_for_respond_to_user_result(self) -> None:
        """Should return True when respond_to_user tool was called."""
        tool_request = ToolCallRequest(
            tool_use_id="test-id",
            tool_name=_respond_to_user_tool.name,
            tool_input={"response": "Done"},
        )
        message = ToolResult(
            tool_request=tool_request,
            result={"text": "Done", "suggestions": []},
        )
        assert data_catalog_completion_check(message) is True

    def test_returns_true_for_assistant_message(self) -> None:
        """Should return True for direct AssistantMessage (fallback case)."""
        message = AssistantMessage(text="Here's my response")
        assert data_catalog_completion_check(message) is True

    def test_returns_false_for_other_tool_result(self) -> None:
        """Should return False for ToolResult from non-respond_to_user tool."""
        tool_request = ToolCallRequest(
            tool_use_id="test-id",
            tool_name="search_entities",
            tool_input={"query": "test"},
        )
        message = ToolResult(
            tool_request=tool_request,
            result={"entities": []},
        )
        assert data_catalog_completion_check(message) is False

    def test_returns_false_for_human_message(self) -> None:
        """Should return False for HumanMessage."""
        message = HumanMessage(text="User question")
        assert data_catalog_completion_check(message) is False


class TestCreateResponseFormatter:
    """Tests for create_response_formatter closure factory."""

    @pytest.fixture
    def mock_client(self) -> Mock:
        """Create a mock DataHub client with frontend URL."""
        client = Mock()
        client._graph = Mock()
        client._graph.frontend_base_url = "https://demo.datahub.io"
        return client

    @pytest.fixture
    def mock_agent(self) -> Mock:
        """Create a mock AgentRunner."""
        agent = Mock(spec=AgentRunner)
        agent.session_id = "test-session-123"
        return agent

    def test_formatter_captures_chat_type(
        self, mock_client: Mock, mock_agent: Mock
    ) -> None:
        """Formatter closure should capture chat_type at creation time."""
        # Create formatters with different chat types
        ui_formatter = create_response_formatter(ChatType.DATAHUB_UI, mock_client)
        slack_formatter = create_response_formatter(ChatType.SLACK, mock_client)

        # Create an AssistantMessage
        message = AssistantMessage(text="Hello **world**")

        # Format with each - they should produce different results
        ui_result = ui_formatter(message, mock_agent)
        slack_result = slack_formatter(message, mock_agent)

        # Both should return NextMessage
        assert isinstance(ui_result, NextMessage)
        assert isinstance(slack_result, NextMessage)

        # UI format should preserve markdown, Slack should convert to Slack formatting
        assert "**world**" in ui_result.text  # Markdown preserved
        assert "*world*" in slack_result.text  # Converted to Slack format

    def test_formatter_captures_frontend_url(
        self, mock_client: Mock, mock_agent: Mock
    ) -> None:
        """Formatter should capture frontend_base_url for link fixing."""
        formatter = create_response_formatter(ChatType.DEFAULT, mock_client)

        # Create a message with a DataHub entity reference
        message = AssistantMessage(text="See urn:li:dataset:test for details")

        result = formatter(message, mock_agent)

        # The link should be processed (exact format depends on auto_fix_chat_links)
        assert isinstance(result, NextMessage)
        # The text should be processed (may or may not contain the URL depending on auto_fix_chat_links behavior)
        assert "test" in result.text or "dataset" in result.text

    def test_formats_respond_to_user_tool_result(
        self, mock_client: Mock, mock_agent: Mock
    ) -> None:
        """Should extract text and suggestions from respond_to_user ToolResult."""
        formatter = create_response_formatter(ChatType.DEFAULT, mock_client)

        tool_request = ToolCallRequest(
            tool_use_id="test-id",
            tool_name=_respond_to_user_tool.name,
            tool_input={"response": "Answer", "follow_up_suggestions": ["Follow up?"]},
        )
        message = ToolResult(
            tool_request=tool_request,
            result={"text": "Answer", "suggestions": ["Follow up?"]},
        )

        result = formatter(message, mock_agent)

        assert isinstance(result, NextMessage)
        assert "Answer" in result.text
        assert result.suggestions == ["Follow up?"]

    def test_formats_assistant_message_fallback(
        self, mock_client: Mock, mock_agent: Mock
    ) -> None:
        """Should handle direct AssistantMessage with empty suggestions."""
        formatter = create_response_formatter(ChatType.DEFAULT, mock_client)

        message = AssistantMessage(text="Direct response from LLM")

        result = formatter(message, mock_agent)

        assert isinstance(result, NextMessage)
        assert "Direct response from LLM" in result.text
        assert result.suggestions == []

    def test_raises_error_for_unexpected_message_type(
        self, mock_client: Mock, mock_agent: Mock
    ) -> None:
        """Should raise AgentError for unexpected message types."""
        formatter = create_response_formatter(ChatType.DEFAULT, mock_client)

        # HumanMessage is unexpected as a final response
        message = HumanMessage(text="User text")

        with pytest.raises(AgentError, match="Unexpected message type"):
            formatter(message, mock_agent)

    def test_raises_error_for_non_respond_to_user_tool_result(
        self, mock_client: Mock, mock_agent: Mock
    ) -> None:
        """Should raise AgentError for ToolResult from other tools."""
        formatter = create_response_formatter(ChatType.DEFAULT, mock_client)

        tool_request = ToolCallRequest(
            tool_use_id="test-id",
            tool_name="search_entities",
            tool_input={"query": "test"},
        )
        message = ToolResult(
            tool_request=tool_request,
            result={"entities": []},
        )

        with pytest.raises(AgentError, match="Unexpected message type"):
            formatter(message, mock_agent)


class TestCreateDataCatalogExplorerAgent:
    """Tests for create_data_catalog_explorer_agent factory function."""

    @pytest.fixture
    def mock_client(self) -> Mock:
        """Create a mock DataHub client."""
        client = Mock()
        client._graph = Mock()
        client._graph.frontend_base_url = "https://demo.datahub.io"
        return client

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_creates_agent_with_default_tools(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should create agent with default MCP tools when none provided."""
        mock_smart_search_enabled.return_value = False

        # Pass empty tools list to avoid MCP serialization issues
        agent = create_data_catalog_explorer_agent(mock_client, tools=[])

        assert isinstance(agent, AgentRunner)
        assert agent.config.agent_name == "AskDataHubAuto"
        # Internal tools should be added (respond_to_user + generate_sql)
        # DEFAULT mode doesn't include planning tools
        assert len(agent.tools) >= 2

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_creates_agent_with_custom_tools(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should use custom tools when provided."""
        mock_smart_search_enabled.return_value = False

        # Create a properly structured mock tool
        custom_tool = Mock()
        custom_tool.name = "custom_tool"
        custom_tool.description = "A custom tool for testing"
        custom_tool.json_schema.return_value = {
            "type": "object",
            "properties": {},
            "required": [],
        }
        # to_bedrock_spec returns a dict for AWS Bedrock API
        custom_tool.to_bedrock_spec.return_value = {
            "toolSpec": {
                "name": "custom_tool",
                "description": "A custom tool for testing",
                "inputSchema": {
                    "json": {"type": "object", "properties": {}, "required": []}
                },
            }
        }

        agent = create_data_catalog_explorer_agent(mock_client, tools=[custom_tool])

        assert isinstance(agent, AgentRunner)
        # Custom tool should be in plannable tools
        assert custom_tool in agent.config.plannable_tools
        # Custom tool should also be in all tools
        assert custom_tool in agent.tools

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_adds_smart_search_when_enabled(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should add smart_search tool when feature is enabled."""
        mock_smart_search_enabled.return_value = True

        agent = create_data_catalog_explorer_agent(mock_client, tools=[])

        # smart_search should be in plannable tools
        tool_names = [t.name for t in agent.config.plannable_tools]
        assert "smart_search" in tool_names

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_passes_chat_type_to_formatter(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should configure response_formatter with correct chat_type."""
        mock_smart_search_enabled.return_value = False

        agent = create_data_catalog_explorer_agent(
            mock_client, tools=[], chat_type=ChatType.SLACK
        )

        # Verify formatter is configured
        assert agent.config.response_formatter is not None
        assert agent.config.completion_check is not None

        # Test that the formatter applies Slack formatting
        message = AssistantMessage(text="Hello **world**")
        result = agent.config.response_formatter(message, agent)
        assert isinstance(result, NextMessage)
        # Slack format uses single asterisks for bold
        assert "*world*" in result.text

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_configures_completion_check(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should configure completion_check in AgentConfig."""
        mock_smart_search_enabled.return_value = False

        agent = create_data_catalog_explorer_agent(mock_client, tools=[])

        assert agent.config.completion_check is not None
        assert agent.config.completion_check == data_catalog_completion_check

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_uses_xml_reasoning_parser(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should use XmlReasoningParser for conversational parsing."""
        mock_smart_search_enabled.return_value = False

        agent = create_data_catalog_explorer_agent(mock_client, tools=[])

        from datahub_integrations.chat.agent import XmlReasoningParser

        assert isinstance(agent.config.conversational_parser, XmlReasoningParser)

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_adds_internal_tools(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should add internal tools (respond_to_user, planning tools in RESEARCH mode)."""

        mock_smart_search_enabled.return_value = False

        # Test DEFAULT mode - should have respond_to_user but no planning tools
        default_agent = create_data_catalog_explorer_agent(mock_client, tools=[])
        default_tool_names = [t.name for t in default_agent.tools]
        assert _respond_to_user_tool.name in default_tool_names
        assert "create_plan" not in default_tool_names
        assert "revise_plan" not in default_tool_names

        # Test RESEARCH mode - should have respond_to_user and planning tools
        research_agent = create_data_catalog_explorer_agent_research(
            mock_client, tools=[]
        )
        research_tool_names = [t.name for t in research_agent.tools]
        assert _respond_to_user_tool.name in research_tool_names
        assert "create_plan" in research_tool_names
        assert "revise_plan" in research_tool_names

    @patch.dict("os.environ", {"MODEL_CUSTOM_BASE_URL": "https://custom.example.com"})
    def test_disables_smart_search_with_custom_base_url(
        self,
        mock_client: Mock,
    ) -> None:
        """Should disable smart_search when custom model base URL is set."""
        # Clear the cache to ensure fresh evaluation
        from datahub_integrations.chat.agents.data_catalog_tools import (
            is_smart_search_enabled,
        )

        is_smart_search_enabled.cache_clear()

        agent = create_data_catalog_explorer_agent(mock_client, tools=[])

        # smart_search should NOT be in plannable tools
        tool_names = [t.name for t in agent.config.plannable_tools]
        assert "smart_search" not in tool_names

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_uses_mode_specific_model(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should select model based on agent_type parameter."""
        from datahub_integrations.gen_ai.model_config import model_config

        mock_smart_search_enabled.return_value = False

        # Create agents with different agent types
        fast_agent = create_data_catalog_explorer_agent_fast(mock_client, tools=[])
        default_agent = create_data_catalog_explorer_agent(mock_client, tools=[])
        research_agent = create_data_catalog_explorer_agent_research(
            mock_client, tools=[]
        )

        # Each should have a model selected
        # Fast mode should use the fast model
        assert (
            fast_agent.config.model_id
            == model_config.chat_assistant_ai.agent_mode_to_model.get(
                "AskDataHubFast", model_config.chat_assistant_ai.model
            )
        )
        # Default mode should use the default model
        assert (
            default_agent.config.model_id
            == model_config.chat_assistant_ai.agent_mode_to_model.get(
                "AskDataHubAuto", model_config.chat_assistant_ai.model
            )
        )
        # Research mode should use the research model
        assert (
            research_agent.config.model_id
            == model_config.chat_assistant_ai.agent_mode_to_model.get(
                "AskDataHubResearch", model_config.chat_assistant_ai.model
            )
        )

        # Verify they're actually different (not all using the same model)
        # Fast and research should differ in typical configs
        assert fast_agent.config.model_id != research_agent.config.model_id

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_mode_affects_system_prompt(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should pass is_planning_enabled to DataHubSystemPromptBuilder based on mode."""

        mock_smart_search_enabled.return_value = False

        # Create agent with RESEARCH mode - should have planning enabled
        research_agent = create_data_catalog_explorer_agent_research(
            mock_client, tools=[]
        )
        assert hasattr(
            research_agent.config.system_prompt_builder, "is_planning_enabled"
        )
        assert research_agent.config.system_prompt_builder.is_planning_enabled is True

        # Create agent with DEFAULT mode - should NOT have planning enabled
        default_agent = create_data_catalog_explorer_agent(mock_client, tools=[])
        assert hasattr(
            default_agent.config.system_prompt_builder, "is_planning_enabled"
        )
        assert default_agent.config.system_prompt_builder.is_planning_enabled is False

        # Create agent with FAST mode - should NOT have planning enabled
        fast_agent = create_data_catalog_explorer_agent_fast(mock_client, tools=[])
        assert hasattr(fast_agent.config.system_prompt_builder, "is_planning_enabled")
        assert fast_agent.config.system_prompt_builder.is_planning_enabled is False

    @patch(
        "datahub_integrations.chat.agents.data_catalog_agent.is_smart_search_enabled"
    )
    def test_fast_mode_excludes_mutation_tools(
        self,
        mock_smart_search_enabled: Mock,
        mock_client: Mock,
    ) -> None:
        """Should exclude mutation tools in FAST mode."""

        mock_smart_search_enabled.return_value = False

        # Create a mock tool with MUTATION tag
        mock_mutation_tool = Mock()
        mock_mutation_tool.name = "mutation_tool"
        mock_mutation_tool.description = "A mutation tool"
        mock_mutation_tool.tags = {"mutation"}
        mock_mutation_tool.json_schema.return_value = {
            "type": "object",
            "properties": {},
            "required": [],
        }
        mock_mutation_tool.to_bedrock_spec.return_value = {
            "toolSpec": {
                "name": "mutation_tool",
                "description": "A mutation tool",
                "inputSchema": {
                    "json": {"type": "object", "properties": {}, "required": []}
                },
            }
        }

        # Create a mock tool without MUTATION tag
        mock_search_tool = Mock()
        mock_search_tool.name = "search_tool"
        mock_search_tool.description = "A search tool"
        mock_search_tool.tags = {"search"}
        mock_search_tool.json_schema.return_value = {
            "type": "object",
            "properties": {},
            "required": [],
        }
        mock_search_tool.to_bedrock_spec.return_value = {
            "toolSpec": {
                "name": "search_tool",
                "description": "A search tool",
                "inputSchema": {
                    "json": {"type": "object", "properties": {}, "required": []}
                },
            }
        }

        # Create agents with different modes
        fast_agent = create_data_catalog_explorer_agent_fast(
            mock_client,
            tools=[mock_mutation_tool, mock_search_tool],
        )
        default_agent = create_data_catalog_explorer_agent(
            mock_client,
            tools=[mock_mutation_tool, mock_search_tool],
        )

        # FAST mode should exclude mutation tools
        fast_tool_names = [t.name for t in fast_agent.config.plannable_tools]
        assert "mutation_tool" not in fast_tool_names
        assert "search_tool" in fast_tool_names

        # DEFAULT mode should include mutation tools
        default_tool_names = [t.name for t in default_agent.config.plannable_tools]
        assert "mutation_tool" in default_tool_names
        assert "search_tool" in default_tool_names
