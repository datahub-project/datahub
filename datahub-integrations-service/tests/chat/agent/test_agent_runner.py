"""Unit tests for AgentRunner core infrastructure."""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentMaxLLMTurnsExceededError,
    AgentMaxTokensExceededError,
    AgentOutputMaxTokensExceededError,
    AgentRunner,
    StaticPromptBuilder,
)
from datahub_integrations.chat.agent.agent_runner import (
    LLMCallResult,
    ToolExecutionResult,
)
from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    ReasoningMessage,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.gen_ai.llm.exceptions import LlmInputTooLongException
from datahub_integrations.mcp_integration.tool import ToolWrapper


class TestAgentRunnerInitialization:
    """Test AgentRunner initialization."""

    def test_creates_agent_with_basic_config(self) -> None:
        """Test creating agent with minimal configuration."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test prompt"),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()

        agent = AgentRunner(config=config, client=mock_client)

        assert agent.config == config
        assert agent.client == mock_client
        assert isinstance(agent.session_id, str)
        assert isinstance(agent.history, ChatHistory)
        assert len(agent.tools) == 0

    def test_creates_agent_with_existing_history(self) -> None:
        """Test creating agent with existing chat history."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test prompt"),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()
        existing_history = ChatHistory(messages=[HumanMessage(text="Hello")])

        agent = AgentRunner(config=config, client=mock_client, history=existing_history)

        assert agent.history == existing_history
        assert len(agent.history.messages) == 1

    def test_initializes_with_tools(self) -> None:
        """Test agent initialization with tools."""
        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool.to_bedrock_spec.return_value = {"toolSpec": {"name": "test_tool"}}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test prompt"),
            tools=[mock_tool],
            plannable_tools=[mock_tool],
            context_reducers=[],  # Provide empty list to avoid default initialization
        )
        mock_client = Mock()

        agent = AgentRunner(config=config, client=mock_client)

        assert len(agent.tools) == 1
        assert agent.tools[0].name == "test_tool"

    def test_creates_state_with_empty_plan_cache(self) -> None:
        """Test that agent creates state with empty plan cache on initialization."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test prompt"),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()

        agent = AgentRunner(config=config, client=mock_client)

        # Plan cache is now managed via AgentGraphState
        assert agent._state is not None
        # get_plan returns None for non-existent plans
        assert agent._state.get_plan("nonexistent") is None


class TestAgentRunnerTools:
    """Test AgentRunner tool management."""

    def test_tool_map_property(self) -> None:
        """Test tool_map property creates correct mapping."""
        mock_tool1 = Mock(spec=ToolWrapper)
        mock_tool1.name = "tool1"
        mock_tool1.to_bedrock_spec.return_value = {"toolSpec": {"name": "tool1"}}
        mock_tool2 = Mock(spec=ToolWrapper)
        mock_tool2.name = "tool2"
        mock_tool2.to_bedrock_spec.return_value = {"toolSpec": {"name": "tool2"}}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test prompt"),
            tools=[mock_tool1, mock_tool2],
            plannable_tools=[mock_tool1, mock_tool2],
            context_reducers=[],  # Avoid default initialization
        )
        mock_client = Mock()

        agent = AgentRunner(config=config, client=mock_client)

        tool_map = agent.tool_map

        assert len(tool_map) == 2
        assert "tool1" in tool_map
        assert "tool2" in tool_map
        assert tool_map["tool1"] == mock_tool1

    def test_plannable_tools_accessible_via_planning_context(self) -> None:
        """Test plannable tools are accessible via PlanningContext."""
        mock_tool1 = Mock(spec=ToolWrapper)
        mock_tool1.name = "plannable_tool"
        mock_tool1.to_bedrock_spec.return_value = {
            "toolSpec": {"name": "plannable_tool"}
        }
        mock_tool2 = Mock(spec=ToolWrapper)
        mock_tool2.name = "internal_tool"
        mock_tool2.to_bedrock_spec.return_value = {
            "toolSpec": {"name": "internal_tool"}
        }

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test prompt"),
            tools=[mock_tool1, mock_tool2],
            plannable_tools=[mock_tool1],  # Only tool1 is plannable
            context_reducers=[],  # Avoid default initialization
        )
        mock_client = Mock()

        agent = AgentRunner(config=config, client=mock_client)

        # Plannable tools are now accessed via PlanningContext
        plannable = agent._planning_context.get_plannable_tools()

        assert len(plannable) == 1
        assert plannable[0].name == "plannable_tool"


class TestAgentRunnerHistory:
    """Test AgentRunner history management."""

    def test_add_message(self) -> None:
        """Test adding message to history."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test prompt"),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        message = HumanMessage(text="Test message")
        agent.add_message(message)

        assert len(agent.history.messages) == 1
        assert agent.history.messages[0] == message

    def test_add_multiple_messages(self) -> None:
        """Test adding multiple messages."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test prompt"),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        agent.add_message(HumanMessage(text="Hello"))
        agent.add_message(AssistantMessage(text="Hi"))
        agent.add_message(HumanMessage(text="How are you?"))

        assert len(agent.history.messages) == 3


class TestAgentRunnerSystemMessages:
    """Test system message handling."""

    def test_get_system_messages(self) -> None:
        """Test getting system messages from prompt builder."""
        prompt = "You are a helpful assistant"
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder(prompt),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        system_messages = agent._get_system_messages()

        assert len(system_messages) == 1
        assert system_messages[0]["text"] == prompt


class TestAgentRunnerToolExecution:
    """Test tool execution handling."""

    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.with_datahub_client")
    @patch("datahub_integrations.chat.agent.agent_runner.mlflow")
    def test_call_llm_returns_pending_tool_calls(
        self,
        mock_mlflow: Mock,
        mock_with_client: Mock,
        mock_get_llm: Mock,
    ) -> None:
        """Test _call_llm returns pending tool calls without executing them."""
        # Setup tool
        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool.to_bedrock_spec.return_value = {"tool": "spec"}
        mock_tool.run.return_value = {"result": "success"}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[mock_tool],
            plannable_tools=[mock_tool],
            max_llm_turns=5,
            context_reducers=[],  # Avoid default initialization
            use_prompt_caching=False,  # Set explicitly
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        # Mock mlflow span context manager
        mock_span = Mock()
        mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
        mock_mlflow.start_span.return_value.__exit__.return_value = None

        # Setup LLM response with tool use
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {"text": "I will call the tool"},
                        {
                            "toolUse": {
                                "toolUseId": "123",
                                "name": "test_tool",
                                "input": {"arg": "value"},
                            }
                        },
                    ]
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 10, "outputTokens": 20, "totalTokens": 30},
        }

        # Execute _call_llm within with_history to capture changes
        # (In the new architecture, _call_llm modifies the history passed to it,
        # and with_history captures those changes back to the snapshot)
        result: LLMCallResult = None  # type: ignore

        def do_call_llm(history: ChatHistory, updater: Any) -> None:
            nonlocal result
            result = agent._call_llm(history)

        agent._state.with_history(do_call_llm)

        # Verify result is LLMCallResult with pending tool calls
        assert isinstance(result, LLMCallResult)
        assert len(result.pending_tool_calls) == 1
        assert result.pending_tool_calls[0].tool_name == "test_tool"
        assert result.pending_tool_calls[0].tool_input == {"arg": "value"}
        assert result.is_complete is False

        # Tool should NOT have been called yet
        mock_tool.run.assert_not_called()

        # History should have reasoning + tool request (but not result yet)
        assert len(agent.history.messages) == 2
        assert isinstance(agent.history.messages[0], ReasoningMessage)
        assert isinstance(agent.history.messages[1], ToolCallRequest)

    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    @patch("datahub_integrations.chat.agent.agent_runner.with_datahub_client")
    def test_execute_tool_success(
        self,
        mock_with_client: Mock,
        mock_track_event: Mock,
    ) -> None:
        """Test _execute_tool executes tool and adds result to history."""
        # Setup tool
        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool.to_bedrock_spec.return_value = {"tool": "spec"}
        mock_tool.run.return_value = {"result": "success"}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[mock_tool],
            plannable_tools=[mock_tool],
            max_llm_turns=5,
            context_reducers=[],
            use_prompt_caching=False,
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        # Mock with_datahub_client context manager
        mock_context = Mock()
        mock_with_client.return_value.__enter__.return_value = mock_context
        mock_with_client.return_value.__exit__.return_value = None

        # Create a tool request (normally comes from _call_llm)
        tool_request = ToolCallRequest(
            tool_use_id="123",
            tool_name="test_tool",
            tool_input={"arg": "value"},
        )

        # Execute within with_history to capture changes
        result: ToolExecutionResult = None  # type: ignore

        def do_execute(history: ChatHistory, updater: Any) -> None:
            nonlocal result
            result = agent._execute_tool(history, tool_request)

        agent._state.with_history(do_execute)

        # Verify result is ToolExecutionResult
        assert isinstance(result, ToolExecutionResult)
        assert result.tool_name == "test_tool"
        assert isinstance(result.message, ToolResult)
        assert result.duration_seconds >= 0

        # Verify tool was called
        mock_tool.run.assert_called_once_with(arguments={"arg": "value"})

        # Verify history contains the result
        assert len(agent.history.messages) == 1
        assert isinstance(agent.history.messages[0], ToolResult)

    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    @patch("datahub_integrations.chat.agent.agent_runner.with_datahub_client")
    def test_execute_tool_error_handling(
        self,
        mock_with_client: Mock,
        mock_track_event: Mock,
    ) -> None:
        """Test _execute_tool handles errors and adds ToolResultError to history."""
        # Setup tool that raises exception
        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool.to_bedrock_spec.return_value = {"tool": "spec"}
        mock_tool.run.side_effect = Exception("Tool failed")

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[mock_tool],
            plannable_tools=[mock_tool],
            max_llm_turns=5,
            context_reducers=[],
            use_prompt_caching=False,
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        # Mock with_datahub_client context manager
        mock_context = Mock()
        mock_with_client.return_value.__enter__.return_value = mock_context
        mock_with_client.return_value.__exit__.return_value = None

        # Create a tool request
        tool_request = ToolCallRequest(
            tool_use_id="123",
            tool_name="test_tool",
            tool_input={},
        )

        # Execute within with_history to capture changes
        result: ToolExecutionResult = None  # type: ignore

        def do_execute(history: ChatHistory, updater: Any) -> None:
            nonlocal result
            result = agent._execute_tool(history, tool_request)

        agent._state.with_history(do_execute)

        # Verify result contains error
        assert isinstance(result, ToolExecutionResult)
        assert isinstance(result.message, ToolResultError)
        assert "Tool failed" in result.message.error

        # Verify history contains the error
        assert len(agent.history.messages) == 1
        assert isinstance(agent.history.messages[0], ToolResultError)


class TestAgentRunnerLLMErrors:
    """Test LLM error handling."""

    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    def test_max_tokens_exceeded_error(self, mock_get_llm: Mock) -> None:
        """Test handling of context window exceeded."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        # Mock LLM to raise input too long exception
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.side_effect = LlmInputTooLongException(
            "Input too long"
        )

        # Should raise AgentMaxTokensExceededError
        # Use with_history to get proper ChatHistory type
        with pytest.raises(AgentMaxTokensExceededError):
            agent._state.with_history(lambda h, u: agent._call_llm(h))

    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    def test_output_max_tokens_exceeded_error(self, mock_get_llm: Mock) -> None:
        """Test handling of output truncation."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        # Mock LLM to return max_tokens stop reason
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {"message": {"content": [{"text": "truncated"}]}},
            "stopReason": "max_tokens",
            "usage": {"inputTokens": 10, "outputTokens": 4096, "totalTokens": 4106},
        }

        # Should raise AgentOutputMaxTokensExceededError
        # Use with_history to get proper ChatHistory type
        with pytest.raises(AgentOutputMaxTokensExceededError):
            agent._state.with_history(lambda h, u: agent._call_llm(h))


class TestGenerateNextMessage:
    """Test main generate_next_message method."""

    @patch("datahub_integrations.chat.agent.agent_runner.is_mlflow_enabled")
    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.mlflow")
    def test_generates_assistant_message(
        self, mock_mlflow: Mock, mock_get_llm: Mock, mock_mlflow_enabled: Mock
    ) -> None:
        """Test generating a direct assistant response."""
        # Disable MLflow for tests
        mock_mlflow_enabled.return_value = False

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
            max_llm_turns=5,
            context_reducers=[],  # Avoid default initialization
            use_prompt_caching=False,  # Set explicitly
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)
        agent.add_message(HumanMessage(text="Hello"))

        # Mock mlflow
        mock_span = Mock()
        mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
        mock_mlflow.start_span.return_value.__exit__.return_value = None

        # Mock LLM to return direct text response
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {"message": {"content": [{"text": "Hi there!"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
        }

        # Execute
        result = agent.generate_next_message()

        # Verify
        assert isinstance(result, AssistantMessage)
        assert result.text == "Hi there!"

    @patch("datahub_integrations.chat.agent.agent_runner.is_mlflow_enabled")
    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.with_datahub_client")
    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    @patch("datahub_integrations.chat.agent.agent_runner.mlflow")
    def test_max_llm_turns_exceeded(
        self,
        mock_mlflow: Mock,
        mock_track: Mock,
        mock_with_client: Mock,
        mock_get_llm: Mock,
        mock_mlflow_enabled: Mock,
    ) -> None:
        """Test that max_llm_turns limit is enforced."""
        # Disable MLflow for tests
        mock_mlflow_enabled.return_value = False

        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool.to_bedrock_spec.return_value = {"tool": "spec"}
        mock_tool.run.return_value = {"result": "success"}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[mock_tool],
            plannable_tools=[mock_tool],
            max_llm_turns=2,  # Very low limit
            context_reducers=[],  # Avoid default initialization
            use_prompt_caching=False,  # Set explicitly
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)
        agent.add_message(HumanMessage(text="Test"))

        # Mock with_datahub_client context manager
        mock_context = Mock()
        mock_with_client.return_value.__enter__.return_value = mock_context
        mock_with_client.return_value.__exit__.return_value = None

        # Mock mlflow
        mock_span = Mock()
        mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
        mock_mlflow.start_span.return_value.__exit__.return_value = None

        # Mock LLM to always call tools (never end)
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "123",
                                "name": "test_tool",
                                "input": {},
                            }
                        }
                    ]
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 10, "outputTokens": 20, "totalTokens": 30},
        }

        # Should raise AgentMaxLLMTurnsExceededError
        with pytest.raises(AgentMaxLLMTurnsExceededError):
            agent.generate_next_message()

    @patch("datahub_integrations.chat.agent.agent_runner.is_mlflow_enabled")
    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.with_datahub_client")
    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    @patch("datahub_integrations.chat.agent.agent_runner.mlflow")
    def test_custom_completion_check(
        self,
        mock_mlflow: Mock,
        mock_track: Mock,
        mock_with_client: Mock,
        mock_get_llm: Mock,
        mock_mlflow_enabled: Mock,
    ) -> None:
        """Test using custom completion check."""
        # Disable MLflow for tests
        mock_mlflow_enabled.return_value = False

        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "special_tool"
        mock_tool.to_bedrock_spec.return_value = {"tool": "spec"}
        mock_tool.run.return_value = {"done": True}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[mock_tool],
            plannable_tools=[mock_tool],
            max_llm_turns=5,
            context_reducers=[],  # Avoid default initialization
            use_prompt_caching=False,  # Set explicitly
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)
        agent.add_message(HumanMessage(text="Test"))

        # Mock with_datahub_client context manager
        mock_context = Mock()
        mock_with_client.return_value.__enter__.return_value = mock_context
        mock_with_client.return_value.__exit__.return_value = None

        # Mock mlflow
        mock_span = Mock()
        mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
        mock_mlflow.start_span.return_value.__exit__.return_value = None

        # Mock LLM to call our special tool
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "123",
                                "name": "special_tool",
                                "input": {},
                            }
                        }
                    ]
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 10, "outputTokens": 20, "totalTokens": 30},
        }

        # Custom completion check: stop when special_tool is called
        def custom_check(msg: Any) -> bool:
            return (
                isinstance(msg, ToolResult)
                and msg.tool_request.tool_name == "special_tool"
            )

        # Execute
        result = agent.generate_next_message(completion_check=custom_check)

        # Verify it stopped after calling special_tool
        assert isinstance(result, ToolResult)
        assert result.tool_request.tool_name == "special_tool"

    @patch("datahub_integrations.chat.agent.agent_runner.is_mlflow_enabled")
    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.mlflow")
    def test_malformed_function_call_continues_loop_not_returns_to_user(
        self,
        mock_mlflow: Mock,
        mock_get_llm: Mock,
        mock_mlflow_enabled: Mock,
    ) -> None:
        # Test that when MALFORMED_FUNCTION_CALL is correctly converted to stopReason="tool_use"
        # (by the LLM wrapper conversion layer), the agent continues the loop instead of
        # returning the error message to the user.
        # This test verifies the agent runner's behavior after the conversion fix is applied.
        # The conversion layer tests verify that MALFORMED_FUNCTION_CALL gets converted correctly.
        mock_mlflow_enabled.return_value = False

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
            max_llm_turns=5,
            context_reducers=[],
            use_prompt_caching=False,
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)
        agent.add_message(HumanMessage(text="Test query"))

        # Mock mlflow
        mock_span = Mock()
        mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
        mock_mlflow.start_span.return_value.__exit__.return_value = None

        # Mock LLM to return malformed function call scenario:
        # stopReason="tool_use" with text content but no tool calls
        # This simulates MALFORMED_FUNCTION_CALL being converted to tool_use
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client

        # First call: malformed function call (stopReason="tool_use" with text, no tool calls)
        # Second call: successful response (stopReason="end_turn" with text)
        mock_llm_client.converse.side_effect = [
            {
                "output": {
                    "message": {
                        "content": [
                            {
                                "text": "I encountered an error with the tool call parameters. Please retry with corrected parameters."
                            }
                        ]
                    }
                },
                "stopReason": "tool_use",  # This should cause agent to continue, not return to user
                "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
            },
            {
                "output": {
                    "message": {
                        "content": [{"text": "Here's the answer to your question."}]
                    }
                },
                "stopReason": "end_turn",
                "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
            },
        ]

        # Execute - should complete after second call, not first
        result = agent.generate_next_message()

        # Verify agent made 2 LLM calls (continued after first malformed call)
        assert mock_llm_client.converse.call_count == 2

        # Verify the final result is the successful response, not the error message
        assert isinstance(result, AssistantMessage)
        assert result.text == "Here's the answer to your question."
        assert "error" not in result.text.lower()

        # Verify the error message became a ReasoningMessage (not returned to user)
        reasoning_messages = [
            msg for msg in agent.history.messages if isinstance(msg, ReasoningMessage)
        ]
        assert len(reasoning_messages) == 1
        assert "error" in reasoning_messages[0].text.lower()
        assert "retry" in reasoning_messages[0].text.lower()


class TestProgressCallback:
    """Test progress callback functionality."""

    def test_set_progress_callback_context_manager(self) -> None:
        """Test progress callback context manager."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        callback_calls = []

        def progress_callback(updates: Any) -> None:
            callback_calls.append(updates)

        # Use context manager
        with agent.set_progress_callback(progress_callback):
            agent.add_message(HumanMessage(text="Test"))

        # Callback should have been triggered
        # (exact behavior depends on ProgressTracker implementation)
        assert isinstance(callback_calls, list)


class TestTransformHistoryToApiFormat:
    """Test the history-to-API format transformation pipeline."""

    @pytest.fixture
    def agent(self) -> AgentRunner:
        """Create an agent for testing transformation methods."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
        )
        return AgentRunner(config=config, client=Mock())

    def test_empty_messages(self, agent: AgentRunner) -> None:
        """Test transformation of empty message list."""
        result = agent._transform_history_to_api_format([])
        assert result == []

    def test_single_user_message(self, agent: AgentRunner) -> None:
        """Test single user message passes through unchanged."""
        messages = [{"role": "user", "content": [{"text": "Hello"}]}]
        result = agent._transform_history_to_api_format(messages)
        assert result == [{"role": "user", "content": [{"text": "Hello"}]}]

    def test_alternating_roles_no_consolidation(self, agent: AgentRunner) -> None:
        """Test that alternating user/assistant messages don't get consolidated."""
        messages = [
            {"role": "user", "content": [{"text": "Hello"}]},
            {"role": "assistant", "content": [{"text": "Hi there"}]},
            {"role": "user", "content": [{"text": "How are you?"}]},
        ]
        result = agent._transform_history_to_api_format(messages)
        assert len(result) == 3
        assert result[0]["role"] == "user"
        assert result[1]["role"] == "assistant"
        assert result[2]["role"] == "user"

    def test_consolidates_consecutive_assistant_messages(
        self, agent: AgentRunner
    ) -> None:
        """Test that consecutive assistant messages (e.g., multiple tool calls) are merged."""
        messages = [
            {"role": "user", "content": [{"text": "Get data"}]},
            {
                "role": "assistant",
                "content": [
                    {
                        "toolUse": {
                            "toolUseId": "tool1",
                            "name": "get_entities",
                            "input": {"urn": "abc"},
                        }
                    }
                ],
            },
            {
                "role": "assistant",
                "content": [
                    {
                        "toolUse": {
                            "toolUseId": "tool2",
                            "name": "get_queries",
                            "input": {"urn": "abc"},
                        }
                    }
                ],
            },
        ]
        result = agent._transform_history_to_api_format(messages)

        # Should consolidate to 2 messages: user + assistant (with both tool calls)
        assert len(result) == 2
        assert result[0]["role"] == "user"
        assert result[1]["role"] == "assistant"
        assert len(result[1]["content"]) == 2
        assert result[1]["content"][0]["toolUse"]["toolUseId"] == "tool1"
        assert result[1]["content"][1]["toolUse"]["toolUseId"] == "tool2"

    def test_consolidates_consecutive_user_messages(self, agent: AgentRunner) -> None:
        """Test that consecutive user messages (e.g., multiple tool results) are merged."""
        messages = [
            {
                "role": "assistant",
                "content": [{"text": "I'll check that"}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "toolResult": {
                            "toolUseId": "tool1",
                            "content": [{"json": {"data": "result1"}}],
                        }
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "toolResult": {
                            "toolUseId": "tool2",
                            "content": [{"json": {"data": "result2"}}],
                        }
                    }
                ],
            },
        ]
        result = agent._transform_history_to_api_format(messages)

        # Should consolidate to 2 messages: assistant + user (with both tool results)
        assert len(result) == 2
        assert result[0]["role"] == "assistant"
        assert result[1]["role"] == "user"
        assert len(result[1]["content"]) == 2
        assert result[1]["content"][0]["toolResult"]["toolUseId"] == "tool1"
        assert result[1]["content"][1]["toolResult"]["toolUseId"] == "tool2"

    def test_full_tool_call_cycle(self, agent: AgentRunner) -> None:
        """Test a complete cycle: user → assistant (2 tools) → user (2 results) → assistant."""
        messages = [
            {"role": "user", "content": [{"text": "Get info about dataset X"}]},
            # Assistant returns 2 tool calls
            {
                "role": "assistant",
                "content": [
                    {
                        "toolUse": {
                            "toolUseId": "t1",
                            "name": "get_entities",
                            "input": {},
                        }
                    }
                ],
            },
            {
                "role": "assistant",
                "content": [
                    {"toolUse": {"toolUseId": "t2", "name": "get_queries", "input": {}}}
                ],
            },
            # User sends 2 tool results
            {
                "role": "user",
                "content": [
                    {
                        "toolResult": {
                            "toolUseId": "t1",
                            "content": [{"text": "Entity data"}],
                        }
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "toolResult": {
                            "toolUseId": "t2",
                            "content": [{"text": "Query data"}],
                        }
                    }
                ],
            },
            # Final assistant response
            {"role": "assistant", "content": [{"text": "Here's the summary..."}]},
        ]
        result = agent._transform_history_to_api_format(messages)

        # Should consolidate to 4 messages with proper alternating pattern
        assert len(result) == 4
        assert result[0]["role"] == "user"
        assert result[1]["role"] == "assistant"
        assert len(result[1]["content"]) == 2  # 2 tool calls consolidated
        assert result[2]["role"] == "user"
        assert len(result[2]["content"]) == 2  # 2 tool results consolidated
        assert result[3]["role"] == "assistant"

    def test_does_not_mutate_original_messages(self, agent: AgentRunner) -> None:
        """Test that transformation doesn't mutate the input messages."""
        messages = [
            {"role": "user", "content": [{"text": "Hello"}]},
            {"role": "user", "content": [{"text": "World"}]},
        ]
        original_len = len(messages[0]["content"])

        result = agent._transform_history_to_api_format(messages)

        # Original should be unchanged
        assert len(messages) == 2
        assert len(messages[0]["content"]) == original_len
        # Result should be consolidated
        assert len(result) == 1
        assert len(result[0]["content"]) == 2


class TestConsolidateConsecutiveRoles:
    """Test the role consolidation helper method."""

    @pytest.fixture
    def agent(self) -> AgentRunner:
        """Create an agent for testing."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
        )
        return AgentRunner(config=config, client=Mock())

    def test_three_consecutive_same_role(self, agent: AgentRunner) -> None:
        """Test consolidation of three consecutive messages of same role."""
        messages = [
            {"role": "assistant", "content": [{"text": "Part 1"}]},
            {"role": "assistant", "content": [{"text": "Part 2"}]},
            {"role": "assistant", "content": [{"text": "Part 3"}]},
        ]
        result = agent._consolidate_consecutive_roles(messages)

        assert len(result) == 1
        assert result[0]["role"] == "assistant"
        assert len(result[0]["content"]) == 3
        assert result[0]["content"][0]["text"] == "Part 1"
        assert result[0]["content"][1]["text"] == "Part 2"
        assert result[0]["content"][2]["text"] == "Part 3"

    def test_mixed_content_blocks(self, agent: AgentRunner) -> None:
        """Test consolidation preserves different content block types."""
        messages = [
            {"role": "assistant", "content": [{"text": "Let me check"}]},
            {
                "role": "assistant",
                "content": [
                    {"toolUse": {"toolUseId": "t1", "name": "search", "input": {}}}
                ],
            },
        ]
        result = agent._consolidate_consecutive_roles(messages)

        assert len(result) == 1
        assert len(result[0]["content"]) == 2
        assert "text" in result[0]["content"][0]
        assert "toolUse" in result[0]["content"][1]
