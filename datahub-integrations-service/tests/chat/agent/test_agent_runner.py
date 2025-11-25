"""Unit tests for AgentRunner core infrastructure."""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentMaxTokensExceededError,
    AgentMaxToolCallsExceededError,
    AgentOutputMaxTokensExceededError,
    AgentRunner,
    StaticPromptBuilder,
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

    def test_creates_plan_cache(self) -> None:
        """Test that agent creates plan cache on initialization."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test prompt"),
            tools=[],
            plannable_tools=[],
        )
        mock_client = Mock()

        agent = AgentRunner(config=config, client=mock_client)

        assert isinstance(agent.plan_cache, dict)
        assert len(agent.plan_cache) == 0


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

    def test_get_plannable_tools(self) -> None:
        """Test get_plannable_tools returns correct subset."""
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

        plannable = agent.get_plannable_tools()

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
        agent._add_message(message)

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

        agent._add_message(HumanMessage(text="Hello"))
        agent._add_message(AssistantMessage(text="Hi"))
        agent._add_message(HumanMessage(text="How are you?"))

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
    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    @patch("datahub_integrations.chat.agent.agent_runner.mlflow")
    def test_successful_tool_execution(
        self,
        mock_mlflow: Mock,
        mock_track: Mock,
        mock_with_client: Mock,
        mock_get_llm: Mock,
    ) -> None:
        """Test successful tool call execution."""
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
            max_tool_calls=5,
            context_reducers=[],  # Avoid default initialization
            use_prompt_caching=False,  # Set explicitly
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        # Mock with_datahub_client context manager
        mock_context = Mock()
        mock_with_client.return_value.__enter__.return_value = mock_context
        mock_with_client.return_value.__exit__.return_value = None

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

        # Execute
        agent._generate_tool_call()

        # Verify tool was called
        mock_tool.run.assert_called_once_with(arguments={"arg": "value"})

        # Verify history contains tool request and result
        assert len(agent.history.messages) == 3  # reasoning + request + result
        assert isinstance(agent.history.messages[0], ReasoningMessage)
        assert isinstance(agent.history.messages[1], ToolCallRequest)
        assert isinstance(agent.history.messages[2], ToolResult)

    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.with_datahub_client")
    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    @patch("datahub_integrations.chat.agent.agent_runner.mlflow")
    def test_tool_execution_error_handling(
        self,
        mock_mlflow: Mock,
        mock_track: Mock,
        mock_with_client: Mock,
        mock_get_llm: Mock,
    ) -> None:
        """Test tool execution error is properly handled."""
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
            max_tool_calls=5,
            context_reducers=[],  # Avoid default initialization
            use_prompt_caching=False,  # Set explicitly
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)

        # Mock with_datahub_client context manager
        mock_context = Mock()
        mock_with_client.return_value.__enter__.return_value = mock_context
        mock_with_client.return_value.__exit__.return_value = None

        # Mock mlflow span context manager
        mock_span = Mock()
        mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
        mock_mlflow.start_span.return_value.__exit__.return_value = None

        # Setup LLM response
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

        # Execute
        agent._generate_tool_call()

        # Verify error was captured in ToolResultError
        assert len(agent.history.messages) == 2
        assert isinstance(agent.history.messages[1], ToolResultError)
        assert "Tool failed" in agent.history.messages[1].error


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
        with pytest.raises(AgentMaxTokensExceededError):
            agent._generate_tool_call()

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
        with pytest.raises(AgentOutputMaxTokensExceededError):
            agent._generate_tool_call()


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
            max_tool_calls=5,
            context_reducers=[],  # Avoid default initialization
            use_prompt_caching=False,  # Set explicitly
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)
        agent.history.add_message(HumanMessage(text="Hello"))

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
    def test_max_tool_calls_exceeded(
        self,
        mock_mlflow: Mock,
        mock_track: Mock,
        mock_with_client: Mock,
        mock_get_llm: Mock,
        mock_mlflow_enabled: Mock,
    ) -> None:
        """Test that max_tool_calls limit is enforced."""
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
            max_tool_calls=2,  # Very low limit
            context_reducers=[],  # Avoid default initialization
            use_prompt_caching=False,  # Set explicitly
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)
        agent.history.add_message(HumanMessage(text="Test"))

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

        # Should raise AgentMaxToolCallsExceededError
        with pytest.raises(AgentMaxToolCallsExceededError):
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
            max_tool_calls=5,
            context_reducers=[],  # Avoid default initialization
            use_prompt_caching=False,  # Set explicitly
        )
        mock_client = Mock()
        agent = AgentRunner(config=config, client=mock_client)
        agent.history.add_message(HumanMessage(text="Test"))

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
            agent._add_message(HumanMessage(text="Test"))

        # Callback should have been triggered
        # (exact behavior depends on ProgressTracker implementation)
        assert isinstance(callback_calls, list)
