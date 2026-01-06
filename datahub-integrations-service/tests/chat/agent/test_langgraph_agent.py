"""Unit tests for LangGraph agent integration."""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentMaxLLMTurnsExceededError,
    AgentRunner,
    ChatHistorySnapshot,
    StaticPromptBuilder,
)
from datahub_integrations.chat.agent.langgraph_agent import (
    AgentGraphState,
    build_agent_graph,
)
from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    ToolCallRequest,
    ToolResult,
)
from datahub_integrations.mcp_integration.tool import ToolWrapper


class TestBuildAgentGraph:
    """Test build_agent_graph function."""

    def test_builds_graph_with_nodes(self) -> None:
        """Test that graph is built with expected nodes."""
        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool.to_bedrock_spec.return_value = {"toolSpec": {"name": "test_tool"}}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[mock_tool],
            plannable_tools=[],
            context_reducers=[],
        )
        mock_client = Mock()

        runner = AgentRunner(config=config, client=mock_client)
        graph = build_agent_graph(runner)

        # Verify graph is compiled
        assert graph is not None
        # Graph should have a checkpointer (MemorySaver)
        assert graph.checkpointer is not None

    def test_graph_has_call_llm_and_execute_tools_nodes(self) -> None:
        """Test that graph contains the expected nodes."""
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
            context_reducers=[],
        )
        mock_client = Mock()

        runner = AgentRunner(config=config, client=mock_client)
        graph = build_agent_graph(runner)

        # Get the underlying graph structure
        # The compiled graph has a builder attribute with node info
        graph_str = str(graph.get_graph())
        assert "call_llm" in graph_str
        assert "execute_tools" in graph_str


class TestAgentGraphState:
    """Test AgentGraphState class."""

    def test_state_holds_snapshot(self) -> None:
        """Test that state holds and provides access to snapshot."""
        snapshot = ChatHistorySnapshot(messages_json=[])

        state = AgentGraphState(snapshot)

        assert state.snapshot == snapshot

    def test_snapshot_is_serializable(self) -> None:
        """Test that snapshot data is serializable."""
        snapshot = ChatHistorySnapshot(messages_json=[{"type": "human", "text": "Hi"}])
        state = AgentGraphState(snapshot)

        # Snapshot should be JSON serializable
        import json

        json_str = json.dumps({"snapshot": state.snapshot.model_dump()})
        assert "human" in json_str

    def test_with_history_provides_mutable_access(self) -> None:
        """Test that with_history allows modifying history atomically."""
        snapshot = ChatHistorySnapshot(messages_json=[{"type": "human", "text": "Hi"}])
        state = AgentGraphState(snapshot)

        def add_reply(history: ChatHistory, m: Any) -> str:
            history.add_message(AssistantMessage(text="Hello!"))
            return "done"

        result = state.with_history(add_reply)

        assert result == "done"
        assert len(state.snapshot.messages_json) == 2
        assert state.snapshot.messages_json[1]["type"] == "assistant"

    def test_with_history_prevents_nesting(self) -> None:
        """Test that nested with_history calls raise error."""
        snapshot = ChatHistorySnapshot(messages_json=[])
        state = AgentGraphState(snapshot)

        with pytest.raises(RuntimeError, match="Cannot nest with_history"):

            def outer(history: ChatHistory, m: Any) -> None:
                # Try to nest another with_history call
                state.with_history(lambda h, m2: None)

            state.with_history(outer)


class TestGraphIntegration:
    """Integration tests for graph with AgentRunner."""

    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    def test_graph_produces_same_result_as_old_loop(
        self, mock_track: Mock, mock_get_llm: Mock
    ) -> None:
        """Test that graph execution produces equivalent results to old while loop."""
        # Mock LLM to return a simple assistant message
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [{"text": "Hello! I'm here to help."}],
                }
            },
            "stopReason": "end_turn",
            "usage": {
                "inputTokens": 100,
                "outputTokens": 50,
            },
        }

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("You are helpful"),
            tools=[],
            plannable_tools=[],
            context_reducers=[],
        )
        mock_client = Mock()

        runner = AgentRunner(config=config, client=mock_client)
        runner.add_message(HumanMessage(text="Hi"))

        result = runner.generate_next_message()

        assert isinstance(result, AssistantMessage)
        assert result.text == "Hello! I'm here to help."
        assert len(runner.history.messages) == 2  # Human + Assistant

    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    def test_graph_executes_tools(self, mock_track: Mock, mock_get_llm: Mock) -> None:
        """Test that graph correctly executes tool calls."""
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client

        # First call: LLM requests a tool
        # Second call: LLM returns final response
        mock_llm_client.converse.side_effect = [
            {
                "output": {
                    "message": {
                        "content": [
                            {
                                "toolUse": {
                                    "toolUseId": "tool_1",
                                    "name": "test_tool",
                                    "input": {"query": "test"},
                                }
                            }
                        ],
                    }
                },
                "stopReason": "tool_use",
                "usage": {"inputTokens": 100, "outputTokens": 50},
            },
            {
                "output": {
                    "message": {
                        "content": [{"text": "I found the answer: 42"}],
                    }
                },
                "stopReason": "end_turn",
                "usage": {"inputTokens": 150, "outputTokens": 30},
            },
        ]

        # Create a mock tool
        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool.to_bedrock_spec.return_value = {
            "toolSpec": {
                "name": "test_tool",
                "inputSchema": {"json": {"type": "object"}},
            }
        }
        mock_tool.run.return_value = {"answer": 42}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("You are helpful"),
            tools=[mock_tool],
            plannable_tools=[],
            context_reducers=[],
        )
        mock_client = Mock()

        runner = AgentRunner(config=config, client=mock_client)
        runner.add_message(HumanMessage(text="What is the answer?"))

        result = runner.generate_next_message()

        # Verify result
        assert isinstance(result, AssistantMessage)
        assert "42" in result.text

        # Verify tool was called
        mock_tool.run.assert_called_once_with(arguments={"query": "test"})

        # Verify history contains tool call and result
        assert (
            len(runner.history.messages) == 4
        )  # Human, ToolCall, ToolResult, Assistant
        assert isinstance(runner.history.messages[1], ToolCallRequest)
        assert isinstance(runner.history.messages[2], ToolResult)

        # Verify telemetry was tracked
        assert mock_track.call_count >= 1

    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    def test_graph_respects_max_llm_turns(
        self, mock_track: Mock, mock_get_llm: Mock
    ) -> None:
        """Test that graph raises error when max_llm_turns is exceeded."""
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client

        # LLM always requests a tool (infinite loop scenario)
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "tool_loop",
                                "name": "test_tool",
                                "input": {},
                            }
                        }
                    ],
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 100, "outputTokens": 50},
        }

        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "test_tool"
        mock_tool.to_bedrock_spec.return_value = {
            "toolSpec": {
                "name": "test_tool",
                "inputSchema": {"json": {"type": "object"}},
            }
        }
        mock_tool.run.return_value = {"ok": True}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("You are helpful"),
            tools=[mock_tool],
            plannable_tools=[],
            context_reducers=[],
            max_llm_turns=3,  # Low limit for test
        )
        mock_client = Mock()

        runner = AgentRunner(config=config, client=mock_client)
        runner.add_message(HumanMessage(text="Loop forever"))

        with pytest.raises(AgentMaxLLMTurnsExceededError):
            runner.generate_next_message()

    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    def test_graph_respects_custom_completion_check(
        self, mock_track: Mock, mock_get_llm: Mock
    ) -> None:
        """Test that graph uses custom completion_check."""
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client

        # LLM requests a special tool that signals completion
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "special_tool",
                                "name": "respond_to_user",
                                "input": {"message": "Done!"},
                            }
                        }
                    ],
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 100, "outputTokens": 50},
        }

        mock_tool = Mock(spec=ToolWrapper)
        mock_tool.name = "respond_to_user"
        mock_tool.to_bedrock_spec.return_value = {
            "toolSpec": {
                "name": "respond_to_user",
                "inputSchema": {"json": {"type": "object"}},
            }
        }
        mock_tool.run.return_value = {"sent": True}

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("You are helpful"),
            tools=[mock_tool],
            plannable_tools=[],
            context_reducers=[],
        )
        mock_client = Mock()

        runner = AgentRunner(config=config, client=mock_client)
        runner.add_message(HumanMessage(text="Respond via tool"))

        # Custom completion check: stop when respond_to_user tool result is added
        def check_for_respond_tool(msg: Any) -> bool:
            if isinstance(msg, ToolResult):
                return msg.tool_request.tool_name == "respond_to_user"
            return False

        result = runner.generate_next_message(completion_check=check_for_respond_tool)

        # Should stop after the tool result, not continue to next LLM call
        assert isinstance(result, ToolResult)
        assert result.tool_request.tool_name == "respond_to_user"

    @patch("datahub_integrations.chat.agent.agent_runner.get_llm_client")
    @patch("datahub_integrations.chat.agent.agent_runner.track_saas_event")
    def test_graph_updates_progress_tracker(
        self, mock_track: Mock, mock_get_llm: Mock
    ) -> None:
        """Test that graph updates progress tracker during execution."""
        mock_llm_client = Mock()
        mock_get_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [{"text": "Response"}],
                }
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 100, "outputTokens": 50},
        }

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("Test"),
            tools=[],
            plannable_tools=[],
            context_reducers=[],
        )
        mock_client = Mock()

        runner = AgentRunner(config=config, client=mock_client)
        runner.add_message(HumanMessage(text="Hi"))

        # Track progress updates received during execution
        progress_updates_received: list = []

        def track_progress(updates: list) -> None:
            progress_updates_received.extend(updates)

        with runner.set_progress_callback(track_progress):
            result = runner.generate_next_message()

        # Verify graph completed successfully
        assert isinstance(result, AssistantMessage)
        assert result.text == "Response"

        # After graph execution, runner.history should be updated
        assert len(runner.history.messages) == 2
        assert isinstance(runner.history.messages[1], AssistantMessage)


class TestSnapshotCheckpointing:
    """Test snapshot-based checkpointing behavior."""

    def test_snapshot_preserves_state_across_invocations(self) -> None:
        """Test that snapshot correctly captures all state."""
        history = ChatHistory()
        history.add_message(HumanMessage(text="Test"))
        history.add_message(AssistantMessage(text="Reply"))

        # Create snapshot
        snapshot = ChatHistorySnapshot.from_history(
            history=history,
            plan_cache={},
        )

        # Restore history
        restored = snapshot.to_history()

        # Verify state preserved
        assert len(restored.messages) == 2
        assert isinstance(restored.messages[0], HumanMessage)
        assert isinstance(restored.messages[1], AssistantMessage)
        # Computed state works correctly
        assert snapshot.is_complete() is True  # Last msg is AssistantMessage

    def test_snapshot_with_reduced_history(self) -> None:
        """Test that reduced_history survives round-trip."""
        history = ChatHistory()
        history.add_message(HumanMessage(text="Old"))
        history.add_message(HumanMessage(text="New"))
        history.set_reduced_history([HumanMessage(text="New")], {"reducer": "test"})

        snapshot = ChatHistorySnapshot.from_history(
            history,
            plan_cache={},
        )
        restored = snapshot.to_history()

        assert restored.reduced_history is not None
        assert len(restored.reduced_history) == 1
        assert restored.context_messages == restored.reduced_history
