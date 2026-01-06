"""Unit tests for ChatHistorySnapshot serialization and round-trip."""

from datahub_integrations.chat.agent.history_snapshot import (
    ChatHistorySnapshot,
    PlanCacheEntry,
    _parse_message,
)
from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    ReasoningMessage,
    SummaryMessage,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)


class TestParseMessage:
    """Test the _parse_message function for all message types."""

    def test_parses_human_message(self) -> None:
        """Test parsing HumanMessage from dict."""
        msg_dict = {"type": "human", "text": "Hello, world!"}
        result = _parse_message(msg_dict)

        assert isinstance(result, HumanMessage)
        assert result.text == "Hello, world!"

    def test_parses_assistant_message(self) -> None:
        """Test parsing AssistantMessage from dict."""
        msg_dict = {"type": "assistant", "text": "Hello! How can I help?"}
        result = _parse_message(msg_dict)

        assert isinstance(result, AssistantMessage)
        assert result.text == "Hello! How can I help?"

    def test_parses_reasoning_message(self) -> None:
        """Test parsing ReasoningMessage from dict."""
        msg_dict = {
            "type": "internal",
            "text": "Let me think about this...",
            "plan_id": "plan_123",
            "plan_step": "s1",
            "step_status": "in_progress",
            "plan_status": "active",
        }
        result = _parse_message(msg_dict)

        assert isinstance(result, ReasoningMessage)
        assert result.text == "Let me think about this..."
        assert result.plan_id == "plan_123"
        assert result.plan_step == "s1"
        assert result.step_status == "in_progress"
        assert result.plan_status == "active"

    def test_parses_tool_call_request(self) -> None:
        """Test parsing ToolCallRequest from dict."""
        msg_dict = {
            "type": "tool_call",
            "tool_use_id": "tool_123",
            "tool_name": "search",
            "tool_input": {"query": "test"},
        }
        result = _parse_message(msg_dict)

        assert isinstance(result, ToolCallRequest)
        assert result.tool_use_id == "tool_123"
        assert result.tool_name == "search"
        assert result.tool_input == {"query": "test"}

    def test_parses_tool_result(self) -> None:
        """Test parsing ToolResult from dict."""
        msg_dict = {
            "type": "tool_result",
            "tool_request": {
                "type": "tool_call",
                "tool_use_id": "tool_123",
                "tool_name": "search",
                "tool_input": {"query": "test"},
            },
            "result": {"found": True, "items": ["item1", "item2"]},
        }
        result = _parse_message(msg_dict)

        assert isinstance(result, ToolResult)
        assert result.tool_request.tool_use_id == "tool_123"
        assert result.result == {"found": True, "items": ["item1", "item2"]}

    def test_parses_tool_result_error(self) -> None:
        """Test parsing ToolResultError from dict."""
        msg_dict = {
            "type": "tool_result_error",
            "tool_request": {
                "type": "tool_call",
                "tool_use_id": "tool_456",
                "tool_name": "dangerous_op",
                "tool_input": {},
            },
            "error": "Permission denied",
        }
        result = _parse_message(msg_dict)

        assert isinstance(result, ToolResultError)
        assert result.tool_request.tool_use_id == "tool_456"
        assert result.error == "Permission denied"

    def test_parses_summary_message(self) -> None:
        """Test parsing SummaryMessage from dict."""
        msg_dict = {"type": "summary", "text": "Summary of conversation..."}
        result = _parse_message(msg_dict)

        assert isinstance(result, SummaryMessage)
        assert result.text == "Summary of conversation..."


class TestChatHistorySnapshotFromHistory:
    """Test ChatHistorySnapshot.from_history factory method."""

    def test_creates_snapshot_from_empty_history(self) -> None:
        """Test creating snapshot from empty ChatHistory."""
        history = ChatHistory()
        snapshot = ChatHistorySnapshot.from_history(
            history,
            plan_cache={},
        )

        assert snapshot.messages_json == []
        assert snapshot.reduced_history_json is None
        assert snapshot.extra_properties == {}
        assert snapshot.pending_approval_json is None
        assert snapshot.plan_cache_json == {}
        assert snapshot.schema_version == 1
        # Computed state from empty history
        assert snapshot.get_pending_tool_calls() == []
        assert snapshot.is_complete() is False
        assert snapshot.get_llm_turns_used() == 0

    def test_creates_snapshot_with_messages(self) -> None:
        """Test creating snapshot from ChatHistory with messages."""
        history = ChatHistory()
        history.add_message(HumanMessage(text="Hello"))
        history.add_message(AssistantMessage(text="Hi there!"))

        snapshot = ChatHistorySnapshot.from_history(
            history,
            plan_cache={},
        )

        assert len(snapshot.messages_json) == 2
        assert snapshot.messages_json[0]["type"] == "human"
        assert snapshot.messages_json[1]["type"] == "assistant"

    def test_creates_snapshot_with_reduced_history(self) -> None:
        """Test creating snapshot from ChatHistory with reduced_history."""
        history = ChatHistory()
        history.add_message(HumanMessage(text="Message 1"))
        history.add_message(AssistantMessage(text="Reply 1"))
        history.add_message(HumanMessage(text="Message 2"))

        # Simulate context reduction
        history.set_reduced_history(
            [SummaryMessage(text="Summary"), HumanMessage(text="Message 2")],
            {"reducer": "test_reducer", "messages_removed": 2},
        )

        snapshot = ChatHistorySnapshot.from_history(
            history,
            plan_cache={},
        )

        assert len(snapshot.messages_json) == 3  # Full history
        assert snapshot.reduced_history_json is not None
        assert len(snapshot.reduced_history_json) == 2  # Reduced history
        assert snapshot.reduced_history_json[0]["type"] == "summary"
        assert "reducers" in snapshot.extra_properties


class TestChatHistorySnapshotToHistory:
    """Test ChatHistorySnapshot.to_history method."""

    def test_restores_empty_history(self) -> None:
        """Test restoring empty ChatHistory from snapshot."""
        snapshot = ChatHistorySnapshot(messages_json=[])
        history = snapshot.to_history()

        assert len(history.messages) == 0
        assert history.reduced_history is None
        assert history.extra_properties == {}

    def test_restores_history_with_messages(self) -> None:
        """Test restoring ChatHistory with messages."""
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {"type": "human", "text": "Hello"},
                {"type": "assistant", "text": "Hi there!"},
            ]
        )
        history = snapshot.to_history()

        assert len(history.messages) == 2
        assert isinstance(history.messages[0], HumanMessage)
        assert isinstance(history.messages[1], AssistantMessage)
        assert history.messages[0].text == "Hello"
        assert history.messages[1].text == "Hi there!"

    def test_restores_reduced_history(self) -> None:
        """Test restoring reduced_history from snapshot."""
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {"type": "human", "text": "Message 1"},
                {"type": "human", "text": "Message 2"},
            ],
            reduced_history_json=[
                {"type": "summary", "text": "Summary of messages"},
                {"type": "human", "text": "Message 2"},
            ],
        )
        history = snapshot.to_history()

        assert history.reduced_history is not None
        assert len(history.reduced_history) == 2
        assert isinstance(history.reduced_history[0], SummaryMessage)

    def test_restores_extra_properties(self) -> None:
        """Test restoring extra_properties from snapshot."""
        snapshot = ChatHistorySnapshot(
            messages_json=[],
            extra_properties={
                "reducers": [{"reducer": "test", "count": 5}],
                "custom_field": "value",
            },
        )
        history = snapshot.to_history()

        assert history.extra_properties["reducers"] == [{"reducer": "test", "count": 5}]
        assert history.extra_properties["custom_field"] == "value"


class TestChatHistorySnapshotRoundTrip:
    """Test round-trip serialization/deserialization."""

    def test_round_trip_all_message_types(self) -> None:
        """Test that all message types survive a round-trip."""
        history = ChatHistory()

        # Add all message types
        history.add_message(HumanMessage(text="Hello"))
        history.add_message(
            ReasoningMessage(
                text="Thinking...",
                plan_id="plan_1",
                plan_step="s0",
                step_status="started",
                plan_status="active",
            )
        )
        tool_request = ToolCallRequest(
            tool_use_id="t1", tool_name="search", tool_input={"q": "test"}
        )
        history.add_message(tool_request)
        history.add_message(ToolResult(tool_request=tool_request, result={"ok": True}))

        tool_request_2 = ToolCallRequest(
            tool_use_id="t2", tool_name="fail", tool_input={}
        )
        history.add_message(tool_request_2)
        history.add_message(
            ToolResultError(tool_request=tool_request_2, error="Failed!")
        )

        history.add_message(AssistantMessage(text="Done!"))

        # Round-trip
        snapshot = ChatHistorySnapshot.from_history(
            history,
            plan_cache={},
        )
        restored = snapshot.to_history()

        # Verify all messages restored correctly
        assert len(restored.messages) == 7
        assert isinstance(restored.messages[0], HumanMessage)
        assert isinstance(restored.messages[1], ReasoningMessage)
        assert isinstance(restored.messages[2], ToolCallRequest)
        assert isinstance(restored.messages[3], ToolResult)
        assert isinstance(restored.messages[4], ToolCallRequest)
        assert isinstance(restored.messages[5], ToolResultError)
        assert isinstance(restored.messages[6], AssistantMessage)

        # Verify reasoning message fields
        reasoning = restored.messages[1]
        assert isinstance(reasoning, ReasoningMessage)
        assert reasoning.plan_id == "plan_1"
        assert reasoning.plan_step == "s0"

    def test_round_trip_with_reduced_history(self) -> None:
        """Test round-trip preserves reduced_history."""
        history = ChatHistory()
        history.add_message(HumanMessage(text="Old message"))
        history.add_message(HumanMessage(text="New message"))

        history.set_reduced_history(
            [SummaryMessage(text="Old"), HumanMessage(text="New message")],
            {"reducer": "summarizer"},
        )

        # Round-trip
        snapshot = ChatHistorySnapshot.from_history(
            history,
            plan_cache={},
        )
        restored = snapshot.to_history()

        assert restored.reduced_history is not None
        assert len(restored.reduced_history) == 2
        assert isinstance(restored.reduced_history[0], SummaryMessage)
        assert restored.extra_properties["reducers"] == [{"reducer": "summarizer"}]

    def test_round_trip_json_serialization(self) -> None:
        """Test that snapshot can be serialized to JSON and back."""
        history = ChatHistory()
        history.add_message(HumanMessage(text="Test"))
        history.add_message(AssistantMessage(text="Response"))

        snapshot = ChatHistorySnapshot.from_history(
            history=history,
            plan_cache={},
        )

        # Serialize to JSON
        json_str = snapshot.model_dump_json()

        # Deserialize from JSON
        restored_snapshot = ChatHistorySnapshot.model_validate_json(json_str)

        # Verify computed state
        assert restored_snapshot.is_complete() is True  # Last msg is AssistantMessage
        assert len(restored_snapshot.messages_json) == 2


class TestChatHistorySnapshotComputedState:
    """Test computed state methods."""

    def test_get_pending_tool_calls_finds_unmatched(self) -> None:
        """Test get_pending_tool_calls finds ToolCallRequests without results."""
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {"type": "human", "text": "Do something"},
                {
                    "type": "tool_call",
                    "tool_use_id": "t1",
                    "tool_name": "a",
                    "tool_input": {},
                },
                {
                    "type": "tool_call",
                    "tool_use_id": "t2",
                    "tool_name": "b",
                    "tool_input": {"x": 1},
                },
            ],
        )

        tool_calls = snapshot.get_pending_tool_calls()

        # Both are pending (no results yet)
        assert len(tool_calls) == 2
        assert all(isinstance(tc, ToolCallRequest) for tc in tool_calls)

    def test_get_pending_tool_calls_excludes_matched(self) -> None:
        """Test get_pending_tool_calls excludes requests with results."""
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {"type": "human", "text": "Do something"},
                {
                    "type": "tool_call",
                    "tool_use_id": "t1",
                    "tool_name": "a",
                    "tool_input": {},
                },
                {
                    "type": "tool_result",
                    "tool_request": {
                        "type": "tool_call",
                        "tool_use_id": "t1",
                        "tool_name": "a",
                        "tool_input": {},
                    },
                    "result": {"ok": True},
                },
                {
                    "type": "tool_call",
                    "tool_use_id": "t2",
                    "tool_name": "b",
                    "tool_input": {},
                },
            ],
        )

        tool_calls = snapshot.get_pending_tool_calls()

        # Only t2 is pending (t1 has a result)
        assert len(tool_calls) == 1
        assert tool_calls[0].tool_use_id == "t2"

    def test_get_pending_tool_calls_handles_errors(self) -> None:
        """Test get_pending_tool_calls treats errors as matched."""
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {
                    "type": "tool_call",
                    "tool_use_id": "t1",
                    "tool_name": "fail",
                    "tool_input": {},
                },
                {
                    "type": "tool_result_error",
                    "tool_request": {
                        "type": "tool_call",
                        "tool_use_id": "t1",
                        "tool_name": "fail",
                        "tool_input": {},
                    },
                    "error": "Failed!",
                },
            ],
        )

        tool_calls = snapshot.get_pending_tool_calls()
        assert len(tool_calls) == 0  # Error counts as "matched"

    def test_get_pending_tool_calls_handles_reused_tool_use_id(self) -> None:
        """Test get_pending_tool_calls handles reused tool_use_id across LLM responses.

        When the LLM reuses the same tool_use_id across multiple responses, a ToolResult
        should only match a ToolCallRequest that came BEFORE it, not after. This is a
        regression test for a bug where an earlier ToolResult would incorrectly "consume"
        a later ToolCallRequest with the same ID.
        """
        # Simulate: LLM calls tool, gets result, LLM calls same tool again (reusing ID)
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {"type": "human", "text": "Do something repeatedly"},
                # First LLM call
                {
                    "type": "tool_call",
                    "tool_use_id": "reused_id",
                    "tool_name": "test_tool",
                    "tool_input": {"iteration": 1},
                },
                # First result
                {
                    "type": "tool_result",
                    "tool_request": {
                        "type": "tool_call",
                        "tool_use_id": "reused_id",
                        "tool_name": "test_tool",
                        "tool_input": {"iteration": 1},
                    },
                    "result": {"ok": True, "iteration": 1},
                },
                # Second LLM call - SAME tool_use_id (LLM bug or mock behavior)
                {
                    "type": "tool_call",
                    "tool_use_id": "reused_id",
                    "tool_name": "test_tool",
                    "tool_input": {"iteration": 2},
                },
            ],
        )

        # The second ToolCallRequest should be pending (not matched by the earlier result)
        tool_calls = snapshot.get_pending_tool_calls()
        assert len(tool_calls) == 1
        assert tool_calls[0].tool_use_id == "reused_id"
        assert tool_calls[0].tool_input == {"iteration": 2}

    def test_get_pending_tool_calls_full_cycle_with_reused_id(self) -> None:
        """Test get_pending_tool_calls returns empty after both cycles complete."""
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {"type": "human", "text": "Do something repeatedly"},
                # First cycle
                {
                    "type": "tool_call",
                    "tool_use_id": "reused_id",
                    "tool_name": "test_tool",
                    "tool_input": {"iteration": 1},
                },
                {
                    "type": "tool_result",
                    "tool_request": {
                        "type": "tool_call",
                        "tool_use_id": "reused_id",
                        "tool_name": "test_tool",
                        "tool_input": {"iteration": 1},
                    },
                    "result": {"ok": True, "iteration": 1},
                },
                # Second cycle
                {
                    "type": "tool_call",
                    "tool_use_id": "reused_id",
                    "tool_name": "test_tool",
                    "tool_input": {"iteration": 2},
                },
                {
                    "type": "tool_result",
                    "tool_request": {
                        "type": "tool_call",
                        "tool_use_id": "reused_id",
                        "tool_name": "test_tool",
                        "tool_input": {"iteration": 2},
                    },
                    "result": {"ok": True, "iteration": 2},
                },
            ],
        )

        # Both tool calls have been satisfied
        tool_calls = snapshot.get_pending_tool_calls()
        assert len(tool_calls) == 0

    def test_is_complete_true_when_last_is_assistant(self) -> None:
        """Test is_complete returns True when last message is AssistantMessage."""
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {"type": "human", "text": "Hello"},
                {"type": "assistant", "text": "Hi there!"},
            ],
        )

        assert snapshot.is_complete() is True

    def test_is_complete_false_when_last_is_not_assistant(self) -> None:
        """Test is_complete returns False when last message is not AssistantMessage."""
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {"type": "human", "text": "Hello"},
                {
                    "type": "tool_call",
                    "tool_use_id": "t1",
                    "tool_name": "x",
                    "tool_input": {},
                },
            ],
        )

        assert snapshot.is_complete() is False

    def test_is_complete_false_when_empty(self) -> None:
        """Test is_complete returns False when history is empty."""
        snapshot = ChatHistorySnapshot(messages_json=[])
        assert snapshot.is_complete() is False

    def test_get_llm_turns_used_counts_llm_sequences(self) -> None:
        """Test get_llm_turns_used counts LLM response sequences."""
        snapshot = ChatHistorySnapshot(
            messages_json=[
                {"type": "human", "text": "Hello"},
                {"type": "internal", "text": "Thinking..."},  # LLM sequence 1 starts
                {
                    "type": "tool_call",
                    "tool_use_id": "t1",
                    "tool_name": "a",
                    "tool_input": {},
                },
                {
                    "type": "tool_result",
                    "tool_request": {
                        "type": "tool_call",
                        "tool_use_id": "t1",
                        "tool_name": "a",
                        "tool_input": {},
                    },
                    "result": {},
                },
                {
                    "type": "internal",
                    "text": "More thinking...",
                },  # LLM sequence 2 starts
                {"type": "assistant", "text": "Done!"},
            ],
        )

        assert snapshot.get_llm_turns_used() == 2

    def test_get_llm_turns_used_empty_history(self) -> None:
        """Test get_llm_turns_used returns 0 for empty history."""
        snapshot = ChatHistorySnapshot(messages_json=[])
        assert snapshot.get_llm_turns_used() == 0

    def test_get_llm_turns_used_resets_on_new_human_message(self) -> None:
        """Test get_llm_turns_used counts only since the LAST HumanMessage.

        This ensures the max_llm_turns limit resets for each user turn,
        allowing long conversations without hitting the limit across turns.
        """
        snapshot = ChatHistorySnapshot(
            messages_json=[
                # First user turn
                {"type": "human", "text": "First question"},
                {"type": "internal", "text": "Thinking..."},  # LLM sequence 1
                {
                    "type": "tool_call",
                    "tool_use_id": "t1",
                    "tool_name": "a",
                    "tool_input": {},
                },
                {
                    "type": "tool_result",
                    "tool_request": {
                        "type": "tool_call",
                        "tool_use_id": "t1",
                        "tool_name": "a",
                        "tool_input": {},
                    },
                    "result": {},
                },
                {"type": "internal", "text": "More thinking..."},  # LLM sequence 2
                {"type": "assistant", "text": "Here's my answer"},
                # Second user turn - counter should reset
                {"type": "human", "text": "Second question"},
                {
                    "type": "internal",
                    "text": "New turn...",
                },  # LLM sequence 1 of this turn
                {
                    "type": "tool_call",
                    "tool_use_id": "t2",
                    "tool_name": "b",
                    "tool_input": {},
                },
            ],
        )

        # Should only count the 1 LLM sequence since the last HumanMessage,
        # not the 2 from the first turn
        assert snapshot.get_llm_turns_used() == 1

    def test_get_pending_approval_returns_none(self) -> None:
        """Test get_pending_approval returns None when no approval pending."""
        snapshot = ChatHistorySnapshot(messages_json=[])

        assert snapshot.get_pending_approval() is None

    def test_get_pending_approval_returns_tool_request(self) -> None:
        """Test get_pending_approval returns typed object when set."""
        snapshot = ChatHistorySnapshot(
            messages_json=[],
            pending_approval_json={
                "type": "tool_call",
                "tool_use_id": "approval_1",
                "tool_name": "dangerous_op",
                "tool_input": {"confirm": True},
            },
        )

        approval = snapshot.get_pending_approval()

        assert approval is not None
        assert isinstance(approval, ToolCallRequest)
        assert approval.tool_use_id == "approval_1"


class TestChatHistorySnapshotImmutableUpdates:
    """Test immutable update helper methods."""

    def test_with_pending_approval(self) -> None:
        """Test with_pending_approval returns new snapshot."""
        original = ChatHistorySnapshot(messages_json=[], pending_approval_json=None)

        tool_request = ToolCallRequest(
            tool_use_id="approve_me",
            tool_name="dangerous",
            tool_input={},
        )
        updated = original.with_pending_approval(tool_request)

        assert original.pending_approval_json is None
        assert updated.pending_approval_json is not None
        assert updated.pending_approval_json["tool_use_id"] == "approve_me"

    def test_with_pending_approval_clear(self) -> None:
        """Test with_pending_approval can clear approval."""
        original = ChatHistorySnapshot(
            messages_json=[],
            pending_approval_json={
                "type": "tool_call",
                "tool_use_id": "old",
                "tool_name": "x",
                "tool_input": {},
            },
        )

        updated = original.with_pending_approval(None)

        assert original.pending_approval_json is not None
        assert updated.pending_approval_json is None


class TestPlanCacheRoundTrip:
    """Test plan_cache serialization and deserialization."""

    def test_empty_plan_cache(self) -> None:
        """Test snapshot with empty plan cache."""
        history = ChatHistory()
        history.add_message(HumanMessage(text="Hello"))

        snapshot = ChatHistorySnapshot.from_history(
            history=history,
            plan_cache={},
        )

        assert snapshot.plan_cache_json == {}
        assert snapshot.get_plan_cache() == {}

    def test_plan_cache_round_trip(self) -> None:
        """Test plan cache serializes and deserializes correctly."""
        from datahub_integrations.chat.planner.models import Constraints, Plan, Step

        # Create a plan
        plan = Plan(
            plan_id="plan_123",
            version=1,
            title="Test Plan",
            goal="Complete the test",
            constraints=Constraints(max_llm_turns=30),
            expected_deliverable="Test results",
            steps=[
                Step(
                    id="s1",
                    description="First step",
                    done_when="Step 1 complete",
                ),
                Step(
                    id="s2",
                    description="Second step",
                    done_when="Step 2 complete",
                ),
            ],
        )

        # Create plan cache structure using PlanCacheEntry
        plan_cache: dict[str, PlanCacheEntry] = {
            "plan_123": PlanCacheEntry(
                plan=plan,
                internal={"template_id": "test-template"},
            )
        }

        history = ChatHistory()
        history.add_message(HumanMessage(text="Hello"))

        # Create snapshot with plan cache
        snapshot = ChatHistorySnapshot.from_history(
            history=history,
            plan_cache=plan_cache,
        )

        # Verify serialized structure
        assert "plan_123" in snapshot.plan_cache_json
        assert snapshot.plan_cache_json["plan_123"]["plan"]["plan_id"] == "plan_123"
        assert (
            snapshot.plan_cache_json["plan_123"]["internal"]["template_id"]
            == "test-template"
        )

        # Deserialize and verify
        restored = snapshot.get_plan_cache()
        assert "plan_123" in restored
        assert isinstance(restored["plan_123"]["plan"], Plan)
        assert restored["plan_123"]["plan"].plan_id == "plan_123"
        assert restored["plan_123"]["plan"].title == "Test Plan"
        assert len(restored["plan_123"]["plan"].steps) == 2
        assert restored["plan_123"]["internal"]["template_id"] == "test-template"

    def test_plan_cache_json_serialization(self) -> None:
        """Test plan cache survives JSON serialization."""
        from datahub_integrations.chat.planner.models import Constraints, Plan, Step

        plan = Plan(
            plan_id="json_test",
            version=2,
            title="JSON Test",
            goal="Test JSON",
            constraints=Constraints(max_llm_turns=30),
            expected_deliverable="JSON output",
            steps=[Step(id="s1", description="Test", done_when="Done")],
        )

        plan_cache: dict[str, PlanCacheEntry] = {
            "json_test": PlanCacheEntry(
                plan=plan,
                internal={"revised": True},
            )
        }

        history = ChatHistory()
        snapshot = ChatHistorySnapshot.from_history(
            history=history,
            plan_cache=plan_cache,
        )

        # Serialize to JSON and back
        json_str = snapshot.model_dump_json()
        restored_snapshot = ChatHistorySnapshot.model_validate_json(json_str)

        # Verify restoration
        restored_cache = restored_snapshot.get_plan_cache()
        assert restored_cache["json_test"]["plan"].plan_id == "json_test"
        assert restored_cache["json_test"]["plan"].version == 2
        assert restored_cache["json_test"]["internal"]["revised"] is True
