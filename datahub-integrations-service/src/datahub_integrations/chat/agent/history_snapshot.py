"""
Serializable snapshot of ChatHistory for LangGraph checkpointing.

This module provides ChatHistorySnapshot, a Pydantic model that can serialize
and deserialize the full ChatHistory state for use with LangGraph's checkpointing
system. The snapshot enables interrupt/resume flows for human-in-the-loop (HITL)
approval.

Key design decisions:
- ChatHistory remains mutable at runtime for simplicity
- ChatHistorySnapshot provides the serializable checkpoint format
- Round-trips to/from mutable ChatHistory via from_history() and to_history()
- Plan cache is serialized alongside chat history for HITL continuity
"""

from typing import TYPE_CHECKING, Any, Optional, TypedDict

from pydantic import BaseModel, TypeAdapter

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    Message,
    ToolCallRequest,
)

if TYPE_CHECKING:
    from datahub_integrations.chat.planner.models import Plan


class PlanCacheEntry(TypedDict):
    """
    Structure for a single entry in the plan cache.

    This matches the structure used in planner/tools.py:
        agent.plan_cache[plan_id] = {
            "plan": plan,         # Plan (Pydantic model)
            "internal": {...},    # {tool_used, template_id, revised, ...}
        }
    """

    plan: "Plan"
    internal: dict[str, Any]


# Type alias for the full plan cache
PlanCache = dict[str, PlanCacheEntry]


# Use Pydantic's TypeAdapter with discriminated union for clean message parsing.
# This handles all 7 message types (HumanMessage, AssistantMessage, ReasoningMessage,
# ToolCallRequest, ToolResult, ToolResultError, SummaryMessage) automatically
# via the "type" discriminator field.
_message_adapter: TypeAdapter[Message] = TypeAdapter(Message)


def _parse_message(msg_dict: dict) -> Message:
    """
    Parse a message dict back to the correct Message type.

    Uses Pydantic's TypeAdapter with discriminated union for automatic
    type resolution based on the "type" field in each message.

    Args:
        msg_dict: Dictionary representation of a Message

    Returns:
        The appropriate Message subclass instance
    """
    return _message_adapter.validate_python(msg_dict)


class ChatHistorySnapshot(BaseModel):
    """
    Serializable snapshot of ChatHistory for LangGraph checkpointing.

    This is the state that gets serialized/deserialized by the checkpointer.
    Round-trips to/from the mutable ChatHistory.

    Design Philosophy: Message-Centric State
    -----------------------------------------
    All execution state (except plans) is computed from messages. This ensures
    messages are the single source of truth with no parallel state that can
    drift out of sync.

    Computed state (derived from messages):
    - pending_tool_calls: ToolCallRequests without matching ToolResult/ToolResultError
    - is_complete: Last message is AssistantMessage
    - tool_calls_used: Count of LLM response sequences since last HumanMessage

    Stored state (not derivable from messages):
    - plan_cache: Execution plans (operational metadata, not conversation)
    - pending_approval: Reserved for Phase 3 HITL

    Field naming mirrors ChatHistory exactly to avoid confusion:
    - messages_json -> ChatHistory.messages
    - reduced_history_json -> ChatHistory.reduced_history
    - extra_properties -> ChatHistory.extra_properties

    Attributes:
        schema_version: Version number for snapshot format migrations
        messages_json: Full message history (serialized from ChatHistory.messages)
        reduced_history_json: Reduced history for LLM context
            (serialized from ChatHistory.reduced_history). None means not reduced.
        extra_properties: Extra properties dict from ChatHistory
            (contains reduction_sequence_json, num_reducers_applied, etc.)
        pending_approval_json: Tool awaiting user approval.
            Reserved for Phase 3 HITL implementation.
        plan_cache_json: Serialized plan cache for multi-step plans
    """

    schema_version: int = 1

    # ===== Conversation content (mirrors ChatHistory fields exactly) =====

    # Full message history (ChatHistory.messages)
    # This is the SINGLE SOURCE OF TRUTH for execution state.
    messages_json: list[dict[str, Any]]

    # Reduced history for LLM context (ChatHistory.reduced_history)
    # None means use messages_json as-is
    reduced_history_json: Optional[list[dict[str, Any]]] = None

    # Extra properties dict (ChatHistory.extra_properties)
    # Contains reduction_sequence_json, num_reducers_applied, etc.
    extra_properties: dict[str, Any] = {}

    # ===== HITL state (Phase 3) =====

    # Tool awaiting user approval (for HITL)
    # Reserved for Phase 3 HITL implementation
    pending_approval_json: Optional[dict[str, Any]] = None

    # ===== Planning state =====

    # Plan cache for multi-step plans
    # Structure: {plan_id: {"plan": {...}, "internal": {...}}}
    # - plan: Serialized Plan model (Pydantic BaseModel)
    # - internal: Metadata (template_id, tool_used, revised, etc.)
    # This must be preserved for HITL so agents can resume mid-plan.
    plan_cache_json: dict[str, dict[str, Any]] = {}

    # ===== Factory Methods =====

    @classmethod
    def from_history(
        cls,
        history: ChatHistory,
        *,  # Force keyword-only args to prevent positional mistakes
        plan_cache: PlanCache,
        pending_approval: Optional[ToolCallRequest] = None,  # Optional (Phase 3)
    ) -> "ChatHistorySnapshot":
        """
        Create snapshot from mutable ChatHistory.

        Execution state (pending_tool_calls, is_complete, tool_calls_used) is
        computed from messages, not stored separately. This ensures messages
        are the single source of truth.

        Args:
            history: The ChatHistory to snapshot
            plan_cache: Plan cache from AgentRunner for preserving plan state.
                Pass {} if no plans.
            pending_approval: Tool awaiting user approval (Phase 3 HITL).
                Optional - None means no pending approval.

        Returns:
            A new ChatHistorySnapshot instance
        """
        # Serialize plan cache - Plan objects need model_dump()
        plan_cache_json: dict[str, dict[str, Any]] = {}
        for plan_id, entry in plan_cache.items():
            plan_cache_json[plan_id] = {
                "plan": entry["plan"].model_dump(),
                "internal": entry["internal"],
            }

        return cls(
            messages_json=[msg.model_dump() for msg in history.messages],
            reduced_history_json=(
                [msg.model_dump() for msg in history.reduced_history]
                if history.reduced_history is not None
                else None
            ),
            extra_properties=dict(history.extra_properties),
            pending_approval_json=(
                pending_approval.model_dump() if pending_approval else None
            ),
            plan_cache_json=plan_cache_json,
        )

    def to_history(self) -> ChatHistory:
        """
        Restore mutable ChatHistory from snapshot.

        Creates a new ChatHistory instance with all data restored from
        the serialized snapshot. This is used when resuming graph execution
        from a checkpoint.

        Returns:
            A new ChatHistory instance with restored state
        """
        history = ChatHistory()

        # Restore messages
        for msg_dict in self.messages_json:
            msg = _parse_message(msg_dict)
            history.messages.append(msg)

        # Restore reduced history (None means not reduced)
        if self.reduced_history_json is not None:
            history.reduced_history = [
                _parse_message(msg_dict) for msg_dict in self.reduced_history_json
            ]

        # Restore extra properties exactly
        history.extra_properties = dict(self.extra_properties)

        return history

    # ===== Computed State (derived from messages) =====

    def get_pending_tool_calls(self) -> list[ToolCallRequest]:
        """
        Compute pending tool calls from messages.

        Finds all ToolCallRequest messages that don't have a matching
        ToolResult or ToolResultError (matched by tool_use_id).

        Note: This handles the case where the same tool_use_id is reused across
        multiple LLM responses by scanning messages in order. A ToolResult only
        matches a ToolCallRequest that came BEFORE it, not after.

        Returns:
            List of ToolCallRequest objects awaiting execution
        """
        # Scan messages in order, tracking pending requests
        # A ToolResult can only match a ToolCallRequest that came before it
        pending: dict[str, ToolCallRequest] = {}

        for msg_dict in self.messages_json:
            msg_type = msg_dict.get("type")

            if msg_type == "tool_call":
                # New tool call request - add to pending
                request = ToolCallRequest.model_validate(msg_dict)
                pending[request.tool_use_id] = request

            elif msg_type in ("tool_result", "tool_result_error"):
                # Tool result - remove matching pending request (if any)
                tool_request_dict = msg_dict.get("tool_request", {})
                tool_use_id = tool_request_dict.get("tool_use_id")
                if tool_use_id and tool_use_id in pending:
                    del pending[tool_use_id]

        return list(pending.values())

    def is_complete(self) -> bool:
        """
        Check if agent loop is complete.

        The loop is complete when the last message is an AssistantMessage
        (the agent's final response to the user).

        Returns:
            True if the last message indicates completion
        """
        if not self.messages_json:
            return False
        last_msg = self.messages_json[-1]
        return last_msg.get("type") == "assistant"

    def get_llm_turns_used(self) -> int:
        """
        Count LLM turns since the last HumanMessage.

        This counts how many times the LLM has responded in the current user turn.
        The counter resets each time the user sends a new message, allowing long
        conversations without hitting the limit.

        An LLM turn is a sequence of LLM-produced messages (internal, assistant,
        tool_call) between user-side messages (tool_result, tool_result_error,
        human). We count transitions from user-side to LLM-side messages.

        Note: Some LLMs may not include reasoning text, only tool calls. So we
        can't just count ReasoningMessage - we need to count any LLM response.

        Scans backwards from the end for efficiency.

        Returns:
            Number of LLM turns since the last HumanMessage
        """
        if not self.messages_json:
            return 0

        count = 0
        in_llm_turn = False  # Are we currently in an LLM response sequence?

        for msg_dict in reversed(self.messages_json):
            msg_type = msg_dict.get("type")

            if msg_type == "human":
                # Reached start of current user turn
                # If we were in an LLM turn, count it
                if in_llm_turn:
                    count += 1
                break

            if msg_type in ("tool_result", "tool_result_error"):
                # User-side message (tool execution result)
                # Marks the end of an LLM turn (when scanning backwards)
                if in_llm_turn:
                    count += 1
                    in_llm_turn = False

            elif msg_type in ("internal", "assistant", "tool_call"):
                # LLM-side message
                in_llm_turn = True

        return count

    # ===== Accessor Helpers (parse JSON to typed objects) =====

    def get_pending_approval(self) -> Optional[ToolCallRequest]:
        """
        Parse pending approval from JSON.

        Reserved for Phase 3 HITL implementation.

        Returns:
            ToolCallRequest awaiting approval, or None
        """
        if self.pending_approval_json:
            return ToolCallRequest.model_validate(self.pending_approval_json)
        return None

    def get_plan_cache(self) -> PlanCache:
        """
        Restore plan cache from JSON.

        Deserializes Plan objects from their JSON representation.

        Returns:
            Plan cache matching AgentRunner.plan_cache structure
        """
        from datahub_integrations.chat.planner.models import Plan

        result: PlanCache = {}
        for plan_id, entry in self.plan_cache_json.items():
            result[plan_id] = {
                "plan": Plan.model_validate(entry["plan"]),
                "internal": entry["internal"],
            }
        return result

    # ===== Immutable Update Helpers (return new snapshot) =====

    def with_pending_approval(
        self, pending_approval: Optional[ToolCallRequest]
    ) -> "ChatHistorySnapshot":
        """
        Return new snapshot with updated pending approval.

        Reserved for Phase 3 HITL implementation.

        Args:
            pending_approval: Tool call awaiting approval, or None

        Returns:
            New snapshot with updated pending_approval_json
        """
        return self.model_copy(
            update={
                "pending_approval_json": (
                    pending_approval.model_dump() if pending_approval else None
                )
            }
        )

    def with_plan(
        self, plan_id: str, plan: "Plan", internal: dict[str, Any]
    ) -> "ChatHistorySnapshot":
        """
        Return new snapshot with a plan added or updated in the cache.

        Args:
            plan_id: Unique identifier for the plan
            plan: The Plan object to store
            internal: Internal metadata (tool_used, template_id, revised, etc.)

        Returns:
            New snapshot with updated plan_cache_json
        """
        updated_cache = dict(self.plan_cache_json)
        updated_cache[plan_id] = {
            "plan": plan.model_dump(),
            "internal": internal,
        }
        return self.model_copy(update={"plan_cache_json": updated_cache})
