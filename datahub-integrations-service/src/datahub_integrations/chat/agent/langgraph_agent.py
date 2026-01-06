"""
LangGraph-based agent orchestration for AgentRunner.

This module provides the StateGraph that replaces the while loop in
generate_next_message(). It enables:
- Checkpointing via ChatHistorySnapshot for interrupt/resume flows
- Clean separation between LLM calling and tool execution
- Human-in-the-loop (HITL) approval (Phase 3)

Key design:
- AgentGraphState is the single source of truth, created at runner init
- SnapshotUpdater provides atomic updates within with_history() callbacks
- Nodes access runner._state directly, LangGraph state is minimal
"""

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Optional,
    Protocol,
    TypedDict,
    TypeVar,
    cast,
)

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import RunnableConfig
from loguru import logger

from datahub_integrations.chat.agent.history_snapshot import (
    ChatHistorySnapshot,
    PlanCache,
    PlanCacheEntry,
)
from datahub_integrations.chat.chat_history import ChatHistory, Message

if TYPE_CHECKING:
    from datahub_integrations.chat.agent.agent_runner import AgentRunner
    from datahub_integrations.chat.planner.models import Plan

T = TypeVar("T")


# =============================================================================
# SnapshotUpdater - Accumulates state changes for atomic application
# =============================================================================


class SnapshotUpdater:
    """
    Accumulates plan cache updates to be applied atomically.

    Used within AgentGraphState.with_history() callbacks to collect plan
    changes and apply them together when the callback returns.

    Execution state (pending_tool_calls, is_complete, tool_calls_used) is
    computed from messages - no explicit setters needed.
    """

    def __init__(self, base_snapshot: ChatHistorySnapshot):
        """
        Initialize updater with base snapshot's plan cache.

        Args:
            base_snapshot: The snapshot to use as starting values
        """
        self._plan_cache: PlanCache = dict(base_snapshot.get_plan_cache())

    def set_plan(
        self, plan_id: str, plan: "Plan", internal: dict[str, Any]
    ) -> "SnapshotUpdater":
        """
        Add or update a plan in the cache.

        Args:
            plan_id: Unique identifier for the plan
            plan: The Plan object
            internal: Internal metadata (tool_used, template_id, etc.)

        Returns:
            Self for chaining
        """
        self._plan_cache[plan_id] = {"plan": plan, "internal": internal}
        return self

    def get_plan(self, plan_id: str) -> Optional[PlanCacheEntry]:
        """
        Get a plan from the accumulated cache.

        Used by tools that need to read plans updated earlier in the same callback.

        Args:
            plan_id: Plan identifier

        Returns:
            PlanCacheEntry or None if not found
        """
        return self._plan_cache.get(plan_id)

    def build(self, history: ChatHistory) -> ChatHistorySnapshot:
        """
        Build final snapshot with all accumulated changes.

        Called by AgentGraphState.with_history() when callback completes.
        Execution state is computed from the history's messages.

        Args:
            history: The (potentially modified) ChatHistory

        Returns:
            New ChatHistorySnapshot with plan cache and history
        """
        return ChatHistorySnapshot.from_history(
            history=history,
            plan_cache=self._plan_cache,
        )


# =============================================================================
# SnapshotHolder Protocol - Interface for PlanningContext
# =============================================================================


class SnapshotHolder(Protocol):
    """
    Interface for state holders that PlanningContext depends on.

    This protocol defines the minimal interface needed by PlanningContext
    to access and modify plan state. AgentGraphState implements this.
    """

    @property
    def snapshot(self) -> ChatHistorySnapshot:
        """Get current snapshot."""
        ...

    def set_plan(self, plan_id: str, plan: "Plan", internal: dict[str, Any]) -> None:
        """Add or update a plan."""
        ...

    def get_plan(self, plan_id: str) -> Optional[PlanCacheEntry]:
        """Get a plan by ID."""
        ...

    def with_history(self, callback: Callable[[ChatHistory, SnapshotUpdater], T]) -> T:
        """Execute callback with exclusive ChatHistory access."""
        ...


# =============================================================================
# AgentGraphState - The single source of truth for agent state
# =============================================================================


class AgentGraphState:
    """
    Holds snapshot and provides controlled access to agent state.

    Created at runner init and lives for the runner's lifetime. Provides:
    - Direct methods for plan access: set_plan(), get_plan()
    - Guarded history access via with_history() callback
    - Atomic updates via SnapshotUpdater

    The with_history() method ensures only one ChatHistory exists at a time,
    preventing divergent state from multiple to_history() calls.
    """

    def __init__(self, snapshot: ChatHistorySnapshot):
        """
        Initialize state holder with snapshot.

        Args:
            snapshot: Initial snapshot state
        """
        self._snapshot = snapshot
        self._in_history_callback = False
        self._current_updater: Optional[SnapshotUpdater] = None

    @property
    def snapshot(self) -> ChatHistorySnapshot:
        """
        Get current snapshot.

        Returns:
            The current ChatHistorySnapshot
        """
        return self._snapshot

    @snapshot.setter
    def snapshot(self, value: ChatHistorySnapshot) -> None:
        """
        Set snapshot directly.

        Use sparingly - prefer with_history() for most updates.

        Args:
            value: New snapshot value
        """
        self._snapshot = value

    def set_plan(self, plan_id: str, plan: "Plan", internal: dict[str, Any]) -> None:
        """
        Add or update a plan in the cache.

        When called inside with_history(), delegates to the current updater
        so changes are included in the atomic update. When called outside,
        updates the snapshot directly.

        Args:
            plan_id: Unique identifier for the plan
            plan: The Plan object
            internal: Internal metadata
        """
        if self._current_updater:
            # Inside with_history - delegate to updater for atomic update
            self._current_updater.set_plan(plan_id, plan, internal)
        else:
            # Outside with_history - update snapshot directly
            self._snapshot = self._snapshot.with_plan(plan_id, plan, internal)

    def get_plan(self, plan_id: str) -> Optional[PlanCacheEntry]:
        """
        Get a plan by ID.

        When called inside with_history(), reads from the updater's cache
        to see any plans set earlier in the same callback.

        Args:
            plan_id: Plan identifier

        Returns:
            PlanCacheEntry or None if not found
        """
        if self._current_updater:
            # Inside with_history - read from updater to see pending changes
            return self._current_updater.get_plan(plan_id)
        else:
            return self._snapshot.get_plan_cache().get(plan_id)

    def with_history(self, callback: Callable[[ChatHistory, SnapshotUpdater], T]) -> T:
        """
        Execute callback with exclusive ChatHistory access and atomic updates.

        Only one ChatHistory can exist at a time - nesting is prevented with
        a runtime error. The callback receives:
        - history: Mutable ChatHistory for message operations
        - updater: SnapshotUpdater for other state changes

        All changes (to history and via updater) are applied atomically when
        the callback returns.

        Args:
            callback: Function taking (ChatHistory, SnapshotUpdater) and returning T

        Returns:
            The callback's return value

        Raises:
            RuntimeError: If called while already inside another with_history
        """
        if self._in_history_callback:
            raise RuntimeError(
                "Cannot nest with_history calls - only one ChatHistory can exist at a time"
            )

        self._in_history_callback = True
        updater = SnapshotUpdater(self._snapshot)
        self._current_updater = updater
        try:
            history = self._snapshot.to_history()
            result = callback(history, updater)
            self._snapshot = updater.build(history)
            return result
        finally:
            self._in_history_callback = False
            self._current_updater = None


# =============================================================================
# LangGraph State Type and Helpers
# =============================================================================


class _LangGraphState(TypedDict):
    """
    Minimal LangGraph state for graph mechanics.

    The real state lives in runner._state (AgentGraphState). This TypedDict
    exists only to satisfy LangGraph's StateGraph type requirements.
    Nodes access runner._state directly and return this unchanged.
    """

    pass  # Empty - all state is in runner._state


def _get_completion_check(
    config: RunnableConfig,
) -> Optional[Callable[[Message], bool]]:
    """
    Extract completion_check from config.

    The completion_check is passed via config["configurable"]["completion_check"]
    rather than in state, because functions are not serializable by the checkpointer.
    """
    configurable = config.get("configurable", {})
    return configurable.get("completion_check")


# =============================================================================
# Graph Builder
# =============================================================================


def build_agent_graph(runner: "AgentRunner") -> CompiledStateGraph:
    """
    Build the LangGraph agent for the given runner.

    Creates a StateGraph with two main nodes:
    - call_llm: Calls the LLM and collects tool call requests
    - execute_tools: Executes pending tool calls

    The graph loops between these nodes until completion:
    - LLM returns end_turn with no tool calls, OR
    - Custom completion_check returns True, OR
    - Max tool calls limit is reached

    Nodes access runner._state (AgentGraphState) directly for all state
    operations. The LangGraph state is minimal and just flows through.

    Args:
        runner: The AgentRunner instance that provides config, tools, and methods

    Returns:
        A compiled StateGraph ready for invocation
    """
    # Import here to avoid circular dependency and to access log_tokens_usage
    from datahub_integrations.chat.agent.agent_runner import log_tokens_usage

    def call_llm_node(state: _LangGraphState) -> _LangGraphState:
        """
        Node that calls the LLM.

        Uses runner._state.with_history() to safely access ChatHistory.
        Execution state (pending_tool_calls, is_complete, tool_calls_used)
        is computed from the messages added by _call_llm.
        """

        def do_llm(history: ChatHistory, m: SnapshotUpdater) -> None:
            llm_result = runner._call_llm(history)

            # Log token usage (per decision #3)
            log_tokens_usage(llm_result.token_usage)

            # No explicit state updates needed - messages are the source of truth
            # pending_tool_calls: computed from unmatched ToolCallRequest messages
            # is_complete: computed from last message being AssistantMessage
            # tool_calls_used: computed from LLM response sequences

        runner._state.with_history(do_llm)
        return state

    def execute_tools_node(state: _LangGraphState) -> _LangGraphState:
        """
        Node that executes pending tool calls.

        Uses runner._state.with_history() to safely access ChatHistory.
        Planning tools that call ctx.set_plan() will route through the
        updater for atomic updates.

        Pending tool calls are computed from messages (unmatched ToolCallRequests).
        After execution, _execute_tool adds ToolResult messages which "match"
        the requests, so get_pending_tool_calls() will return empty.

        Telemetry is handled inside _execute_tool (per decision #2).
        """

        def execute_all_tools(history: ChatHistory, m: SnapshotUpdater) -> None:
            # Get pending tools computed from current messages
            pending_tool_calls = runner._state.snapshot.get_pending_tool_calls()

            for tool_request in pending_tool_calls:
                # PHASE 3: Add approval check here
                runner._execute_tool(history, tool_request)

            # No explicit clear needed - adding ToolResult messages
            # automatically "matches" the ToolCallRequests

        runner._state.with_history(execute_all_tools)
        return state

    # Type aliases for router return types
    _ShouldContinueReturn = Literal["execute_tools", "call_llm", "__end__"]
    _AfterToolsReturn = Literal["call_llm", "__end__"]

    def should_continue(
        state: _LangGraphState, config: RunnableConfig
    ) -> _ShouldContinueReturn:
        """
        Router after LLM call.

        Determines whether to execute tools, call LLM again, or end based on:
        - Completion status (computed from messages - last msg is AssistantMessage)
        - Tool call limit (computed from messages)
        - Whether there are pending tool calls (computed from messages)
        - Custom completion check (if provided via config)

        The "call_llm" return handles cases like malformed function calls where
        stopReason="tool_use" but no actual tool calls were made - the agent
        should continue looping instead of returning to the user.
        """
        snapshot = runner._state.snapshot

        # If complete (last message is AssistantMessage)
        if snapshot.is_complete():
            return cast(_ShouldContinueReturn, END)

        # Check LLM turn limit (resets each user message)
        # This prevents runaway loops within a single user turn
        if snapshot.get_llm_turns_used() >= runner.config.max_llm_turns:
            logger.warning(
                f"Max LLM turns ({runner.config.max_llm_turns}) reached in should_continue, "
                f"stopping agent loop"
            )
            return cast(
                _ShouldContinueReturn, END
            )  # Will raise error in generate_next_message

        # If there are pending tool calls (unmatched ToolCallRequests), execute them
        if snapshot.get_pending_tool_calls():
            return "execute_tools"

        # No tools and not complete - check custom completion
        # This handles cases like malformed function calls where the LLM
        # returns stopReason="tool_use" with text but no actual tool calls
        completion_check = _get_completion_check(config)
        if completion_check:
            history = snapshot.to_history()
            if history.messages and completion_check(history.messages[-1]):
                return cast(_ShouldContinueReturn, END)

        # Not complete, no tools, completion check says continue - call LLM again
        # This handles the malformed function call case
        logger.warning(
            "Malformed LLM response detected: stopReason suggests tool use but no tool "
            "calls were made. Retrying LLM call. This may indicate model confusion."
        )
        return "call_llm"

    def after_tools(
        state: _LangGraphState, config: RunnableConfig
    ) -> _AfterToolsReturn:
        """
        Router after tool execution.

        Determines whether to call LLM again or end based on:
        - Completion status (computed from messages)
        - Custom completion check (if provided via config) - checked BEFORE tool limit
        - Tool call limit (computed from messages)

        Note: completion_check is checked before tool limit so respond_to_user
        can properly end the loop even when at the max_llm_turns limit.
        """
        snapshot = runner._state.snapshot

        # If complete (last message is AssistantMessage)
        if snapshot.is_complete():
            return cast(_AfterToolsReturn, END)

        # Check custom completion BEFORE tool limit
        # This ensures respond_to_user can properly end the loop
        completion_check = _get_completion_check(config)
        if completion_check:
            history = snapshot.to_history()
            if history.messages and completion_check(history.messages[-1]):
                return cast(_AfterToolsReturn, END)

        # Check tool call limit (computed from message sequences)
        if snapshot.get_llm_turns_used() >= runner.config.max_llm_turns:
            logger.warning(
                f"Max tool calls ({runner.config.max_llm_turns}) reached, "
                f"stopping agent loop"
            )
            return cast(
                _AfterToolsReturn, END
            )  # Will raise error in generate_next_message

        # Continue the loop
        return "call_llm"

    # Build graph with minimal state type
    builder: StateGraph = StateGraph(_LangGraphState)

    builder.add_node("call_llm", call_llm_node)
    builder.add_node("execute_tools", execute_tools_node)

    builder.add_edge(START, "call_llm")
    builder.add_conditional_edges("call_llm", should_continue)
    builder.add_conditional_edges("execute_tools", after_tools)

    # Use MemorySaver for checkpointing (per decision #9)
    # This provides in-memory checkpointing equivalent to current behavior.
    # State can be recovered from messages if needed for persistence.
    return builder.compile(checkpointer=MemorySaver())
