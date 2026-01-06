# LangGraph Migration Plan

## Executive Summary

**Goal**: Migrate the custom agent orchestration in `AgentRunner` to use LangGraph, enabling human-in-the-loop (HITL) approval for tool execution.

**Primary Motivation**: Allow specific tools to require user approval before execution, with the ability to pause the agent loop, return control to the caller (frontend/Slack/Teams), and resume after approval.

**Approach**: Get HITL working with minimal changes first. Keep `ChatHistory` mutable, introduce a serializable snapshot for checkpointing. Immutability cleanup is optional future work.

**Revised Phase Order** (lower risk, faster to HITL):

1. **Phase 1**: Split `_call_llm()` / `_execute_tool()` with structured return types
2. **Phase 2**: LangGraph integration with `ChatHistorySnapshot` for checkpointing
3. **Phase 3**: HITL with `interrupt()` / `resume()`
4. **(Optional later)**: Immutability cleanup if desired

---

## Current Architecture

### Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         AgentRunner                              │
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │   ChatHistory    │    │   AgentConfig    │                   │
│  │   (mutable)      │    │   (immutable)    │                   │
│  └──────────────────┘    └──────────────────┘                   │
│                                                                  │
│  generate_next_message()                                         │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  while not complete:                                     │    │
│  │    _generate_tool_call()  ──┬── calls LLM               │    │
│  │                             ├── adds messages to history │    │
│  │                             └── executes tools inline    │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

#### ChatHistory (`chat_history.py`)

- **Mutable** Pydantic model
- `messages: list[Message]` - full conversation history
- `reduced_history: Optional[list[Message]]` - context-reduced version for LLM
- **Mutation methods**:
  - `add_message(msg)` - appends to messages list
  - `set_reduced_history(history, metadata)` - sets reduced context

#### Context Reducers (`context_reducer.py`, `reducers/`)

- `ChatContextReducer` abstract base class
- `reduce(history)` method **mutates** history via `set_reduced_history()`
- Implementations: `ConversationSummarizer`, `SlidingWindowReducer`

#### AgentRunner (`agent_runner.py`)

- Holds mutable `ChatHistory` instance
- `_generate_tool_call()` does multiple things:
  1. Prepares messages (applies context reduction - **side effect**)
  2. Calls LLM
  3. Processes response blocks
  4. For each `toolUse` block: creates ToolCallRequest AND **immediately executes** tool
- `_handle_tool_call_request()` executes tool and adds result to history
- `generate_next_message()` runs the while loop

### Current Flow

```
generate_next_message()
│
├─► _generate_tool_call()
│   │
│   ├─► _prepare_messages()
│   │   ├─► reducer.reduce(history)  // MUTATES history.reduced_history
│   │   └─► format for Bedrock API
│   │
│   ├─► llm_client.converse(...)
│   │
│   └─► for each content_block:
│       ├─► if text: _add_message(ReasoningMessage or AssistantMessage)
│       └─► if toolUse:
│           ├─► _add_message(ToolCallRequest)
│           └─► _handle_tool_call_request()  // EXECUTES TOOL IMMEDIATELY
│               ├─► tool.run(...)
│               ├─► _add_message(ToolResult or ToolResultError)
│               └─► track_saas_event(...)
│
├─► completion_check(last_message)
│
└─► (loop or return)
```

### Problems for HITL

1. **Tool execution is inline**: No point to pause between LLM response and tool execution
2. **State is mutable**: Hard to checkpoint/restore for resumption
3. **Side effects everywhere**: Reducers mutate, `_add_message` mutates, telemetry fires inline
4. **Tight coupling**: LLM calling, tool execution, progress tracking all interleaved

---

## Target Architecture

### Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         AgentRunner                              │
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │   ChatHistory    │    │   AgentConfig    │                   │
│  │   (mutable)      │    │   (immutable)    │                   │
│  └────────┬─────────┘    └──────────────────┘                   │
│           │                                                      │
│           ▼                                                      │
│  ┌────────────────────────────────────────┐                     │
│  │       ChatHistorySnapshot              │                     │
│  │       (Pydantic, serializable)         │                     │
│  │  • messages_json                       │                     │
│  │  • pending_tool_calls_json             │                     │
│  │  • pending_approval_json               │                     │
│  │  • schema_version                      │                     │
│  └────────────────────────────────────────┘                     │
│                                                                  │
│  generate_next_message()                                         │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  LangGraph StateGraph                                    │    │
│  │                                                          │    │
│  │  ┌─────────┐     ┌───────────────┐     ┌─────────────┐  │    │
│  │  │call_llm │────►│execute_tools  │────►│  (loop)     │  │    │
│  │  │  node   │     │    node       │     │             │  │    │
│  │  └─────────┘     └───────┬───────┘     └─────────────┘  │    │
│  │                          │                               │    │
│  │                   requires_approval?                     │    │
│  │                          │                               │    │
│  │                    interrupt()                           │    │
│  │                       ┌──┴──┐                            │    │
│  │               checkpoint saved                           │    │
│  │                       └──┬──┘                            │    │
│  │                    resume(value)                         │    │
│  │                          │                               │    │
│  │                   approved? ──► execute or skip          │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **ChatHistory stays mutable**: No invasive changes to existing code. Mutable for runtime, snapshot for checkpointing.

2. **ChatHistorySnapshot for checkpointing**: Pydantic model that serializes/deserializes the full state. This is what LangGraph checkpoints.

3. **Structured return types**: `_call_llm()` returns `LLMCallResult`, `_execute_tool()` returns `ToolExecutionResult`. Makes graph nodes simple.

4. **LangGraph orchestration**: StateGraph manages flow, checkpointing, and interrupts. Replaces while loop.

5. **Separated concerns**: LLM calling is separate from tool execution, enabling HITL pause point.

6. **Rejection adds message**: When user rejects a tool, we add `ToolResultError` to history so LLM doesn't retry.

---

## Phase 1: Separate LLM Calling from Tool Execution

### Goal

Split `_generate_tool_call()` so that LLM calling and tool execution are separate steps. This is the key prerequisite for HITL (need to pause between getting tool calls and executing them).

**Key principle**: Keep `ChatHistory` mutable. No immutability changes yet.

### Current Code (simplified)

```python
def _generate_tool_call(self) -> None:
    messages = self._prepare_messages()
    response = llm_client.converse(...)

    for content_block in response_content:
        if "text" in content_block:
            self._handle_text_content(...)
        elif "toolUse" in content_block:
            # Creates ToolCallRequest AND executes tool
            self._handle_tool_call_request(content_block)
```

### Structured Return Types

```python
@dataclass
class LLMCallResult:
    """Structured output from calling LLM."""
    new_messages: List[Message]           # Messages added to history
    pending_tool_calls: List[ToolCallRequest]  # Tools to execute
    is_complete: bool                     # True if end_turn with no tools
    stop_reason: str                      # For debugging/quota enforcement
    token_usage: dict                     # For logging
    output_truncated: bool                # True if stopReason == "max_tokens"


@dataclass
class ToolExecutionResult:
    """Structured output from executing a tool."""
    message: ToolResult | ToolResultError
    duration_seconds: float
    tool_name: str
```

### Token Quota Enforcement

Enforce token limits in Phase 1 so Phase 2/3 inherit correct behavior automatically.

```python
class AgentOutputMaxTokensExceededError(Exception):
    """Raised when LLM output was truncated due to max_tokens limit."""
    pass


def _call_llm(self, history: ChatHistory) -> LLMCallResult:
    # ... call LLM ...

    stop_reason = response["stopReason"]
    output_truncated = stop_reason == "max_tokens"

    # Fail fast if output was truncated
    if output_truncated:
        raise AgentOutputMaxTokensExceededError(
            f"LLM output was truncated (stopReason={stop_reason}). "
            "Response may be incomplete."
        )

    # ... rest of processing ...

    return LLMCallResult(
        # ...
        stop_reason=stop_reason,
        output_truncated=output_truncated,  # Always False if we get here
    )
```

### Target Code

**Key design decisions**:

1. `_call_llm` and `_execute_tool` accept `history` as an explicit parameter (thread-safe, no hidden coupling)
2. Keep `_add_message` helper - it handles logging + progress tracking + history mutation
3. `_add_message` also accepts `history` parameter so it works with any history instance

```python
def _add_message(self, history: ChatHistory, message: Message) -> None:
    """
    Add a message to history with logging and progress notification.

    Args:
        history: The ChatHistory to mutate. Caller owns this instance.
        message: Message to add to history.
    """
    # Debug logging (unchanged from current implementation)
    if isinstance(message, ToolResult):
        self._logger.debug(
            f"Adding ToolResult for {message.tool_request.tool_name}: "
            f"{truncate(str(message), max_length=1000, show_length=True)}"
        )
    elif isinstance(message, ToolResultError):
        self._logger.debug(
            f"Adding ToolResultError for {message.tool_request.tool_name}: "
            f"{truncate(str(message), max_length=1000, show_length=True)}"
        )
    else:
        self._logger.debug(
            f"Adding {type(message).__name__} message: "
            f"{truncate(str(message), max_length=400, show_length=True)}"
        )

    # Mutate the passed history
    history.add_message(message)

    # Progress tracking
    self._progress_tracker.handle_history_updated()


def _call_llm(self, history: ChatHistory) -> LLMCallResult:
    """
    Call LLM and add messages to history.
    Returns structured result with pending tool calls (but does NOT execute them).

    Args:
        history: The ChatHistory to use and mutate. Caller owns this instance.
    """
    messages = self._prepare_messages(history)
    response = llm_client.converse(...)

    new_messages = []
    pending_tool_calls = []
    is_end_turn = response["stopReason"] == "end_turn"

    for i, content_block in enumerate(response_content):
        is_last = i == len(response_content) - 1

        if "text" in content_block:
            if is_last and is_end_turn:
                msg = AssistantMessage(text=_strip_reasoning_tag(content_block["text"]))
            else:
                msg = ReasoningMessage(text=content_block["text"])
            self._add_message(history, msg)  # Logging + progress + mutation
            new_messages.append(msg)

        elif "toolUse" in content_block:
            tool_request = ToolCallRequest(
                tool_use_id=content_block["toolUse"]["toolUseId"],
                tool_name=content_block["toolUse"]["name"],
                tool_input=content_block["toolUse"]["input"],
            )
            self._add_message(history, tool_request)  # Logging + progress + mutation
            new_messages.append(tool_request)
            pending_tool_calls.append(tool_request)

    return LLMCallResult(
        new_messages=new_messages,
        pending_tool_calls=pending_tool_calls,
        is_complete=is_end_turn and not pending_tool_calls,
        stop_reason=response["stopReason"],
        token_usage=response["usage"],
    )

def _execute_tool(
    self,
    history: ChatHistory,
    tool_request: ToolCallRequest
) -> ToolExecutionResult:
    """
    Execute a single tool and add result to history.
    Returns structured result for telemetry.

    Args:
        history: The ChatHistory to mutate. Caller owns this instance.
        tool_request: The tool call to execute.
    """
    tool = self.tool_map[tool_request.tool_name]
    timer = PerfTimer()

    try:
        with timer, with_datahub_client(self.client):
            result = tool.run(arguments=tool_request.tool_input)
        msg = ToolResult(tool_request=tool_request, result=result)
    except Exception as e:
        msg = ToolResultError(tool_request=tool_request, error=f"{type(e).__name__}: {e}")

    self._add_message(history, msg)  # Logging + progress + mutation

    return ToolExecutionResult(
        message=msg,
        duration_seconds=timer.elapsed_seconds(),
        tool_name=tool_request.tool_name,
    )
```

### Updated Main Loop

```python
def generate_next_message(self, ...) -> Message:
    for i in range(self.config.max_llm_turns):
        # Pass self.history explicitly (still the runner's history for non-graph path)
        llm_result = self._call_llm(self.history)

        # Log token usage
        log_tokens_usage(llm_result.token_usage)

        # Execute tools
        for tool_request in llm_result.pending_tool_calls:
            # FUTURE PHASE 3: if tool.requires_approval: return ApprovalRequired(...)
            tool_result = self._execute_tool(self.history, tool_request)

            # Telemetry
            track_saas_event(ChatbotToolCallEvent(
                tool_name=tool_result.tool_name,
                tool_execution_duration_sec=tool_result.duration_seconds,
                tool_result_is_error=isinstance(tool_result.message, ToolResultError),
                ...
            ))

        # Check completion
        if llm_result.is_complete:
            return self.history.messages[-1]

        if self.config.completion_check(self.history.messages[-1]):
            return self.history.messages[-1]

    raise AgentMaxLLMTurnsExceededError(...)
```

### Changes Required

#### Modify: `agent_runner.py`

1. **Add dataclasses** `LLMCallResult` and `ToolExecutionResult`

2. **Update** `_add_message(history: ChatHistory, message: Message)`

   - Add `history` parameter (caller passes it)
   - Keep: debug logging, progress tracker notification
   - Change: `self.history.add_message(message)` → `history.add_message(message)`

3. **Rename** `_generate_tool_call()` → `_call_llm(history: ChatHistory)`

   - Add `history` parameter (caller passes it, method mutates it via `_add_message`)
   - Change return type: `None` → `LLMCallResult`
   - Change: `self._add_message(msg)` → `self._add_message(history, msg)`
   - In `toolUse` branch: collect ToolCallRequest but don't execute
   - Return structured result

4. **Rename** `_handle_tool_call_request()` → `_execute_tool(history: ChatHistory, tool_request: ToolCallRequest)`

   - Add `history` parameter (caller passes it, method mutates it via `_add_message`)
   - Change return type: `None` → `ToolExecutionResult`
   - Change: `self._add_message(msg)` → `self._add_message(history, msg)`
   - Remove ToolCallRequest creation (already created in `_call_llm`)
   - Keep: tool lookup, execution, result/error handling

5. **Update** `_prepare_messages(history: ChatHistory)`

   - Add `history` parameter
   - Use passed history instead of `self.history`

6. **Update** `generate_next_message()` loop
   - Pass `self.history` explicitly to `_call_llm(self.history)` and `_execute_tool(self.history, ...)`
   - Telemetry uses structured result

**Why explicit history parameter**: Enables thread-safe concurrent sessions (Phase 2 passes snapshot-restored history without swapping `self.history`).

**Why keep `_add_message`**: Preserves debug logging and progress tracker notifications without duplicating that logic.

### Verification

- All existing tests pass (behavior unchanged)
- Add test that verifies tool execution happens AFTER all content blocks processed
- Add test for LLMCallResult and ToolExecutionResult structure
- Add test: `stopReason == "max_tokens"` raises `AgentOutputMaxTokensExceededError`
- Add test: `output_truncated` field is set correctly

### Files Changed

| File                         | Change                                                                       |
| ---------------------------- | ---------------------------------------------------------------------------- |
| `chat/agent/agent_runner.py` | Refactor as described                                                        |
| `chat/agent/exceptions.py`   | Add `AgentOutputMaxTokensExceededError` (or add to existing exceptions file) |

---

## Phase 2: LangGraph Integration with Checkpointing

### Goal

Replace the while loop with a LangGraph StateGraph, enabling checkpointing for HITL resume. **Key insight**: We don't need immutable state. We need **checkpointable** state via serializable snapshots.

### ChatHistorySnapshot (Serializable State)

Instead of making ChatHistory immutable, we create a serializable snapshot that can round-trip:

```python
# chat/agent/history_snapshot.py

from pydantic import BaseModel
from typing import List, Optional

class ChatHistorySnapshot(BaseModel):
    """
    Serializable snapshot of ChatHistory for LangGraph checkpointing.

    This is the state that gets serialized/deserialized by the checkpointer.
    Round-trips to/from the mutable ChatHistory.

    Field naming matches ChatHistory exactly to avoid confusion:
    - messages_json -> ChatHistory.messages
    - reduced_history_json -> ChatHistory.reduced_history
    - extra_properties -> ChatHistory.extra_properties
    """
    schema_version: int = 1

    # ===== Conversation content (mirrors ChatHistory fields exactly) =====

    # Full message history (ChatHistory.messages)
    messages_json: List[dict]

    # Reduced history for LLM context (ChatHistory.reduced_history)
    # None means use messages_json as-is
    reduced_history_json: Optional[List[dict]] = None

    # Extra properties dict (ChatHistory.extra_properties)
    # Contains reduction_sequence_json, num_reducers_applied, etc.
    extra_properties: dict = {}

    # ===== Execution state (agent loop specific) =====

    # Tool calls from LLM that haven't been executed yet
    pending_tool_calls_json: List[dict] = []

    # Tool awaiting user approval (for HITL)
    pending_approval_json: Optional[dict] = None

    # Whether agent loop is complete
    is_complete: bool = False

    # Number of LLM calls made (for max_llm_turns limit)
    tool_calls_used: int = 0

    # ===== Factory Methods =====

    @classmethod
    def from_history(
        cls,
        history: ChatHistory,
        pending_tool_calls: List[ToolCallRequest] = None,
        pending_approval: Optional[ToolCallRequest] = None,
        is_complete: bool = False,
        tool_calls_used: int = 0,
    ) -> "ChatHistorySnapshot":
        """Create snapshot from mutable ChatHistory."""
        return cls(
            messages_json=[msg.model_dump() for msg in history.messages],
            reduced_history_json=(
                [msg.model_dump() for msg in history.reduced_history]
                if history.reduced_history is not None
                else None
            ),
            extra_properties=dict(history.extra_properties),
            pending_tool_calls_json=[tc.model_dump() for tc in (pending_tool_calls or [])],
            pending_approval_json=pending_approval.model_dump() if pending_approval else None,
            is_complete=is_complete,
            tool_calls_used=tool_calls_used,
        )

    def to_history(self) -> ChatHistory:
        """Restore mutable ChatHistory from snapshot."""
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

    # ===== Accessor Helpers (parse JSON to typed objects) =====

    def get_pending_tool_calls(self) -> List[ToolCallRequest]:
        """Parse pending tool calls from JSON."""
        return [
            ToolCallRequest.model_validate(tc)
            for tc in self.pending_tool_calls_json
        ]

    def get_pending_approval(self) -> Optional[ToolCallRequest]:
        """Parse pending approval from JSON."""
        if self.pending_approval_json:
            return ToolCallRequest.model_validate(self.pending_approval_json)
        return None

    # ===== Immutable Update Helpers (return new snapshot) =====

    def with_pending_tool_calls(
        self,
        pending_tool_calls: List[ToolCallRequest]
    ) -> "ChatHistorySnapshot":
        """Return new snapshot with updated pending tool calls."""
        return self.model_copy(update={
            "pending_tool_calls_json": [tc.model_dump() for tc in pending_tool_calls]
        })

    def with_pending_approval(
        self,
        pending_approval: Optional[ToolCallRequest]
    ) -> "ChatHistorySnapshot":
        """Return new snapshot with updated pending approval."""
        return self.model_copy(update={
            "pending_approval_json": pending_approval.model_dump() if pending_approval else None
        })

    def with_is_complete(self, is_complete: bool) -> "ChatHistorySnapshot":
        """Return new snapshot with updated completion status."""
        return self.model_copy(update={"is_complete": is_complete})

    def with_tool_calls_used(self, count: int) -> "ChatHistorySnapshot":
        """Return new snapshot with updated tool call count."""
        return self.model_copy(update={"tool_calls_used": count})


def _parse_message(msg_dict: dict) -> Message:
    """Parse a message dict back to the correct Message type."""
    msg_type = msg_dict.get("type")
    if msg_type == "human":
        return HumanMessage.model_validate(msg_dict)
    elif msg_type == "assistant":
        return AssistantMessage.model_validate(msg_dict)
    # ... etc for all message types
```

### LangGraph State (TypedDict)

**Key design**: Single source of truth. The snapshot contains everything; no duplicate fields.

```python
# chat/agent/langgraph_agent.py

from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

class AgentGraphState(TypedDict):
    """
    LangGraph state using snapshots for checkpointing.

    The snapshot is the ONLY source of truth. No duplicate fields.
    Use snapshot helper methods to access pending_tool_calls, etc.
    """
    snapshot: ChatHistorySnapshot
```

### Graph Definition

**Key design decisions**:

1. No `runner.history` swapping - thread-safe
2. Snapshot is single source of truth - no duplicate fields in state
3. Use snapshot helper methods for immutable updates

```python
def build_agent_graph(
    runner: "AgentRunner",  # Pass runner for access to config, tools, client
) -> CompiledStateGraph:
    """Build the LangGraph agent."""

    def call_llm_node(state: AgentGraphState) -> dict:
        """Node that calls the LLM."""
        snapshot = state["snapshot"]

        # Restore history from snapshot - this is our working copy
        history = snapshot.to_history()

        # Pass history explicitly - no swapping, thread-safe
        llm_result = runner._call_llm(history)

        # Create new snapshot with updated state (immutable update)
        new_snapshot = ChatHistorySnapshot.from_history(
            history=history,
            pending_tool_calls=llm_result.pending_tool_calls,
            is_complete=llm_result.is_complete,
            tool_calls_used=snapshot.tool_calls_used + 1,
        )

        return {"snapshot": new_snapshot}

    def execute_tools_node(state: AgentGraphState) -> dict:
        """Node that executes pending tool calls."""
        snapshot = state["snapshot"]

        # Restore history from snapshot - this is our working copy
        history = snapshot.to_history()

        # Get pending tools from snapshot (single source of truth)
        pending_tool_calls = snapshot.get_pending_tool_calls()

        # Pass history explicitly - no swapping, thread-safe
        for tool_request in pending_tool_calls:
            # PHASE 3: Add approval check here
            runner._execute_tool(history, tool_request)

        # Create new snapshot with tools executed
        new_snapshot = ChatHistorySnapshot.from_history(
            history=history,
            pending_tool_calls=[],  # Cleared
            is_complete=snapshot.is_complete,
            tool_calls_used=snapshot.tool_calls_used,
        )

        return {"snapshot": new_snapshot}

    def should_continue(state: AgentGraphState) -> Literal["execute_tools", "end"]:
        """Router after LLM call."""
        snapshot = state["snapshot"]
        if snapshot.is_complete:
            return "end"
        if snapshot.pending_tool_calls_json:  # Check non-empty
            return "execute_tools"
        return "end"

    def after_tools(state: AgentGraphState) -> Literal["call_llm", "end"]:
        """Router after tool execution."""
        snapshot = state["snapshot"]
        if snapshot.is_complete:
            return "end"
        # Check tool call limit
        if snapshot.tool_calls_used >= runner.config.max_llm_turns:
            return "end"  # Will raise error in generate_next_message
        return "call_llm"

    # Build graph
    builder = StateGraph(AgentGraphState)

    builder.add_node("call_llm", call_llm_node)
    builder.add_node("execute_tools", execute_tools_node)

    builder.add_edge(START, "call_llm")
    builder.add_conditional_edges("call_llm", should_continue)
    builder.add_conditional_edges("execute_tools", after_tools)

    return builder.compile(checkpointer=MemorySaver())
```

### AgentRunner with Graph

```python
class AgentRunner:
    def __init__(self, ...):
        # ... existing init ...
        self._graph = build_agent_graph(self)

    def generate_next_message(self, ...) -> Message:
        # Create initial snapshot from current history
        initial_snapshot = ChatHistorySnapshot.from_history(
            history=self.history,
            pending_tool_calls=[],
            is_complete=False,
            tool_calls_used=0,
        )

        # State only contains snapshot (single source of truth)
        initial_state: AgentGraphState = {"snapshot": initial_snapshot}

        config = {"configurable": {"thread_id": self.session_id}}

        # Run graph
        final_state = self._graph.invoke(initial_state, config)

        # Restore history from final snapshot
        final_snapshot = final_state["snapshot"]
        self.history = final_snapshot.to_history()

        # Check for errors
        if final_snapshot.tool_calls_used >= self.config.max_llm_turns:
            if not final_snapshot.is_complete:
                raise AgentMaxLLMTurnsExceededError(...)

        return self.history.messages[-1]
```

### Verification

- All existing tests pass
- Snapshot round-trip: `ChatHistory → Snapshot → ChatHistory` preserves all data
- Graph can be visualized with `graph.get_graph().draw_mermaid()`
- Add test for snapshot serialization with various message types

### Files Changed

| File                             | Change                          |
| -------------------------------- | ------------------------------- |
| `chat/agent/history_snapshot.py` | Create - ChatHistorySnapshot    |
| `chat/agent/langgraph_agent.py`  | Create - Graph definition       |
| `chat/agent/agent_runner.py`     | Use graph instead of while loop |
| `chat/agent/__init__.py`         | Export new classes              |

---

## Phase 3: Human-in-the-Loop

### Goal

Add ability for tools to require approval, with interrupt/resume flow.

### ToolWrapper Enhancement

```python
# tool.py

@dataclasses.dataclass
class ToolWrapper:
    _tool: fastmcp.tools.Tool
    name_prefix: Optional[str] = dataclasses.field(default=None)
    requires_approval: bool = dataclasses.field(default=False)  # NEW
```

### Execute Tools Node with Interrupt (Correct Usage)

**Important**: In LangGraph, `interrupt()` pauses execution at that point. When resumed, execution continues from after the `interrupt()` call.

**Note on tool lookup**: `tool_map` uses the tool's full name (including `name_prefix` if any), so `tool_request.tool_name` must match exactly.

```python
from langgraph.types import interrupt, Command

def execute_tools_node(state: AgentGraphState) -> dict:
    """Node that executes pending tool calls, with HITL support."""
    snapshot = state["snapshot"]

    # Restore history from snapshot - this is our working copy
    history = snapshot.to_history()

    # Get pending tools from snapshot (single source of truth)
    remaining_tools = snapshot.get_pending_tool_calls()
    remaining_tools = list(remaining_tools)  # Make mutable copy

    while remaining_tools:
        tool_request = remaining_tools[0]
        tool = runner.tool_map[tool_request.tool_name]  # Uses full name with prefix

        # CHECK: Does this tool require approval?
        if tool.requires_approval:
            # Interrupt and wait for user decision
            # Execution PAUSES here. On resume, continues from next line.
            resume_value = interrupt({
                "type": "approval_required",
                "tool_name": tool_request.tool_name,
                "tool_input": tool_request.tool_input,
                "tool_use_id": tool_request.tool_use_id,
            })

            # === EXECUTION RESUMES HERE AFTER USER DECISION ===

            if not resume_value.get("approved", True):
                # User rejected - add rejection message to history
                rejection_msg = ToolResultError(
                    tool_request=tool_request,
                    error=f"User rejected execution: {resume_value.get('reason', 'No reason provided')}"
                )
                runner._add_message(history, rejection_msg)  # Logging + progress + mutation
                remaining_tools.pop(0)
                continue  # Skip to next tool

        # Execute the tool (either no approval needed, or user approved)
        # Pass history explicitly - no swapping, thread-safe
        tool_result = runner._execute_tool(history, tool_request)
        remaining_tools.pop(0)

    # Create new snapshot with all tools executed
    new_snapshot = ChatHistorySnapshot.from_history(
        history=history,
        pending_tool_calls=[],
        pending_approval=None,
        is_complete=snapshot.is_complete,
        tool_calls_used=snapshot.tool_calls_used,
    )

    return {"snapshot": new_snapshot}
```

### Key Points About Interrupt/Resume

1. **`interrupt()` pauses execution** - The function stops at that line
2. **Checkpointer saves state** - LangGraph automatically checkpoints
3. **Graph returns to caller** - Caller receives interrupt information
4. **On `resume()`** - Execution continues from after `interrupt()`
5. **`resume_value`** - The value passed to `Command(resume=value)` is returned by `interrupt()`

### Handling Multiple Tool Calls

When LLM returns multiple `toolUse` blocks:

1. We preserve `remaining_tools` list in the snapshot
2. If tool #k needs approval, we interrupt
3. On resume, we continue with tool #k (or skip it if rejected)
4. Then proceed to tool #k+1 without calling LLM again
5. Ordering is preserved via `tool_use_id`

### Graph Invocation Wrappers

**Key design**: Hide LangGraph interrupt detection behind wrapper functions. The exact mechanism for detecting interrupts is version-sensitive and should be an implementation detail.

```python
# chat/agent/graph_runner.py

from dataclasses import dataclass
from typing import Union
from langgraph.types import Command

@dataclass(frozen=True)
class ApprovalRequired:
    """Returned when agent needs user approval to continue."""
    tool_name: str
    tool_input: dict
    tool_use_id: str
    thread_id: str  # For resuming later
    description: str  # Human-readable description


@dataclass(frozen=True)
class GraphResult:
    """Result of running the agent graph."""
    snapshot: ChatHistorySnapshot


def invoke_graph(
    graph: CompiledStateGraph,
    initial_state: AgentGraphState,
    thread_id: str,
) -> Union[GraphResult, ApprovalRequired]:
    """
    Invoke graph, normalizing interrupt handling.

    Hides the LangGraph-specific interrupt detection mechanism.
    This is the only place that needs to change if LangGraph API changes.
    """
    config = {"configurable": {"thread_id": thread_id}}

    # Run graph
    result = graph.invoke(initial_state, config)

    # Normalize interrupt detection (version-sensitive implementation detail)
    interrupt_value = _extract_interrupt(result)

    if interrupt_value is not None:
        return ApprovalRequired(
            tool_name=interrupt_value["tool_name"],
            tool_input=interrupt_value["tool_input"],
            tool_use_id=interrupt_value["tool_use_id"],
            thread_id=thread_id,
            description=f"Tool '{interrupt_value['tool_name']}' wants to execute",
        )

    return GraphResult(snapshot=result["snapshot"])


def resume_graph(
    graph: CompiledStateGraph,
    thread_id: str,
    approved: bool,
    rejection_reason: str = "User rejected",
) -> Union[GraphResult, ApprovalRequired]:
    """
    Resume graph after user approval decision.

    Hides the LangGraph-specific resume mechanism.
    """
    config = {"configurable": {"thread_id": thread_id}}

    resume_value = {
        "approved": approved,
        "reason": rejection_reason if not approved else None,
    }

    result = graph.invoke(Command(resume=resume_value), config)

    # Check for another interrupt
    interrupt_value = _extract_interrupt(result)

    if interrupt_value is not None:
        return ApprovalRequired(
            tool_name=interrupt_value["tool_name"],
            tool_input=interrupt_value["tool_input"],
            tool_use_id=interrupt_value["tool_use_id"],
            thread_id=thread_id,
            description=f"Tool '{interrupt_value['tool_name']}' wants to execute",
        )

    return GraphResult(snapshot=result["snapshot"])


def _extract_interrupt(result: dict) -> Optional[dict]:
    """
    Extract interrupt payload from graph result.

    This is version-sensitive - different LangGraph versions may expose
    interrupts differently. Encapsulate the detection logic here.

    Returns interrupt payload dict if interrupted, None if completed normally.
    """
    # TODO: Test against pinned LangGraph version and update as needed
    # Option 1: Check for __interrupt__ key
    if "__interrupt__" in result:
        return result["__interrupt__"]

    # Option 2: Check for interrupt in state
    if result.get("snapshot") and result["snapshot"].pending_approval_json:
        # Snapshot has pending approval -> we were interrupted
        return result["snapshot"].pending_approval_json

    # Option 3: Result may be an Interrupt object in some versions
    # Add other detection methods as needed

    return None
```

### AgentRunner API for HITL

```python
class AgentRunner:
    def generate_next_message(
        self,
        ...
    ) -> Union[Message, ApprovalRequired]:
        """
        Generate next message, possibly requiring approval.

        Returns:
            Message: If agent completed successfully
            ApprovalRequired: If a tool needs user approval before executing
        """
        initial_snapshot = ChatHistorySnapshot.from_history(
            history=self.history,
            tool_calls_used=0,
        )

        initial_state: AgentGraphState = {"snapshot": initial_snapshot}

        # Use wrapper that normalizes interrupt handling
        result = invoke_graph(self._graph, initial_state, self.session_id)

        if isinstance(result, ApprovalRequired):
            return result

        # Agent completed normally
        self.history = result.snapshot.to_history()
        return self.history.messages[-1]

    def resume(
        self,
        approved: bool = True,
        rejection_reason: str = "User rejected"
    ) -> Union[Message, ApprovalRequired]:
        """
        Resume execution after user approval decision.

        Args:
            approved: If True, execute the pending tool.
                     If False, skip it and add rejection message.
            rejection_reason: Reason for rejection (shown to LLM)

        Returns:
            Message: If agent completed successfully
            ApprovalRequired: If another tool needs approval
        """
        # Use wrapper that normalizes resume handling
        result = resume_graph(
            self._graph,
            self.session_id,
            approved,
            rejection_reason
        )

        if isinstance(result, ApprovalRequired):
            return result

        # Agent completed
        self.history = result.snapshot.to_history()
        return self.history.messages[-1]
```

### Why Rejection Adds a Message

When user rejects a tool, we add `ToolResultError` to history:

```python
ToolResultError(
    tool_request=tool_request,
    error="User rejected execution: <reason>"
)
```

This is important because:

1. **LLM sees the rejection** in its context
2. **Prevents retry loops** - LLM knows the tool was attempted and failed
3. **Audit trail** - History shows what happened
4. **Consistent with errors** - Same pattern as tool execution failures

### Usage Example

```python
# Frontend/Slack handler
agent = create_data_catalog_explorer_agent(client, history)
agent.history.add_message(HumanMessage(text="Delete all my data"))

result = agent.generate_next_message()

if isinstance(result, ApprovalRequired):
    # Show approval UI to user
    print(f"Agent wants to run: {result.description}")
    print(f"Tool: {result.tool_name}")
    print(f"Input: {result.tool_input}")

    user_approved = show_approval_dialog(result)

    # Resume with user's decision
    if user_approved:
        result = agent.resume(approved=True)
    else:
        result = agent.resume(approved=False, rejection_reason="User declined deletion")

# result is now a Message (or another ApprovalRequired if more tools need approval)
if isinstance(result, Message):
    print(result.text)
```

### Verification

- Existing tests pass (no tools require approval by default)
- New test: tool with `requires_approval=True` returns `ApprovalRequired`
- New test: `resume(approved=True)` executes tool and continues
- New test: `resume(approved=False)` adds rejection message and continues
- New test: rejection message appears in LLM context
- New test: multiple tools requiring approval are handled sequentially (preserves order)
- New test: interrupt mid-execution preserves remaining_tools list
- New test: `invoke_graph` wrapper correctly detects interrupts (pinned LangGraph version)
- New test: `resume_graph` wrapper correctly handles resume (pinned LangGraph version)
- New test: `_extract_interrupt` handles various LangGraph response formats

### Files Changed

| File                            | Change                                                                 |
| ------------------------------- | ---------------------------------------------------------------------- |
| `mcp_integration/tool.py`       | Add `requires_approval` field                                          |
| `chat/agent/graph_runner.py`    | Create - `invoke_graph`, `resume_graph`, `_extract_interrupt` wrappers |
| `chat/agent/langgraph_agent.py` | Add interrupt logic in execute_tools_node                              |
| `chat/agent/agent_runner.py`    | Add `ApprovalRequired` type and `resume()` method using wrappers       |
| `chat/agent/__init__.py`        | Export `ApprovalRequired`, `invoke_graph`, `resume_graph`              |

---

## Phase Summary

| Phase | Goal                      | Key Deliverable                                                            |
| ----- | ------------------------- | -------------------------------------------------------------------------- |
| 1     | Separate LLM from tools   | `_call_llm()` → `LLMCallResult`, `_execute_tool()` → `ToolExecutionResult` |
| 2     | LangGraph + checkpointing | `ChatHistorySnapshot`, `StateGraph`, `MemorySaver`                         |
| 3     | HITL                      | `requires_approval` + `interrupt()` + `resume()` + rejection handling      |

---

## Dependencies

```
Phase 1 ──────────────► Phase 2 ──────────────► Phase 3
    │                       │                       │
    │                       │                       └── interrupt/resume, rejection messages
    │                       └── checkpointing via snapshot
    └── separation creates pause point for HITL
```

### Phase Dependencies Explained

- **Phase 1 → Phase 2**: Separating `_call_llm` from `_execute_tool` creates the natural point where we can pause for approval
- **Phase 1 → Phase 2**: Structured return types (`LLMCallResult`, `ToolExecutionResult`) make it easy to build graph nodes
- **Phase 2 → Phase 3**: `ChatHistorySnapshot` enables checkpointing; checkpointing enables interrupt/resume
- **Phase 3 builds on Phase 2**: Uses the snapshot to preserve state during interrupt, uses graph to handle resume

---

## Optional Future Work: Immutability Cleanup

If desired later for maintainability:

### Make ChatHistory Immutable

- Change from Pydantic `BaseModel` to frozen dataclass
- Replace `add_message()` with `with_message() -> ChatHistory`
- Replace `set_reduced_history()` with `with_context_reduced() -> ChatHistory`

### Make Reducers Pure

- Change `reduce(history) -> None` to `reduce(history) -> ChatHistory`
- No mutation, return new history

### Extract Stateless Core Functions

- `call_llm(state, config) -> LLMCallResult` (pure, no side effects)
- `execute_tool(request, tool, client) -> ToolExecutionResult` (pure)
- AgentRunner becomes thin orchestrator

**Why defer**: These changes have high blast radius (touch many tests and consumers) and are not required for HITL. The snapshot approach in Phase 2 achieves checkpointability without deep immutability.

---

## Risks and Mitigations

| Risk                            | Mitigation                                                |
| ------------------------------- | --------------------------------------------------------- |
| Breaking existing tests         | Each phase must pass all existing tests before merge      |
| Snapshot serialization bugs     | Add comprehensive round-trip tests for all Message types  |
| LangGraph version compatibility | Pin version in requirements, test upgrades separately     |
| Interrupt semantics confusion   | Add detailed comments, test interrupt/resume flows        |
| Rejection not visible to LLM    | Ensure `ToolResultError` is added to history on rejection |

---

## Open Questions

1. **Approval timeout**: What happens if user never approves? Options:

   - Leave to caller to manage (simplest)
   - Add `expires_at` field to `ApprovalRequired`
   - Periodic cleanup of stale checkpoints

2. **Progress tracking with LangGraph**: Options:

   - Add callbacks to graph nodes that emit progress
   - Keep existing `ProgressTracker` mechanism
   - Emit progress from within nodes

3. **MLflow tracing with LangGraph**: Options:

   - Wrap graph invocation in MLflow span (simplest)
   - Use LangGraph's built-in tracing
   - Both (may be redundant)

4. **Durable checkpointer for production**: Current plan uses `MemorySaver`. For cross-machine resume:

   - Postgres checkpointer
   - Redis checkpointer
   - Make checkpointer configurable

5. **Schema versioning**: `ChatHistorySnapshot.schema_version` needs migration strategy when format changes.

---

## Testing Strategy

### Phase 1 Tests

- Existing tests pass unchanged
- New: `LLMCallResult` structure is correct
- New: `ToolExecutionResult` structure is correct
- New: Tool execution happens AFTER all content blocks processed
- New: `stopReason == "max_tokens"` raises `AgentOutputMaxTokensExceededError`
- New: `output_truncated` field is set correctly
- New: `_call_llm(history)` and `_execute_tool(history, ...)` accept explicit history parameter
- New: `_add_message(history, msg)` logs, tracks progress, and mutates passed history
- New: Progress tracker is notified for each message added

### Phase 2 Tests

- Existing tests pass unchanged
- New: `ChatHistorySnapshot` round-trip for all Message types
- New: `ChatHistorySnapshot.reduced_history_json` correctly mirrors `ChatHistory.reduced_history`
- New: Graph produces same results as while loop
- New: Snapshot helper methods (`get_pending_tool_calls()`, `with_pending_tool_calls()`) work correctly
- New: State contains only `snapshot` (no duplicate fields)

### Phase 3 Tests

- Existing tests pass (no tools require approval by default)
- New: `requires_approval=True` causes `ApprovalRequired` return
- New: `resume(approved=True)` executes tool
- New: `resume(approved=False)` adds `ToolResultError` to history
- New: LLM sees rejection in context (doesn't retry)
- New: Multiple tools with approval handled sequentially
- New: `remaining_tools` preserved across interrupt/resume
- New: `invoke_graph` wrapper correctly detects interrupts (test against pinned LangGraph version)
- New: `resume_graph` wrapper correctly handles resume
- New: `_extract_interrupt` handles various LangGraph response formats
- New: Tool lookup uses correct name (with `name_prefix` if any)

---

## Plan Corrections

This section documents corrections and additions to the original plan discovered during implementation.

### Phase 2: Make `from_history()` Parameters Required

**Issue**: `ChatHistorySnapshot.from_history()` had default values for all execution state parameters (`pending_tool_calls=None`, `is_complete=False`, `tool_calls_used=0`, `plan_cache=None`). This made it easy to forget a parameter and silently lose state.

**Correction**: Made all execution state parameters required (except `pending_approval` which is optional for Phase 3):

```python
@classmethod
def from_history(
    cls,
    history: ChatHistory,
    *,  # Force keyword-only args
    pending_tool_calls: Sequence[ToolCallRequest],  # Required (pass [])
    is_complete: bool,                               # Required
    tool_calls_used: int,                            # Required
    plan_cache: dict[str, dict[str, Any]],          # Required (pass {})
    pending_approval: Optional[ToolCallRequest] = None,  # Optional (Phase 3)
) -> "ChatHistorySnapshot":
```

**Benefits**:

- Forgetting a parameter is now a compile-time error, not silent data loss
- Production code unchanged (already passed all params explicitly)
- Tests now explicitly document what state they're testing

### Phase 2: Simplify ProgressTracker to Push-Based

**Issue**: The original ProgressTracker used a pull-based design that held a reference to `ChatHistory`. This caused problems with LangGraph because graph nodes work with snapshot-restored history instances, requiring manual syncing of `runner._progress_tracker.history = history` in every node.

**Root Cause**: ProgressTracker was designed to re-iterate over history messages on every update, comparing with previous state to detect changes. This is overly complex for what it does (only track ReasoningMessage for progress).

**Correction**: Refactored ProgressTracker to push-based design:

| Removed                                | Added                                       |
| -------------------------------------- | ------------------------------------------- |
| `history` parameter                    | (not needed)                                |
| `start_offset` parameter               | `reset()` method                            |
| `get_progress_updates()`               | `get_updates()` (returns accumulated list)  |
| `handle_history_updated()`             | `on_message(message)` (push single message) |
| `_last_progress_updates` deduplication | (not needed with push)                      |
| Manual syncing in graph nodes          | (not needed)                                |

**Note**: `agent` parameter is kept temporarily because `XmlReasoningParser` uses `agent.plan_cache` for plan formatting. Added TODO comments to remove once plan_cache is properly encapsulated.

### Phase 2: Add `plan_cache` to ChatHistorySnapshot

**Issue**: The original Phase 3 plan does not mention preserving `plan_cache` during HITL interrupts.

**Context**: `AgentRunner.plan_cache` stores active plans with their progress:

```python
agent.plan_cache[plan_id] = {
    "plan": plan,       # Plan (Pydantic BaseModel)
    "progress": {},     # Step progress tracking
    "internal": {...},  # Metadata (template_id, etc.)
}
```

When an agent is paused mid-plan for human approval, the plan context must be preserved. Otherwise, when the agent resumes, it won't know what plan it was executing.

**Implemented in Phase 2** (moved forward from Phase 3):

1. Added `plan_cache_json` field to `ChatHistorySnapshot`:

```python
class ChatHistorySnapshot(BaseModel):
    # ... existing fields ...

    # Planning state (serializable - Plan is Pydantic BaseModel)
    plan_cache_json: dict[str, dict[str, Any]] = {}
```

2. Updated `from_history()` to accept `plan_cache` parameter and serialize Plan objects

3. Added `get_plan_cache()` method to deserialize plans

4. Updated graph nodes:

   - `call_llm_node`: Preserves plan_cache from previous snapshot (LLM calls don't modify it)
   - `execute_tools_node`: Captures `runner.plan_cache` after tool execution (tools may create/revise plans)

5. Updated `generate_next_message()`:

   - Passes `plan_cache` when creating initial snapshot
   - Restores `plan_cache` from final snapshot after graph completion

6. Added tests for plan_cache round-trip serialization

---

## Changelog

### Phase 1 Completed - 2025-12-26

**Summary**: Refactored `AgentRunner` to separate LLM calling from tool execution, creating the prerequisite structure for HITL integration.

**Changes Made**:

1. **Added immutable structured return types** (`frozen=True`):

   - `LLMCallResult` dataclass for LLM call outputs (new_messages, pending_tool_calls, is_complete, stop_reason, token_usage, output_truncated)
   - `ToolExecutionResult` dataclass for tool execution outputs (message, duration_seconds, tool_name)
   - Both use `frozen=True` to prevent accidental mutation and ensure thread safety

2. **Refactored methods to accept explicit history parameter**:

   - `_add_message(self, history: ChatHistory, message: Message)` - enables thread-safe operation
   - `_prepare_messages(self, history: ChatHistory)` - context reduction works on passed history
   - `_call_llm(self, history: ChatHistory) -> LLMCallResult` - renamed from `_generate_tool_call`, returns pending tool calls without executing
   - `_execute_tool(self, history: ChatHistory, tool_request: ToolCallRequest) -> ToolExecutionResult` - renamed from `_handle_tool_call_request`, executes single tool
   - `_handle_text_content(self, history: ChatHistory, ...)` - now accepts history and returns the message created

3. **Updated `generate_next_message()` loop**:

   - Calls `_call_llm(self.history)` to get LLM response
   - Executes tools in separate loop using `_execute_tool(self.history, tool_request)`
   - Telemetry tracking moved from `_execute_tool` to the main loop

4. **Updated tests**:
   - All tests updated to use new method signatures
   - Added new tests for `_call_llm` returning pending tool calls
   - Added new tests for `_execute_tool` success and error handling
   - All 67 agent tests pass

**Deviations from Original Plan**:

- None significant; followed the plan as specified

**Notes for Future Phases**:

- The separation of `_call_llm` and `_execute_tool` creates the natural pause point for HITL approval
- Progress tracking continues to work via `_add_message` helper
- MLflow spans are preserved in `_call_llm` and `_handle_text_content`
- `AgentOutputMaxTokensExceededError` already existed in the codebase (no new file needed)

### Phase 2 Planning Decisions - 2025-12-26

**Summary**: Reviewed Phase 2 plan for inconsistencies and gaps. Made decisions on implementation details before starting work.

**Issues Identified and Resolutions**:

1. **Progress Tracker Mismatch**: `ProgressTracker` is initialized with `self.history` but graph nodes work with snapshot-restored history. Progress updates wouldn't be visible.

   - **Decision**: Make `ProgressTracker` work with the snapshot's history. Update tracker reference when restoring history from snapshot in `generate_next_message`.

2. **Missing Telemetry in Graph Nodes**: `track_saas_event()` calls for tool execution missing from plan's `execute_tools_node`.

   - **Decision**: Add telemetry to `_execute_tool` method itself (keeps telemetry co-located with execution, simpler than adding to node).

3. **Missing Token Usage Logging**: `log_tokens_usage()` not shown in `call_llm_node`.

   - **Decision**: Add `log_tokens_usage(llm_result.token_usage)` to `call_llm_node`.

4. **Custom `completion_check` Not Supported**: Graph routers only check `is_complete`, ignoring custom completion callbacks from the current API.

   - **Decision**: Pass `completion_check` via graph state or config to preserve existing API behavior.

5. **Type Mismatch for `pending_tool_calls`**: `LLMCallResult.pending_tool_calls` is a tuple (frozen dataclass), but `from_history` signature shows `List`.

   - **Decision**: Change `ChatHistorySnapshot.from_history` to accept `Sequence[ToolCallRequest]`.

6. **Incomplete `_parse_message` Function**: Plan shows manual type checking with "# ... etc" comment.

   - **Decision**: Use Pydantic's `TypeAdapter` with discriminated union for cleaner, complete parsing of all 7 message types:
     ```python
     from pydantic import TypeAdapter
     message_adapter = TypeAdapter(Message)
     def _parse_message(msg_dict: dict) -> Message:
         return message_adapter.validate_python(msg_dict)
     ```

7. **`generate_formatted_message` Not Addressed**: Method not mentioned in Phase 2 plan.
   - **Decision**: No changes needed - method wraps `generate_next_message()` which will use graph internally.

**Open Questions Resolved**:

8. **MLflow Tracing Strategy** (from Open Questions section):

   - **Decision**: Keep `@mlflow.trace` decorator on `generate_next_message()` for now. Consider LangGraph's built-in tracing in future phases.

9. **Durable Checkpointer for Production** (from Open Questions section):

   - **Decision**: Use `MemorySaver` for Phase 2 (equivalent to current in-memory behavior). State can be recovered from messages if needed, preserving existing capability. Durable checkpointer deferred to future work.

10. **LangGraph Version**:

    - **Decision**: Use latest LangGraph version. Update pinned version in requirements.txt.

11. **`pending_approval_json` in Phase 2 Snapshot**:
    - **Decision**: Keep field in `ChatHistorySnapshot` definition but document as "reserved for Phase 3 HITL implementation".

**Changes to Phase 2 Plan Content** (to be applied during implementation):

| Location                             | Change                                                                                                |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `ChatHistorySnapshot.from_history()` | Change `pending_tool_calls: List[ToolCallRequest]` to `pending_tool_calls: Sequence[ToolCallRequest]` |
| `_parse_message()` function          | Replace manual type checking with `TypeAdapter(Message).validate_python(msg_dict)`                    |
| `call_llm_node()`                    | Add `log_tokens_usage(llm_result.token_usage)` after LLM call                                         |
| `execute_tools_node()`               | Telemetry already handled in `_execute_tool` (no node change needed)                                  |
| `AgentGraphState` or graph config    | Add `completion_check` field to support custom completion callbacks                                   |
| `generate_next_message()`            | After restoring history from snapshot, update `ProgressTracker.history` reference                     |
| Dependencies                         | Update to latest LangGraph version in requirements                                                    |
| `pending_approval_json` field        | Add comment: "Reserved for Phase 3 HITL"                                                              |

### Phase 2 Immutable State Refactor - 2025-12-27

**Summary**: Major refactor to remove mutable state (`history`, `plan_cache`) from `AgentRunner`. Introduced `AgentGraphState` as the single source of truth with controlled access patterns.

**Goals Achieved**:

1. Remove mutable `self.history` and `self.plan_cache` from `AgentRunner`
2. Introduce `AgentGraphState` as the single holder of state (created at runner init)
3. Create `PlanningContext` abstraction for planning tools
4. Enable atomic state updates via `SnapshotUpdater`

**Key Design Decisions**:

1. **AgentGraphState as class, not TypedDict**: Changed from TypedDict to a class that implements `SnapshotHolder` protocol. This enables methods for controlled state access.

2. **Created at runner init, not per invocation**: `AgentGraphState` lives on the runner for its lifetime (like `self.history` did before). Planning tools get a stable reference.

3. **Direct methods for plan access**: `set_plan(plan_id, plan, internal)` and `get_plan(plan_id)` instead of generic `mutate()` pattern.

4. **Guarded history access via `with_history(callback)`**:

   - Only one `ChatHistory` can exist at a time (nesting prevented)
   - Callback receives `(history, updater)` for atomic updates
   - All changes applied when callback returns
   - Prevents divergent state from multiple `to_history()` calls

5. **SnapshotUpdater for atomic updates**:

   - Accumulates state changes within `with_history()` callback
   - Methods: `set_pending_tools()`, `set_is_complete()`, `set_tool_calls_used()`, `set_plan()`
   - Changes applied atomically via `build(history)` when callback completes

6. **PlanningContext abstracts state access for tools**:

   - Planning tools take `ctx: PlanningContext` instead of `agent: AgentRunner`
   - Provides `get_plannable_tools()`, `get_plan()`, `set_plan()`
   - Captures `SnapshotHolder` reference in closure

7. **`history` property for backwards compatibility**:
   - Returns copy from current snapshot
   - Added `add_message()` method for convenient message addition
   - Updated all usage sites to use `agent.add_message()` instead of `agent.history.add_message()`

**Files Modified**:

| File                      | Changes                                                                                                                                                                  |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `langgraph_agent.py`      | Added `SnapshotHolder` protocol, `SnapshotUpdater` class, `AgentGraphState` class; updated graph nodes to use `runner._state.with_history()`                             |
| `history_snapshot.py`     | Added `with_plan()` immutable update helper                                                                                                                              |
| `planner/tools.py`        | Added `PlanningContext` class; refactored `create_plan`, `revise_plan`, `report_step_progress` to take `ctx: PlanningContext`; updated `get_planning_tool_wrappers(ctx)` |
| `agent_runner.py`         | Replaced `self.history`/`self.plan_cache` with `self._state`; added `self._planning_context`; added `history` property and `add_message()` method                        |
| `data_catalog_tools.py`   | Updated `get_data_catalog_internal_tools()` to use `agent._planning_context`                                                                                             |
| `chat_session_manager.py` | Updated `add_user_message()` to use `agent.add_message()`                                                                                                                |
| `teams/command/ask.py`    | Updated to use `agent.add_message()`                                                                                                                                     |
| Tests                     | Updated `test_langgraph_agent.py` and `test_agent_runner.py` to use new APIs                                                                                             |
| Docs/Examples             | Updated README and example agents to use `agent.add_message()`                                                                                                           |

**Example Usage After Refactor**:

```python
# Old (no longer works - history is now a property returning a copy)
agent.history.add_message(HumanMessage(text="Hello"))

# New (recommended)
agent.add_message(HumanMessage(text="Hello"))

# For complex operations, use with_history
def do_work(history: ChatHistory, m: SnapshotUpdater):
    history.add_message(...)
    m.set_pending_tools([]).set_is_complete(True)
agent._state.with_history(do_work)
```

**Graph Node Pattern**:

```python
def call_llm_node(state):
    def do_llm(history: ChatHistory, m: SnapshotUpdater):
        llm_result = runner._call_llm(history)
        m.set_pending_tools(llm_result.pending_tool_calls)
        m.set_is_complete(llm_result.is_complete)
        m.set_tool_calls_used(m._tool_calls_used + 1)

    runner._state.with_history(do_llm)
    return state
```

---

### Message-Centric State Refactor - 2024-12-29

**Summary**: Refactored `ChatHistorySnapshot` to compute execution state from messages rather than storing it as separate fields. This eliminates synchronization issues and establishes messages as the single source of truth for all agent interactions.

**Design Philosophy**:

All agent execution state (except plans) is derived from messages. Messages are the single source of truth - there are no parallel state fields that can drift out of sync. This follows the principle that user interactions are fully represented by the message stream.

**Computed State (derived from messages)**:

- `get_pending_tool_calls()`: Finds `ToolCallRequest` messages without matching `ToolResult`/`ToolResultError`
- `is_complete()`: Returns `True` if last message is `AssistantMessage`
- `get_llm_turns_used()`: Counts LLM turns (ReasoningMessages) since last `HumanMessage`

**Stored State (not derivable from messages)**:

- `plan_cache`: Execution plans (operational metadata, not conversation content)

**Future: HITL Approvals via Internal Messages (Phase 3)**:

Human-in-the-Loop (HITL) approvals will also follow the message-centric model. Instead of storing `pending_approval` as a separate field, we'll introduce internal message types:

- `ApprovalRequest`: Added when a tool requires user approval before execution
- `ApprovalResponse`: Added when the user approves/rejects the tool call

These messages will be stored in the message history but **filtered out** before sending to the LLM (similar to how we could filter internal reasoning). The approval state can then be computed:

```python
def get_pending_approval(self) -> Optional[ToolCallRequest]:
    """Find ApprovalRequest without matching ApprovalResponse."""
    for msg in reversed(self.messages_json):
        if msg.get("type") == "approval_response":
            return None  # Most recent approval was handled
        if msg.get("type") == "approval_request":
            return ToolCallRequest.model_validate(msg.get("tool_request"))
    return None

def is_tool_approved(self, tool_use_id: str) -> bool:
    """Check if a specific tool call has been approved."""
    # Look for ApprovalResponse matching this tool_use_id
    # Also consider "approve all" user preferences stored as messages
    ...
```

This approach keeps all user interactions in the message stream, making the conversation fully replayable and auditable. The `pending_approval_json` field in `ChatHistorySnapshot` is currently reserved but may be removed once the message-based approach is implemented.

**Key Changes**:

1. **ChatHistorySnapshot simplification**:

   - Removed stored fields: `pending_tool_calls_json`, `is_complete`, `tool_calls_used`
   - Removed `from_history()` parameters for these fields
   - Added computed methods that analyze messages
   - Removed immutable update helpers: `with_pending_tool_calls()`, `with_is_complete()`, `with_tool_calls_used()`

2. **SnapshotUpdater simplification**:

   - Removed state setters: `set_pending_tools()`, `set_is_complete()`, `set_tool_calls_used()`
   - Kept `set_plan()` for plan cache (plans remain stored)
   - Simplified `build()` to only take history and plan_cache

3. **Graph nodes no longer update state explicitly**:

   - `call_llm_node`: Just calls `_call_llm()` which adds messages; state computed automatically
   - `execute_tools_node`: Just executes tools adding `ToolResult` messages; pending tools computed automatically
   - Routers use computed methods: `snapshot.is_complete()`, `snapshot.get_pending_tool_calls()`, etc.

4. **AgentRunner simplified**:
   - Removed `_reset_loop_control_state()` - no stored state to reset
   - Error check uses computed methods: `snapshot.get_llm_turns_used()`, `snapshot.is_complete()`

**Computed State Algorithms**:

```python
def get_pending_tool_calls(self) -> list[ToolCallRequest]:
    """Find ToolCallRequests without matching results."""
    requests = {msg.tool_use_id: msg for msg in messages if msg.type == "tool_call"}
    for msg in messages:
        if msg.type in ("tool_result", "tool_result_error"):
            requests.pop(msg.tool_request.tool_use_id, None)
    return list(requests.values())

def is_complete(self) -> bool:
    """Last message is AssistantMessage."""
    return messages[-1].type == "assistant" if messages else False

def get_llm_turns_used(self) -> int:
    """Count LLM turns (ReasoningMessages) since last HumanMessage."""
    count = 0
    last_was_llm = False
    for msg in messages:
        if msg.type in ("human", "tool_result", "tool_result_error"):
            last_was_llm = False
        elif msg.type in ("internal", "assistant", "tool_call"):
            if not last_was_llm:
                count += 1
                last_was_llm = True
    return count
```

**Updated Graph Node Pattern** (simplified):

```python
def call_llm_node(state):
    def do_llm(history: ChatHistory, m: SnapshotUpdater):
        llm_result = runner._call_llm(history)
        log_tokens_usage(llm_result.token_usage)
        # No explicit state updates - messages are the source of truth

    runner._state.with_history(do_llm)
    return state

def execute_tools_node(state):
    def execute_all(history: ChatHistory, m: SnapshotUpdater):
        for tool_request in runner._state.snapshot.get_pending_tool_calls():
            runner._execute_tool(history, tool_request)
        # No explicit clear - adding ToolResult messages "matches" requests

    runner._state.with_history(execute_all)
    return state
```

**Benefits**:

- **No state synchronization bugs**: Can't have stale `is_complete` or `pending_tool_calls`
- **Simpler API**: `from_history()` only needs `history` and `plan_cache`
- **Self-documenting**: State computation is explicit in code
- **Replay-friendly**: State can be reconstructed from message history alone

**Files Modified**:

| File                       | Changes                                                                         |
| -------------------------- | ------------------------------------------------------------------------------- |
| `history_snapshot.py`      | Removed stored fields, added computed methods, simplified `from_history()`      |
| `langgraph_agent.py`       | Simplified `SnapshotUpdater`, removed state setters from nodes, updated routers |
| `agent_runner.py`          | Removed reset logic, use computed methods for error check                       |
| `test_history_snapshot.py` | Updated tests for computed behavior, removed immutable update tests             |
| `test_langgraph_agent.py`  | Updated tests for new API                                                       |
