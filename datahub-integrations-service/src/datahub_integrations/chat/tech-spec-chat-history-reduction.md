# Chat Context Reduction Technical Specification

## Problem Statement

The DataHub chat integrations (Slack and Teams) currently experience token limit errors when chat histories grow too long, resulting in poor user experience where users must start new conversations. Current error handling is reactive - errors are only caught after expensive LLM API calls fail with `ChatSessionMaxTokensExceededError`.

### Current Issues

- **Reactive Error Handling**: Token limit exceeded errors are handled at the presentation layer after API failures
- **Poor UX**: Users receive ":x: Uh, oh! Looks like I fetched too much information here. Please try asking your question in a new thread" messages
- **No Proactive Management**: No token estimation or history management before LLM calls
- **Duplicated Logic**: Slack and Teams would need separate solutions for the same problem
- **API Waste**: Failed API calls due to token limits waste compute and increase latency

### Technical Context

- Token counting capability exists in experimentation code (`eval_helpers.py` using `tiktoken`)
- Current rough estimation: 4 characters per token (used in `gen_ai/router.py`)
- Chat architecture uses `ChatHistory` with multiple message types: `HumanMessage`, `AssistantMessage`, `ReasoningMessage`, `ToolCallRequest`, `ToolResult`, `ToolResultError`, `SummaryMessage`
- **Dual History Architecture**: ChatHistory maintains both original `messages` and `reduced_history` for complete audit trail and reliable telemetry
- Primary integration point: `ChatSession._prepare_messages()` called before `_generate_tool_call()`

## Desired Outcome (Success Criteria)

1. **Reduced Token Errors**: 90% reduction in `ChatSessionMaxTokensExceededError` occurrences
2. **Improved UX**: Users can continue conversations longer without "start new thread" messages
3. **Maintained Quality**: Conversation quality remains high after context reduction (measured via user engagement and task completion)
4. **Performance**: Minimal token estimation overhead per chat session
5. **Shared Implementation**: Single context reduction logic used across both Slack and Teams platforms
6. **Prompt Caching**: Subsequent prompts should make use of prompt caching.

## Technical Plan ✅ IMPLEMENTED

### Architecture Overview

The solution implements three core abstractions: `TokenCountEstimator` for character-based token estimation with LRU caching, `ChatContextReducer` for context preparation with MLflow integration, and a **dual history architecture** in `ChatHistory` that maintains both original messages and reduced context. This integrates with `ChatSession._prepare_messages()` via `create_default_context_reducer_chain()` to proactively manage token limits before LLM API calls while preserving complete audit trails.

### Core Abstractions

#### 1. Dual History Architecture ✅ IMPLEMENTED

```python
class ChatHistory(BaseModel):
    messages: list[Message] = []  # Original complete history
    reduced_history: Optional[list[Message]] = None  # Reduced context for LLM
    extra_properties: dict = {}  # Metadata including reduction history

    @property
    def context_messages(self) -> list[Message]:
        """Returns the messages to use for the current chat session.

        If reduced history is available, it will be used. Otherwise, the full
        history will be used.
        """
        return self.reduced_history or self.messages

    def set_reduced_history(
        self, reduced_history: list[Message], reducer_metadata: dict
    ) -> None:
        """Set reduced history and track reduction metadata."""
        self.reduced_history = reduced_history
        self.extra_properties.setdefault("reducers", [])
        self.extra_properties["reducers"].append(reducer_metadata)
```

**Benefits:**

- **Complete Audit Trail**: Original messages always preserved
- **Reliable Telemetry**: Can measure tool calls and conversation metrics on original history
- **Clean Abstraction**: `context_messages` provides unified access to active context
- **Reduction Tracking**: Full metadata about all reduction operations applied

#### 2. Token Count Estimator ✅ IMPLEMENTED

```python
from functools import lru_cache

class TokenCountEstimator:
    """Estimates token count for chat history and individual messages."""

    def __init__(self, model: str):
        """Initialize the token estimator with model name for future optimizations."""
        self.model = model

    @staticmethod
    @lru_cache(maxsize=100)
    def estimate_tokens(text: str) -> int:
        """Estimate tokens using character-based approximation.

        Based on eval corpus analysis: 1.3 * len(text) / 4
        LRU cache provides performance optimization for repeated estimations.
        """
        return int(1.3 * len(text) / 4)
```

#### 3. Chat Context Reducer ✅ IMPLEMENTED

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class ContextReducerConfig:
    """Configuration for context reduction."""
    llm_token_limit: int = 200000  # Model-specific token limit
    safety_buffer: int = 10000     # Buffer for estimation errors
    system_message_tokens: int = 0 # Tokens used by system message
    tool_config_tokens: int = 0    # Tokens used by tool configuration


@dataclass
class ReductionMetadata:
    """Metadata about a context reduction operation."""
    reducer_name: str
    num_tokens_before: int
    num_tokens_after: int
    num_messages_before: int
    num_messages_after: int

class ChatContextReducer(ABC):
    """Reduces chat context for LLM calls when token limits are exceeded."""

    def __init__(self, token_estimator: TokenCountEstimator, config: ContextReducerConfig):
        self.token_estimator = token_estimator
        self.config = config

    @abstractmethod
    def _reduce(self, history: ChatHistory) -> List[Message]:
        """Apply reduction strategy and return reduced message list."""
        pass

    def reduce(self, history: ChatHistory) -> None:
        """Apply reduction strategy in-place, updating history.reduced_history if needed."""
        pass

    def needs_reduction(self, history: ChatHistory) -> bool:
        """Check if context reduction is needed."""
        return self.token_estimator.estimate_history_tokens(history) > self.config.llm_token_limit
```

### Implementation Strategies

#### Strategy 1: Conversation Summarizer [Similar to Langchain's ConversationSummaryBufferMemory] ✅ IMPLEMENTED

**Approach**: Summarize entire conversation on reaching token limit. Keep summary + last N messages in chat history.

```python
class ConversationSummarizer(ChatContextReducer):
    """Summarizes older messages when hitting token limit."""

    def _reduce(self, history: ChatHistory) -> List[Message]:
        split_index = self.split_at_context_fit(history.context_messages)
        messages_to_summarize = history.context_messages[:split_index]
        remaining_messages = history.context_messages[split_index:]

        # Create or update summary using Bedrock LLM
        summary_text = self._create_or_update_summary(messages_to_summarize)
        remaining_messages = self.adjust_remaining_messages(remaining_messages)

        return [SummaryMessage(text=summary_text)] + remaining_messages
```

**Pros:**

- Preserves high-level context and conversation flow
- Maintains user intent and conversation goals
- Effective for long conversations with multiple topics
- Reduces token count significantly while retaining meaning
- Good for maintaining conversation continuity

**Cons:**

- Requires additional LLM call for summarization (cost + latency)
- May lose specific details and exact user queries
- Summary quality depends on LLM summarization capabilities
- Risk of hallucination in summaries
- Complex implementation with error handling for summarization failures

#### Strategy 2: Selective Message Filter

**Approach**: Progressive removal of ToolResult messages, starting with oldest first.

```python
class SelectiveMessageFilter(ChatContextReducer):
    """Selectively removes ToolResult messages using progressive strategy"""

    def _reduce(self, history: ChatHistory) -> List[Message]:
        # Progressive removal of ToolResult messages, starting with oldest first
        # Implementation not yet completed in current codebase
        pass
```

**Pros:**

- Simple and fast implementation (no LLM calls needed)
- Preserves exact user queries and tool call requests
- Maintains conversation flow and user intent
- Low latency and cost
- Predictable behavior
- Progressive approach preserves recent tool results

**Cons:**

- May not reduce tokens enough if non-ToolResult messages are large
- Loses older tool execution results which might be important for context
- May break conversation continuity if tool results contained important information
- Less effective for conversations with many user messages

#### Strategy 3: Tool Output Reducer (DISCARDED - this has to be happen at tool level and not at chat history framework level)

**Approach**: Specifically manages large tool call outputs by summarizing/truncating them.

```python
class ToolOutputReducer(ChatContextReducer):
    """Specifically manages large tool call outputs by summarizing/truncating them."""

    def _reduce_(self, history: ChatHistory) -> ChatHistory:
        # Identify and summarize large tool outputs (ToolResult messages)
        # Keep tool calls and user messages intact
        # Useful for tools that return large datasets/results
```

**Pros:**

- Targets the most common source of token bloat (large tool outputs)
- Preserves user queries and tool call requests
- Can use simple truncation or intelligent summarization
- Maintains conversation structure
- Effective for data-heavy conversations

**Cons:**

- May lose important data from tool results
- Truncation might break data integrity
- Summarization of structured data can be challenging
- May not be sufficient if user messages are also large

#### Strategy 4: Sliding Window Reducer [Similar to langchain's ConversationBufferWindowMemory] ✅ IMPLEMENTED

**Approach**: Keep only the most recent N messages, discarding older ones completely.

```python
class SlidingWindowReducer(ChatContextReducer):
    """Keeps only the most recent N messages, discarding older ones."""

    def _reduce(self, history: ChatHistory) -> List[Message]:
        if len(history.context_messages) <= self.max_messages:
            return history.context_messages  # NOOP

        # Keep only the most recent messages
        reduced_messages = history.context_messages[-self.max_messages:]
        reduced_messages = self.adjust_remaining_messages(reduced_messages)

        # Ensure human/summary message exists for context
        if not any(isinstance(m, (HumanMessage, SummaryMessage)) for m in reduced_messages):
            last_intent_message = next(
                (m for m in reversed(history.context_messages)
                 if isinstance(m, (HumanMessage, SummaryMessage))), None
            )
            reduced_messages = [last_intent_message] + reduced_messages

        return reduced_messages
```

**Pros:**

- Extremely simple and fast implementation
- Predictable token reduction
- No LLM calls required
- Preserves exact recent context
- Low latency and cost
- **Reliable backstop** - guarantees token limit compliance

**Cons:**

- Loses all historical context
- May break conversation continuity
- User may need to repeat previous context
- Not suitable for long-running conversations
- May lose important earlier decisions or context

#### Strategy 5: Semantic Chunking Reducer

**Approach**: Groups related messages into semantic chunks and summarizes each chunk.
For example - summarize each turn of human-assistant conversation (all messages in between answering single question) into a summary, therefore generating one summary per user question.

```python
class SemanticChunkingReducer(ChatContextReducer):
    """Groups related messages into semantic chunks and summarizes each chunk."""

    def _reduce(self, history: ChatHistory) -> ChatHistory:
        # Group messages by semantic similarity or topic
        # Summarize each chunk while preserving key information
        # Maintain chunk boundaries and relationships
```

**Pros:**

- Preserves semantic structure of conversation
- Maintains topic boundaries
- More intelligent than simple truncation
- Can preserve important context within chunks
- Good for multi-topic conversations

**Cons:**

- Requires semantic analysis (complexity + cost)
- May not work well for all conversation types
- Chunking logic can be subjective
- Additional LLM calls for summarization
- Risk of losing cross-chunk relationships

**Implementation Priority:** This approach is very complex and should be considered only after exhausting simpler strategies. Focus on tool-level optimization and basic reduction strategies first.

### Integration Points

#### ChatSession Integration

Context reducers are automatically configured in `ChatSession.__init__()` and applied during message preparation.

```python
class ChatSession:
    def __init__(
        self,
        tools: Sequence[ToolWrapper | FastMCP],
        client: DataHubClient,
        history: Optional[ChatHistory] = None,
        # context_reducers auto-configured via create_default_context_reducer_chain
    ):
        # ... existing initialization ...
        self.context_reducers: Iterable[ChatContextReducer] = (
            create_default_context_reducer_chain(
                self._get_model_id(), self._get_tools_config()
            )
        )

    def _prepare_messages(self) -> list[dict]:
        # Apply context reduction if configured
        working_history = self.history

        for reducer in self.context_reducers:
            reducer.reduce(working_history)

        # Use context_messages which returns reduced_history or original messages
        messages = working_history.context_messages
        formatted_messages = [message.to_obj() for message in messages]

        # Apply prompt caching logic
        if self._use_prompt_caching:
            # ... existing caching logic ...

        return formatted_messages
```

#### Reducer Chain Pattern

**Recommended Architecture:** Use a transformer/reducer chain where multiple strategies are applied in priority order:

1. **Highest Priority:** Tool-level output optimization (push down to tools)
2. **Medium Priority:** Selective message filtering (remove old tool results)
3. **Backstop Priority:** Sliding window truncation (ensure no context window errors)

Each reducer in the chain re-estimates tokens after execution and decides whether to call the next reducer. This ensures we have reliable backstop reducers that prevent context window errors even if earlier strategies are insufficient.

#### Platform Usage

```python
# Context reducers are automatically configured in ChatSession
def create_default_context_reducer_chain(
    model_id: str,
    tools_config: dict,
) -> Iterable[ChatContextReducer]:
    estimator = TokenCountEstimator(model_id)

    config = ContextReducerConfig(
        llm_token_limit=CLAUDE_TOKEN_LIMIT if "claude" in model_id else 100000,
        safety_buffer=int(CLAUDE_TOKEN_LIMIT * 0.1),
        system_message_tokens=estimator.estimate_tokens(_SYSTEM_PROMPT),
        tool_config_tokens=estimator.estimate_tokens(json.dumps(tools_config)),
    )

    # Return iterable of reducers: ConversationSummarizer first, then SlidingWindowReducer
    return [
        ConversationSummarizer(estimator, config, max_messages_to_preserve=10),
        SlidingWindowReducer(estimator, config, max_messages=20),
    ]

# In platform handlers (mention.py, teams handlers)
chat_session = ChatSession(
    tools=tools,
    client=client,
    history=history,
    # context_reducers automatically configured
)
response = chat_session.generate_next_message()
```

### Implementation Plan

#### Phase 1: Core Abstractions and Implementations ✅ COMPLETED

Abstractions:

- ✅ Implement `TokenCountEstimator` with character-based estimation (1.3 \* len(text) / 4)
- ✅ Implement `ContextReducerConfig` dataclass with system/tool token accounting
- ✅ Create `ChatContextReducer` abstract base class with `reduce()` method and MLflow integration
- ✅ Add `ReductionMetadata` for comprehensive telemetry tracking
- ✅ Implement dual history architecture in `ChatHistory` with `context_messages` property

Implementations:

- ✅ Implement `ConversationSummarizer` with full LLM-based summarization using Bedrock
- ✅ Implement `SlidingWindowReducer` as reliable backstop with edge case handling
- ✅ Integrate reducer chain with `ChatSession._prepare_messages()`
- ✅ Add model-aware configuration management in `create_default_context_reducer_chain`
- ✅ Comprehensive test suite with mock-based testing in `tests/chat/test_context_reducer.py`
- ✅ MLflow integration for production observability with span tracking

#### Phase 2: Integration & Testing ✅ COMPLETED

- ✅ Integrated with `ChatSession` - automatic reducer chain configuration
- ✅ MLflow tracing integration for production monitoring
- ✅ Comprehensive test coverage in `tests/chat/test_context_reducer.py`
- ✅ Edge case handling for Bedrock API requirements (`adjust_remaining_messages`)
- ✅ Dual history architecture for reliable telemetry

### Future Iterations

Based on testing and telemetry, additional implementations can be developed:

- ✅ `ConversationSummarizer` implemented with full LLM-based summarization via Bedrock
- ✅ `SlidingWindowReducer` implemented as backstop strategy
- 📋 `SelectiveMessageFilter` for progressive ToolResult removal
- 📋 `ToolOutputReducer` for large tool result handling
- 📋 `SemanticChunkingReducer` for intelligent topic-based reduction
- 📋 Model-specific optimizations based on context window differences
- 📋 User preference-based strategy selection
- 📋 Dynamic strategy switching based on conversation patterns

**Legend:** ✅ Complete | 📋 Planned

### File Structure

```
datahub_integrations/chat/
├── context_reducer.py                # ✅ TokenCountEstimator, ChatContextReducer, ContextReducerConfig, ReductionMetadata
├── chat_history.py                   # ✅ ChatHistory with dual history architecture, SummaryMessage
├── chat_session.py                   # ✅ ChatSession with integrated reducer chain via create_default_context_reducer_chain()
├── reducers/
│   ├── __init__.py                   # ✅ Package initialization
│   ├── conversation_summarizer.py    # ✅ ConversationSummarizer with full Bedrock LLM integration
│   ├── sliding_window_reducer.py     # ✅ SlidingWindowReducer implementation
│   ├── selective_message_filter.py   # 📋 SelectiveMessageFilter (planned)
│   ├── tool_output_reducer.py        # 📋 ToolOutputReducer (future, lower priority)
│   └── semantic_chunking_reducer.py  # 📋 SemanticChunkingReducer (future, lowest priority)
├── tech-spec-chat-history-reduction.md  # ✅ This technical specification
└── tests/
    ├── test_context_reducer.py       # ✅ Comprehensive test coverage with mock-based testing
    ├── test_chat_history.py          # ✅ ChatHistory tests including dual history architecture
    └── test_chat_session.py          # ✅ ChatSession integration tests
```

### Required Changes

1. ✅ **ChatSession**: Integrated context reducers with automatic configuration via `create_default_context_reducer_chain()`
2. ✅ **Platform Handlers**: Context reducers automatically configured in ChatSession (no changes needed to platform handlers)
3. ✅ **Error Handling**: Bedrock API edge case handling via `adjust_remaining_messages()`
4. ✅ **MLflow Telemetry**: Full telemetry integration with detailed reduction metadata tracking
5. ✅ **Dual History Architecture**: Complete audit trail preservation with `reduced_history` field

## Testing Plan ✅ COMPLETED

- ✅ **Token Estimation Tests**: Mock-based testing with character-based estimation (1.3 \* len(text) / 4)
- ✅ **Reducer Trigger Tests**: Comprehensive test coverage for both reducers with needs_reduction() validation
- ✅ **Edge Case Testing**: Tool call balancing via adjust_remaining_messages(), human/summary message preservation
- ✅ **Integration Tests**: Full ChatSession integration with create_default_context_reducer_chain()
- ✅ **Summarization Tests**: Full LLM-based summarization with Bedrock integration, existing/new summary handling
- ✅ **Window Reduction Tests**: Message preservation, adjustment logic, and intent message handling
- ✅ **MLflow Integration Tests**: Span creation and metadata tracking validation
- 📋 **Performance Tests on Eval Corpus**: Planned for production telemetry analysis

### Testing Scenarios ✅ IMPLEMENTED

- ✅ **No Reduction Needed**: Small conversations pass through unchanged via needs_reduction() check
- ✅ **ConversationSummarizer Only**: Large conversations get summarized while preserving recent messages
- ✅ **Reducer Chain**: Both ConversationSummarizer and SlidingWindowReducer applied in sequence
- ✅ **Summary Updates**: Existing summaries get updated with new content via \_update_summary_text()
- ✅ **Edge Cases**: Tool call balancing via adjust_remaining_messages(), human/summary message preservation
- ✅ **Multiple Reductions**: Reducer chain supports multiple reducers applied sequentially
- ✅ **Followup Questions**: Reduced history persists across conversation turns via dual history architecture

## Telemetry Plan

### Extend Existing Telemetry Events

#### MLflow Integration ✅ IMPLEMENTED

Context reduction operations are fully instrumented with MLflow tracing:

```python
# In ChatContextReducer.reduce()
with mlflow.start_span(
    f"reduce_history_{self.__class__.__name__}",
    span_type=mlflow.entities.SpanType.TOOL,
    attributes={"reducer_name": self.__class__.__name__},
) as span:
    # ... reduction logic ...
    span.set_attributes(asdict(reduction_metadata))
```

#### ChatHistory Metadata Enhancement ✅ IMPLEMENTED

Reduction metadata is tracked directly in ChatHistory:

```python
@property
def reduction_sequence_json(self) -> Optional[str]:
    return (
        json.dumps(self.extra_properties.get("reducers"))
        if self.extra_properties.get("reducers")
        else None
    )

@property
def num_reducers_applied(self) -> int:
    return len(self.extra_properties.get("reducers", []))
```

#### Chat Session Tracking

Add context reduction metrics to chat session lifecycle:

### Production Monitoring ✅ IMPLEMENTED

#### MLflow Observability

Full production monitoring via MLflow spans:

- ✅ **Reduction Operations**: Each reducer creates detailed MLflow spans
- ✅ **Performance Metrics**: Token counts before/after, message counts, execution time
- ✅ **Reducer Attribution**: Which reducers were applied in what sequence
- ✅ **Session Tracking**: Reduction history preserved across conversation turns

#### Token Error Reduction Tracking ✅ READY

Context reduction proactively prevents `ChatSessionMaxTokensExceededError`:

- ✅ **Proactive Prevention**: Reduction applied before LLM calls
- ✅ **Fallback Strategy**: SlidingWindowReducer ensures token limit compliance
- ✅ **Error Context**: Reduction metadata available when errors do occur

#### Performance Impact Measurement

Use existing performance monitoring to track:

- End-to-end chat session latency impact
- Memory usage during context reduction operations
- CPU overhead from token estimation and summarization

#### Conversation Quality Indicators

Leverage existing conversation metrics:

- Average conversation length (message count per session)
- Session completion rates vs. early termination
- User engagement patterns post-context reduction

### Operational Monitoring

#### Context Reduction Usage ✅ AVAILABLE

Comprehensive metrics available through ChatHistory properties:

```python
# Available metrics
history.num_reducers_applied  # Number of reducers applied
history.reduction_sequence_json  # Complete reduction audit trail
history.context_messages  # Active context size
len(history.messages)  # Original conversation size

# MLflow span attributes include:
# - reducer_name
# - num_tokens_before/after
# - num_messages_before/after
```

#### Error Pattern Analysis

Monitor context reduction failures:

- Token estimation errors or timeouts
- Summarization failures
- Performance degradation beyond acceptable thresholds
