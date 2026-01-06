"""
Core agent infrastructure for running agentic loops.

This module provides AgentRunner, which implements the reusable agentic loop
infrastructure extracted from ChatSession. It's completely agent-agnostic and
configured via AgentConfig.
"""

from datahub_integrations.gen_ai.mlflow_init import is_mlflow_enabled

import contextlib
import json
import os
import platform
import socket
import uuid
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
    cast,
)

import mlflow
import mlflow.entities
from datahub.sdk.main_client import DataHubClient
from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.chat.agent.agent_config import AgentConfig
from datahub_integrations.chat.agent.history_snapshot import ChatHistorySnapshot
from datahub_integrations.chat.agent.langgraph_agent import (
    AgentGraphState,
    SnapshotUpdater,
    build_agent_graph,
)
from datahub_integrations.chat.agent.progress_tracker import (
    ProgressCallback,
    ProgressTracker,
)
from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    ImmutableChatHistory,
    Message,
    ReasoningMessage,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.chat.planner.planning_context import PlanningContext
from datahub_integrations.gen_ai.llm.exceptions import LlmInputTooLongException
from datahub_integrations.gen_ai.llm.factory import get_llm_client
from datahub_integrations.mcp.mcp_server import with_datahub_client
from datahub_integrations.mcp_integration.tool import ToolWrapper
from datahub_integrations.observability import (
    detect_provider_and_normalize_model,
    get_cost_tracker,
)
from datahub_integrations.observability.cost import TokenUsage as ObsTokenUsage
from datahub_integrations.slack.utils.string import truncate
from datahub_integrations.telemetry.chat_events import (
    ChatbotInteractionEvent,
    ChatbotToolCallEvent,
)
from datahub_integrations.telemetry.telemetry import track_saas_event

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        ContentBlockOutputTypeDef,
        TokenUsageTypeDef,
    )

    from datahub_integrations.chat.planner.models import Plan


class AgentError(Exception):
    """Base exception for agent errors."""

    pass


class AgentMaxTokensExceededError(AgentError):
    """Raised when input exceeds model's context window."""

    pass


class AgentOutputMaxTokensExceededError(AgentError):
    """Raised when output is truncated due to max_tokens limit."""

    pass


class AgentMaxLLMTurnsExceededError(AgentError):
    """Raised when agent exceeds maximum allowed tool calls."""

    pass


@dataclass(frozen=True)
class LLMCallResult:
    """
    Structured output from calling LLM (immutable).

    This dataclass captures all relevant information from an LLM call,
    separating the LLM response from tool execution. This separation
    enables HITL (human-in-the-loop) approval in future phases.

    Immutable because:
    - It's a return value that should not be modified after creation
    - Prevents accidental mutation bugs
    - Thread-safe for concurrent session handling (Phase 2)

    Attributes:
        new_messages: Messages added to history during this LLM call
            (reasoning, assistant message, or tool call requests)
        pending_tool_calls: Tool calls requested by the LLM that need execution.
            These are NOT executed yet - caller decides when to execute.
        is_complete: True if this is a final response (end_turn with no tools)
        stop_reason: The LLM's stop reason for debugging/logging
        token_usage: Token usage statistics from the LLM response
        output_truncated: True if stopReason was "max_tokens".
            Note: If True, an exception is raised before returning,
            so this will always be False in a successfully returned result.
            Kept for potential future use with graceful truncation handling.
    """

    new_messages: tuple  # Use tuple for immutability (contains Message objects)
    pending_tool_calls: tuple  # Use tuple for immutability (contains ToolCallRequest)
    is_complete: bool
    stop_reason: str
    token_usage: "TokenUsageTypeDef"  # Token usage statistics from the LLM response
    output_truncated: bool


@dataclass(frozen=True)
class ToolExecutionResult:
    """
    Structured output from executing a tool (immutable).

    This dataclass captures the result of a single tool execution,
    providing structured data for telemetry and caller inspection.

    Immutable because:
    - It's a return value that should not be modified after creation
    - Prevents accidental mutation bugs

    Attributes:
        message: The ToolResult or ToolResultError added to history
        duration_seconds: How long the tool took to execute
        tool_name: Name of the tool that was executed
    """

    message: Union["ToolResult", "ToolResultError"]
    duration_seconds: float
    tool_name: str


def _strip_reasoning_tag(text: str) -> str:
    """
    Remove <reasoning> tags and their contents from the text.

    This is used to handle cases where the LLM includes a <reasoning> tag
    with internal thoughts that should not be shown to the user.
    """
    from bs4 import BeautifulSoup

    try:
        soup = BeautifulSoup(text, "html.parser")
        for reasoning_tag in soup.find_all("reasoning"):
            reasoning_tag.decompose()
        return soup.get_text()
    except Exception as e:
        logger.warning(f"Failed to strip reasoning tag from response: {e}")
        return text


def log_tokens_usage(response: "TokenUsageTypeDef") -> None:
    """Log token usage statistics from LLM response."""
    input_tokens = response["inputTokens"]
    output_tokens = response["outputTokens"]
    cache_read_input_tokens = response.get("cacheReadInputTokens", 0)
    cache_creation_input_tokens = response.get("cacheWriteInputTokens", 0)
    total_input_tokens = (
        input_tokens + cache_read_input_tokens + cache_creation_input_tokens
    )

    logger.info(
        f"Tokens usage: total input tokens: {total_input_tokens}, total output tokens: {output_tokens}"
    )


def enrich_event_with_agent_data(
    event_data: ChatbotInteractionEvent, agent: Optional["AgentRunner"]
) -> None:
    """
    Enrich telemetry event with agent data.

    Uses agent.history property which accesses the current snapshot's history.
    """
    if agent:
        event_data.chat_session_id = agent.session_id
        history = agent.history
        if history:
            event_data.num_tool_calls = history.num_tool_calls
            event_data.num_tool_call_errors = history.num_tool_call_errors
            event_data.num_history_messages = len(history.messages)
            event_data.full_history = history.json(indent=None)
            event_data.reduction_sequence = history.reduction_sequence_json
            event_data.num_reducers_applied = history.num_reducers_applied


class AgentRunner:
    """
    Core infrastructure for running agentic loops.

    AgentRunner is completely agent-agnostic and configured via AgentConfig.
    It handles:
    - Agentic loop (generate_next_message)
    - LLM interaction with error handling
    - Tool execution with MLflow tracing
    - Message history management
    - Progress tracking
    - Token usage tracking

    Example:
        ```python
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("You are a helpful assistant"),
            tools=[mcp],
        )
        runner = AgentRunner(config=config, client=client)
        runner.add_message(HumanMessage(text="Hello"))
        response = runner.generate_next_message()
        ```
    """

    def __init__(
        self,
        config: AgentConfig,
        client: DataHubClient,
        history: Optional[ChatHistory] = None,
    ):
        """
        Initialize agent runner with configuration.

        Args:
            config: Agent configuration defining behavior
            client: DataHub client for tool execution
            history: Optional existing chat history to continue from
        """
        self.config = config
        self.client = client
        self.session_id = str(uuid.uuid4())

        # Use tools from config (already prepared)
        self.tools: List[ToolWrapper] = config.tools
        self.plannable_tools: List[ToolWrapper] = config.plannable_tools

        # Create initial snapshot from provided history (or empty)
        # This is the single source of truth for all agent state
        initial_history = history or ChatHistory()
        initial_snapshot = ChatHistorySnapshot.from_history(
            history=initial_history,
            plan_cache={},
        )
        self._state = AgentGraphState(initial_snapshot)

        # Create planning context with reference to state holder
        # Planning tools use this for plan state access
        self._planning_context = PlanningContext(
            plannable_tools=self.plannable_tools,
            holder=self._state,
        )

        # Set up context reducers
        if config.context_reducers is not None:
            self.context_reducers = config.context_reducers
        else:
            # Use default reducers
            from datahub_integrations.chat.context_reducer import ContextReducerConfig
            from datahub_integrations.chat.reducers.conversation_summarizer import (
                ConversationSummarizer,
            )
            from datahub_integrations.chat.reducers.sliding_window_reducer import (
                SlidingWindowReducer,
            )
            from datahub_integrations.gen_ai.model_config import model_config
            from datahub_integrations.mcp._token_estimator import (
                TokenCountEstimator,
                get_token_limit,
            )

            TOKEN_LIMIT = get_token_limit(config.model_id)
            estimator = TokenCountEstimator(config.model_id)

            # Get system messages for token estimation
            system_messages = config.system_prompt_builder.build_system_messages(client)
            system_prompt_text = "\n".join(
                msg.get("text", "") for msg in system_messages
            )

            tools_config = {"tools": [tool.to_bedrock_spec() for tool in self.tools]}

            reducer_config = ContextReducerConfig(
                llm_token_limit=TOKEN_LIMIT,
                safety_buffer=int(TOKEN_LIMIT * 0.1),
                system_message_tokens=estimator.estimate_tokens(system_prompt_text),
                tool_config_tokens=estimator.estimate_tokens(json.dumps(tools_config)),
            )

            self.context_reducers = [
                ConversationSummarizer(
                    estimator,
                    reducer_config,
                    max_num_messages_to_keep=5,
                    min_num_messages_to_keep=3,
                    summarization_model=model_config.chat_assistant_ai.summary_model,
                ),
                SlidingWindowReducer(estimator, reducer_config, max_messages=10),
            ]

        # Create progress tracker with conversational parser (push-based)
        self._progress_tracker = ProgressTracker(
            progress_callback=None,
            get_plan=self._get_plan,
            conversational_parser=config.conversational_parser,
        )

        # Create bound logger with session_id for consistent logging
        self._logger = logger.bind(session_id=self.session_id)

        # Build the LangGraph for agent orchestration
        self._graph = build_agent_graph(self)

        self._logger.info(
            f"Initialized {config.agent_name} (session={self.session_id}) with {len(self.tools)} tools"
        )

    @property
    def tool_map(self) -> Dict[str, ToolWrapper]:
        """Get mapping of tool names to tool instances."""
        return {tool.name: tool for tool in self.tools}

    @property
    def history(self) -> ImmutableChatHistory:
        """
        Get read-only view of current chat history.

        Returns an ImmutableChatHistory to prevent accidental mutations.
        The type system will flag attempts to call add_message() or other
        mutating methods on the returned object.

        To add messages, use agent.add_message().
        For complex history modifications, use agent._state.with_history().

        Returns:
            ImmutableChatHistory - read-only view of the current history
        """
        return self._state.snapshot.to_history()

    def _get_plan(self, plan_id: str) -> Optional["Plan"]:
        """
        Retrieve a plan by ID from the agent's state.

        This method conforms to the PlanGetter protocol and is used by
        ProgressTracker and ConversationalParser for plan progress formatting.

        Args:
            plan_id: The unique identifier of the plan

        Returns:
            The Plan object if found, None otherwise
        """
        plan_entry = self._state.get_plan(plan_id)
        return plan_entry.get("plan") if plan_entry else None

    def add_message(self, message: Message) -> None:
        """
        Add a message to the chat history.

        This method provides a convenient way to add messages without
        using the lower-level with_history() callback. It also notifies
        the progress tracker for user-facing progress updates.

        Args:
            message: The message to add to history
        """

        def add_msg(history: ChatHistory, m: SnapshotUpdater) -> None:
            self._add_message(history, message)

        self._state.with_history(add_msg)

    def _get_system_messages(self) -> List[Any]:
        """Get system messages from the configured prompt builder."""
        return self.config.system_prompt_builder.build_system_messages(self.client)

    def _add_message(self, history: ChatHistory, message: Message) -> None:
        """
        Add a message to history with logging and progress notification.

        This helper method handles:
        - Debug logging of the message being added
        - Mutating the passed history instance
        - Notifying the progress tracker of the update

        Args:
            history: The ChatHistory to mutate. Caller owns this instance.
            message: Message to add to history
        """
        # Log messages for debugging
        if isinstance(message, ToolResult):
            self._logger.debug(
                f"Adding ToolResult for {message.tool_request.tool_name}: {truncate(str(message), max_length=1000, show_length=True)}"
            )
        elif isinstance(message, ToolResultError):
            self._logger.debug(
                f"Adding ToolResultError for {message.tool_request.tool_name}: {truncate(str(message), max_length=1000, show_length=True)}"
            )
        else:
            self._logger.debug(
                f"Adding {type(message).__name__} message: {truncate(str(message), max_length=400, show_length=True)}"
            )

        history.add_message(message)
        self._progress_tracker.on_message(message)

    @contextlib.contextmanager
    def set_progress_callback(
        self, progress_callback: ProgressCallback
    ) -> Iterator[None]:
        """
        Set a callback for progress updates during generation.

        This is a context manager that temporarily sets a progress callback,
        which will be called whenever there are new progress updates.

        Args:
            progress_callback: Callback that receives List[ProgressUpdate]

        Example:
            ```python
            def my_callback(updates):
                for update in updates:
                    print(f"{update.message_type}: {update.text}")

            with runner.set_progress_callback(my_callback):
                response = runner.generate_next_message()
            ```
        """
        prev_tracker = self._progress_tracker
        self._progress_tracker = ProgressTracker(
            progress_callback=progress_callback,
            get_plan=self._get_plan,
            conversational_parser=self.config.conversational_parser,
        )
        try:
            yield
        finally:
            self._progress_tracker = prev_tracker

    def _prepare_messages(self, history: ChatHistory) -> List[dict]:
        """
        Prepare messages for LLM with context reduction and prompt caching.

        Args:
            history: The ChatHistory to prepare messages from.
                Context reducers will mutate this history if reduction is needed.

        Returns:
            List of formatted message dictionaries ready for LLM API
        """
        # Apply context reduction if configured
        for reducer in self.context_reducers:
            reducer.reduce(history)

        raw_messages = [message.to_obj() for message in history.context_messages]

        # Transform internal history format to LLM API format.
        # This handles:
        # 1. Filtering out internal messages (e.g., ApprovalRequest/Response for HITL)
        # 2. Consolidating consecutive same-role messages (required by Bedrock)
        formatted_messages = self._transform_history_to_api_format(raw_messages)

        # Add prompt caching markers if enabled
        if self.config.use_prompt_caching:
            # Mark potential cache points on the consolidated messages.
            # We add cache points after user or assistant messages (not in the middle
            # of tool call sequences). Use at most 2 cache points for optimal performance.
            # Note: After consolidation, each message contains all content blocks for that
            # turn, so we simply pick the last 2 messages as cache points.
            if len(formatted_messages) >= 2:
                # Add cache points to the last 2 messages
                for index in range(
                    max(0, len(formatted_messages) - 2), len(formatted_messages)
                ):
                    formatted_messages[index]["content"].append(
                        {"cachePoint": {"type": "default"}}
                    )
            elif len(formatted_messages) == 1:
                # Only one message - add cache point to it
                formatted_messages[0]["content"].append(
                    {"cachePoint": {"type": "default"}}
                )

        return formatted_messages

    def _transform_history_to_api_format(self, messages: List[dict]) -> List[dict]:
        """
        Transform internal history representation to LLM API format.

        The internal history stores each logical event as a separate message for:
        - Complete audit trail and debugging
        - Progress tracking on individual messages
        - Clean checkpointing/serialization

        The LLM API (Bedrock) requires a different format:
        - No internal messages (ApprovalRequest/Response for HITL)
        - Consecutive same-role messages consolidated into one
        - Strict alternating user/assistant pattern

        This method bridges the two representations by:
        1. Filtering out internal message types
        2. Consolidating consecutive same-role messages

        Args:
            messages: List of message dicts from internal history

        Returns:
            Transformed list ready for LLM API
        """
        if not messages:
            return messages

        # Step 1: Filter out internal messages
        # (Currently none, but ApprovalRequest/Response will be added for HITL in Phase 3)
        filtered = self._filter_internal_messages(messages)

        # Step 2: Consolidate consecutive same-role messages
        return self._consolidate_consecutive_roles(filtered)

    def _filter_internal_messages(self, messages: List[dict]) -> List[dict]:
        """
        Filter out internal message types that shouldn't be sent to the LLM.

        Internal messages are used for state tracking (e.g., HITL approvals)
        but should not be part of the LLM conversation.

        Args:
            messages: List of message dicts

        Returns:
            Filtered list with internal messages removed
        """
        # TODO: Filter ApprovalRequest/ApprovalResponse when HITL is implemented (Phase 3)
        # For now, pass through all messages
        return messages

    def _consolidate_consecutive_roles(self, messages: List[dict]) -> List[dict]:
        """
        Consolidate consecutive messages of the same role into single messages.

        Bedrock's Converse API requires that:
        - Multiple tool calls from one LLM response be in ONE assistant message
        - Multiple tool results be in ONE user message

        Without consolidation, sending separate messages causes validation errors like:
        "Expected toolResult blocks for Ids: X, but found: Y"

        Args:
            messages: List of message dicts with 'role' and 'content' keys

        Returns:
            Consolidated list where consecutive same-role messages are merged
        """
        if not messages:
            return messages

        consolidated: List[dict] = []

        for msg in messages:
            if consolidated and consolidated[-1]["role"] == msg["role"]:
                # Same role as previous - merge content blocks
                consolidated[-1]["content"].extend(msg["content"])
            else:
                # Different role - start new message
                # Make a copy to avoid mutating the original
                consolidated.append(
                    {"role": msg["role"], "content": list(msg["content"])}
                )

        return consolidated

    def _call_llm(self, history: ChatHistory) -> LLMCallResult:
        """
        Call the LLM and process the response, but do NOT execute tools.

        This method:
        1. Prepares messages with context reduction
        2. Calls the LLM with tools
        3. Processes the response (reasoning, tool calls, or final message)
        4. Adds appropriate messages to history
        5. Returns structured result with pending tool calls for caller to execute

        The separation of LLM calling from tool execution enables:
        - HITL (human-in-the-loop) approval before tool execution
        - Better testing of LLM responses
        - Clearer control flow in the agent loop

        Args:
            history: The ChatHistory to use and mutate. Caller owns this instance.

        Returns:
            LLMCallResult with new messages and pending tool calls

        Raises:
            AgentMaxTokensExceededError: If input exceeds context window
            AgentOutputMaxTokensExceededError: If output is truncated
            AgentError: For other LLM errors
        """
        llm_client = get_llm_client(self.config.model_id)

        messages = self._prepare_messages(history)

        # Prepare tools with optional caching
        tools = [tool.to_bedrock_spec() for tool in self.tools]
        if self.config.use_prompt_caching:
            tools.append({"cachePoint": {"type": "default"}})

        try:
            response = llm_client.converse(
                system=self._get_system_messages(),
                messages=messages,  # type: ignore
                toolConfig={"tools": tools},  # type: ignore
                inferenceConfig={
                    "temperature": self.config.temperature,
                    "maxTokens": self.config.max_tokens,
                },
            )
        except LlmInputTooLongException as e:
            raise AgentMaxTokensExceededError(str(e)) from e
        except Exception:
            # Track failed LLM calls
            tracker = get_cost_tracker()
            provider, model_name = detect_provider_and_normalize_model(
                self.config.model_id
            )

            tracker.record_llm_call(
                provider=provider,
                model=model_name,
                usage=ObsTokenUsage(
                    prompt_tokens=0, completion_tokens=0, total_tokens=0
                ),
                ai_module=self.config.ai_module,
                success=False,
            )
            raise

        log_tokens_usage(response["usage"])  # type: ignore[arg-type]

        # Track cost for observability
        usage = response.get("usage", {})
        if usage:
            tracker = get_cost_tracker()
            provider, model_name = detect_provider_and_normalize_model(
                self.config.model_id
            )

            # Extract token counts, including cache tokens if present
            prompt_tokens = cast(int, usage.get("inputTokens", 0))
            completion_tokens = cast(int, usage.get("outputTokens", 0))
            total_tokens = cast(
                int, usage.get("totalTokens", prompt_tokens + completion_tokens)
            )

            # Bedrock cache token fields (if using prompt caching)
            cache_read_tokens = cast(int, usage.get("cacheReadInputTokens", 0))
            cache_write_tokens = cast(int, usage.get("cacheCreationInputTokens", 0))

            tracker.record_llm_call(
                provider=provider,
                model=model_name,
                usage=ObsTokenUsage(
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=total_tokens,
                    cache_read_tokens=cache_read_tokens,
                    cache_write_tokens=cache_write_tokens,
                ),
                ai_module=self.config.ai_module,
                success=True,
            )

        # Process stop reason
        output = response["output"]
        stop_reason = response["stopReason"]
        output_truncated = stop_reason == "max_tokens"

        # Fail fast if output was truncated
        if output_truncated:
            raise AgentOutputMaxTokensExceededError(str(response))

        is_end_turn = stop_reason == "end_turn"
        if stop_reason not in ("tool_use", "end_turn"):
            raise AgentError(f"Unknown stop reason {stop_reason}: {response}")

        message = output.get("message")
        if message is None:
            raise AgentError(f"No message in response {response}")

        response_content = message["content"]

        # Log multiple tool calls in single response
        tool_use_blocks = [block for block in response_content if "toolUse" in block]
        if len(tool_use_blocks) > 1:
            self._logger.info(
                f"LLM returned {len(tool_use_blocks)} tool calls in a single response. "
                f"Will execute sequentially. Tools: {[block['toolUse']['name'] for block in tool_use_blocks]}"
            )

        # Process each content block and collect results
        new_messages: List[Message] = []
        pending_tool_calls: List[ToolCallRequest] = []

        for i, content_block in enumerate(response_content):
            is_last_block = i == len(response_content) - 1

            if "text" in content_block:
                # Handle text content (reasoning or final response)
                msg = self._handle_text_content(
                    history, content_block, is_end_turn, is_last_block
                )
                new_messages.append(msg)

            elif "toolUse" in content_block:
                # Create tool call request but do NOT execute - caller will execute
                tool_use = content_block["toolUse"]
                tool_request = ToolCallRequest(
                    tool_use_id=tool_use["toolUseId"],
                    tool_name=tool_use["name"],
                    tool_input=tool_use["input"],
                )
                self._add_message(history, tool_request)
                new_messages.append(tool_request)
                pending_tool_calls.append(tool_request)

            else:
                raise AgentError(f"Unknown content block type {content_block}")

        return LLMCallResult(
            new_messages=tuple[Message, ...](new_messages),
            pending_tool_calls=tuple[ToolCallRequest, ...](pending_tool_calls),
            is_complete=is_end_turn and not pending_tool_calls,
            stop_reason=stop_reason,
            token_usage=cast("TokenUsageTypeDef", response["usage"]),
            output_truncated=False,  # Always False here since we raise if truncated
        )

    def _handle_text_content(
        self,
        history: ChatHistory,
        content_block: "ContentBlockOutputTypeDef",
        is_end_turn: bool,
        is_last_block: bool,
    ) -> Message:
        """
        Handle text content from LLM response.

        This can be either:
        - Reasoning (internal thoughts before tool use)
        - Final assistant message (when no tools are used)

        Args:
            history: The ChatHistory to mutate. Caller owns this instance.
            content_block: Content block from LLM response
            is_end_turn: Whether this is the final block in a turn
            is_last_block: Whether this is the last block in the response

        Returns:
            The message that was created and added to history
        """
        is_final_response = is_last_block and is_end_turn

        if is_final_response:
            # Direct response without using tools (fallback case)
            response_text = content_block["text"]
            response_text = _strip_reasoning_tag(response_text)
            self._logger.info(f"Adding AssistantMessage: {response_text}")
            msg: Message = AssistantMessage(text=response_text)
            self._add_message(history, msg)

            # Log to MLflow
            attributes = {
                "response_length": len(response_text),
                "message_index": len(history.messages),
                "is_unexpected_direct_response": True,
            }

            with mlflow.start_span(
                f"assistant_message_{len(history.messages)}",
                span_type=mlflow.entities.SpanType.LLM,
                attributes=attributes,
            ) as span:
                span.set_inputs(
                    {"context": "Direct LLM response (bypassed internal tools)"}
                )
                span.set_outputs({"response": response_text})

            return msg
        else:
            # Reasoning message (internal thoughts)
            reasoning_text = content_block["text"]
            msg = ReasoningMessage(text=reasoning_text)
            self._add_message(history, msg)

            # Log to MLflow
            attributes = {
                "reasoning_length": len(reasoning_text),
                "message_index": len(history.messages),
            }

            with mlflow.start_span(
                f"reasoning_step_{len(history.messages)}",
                span_type=mlflow.entities.SpanType.LLM,
                attributes=attributes,
            ) as span:
                span.set_inputs({"context": "LLM internal thinking"})
                span.set_outputs({"reasoning": reasoning_text})

            return msg

    def _execute_tool(
        self, history: ChatHistory, tool_request: ToolCallRequest
    ) -> ToolExecutionResult:
        """
        Execute a single tool and add the result to history.

        This method executes the tool specified in the tool_request and adds
        the result (success or error) to the history. Telemetry is tracked
        within this method to keep it co-located with execution.

        Args:
            history: The ChatHistory to mutate. Caller owns this instance.
            tool_request: The tool call request to execute (already added to history
                by _call_llm).

        Returns:
            ToolExecutionResult with the result message and execution metadata
        """
        tool_name = tool_request.tool_name
        result = None
        error = None
        timer = PerfTimer()

        try:
            tool = self.tool_map[tool_name]
            with timer, with_datahub_client(self.client):
                result = tool.run(arguments=tool_request.tool_input)

        except Exception as e:
            logger.exception(
                f"Tool execution failed for {tool_name} in session {self.session_id}"
            )
            error = f"{type(e).__name__}: {e}"
            msg: Union[ToolResult, ToolResultError] = ToolResultError(
                tool_request=tool_request,
                error=error,
            )
            self._add_message(history, msg)
            tracker = get_cost_tracker()
            tracker.record_tool_call(
                ai_module=self.config.ai_module, tool=tool_name, success=False
            )
        else:
            msg = ToolResult(tool_request=tool_request, result=result)
            self._add_message(history, msg)
            # Track successful tool call (Tier 3)
            tracker = get_cost_tracker()
            tracker.record_tool_call(
                ai_module=self.config.ai_module, tool=tool_name, success=True
            )
        tool_result = ToolExecutionResult(
            message=msg,
            duration_seconds=timer.elapsed_seconds(),
            tool_name=tool_name,
        )

        # Track telemetry (co-located with execution for LangGraph compatibility)
        track_saas_event(
            ChatbotToolCallEvent(
                chat_session_id=self.session_id,
                tool_name=tool_result.tool_name,
                tool_input=tool_request.tool_input,
                tool_execution_duration_sec=tool_result.duration_seconds,
                tool_result_length=(
                    len(str(tool_result.message.result))
                    if isinstance(tool_result.message, ToolResult)
                    else None
                ),
                tool_result_is_error=isinstance(tool_result.message, ToolResultError),
                tool_error=(
                    tool_result.message.error
                    if isinstance(tool_result.message, ToolResultError)
                    else None
                ),
            )
        )

        return tool_result

    @mlflow.trace
    def generate_next_message(
        self, completion_check: Optional[Callable[[Message], bool]] = None
    ) -> Message:
        """
        Generate the next message via LangGraph-based agentic loop.

        This is the main method that orchestrates the agent's behavior using
        a LangGraph StateGraph:
        1. Generate reasoning and tool calls (call_llm node)
        2. Execute tools (execute_tools node)
        3. Repeat until completion condition is met

        The agent loop continues until the completion_check returns True. Each iteration
        generates a message which can be:
        - ToolRequest: Agent wants to call a tool (loop continues after execution)
        - AssistantMessage: Agent provides a direct text response (typically final)
        - ToolResult/ToolResultError: Result from tool execution (loop continues)

        Args:
            completion_check: Optional function to check if agent should stop.
                            If None, uses default behavior: stops when agent generates
                            an AssistantMessage (direct text response instead of tool call).

                            This default is appropriate for most use cases where the agent
                            calls tools as needed and eventually responds with text.

                            Custom completion checks are useful when:
                            - Waiting for a specific tool to be called (e.g., respond_to_user)
                            - Enforcing that certain actions must be taken
                            - Implementing custom termination logic

                            Function signature: (Message) -> bool
                            Returns True if the agent should stop, False to continue.

        Returns:
            The final message when completion_check returns True.
            Typically an AssistantMessage, but can be any Message type depending
            on the completion_check logic.

        Raises:
            AgentMaxLLMTurnsExceededError: If max_llm_turns is exceeded
            AgentMaxTokensExceededError: If input exceeds context window
            AgentOutputMaxTokensExceededError: If output is truncated
        """
        if is_mlflow_enabled():
            # Add session and environment metadata
            mlflow.update_current_trace(
                tags={
                    "session_id": self.session_id,
                    "agent_name": self.config.agent_name,
                    "model_id": self.config.model_id,
                    "machine.hostname": socket.gethostname(),
                    "machine.user": os.getenv("USER")
                    or os.getenv("USERNAME")
                    or "unknown",
                    "machine.os": f"{platform.system()} {platform.release()}",
                    "machine.python_version": platform.python_version(),
                }
            )

        logger.info(
            f"Generating next message for {self.config.agent_name} (session={self.session_id}), "
            f"currently have {len(self._state.snapshot.messages_json)} messages in history"
        )

        # Default completion check: Stop when agent generates a direct text response (AssistantMessage)
        # rather than calling more tools. This is the natural completion point for most agents -
        # they call tools as needed, then respond to the user with their findings.
        if completion_check is None:

            def completion_check(msg: Message) -> bool:
                return isinstance(msg, AssistantMessage)

        # No explicit state reset needed - execution state is computed from messages:
        # - pending_tool_calls: computed from unmatched ToolCallRequest messages
        # - is_complete: computed from last message being AssistantMessage
        # - tool_calls_used: computed from LLM response sequences since last HumanMessage

        # LangGraph config - state lives in self._state, graph nodes access it directly
        # completion_check is passed via config because functions can't be serialized
        # Cast to Any to satisfy mypy since RunnableConfig accepts dict-like structures
        graph_config = {
            "configurable": {
                "thread_id": self.session_id,
                "completion_check": completion_check,
            }
        }

        # Run graph (nodes update self._state directly via with_history)
        # The empty initial_state is just for LangGraph mechanics
        self._graph.invoke({}, cast(Any, graph_config))

        # State is now in self._state (updated by graph nodes)
        snapshot = self._state.snapshot

        # Check for max tool calls error (computed from message sequences)
        if snapshot.get_llm_turns_used() >= self.config.max_llm_turns:
            if not snapshot.is_complete():
                raise AgentMaxLLMTurnsExceededError(
                    f"Failed to generate next message after {self.config.max_llm_turns} tool calls"
                )

        # Return the last message
        final_history = snapshot.to_history()
        if not final_history.messages:
            raise AgentError("No messages in chat history")

        last_message = final_history.messages[-1]
        logger.info(
            f"Agent completed for {self.config.agent_name} (session={self.session_id})"
        )
        return last_message

    def generate_formatted_message(
        self, completion_check: Optional[Callable[[Message], bool]] = None
    ) -> Any:
        """
        Generate the next message and apply response formatter if configured.

        This is a convenience method that:
        1. Calls generate_next_message() to run the agentic loop
        2. Applies the response_formatter (if configured) to convert the Message
           to a domain-specific response format

        Args:
            completion_check: Optional override for completion check. If not provided,
                            uses config.completion_check. This allows callers to use
                            agent-specific defaults without knowing implementation details.

        Returns:
            If response_formatter is configured: The formatted response (any type)
            If no formatter: The raw Message from generate_next_message()

        Raises:
            Same exceptions as generate_next_message()

        Example:
            ```python
            # Agent configured with response_formatter that returns NextMessage
            runner = AgentRunner(config=config, client=client)
            runner.add_message(HumanMessage(text="Hello"))

            # Returns NextMessage (formatted) - uses config's completion_check
            response = runner.generate_formatted_message()

            # Or use generate_next_message() for raw Message
            message = runner.generate_next_message()
            ```
        """
        # Use provided override or fall back to config's completion check
        effective_check = completion_check or self.config.completion_check
        last_message = self.generate_next_message(effective_check)

        if self.config.response_formatter:
            return self.config.response_formatter(last_message, self)

        return last_message
