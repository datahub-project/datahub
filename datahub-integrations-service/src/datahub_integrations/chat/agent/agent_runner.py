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
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional

import mlflow
import mlflow.entities
from datahub.sdk.main_client import DataHubClient
from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.chat.agent.agent_config import AgentConfig
from datahub_integrations.chat.agent.progress_tracker import (
    ProgressCallback,
    ProgressTracker,
)
from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    Message,
    ReasoningMessage,
    SummaryMessage,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.gen_ai.llm.exceptions import LlmInputTooLongException
from datahub_integrations.gen_ai.llm.factory import get_llm_client
from datahub_integrations.mcp.mcp_server import with_datahub_client
from datahub_integrations.mcp_integration.tool import ToolWrapper
from datahub_integrations.slack.utils.string import truncate
from datahub_integrations.telemetry.chat_events import ChatbotToolCallEvent
from datahub_integrations.telemetry.telemetry import track_saas_event

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        ContentBlockOutputTypeDef,
        TokenUsageTypeDef,
    )


class AgentError(Exception):
    """Base exception for agent errors."""

    pass


class AgentMaxTokensExceededError(AgentError):
    """Raised when input exceeds model's context window."""

    pass


class AgentOutputMaxTokensExceededError(AgentError):
    """Raised when output is truncated due to max_tokens limit."""

    pass


class AgentMaxToolCallsExceededError(AgentError):
    """Raised when agent exceeds maximum allowed tool calls."""

    pass


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
        runner.history.add_message(HumanMessage(text="Hello"))
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
        self.history: ChatHistory = history or ChatHistory()

        # Use tools from config (already prepared)
        self.tools: List[ToolWrapper] = config.tools
        self.plannable_tools: List[ToolWrapper] = config.plannable_tools

        # Planning support (infrastructure-level)
        # Agents can use create_plan/revise_plan tools if they include them
        self.plan_cache: Dict[str, Dict[str, Any]] = {}

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

        # Create progress tracker with conversational parser
        self._progress_tracker = ProgressTracker(
            history=self.history,
            progress_callback=None,
            agent=self,
            conversational_parser=config.conversational_parser,
        )

        # Create bound logger with session_id for consistent logging
        self._logger = logger.bind(session_id=self.session_id)

        self._logger.info(
            f"Initialized {config.agent_name} (session={self.session_id}) with {len(self.tools)} tools"
        )

    @property
    def tool_map(self) -> Dict[str, ToolWrapper]:
        """Get mapping of tool names to tool instances."""
        return {tool.name: tool for tool in self.tools}

    def get_plannable_tools(self) -> List[ToolWrapper]:
        """
        Get tools that can be used in execution plans.

        Returns the plannable_tools configured at initialization,
        which typically excludes internal/control-flow tools.

        Returns:
            List of ToolWrapper objects suitable for planning
        """
        return self.plannable_tools

    def _get_system_messages(self) -> List[Any]:
        """Get system messages from the configured prompt builder."""
        return self.config.system_prompt_builder.build_system_messages(self.client)

    def _add_message(self, message: Message) -> None:
        """
        Add a message to history and notify progress tracker.

        Args:
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

        self.history.add_message(message)
        self._progress_tracker.handle_history_updated()

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
            history=self.history,
            progress_callback=progress_callback,
            agent=self,
            start_offset=len(self.history.messages),
            conversational_parser=self.config.conversational_parser,
        )
        try:
            yield
        finally:
            self._progress_tracker = prev_tracker

    def _prepare_messages(self) -> List[dict]:
        """
        Prepare messages for LLM with context reduction and prompt caching.

        Returns:
            List of formatted message dictionaries ready for LLM API
        """
        # Apply context reduction if configured
        for reducer in self.context_reducers:
            reducer.reduce(self.history)

        formatted_messages = [
            message.to_obj() for message in self.history.context_messages
        ]

        # Add prompt caching markers if enabled
        if self.config.use_prompt_caching:
            # Mark potential cache points (after user/assistant/tool result messages)
            potential_cache_point_indexes = [
                i
                for i, message in enumerate(self.history.context_messages)
                if isinstance(
                    message,
                    (
                        HumanMessage,
                        AssistantMessage,
                        SummaryMessage,
                        ToolResult,
                        ToolResultError,
                    ),
                )
            ]
            # Use at most 2 cache points for optimal performance
            if len(potential_cache_point_indexes) > 2:
                potential_cache_point_indexes = potential_cache_point_indexes[-2:]
            for index in potential_cache_point_indexes:
                formatted_messages[index]["content"].append(
                    {"cachePoint": {"type": "default"}}
                )

        return formatted_messages

    def _generate_tool_call(self) -> None:
        """
        Generate a single tool call cycle (reasoning -> tool use).

        This method:
        1. Prepares messages with context reduction
        2. Calls the LLM with tools
        3. Processes the response (reasoning, tool calls, or final message)
        4. Adds appropriate messages to history

        Raises:
            AgentMaxTokensExceededError: If input exceeds context window
            AgentOutputMaxTokensExceededError: If output is truncated
            AgentError: For other LLM errors
        """
        llm_client = get_llm_client(self.config.model_id)

        messages = self._prepare_messages()

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

        log_tokens_usage(response["usage"])  # type: ignore[arg-type]

        # Process stop reason
        is_end_turn = False
        output = response["output"]
        stop_reason = response["stopReason"]

        if stop_reason == "max_tokens":
            raise AgentOutputMaxTokensExceededError(str(response))
        elif stop_reason == "tool_use":
            pass  # Expected
        elif stop_reason == "end_turn":
            is_end_turn = True
        else:
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
                f"Executing sequentially. Tools: {[block['toolUse']['name'] for block in tool_use_blocks]}"
            )

        # Process each content block.
        # When the LLM returns multiple tool calls in a single response, we execute them
        # sequentially. This is correct behavior - all tool results will be added to history
        # before the next LLM call.
        # TODO: Consider executing independent tool calls in parallel for better latency.
        # This would require checking for tool dependencies and batching the execution.
        for i, content_block in enumerate(response_content):
            is_last_block = i == len(response_content) - 1
            if "text" in content_block:
                self._handle_text_content(content_block, is_end_turn, is_last_block)
            elif "toolUse" in content_block:
                self._handle_tool_call_request(content_block)
            else:
                raise AgentError(f"Unknown content block type {content_block}")

    def _handle_text_content(
        self,
        content_block: "ContentBlockOutputTypeDef",
        is_end_turn: bool,
        is_last_block: bool,
    ) -> None:
        """
        Handle text content from LLM response.

        This can be either:
        - Reasoning (internal thoughts before tool use)
        - Final assistant message (when no tools are used)

        Args:
            content_block: Content block from LLM response
            is_end_turn: Whether this is the final block in a turn
            is_last_block: Whether this is the last block in the response
        """
        is_final_response = is_last_block and is_end_turn

        if is_final_response:
            # Direct response without using tools (fallback case)
            response_text = content_block["text"]
            response_text = _strip_reasoning_tag(response_text)
            self._logger.info(f"Adding AssistantMessage: {response_text}")
            self._add_message(AssistantMessage(text=response_text))

            # Log to MLflow
            attributes = {
                "response_length": len(response_text),
                "message_index": len(self.history.messages),
                "is_unexpected_direct_response": True,
            }

            with mlflow.start_span(
                f"assistant_message_{len(self.history.messages)}",
                span_type=mlflow.entities.SpanType.LLM,
                attributes=attributes,
            ) as span:
                span.set_inputs(
                    {"context": "Direct LLM response (bypassed internal tools)"}
                )
                span.set_outputs({"response": response_text})
        else:
            # Reasoning message (internal thoughts)
            reasoning_text = content_block["text"]
            self._add_message(ReasoningMessage(text=reasoning_text))

            # Log to MLflow
            attributes = {
                "reasoning_length": len(reasoning_text),
                "message_index": len(self.history.messages),
            }

            with mlflow.start_span(
                f"reasoning_step_{len(self.history.messages)}",
                span_type=mlflow.entities.SpanType.LLM,
                attributes=attributes,
            ) as span:
                span.set_inputs({"context": "LLM internal thinking"})
                span.set_outputs({"reasoning": reasoning_text})

    def _handle_tool_call_request(
        self, content_block: "ContentBlockOutputTypeDef"
    ) -> None:
        """
        Handle tool call request from LLM.

        Executes the requested tool and adds the result to history.

        Args:
            content_block: Content block containing tool use information
        """
        tool_use = content_block["toolUse"]
        tool_name = tool_use["name"]

        tool_request = ToolCallRequest(
            tool_use_id=tool_use["toolUseId"],
            tool_name=tool_name,
            tool_input=tool_use["input"],
        )
        self._add_message(tool_request)

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
            self._add_message(
                ToolResultError(
                    tool_request=tool_request,
                    error=error,
                )
            )
        else:
            self._add_message(ToolResult(tool_request=tool_request, result=result))

        # Track telemetry
        track_saas_event(
            ChatbotToolCallEvent(
                chat_session_id=self.session_id,
                tool_name=tool_name,
                tool_input=tool_request.tool_input,
                tool_execution_duration_sec=timer.elapsed_seconds(),
                tool_result_length=len(str(result)) if result else None,
                tool_result_is_error=error is not None,
                tool_error=error,
            )
        )

    @mlflow.trace
    def generate_next_message(
        self, completion_check: Optional[Callable[[Message], bool]] = None
    ) -> Message:
        """
        Generate the next message via agentic loop.

        This is the main method that orchestrates the agent's behavior:
        1. Generate reasoning and tool calls
        2. Execute tools
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
            AgentMaxToolCallsExceededError: If max_tool_calls is exceeded
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
            f"currently have {len(self.history.messages)} messages in history"
        )

        # Default completion check: Stop when agent generates a direct text response (AssistantMessage)
        # rather than calling more tools. This is the natural completion point for most agents -
        # they call tools as needed, then respond to the user with their findings.
        if completion_check is None:

            def completion_check(msg: Message) -> bool:
                return isinstance(msg, AssistantMessage)

        for i in range(self.config.max_tool_calls):
            logger.info(
                f"Generating tool call {i} for {self.config.agent_name} (session={self.session_id})"
            )
            self._generate_tool_call()

            if not self.history.messages:
                raise AgentError("No messages in chat history")

            last_message = self.history.messages[-1]
            if completion_check(last_message):
                logger.info(
                    f"Agent completed for {self.config.agent_name} (session={self.session_id})"
                )
                return last_message

        raise AgentMaxToolCallsExceededError(
            f"Failed to generate next message after {self.config.max_tool_calls} tool calls"
        )

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
            runner.history.add_message(HumanMessage(text="Hello"))

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
