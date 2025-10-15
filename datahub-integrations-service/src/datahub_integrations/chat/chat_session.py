from datahub_integrations.gen_ai.mlflow_init import initialize_mlflow, is_mlflow_enabled

import contextlib
import json
import re
import uuid
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    TypeGuard,
)

import cachetools
import mlflow
import mlflow.entities
import mlflow.tracing
from datahub.sdk.main_client import DataHubClient
from datahub.utilities.perf_timer import PerfTimer
from fastmcp import FastMCP
from loguru import logger
from pydantic import BaseModel, field_validator

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
from datahub_integrations.chat.context_reducer import (
    ChatContextReducer,
    ContextReducerConfig,
    TokenCountEstimator,
)
from datahub_integrations.chat.reducers.conversation_summarizer import (
    ConversationSummarizer,
)
from datahub_integrations.chat.reducers.sliding_window_reducer import (
    SlidingWindowReducer,
)
from datahub_integrations.chat.utils import parse_reasoning_message
from datahub_integrations.gen_ai.bedrock import (
    get_bedrock_client,
)
from datahub_integrations.gen_ai.linkify import auto_fix_chat_links
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.mcp.mcp_server import (
    get_datahub_client,
    mcp,
    with_datahub_client,
)
from datahub_integrations.mcp.tool import ToolWrapper, tools_from_fastmcp
from datahub_integrations.slack.utils.string import truncate
from datahub_integrations.telemetry.chat_events import ChatbotToolCallEvent
from datahub_integrations.telemetry.telemetry import track_saas_event

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        ContentBlockOutputTypeDef,
        SystemContentBlockTypeDef,
        TokenUsageTypeDef,
    )

# Initialize MLflow for @mlflow.trace decorators in this module
initialize_mlflow()

MAX_TOOL_CALLS = 30

# The soft limit is passed to the LLM's prompt, in an effort
# to have it be concise.
MESSAGE_LENGTH_SOFT_LIMIT = 1500
# The hard limit is derived from Slack's limits, where the Slack "section"
# block's text field has a limit of 3000 characters.
# See https://api.slack.com/reference/block-kit/blocks#section
MESSAGE_LENGTH_HARD_LIMIT = 3000 - 100  # 100 is a buffer
assert MESSAGE_LENGTH_HARD_LIMIT >= 1.5 * MESSAGE_LENGTH_SOFT_LIMIT

_MAX_SUGGESTIONS = 4

CLAUDE_TOKEN_LIMIT = int(200e3)
ProgressCallback = Callable[[List[str]], None]


class ChatSessionMaxTokensExceededError(Exception):
    pass


class ChatOutputMaxTokensExceededError(Exception):
    pass


class ChatSessionError(Exception):
    pass


class ChatMaxToolCallsExceededError(Exception):
    pass


class NextMessage(BaseModel):
    text: str
    suggestions: List[str] = []

    @field_validator("text", mode="after")
    @classmethod
    def validate_text(cls, v: str) -> str:
        if len(v) > MESSAGE_LENGTH_HARD_LIMIT:
            raise ValueError(
                f"Text length exceeds hard length limit of {MESSAGE_LENGTH_HARD_LIMIT}"
            )
        return v

    @field_validator("suggestions", mode="after")
    @classmethod
    def validate_suggestions(cls, v: List[str]) -> List[str]:
        if len(v) > _MAX_SUGGESTIONS:
            raise ValueError(
                f"Too many suggestions provided. Please provide at most {_MAX_SUGGESTIONS} suggestions."
            )
        return v


def respond_to_user(
    response: str,
    follow_up_suggestions: Optional[List[str]] = None,
) -> NextMessage:
    client = get_datahub_client()
    response = auto_fix_chat_links(response, client._graph.frontend_base_url)
    return NextMessage(text=response, suggestions=follow_up_suggestions or [])


_respond_to_user_tool = ToolWrapper.from_function(
    fn=respond_to_user,
    name="respond_to_user",
    description=f"""\
Respond to the user with a message formatted using Markdown. \
However, do not use any headers (e.g. #, ##, ###, etc.) or tables, as these are not supported.

The first reference to each entity must be formatted as a link to the entity in DataHub.

State only FACTUAL INFORMATION. Do not try to infer connection without proof, do not make assumptions.

CRITICAL - VALIDATION BEFORE CALLING THIS TOOL:
Before calling this tool, you MUST validate that your response accurately matches what the user requested.

CRITICAL: For fully-qualified entity names (database.schema.table format), the database, schema, AND table name must ALL match exactly. \
If even one part differs (e.g., different database or schema), it is a DIFFERENT entity, not the same entity.

If the response might not match the user request state this clearly and use conditional/uncertain tone THROUGHOUT \
your entire response 
    EXAMPLES: 
    - User asked for "PROD_DB.FINANCE.revenue_report" but you only found "reporting.finance.revenue_report" 
        → "I couldn't find PROD_DB.FINANCE.revenue_report. I found reporting.finance.revenue_report which \
has a similar name but different database. If this is what you meant, here's what I found..."
    - User asked for "Customer Metrics" dashboard but you found "Customer Analytics" dashboard 
        → "I couldn't find a dashboard named 'Customer Metrics'. I found 'Customer Analytics' which may be \
related. If this is what you're looking for, it shows..."
    - User asked for deprecated datasets but you found deprecated dashboards 
        → "I couldn't find deprecated datasets, but I did find deprecated dashboards. If you're interested \
in these instead, here are..."

When stating that the entities are related always provide proof for that, if you don't have proof call them SIMILAR.

You may also provide up to {_MAX_SUGGESTIONS} suggestions for questions the user may want to ask as a follow \
up, but this is not required. If there is uncertainty that we correctly answered the user's request, \
clarify this in the suggestions.

IMPORTANT: Keep your response concise and under {MESSAGE_LENGTH_SOFT_LIMIT} characters. \
If you need to provide more information, focus on the most relevant points and summarize the rest. \
Break down complex information into bullet points for better readability.""",
)

_SYSTEM_PROMPT = """\
The assistant is DataHub AI, created by Acryl Data.

DataHub AI is a helpful assistant that can answer questions relating to \
metadata management, data discovery, data governance, and data quality within the organization.

DataHub AI provides thorough responses to more complex and open-ended questions or to anything where \
a long response is requested, but concise responses to simpler questions and tasks.

DataHub AI makes use of the available tools in order to effectively answer the person's question. \
DataHub AI will typically make multiple tool calls in order to answer a single question, and will stop asking for more tool calls once it has enough information to answer the question.
DataHub AI will not make more than 10 tool calls in a single response.

DataHub AI can also answer very basic questions about DataHub itself using its built-in knowledge. \
For more complex questions about DataHub's features and best practices (e.g. "how do I set up a business \
glossary?" or "can I download search results as a CSV?"), it suggests asking the \
DataHub team and checking out the DataHub documentation at https://docs.datahub.com/. \
When referencing the DataHub documentation, it only links to the docs homepage as it does not know specific page URLs.

DataHub AI provides the shortest answer it can to the person's message, while respecting any stated length and comprehensiveness preferences given by the person. DataHub AI addresses the specific query or task at hand, avoiding tangential information unless absolutely critical for completing the request.

DataHub AI avoids writing lists, but if it does need to write a list, DataHub AI focuses on key info instead of trying to be comprehensive. If DataHub AI can answer the human in 1-3 sentences or a short paragraph, it does. If DataHub AI can write a natural language list of a few comma separated items instead of a numbered or bullet-pointed list, it does so. DataHub AI tries to stay focused and share fewer, high quality examples or ideas rather than many.

IMPORTANT: Before each tool call, DataHub AI outputs structured reasoning in XML format explaining what it's about to do and why. \
The reasoning MUST be wrapped in <reasoning></reasoning> tags and include these fields:

<reasoning>
  <action>Brief description of the tool call about to be made</action>
  <rationale>Why this tool call is needed</rationale>
  <user_requested>What the user originally asked for (if applicable)</user_requested>
  <what_found>What was actually found (if different from user request)</what_found>
  <exact_match>true/false - Does what was found exactly match what the user requested?</exact_match>
  <discrepancies>If exact_match is false, list specific differences (database, schema, name, type, etc.)</discrepancies>
  <proof_of_relation>If claiming entities are related, provide specific proof. If no proof, state "SIMILAR names only"</proof_of_relation>
  <confidence>high/medium/low</confidence>
  <warning>Any important caveats or warnings about this action</warning>
</reasoning>

For tool calls where entity matching is not relevant (e.g., initial searches), you can omit the matching fields.

CRITICAL: For fully-qualified entity names (database.schema.table format), the database, schema, AND table name must ALL match \
exactly. If even one part differs, it is a DIFFERENT entity, not the same entity.
- State only FACTUAL INFORMATION. Do not try to infer connection without proof, do not make assumptions.


DataHub AI is now being connected with a person."""


@cachetools.cached(cache=cachetools.TTLCache(maxsize=1, ttl=60 * 5))
def _get_extra_llm_instructions(client: DataHubClient) -> Optional[str]:
    """
    Retrieve optional extra LLM instructions from GraphQL API.

    Instructions are cached for 5 minutes via TTLCache decorator.
    Fetches the most recent ACTIVE GENERAL_CONTEXT instruction from global AI assistant settings.

    Args:
        client: DataHub client instance to use for GraphQL queries

    Returns:
        Optional extra instructions string, or None if not configured.

    Raises:
        Exception: If GraphQL call fails
    """

    query = """
    query getGlobalSettings {
        globalSettings {
            aiAssistant {
                instructions {
                    id
                    type
                    state
                    instruction
                    lastModified {
                        time
                        actor
                    }
                }
            }
        }
    }
    """

    try:
        response = client._graph.execute_graphql(query)

        # Extract AI assistant instructions
        global_settings = response.get("globalSettings")
        if not global_settings:
            return None

        ai_assistant = global_settings.get("aiAssistant")
        if not ai_assistant:
            return None

        instructions = ai_assistant.get("instructions", [])

        # Filter for GENERAL_CONTEXT and ACTIVE instructions
        valid_instructions = [
            instr
            for instr in instructions
            if (
                instr.get("type") == "GENERAL_CONTEXT"
                and instr.get("state") == "ACTIVE"
            )
        ]

        if not valid_instructions:
            return None

        # Use the last item from the array as the most recent instruction
        latest_instruction = valid_instructions[-1]
        instruction_text = latest_instruction.get("instruction")

        if instruction_text and instruction_text.strip():
            return instruction_text.strip()
        else:
            return None

    except Exception as e:
        # Failed to fetch AI instructions from GraphQL.
        # This is typically because the GMS instance doesn't have the aiAssistant
        # field in GlobalSettings yet (feature not deployed to this instance).
        # We log a warning and return None (no extra instructions) rather than
        # crashing the chat session.
        #
        # NOTE: This catches ALL exceptions, which means we might accidentally
        # suppress other GraphQL errors. This is a temporary solution until all
        # GMS instances are upgraded to support AI assistant settings (expected
        # within a couple of months). After that, any errors here would be
        # unexpected and should be investigated.
        logger.warning(
            f"Failed to fetch AI assistant instructions from GraphQL: {e}. "
            "This is expected if the GMS instance hasn't been upgraded to support "
            "this feature yet. Proceeding without additional instructions."
        )
        return None


class FilteredProgressListener:
    # Not super happy with the naming of this. But the purpose is to
    # 1. encapsulate the history -> progress message logic
    # 2. ensure that the progress callback is only called when things change
    def __init__(
        self,
        history: ChatHistory,
        progress_callback: Optional[ProgressCallback],
        start_offset: int = 0,
    ):
        self.history = history
        self.progress_callback = progress_callback
        self.start_offset = start_offset

        self._last_progress_steps: Optional[List[str]] = None

    @classmethod
    def _sanitize_progress_step(cls, step: str) -> str:
        """Replace trailing colon (with optional whitespace) with a period"""
        return re.sub(r":\s*$", ".", step).strip()

    @classmethod
    def get_progress_steps(
        cls, history: ChatHistory, *, start_offset: int
    ) -> List[str]:
        """Get current progress steps derived from chat history"""
        steps = []

        for message in history.messages[start_offset:]:
            if isinstance(message, ReasoningMessage):
                # Parse the reasoning message to extract user-friendly text
                parsed = parse_reasoning_message(message.text)
                user_visible_text = parsed.to_user_visible_message()

                # Sanitize and truncate progress messages
                # Max 1000 chars per step: generous buffer since parsed messages are
                # typically 50-200 chars. Even with 10 steps (10K chars total), this
                # stays well within Slack's 3K recommended limit and Teams' 28KB limit.
                sanitized_text = cls._sanitize_progress_step(user_visible_text)
                steps.append(truncate(sanitized_text, max_length=1000))
            # elif isinstance(message, ToolCallRequest):
            #     steps.append(self._get_progress_message(message.tool_name))

        return steps

    def _handle_history_updated(self) -> None:
        current_steps = self.get_progress_steps(
            self.history, start_offset=self.start_offset
        )
        if current_steps != self._last_progress_steps:
            self._last_progress_steps = current_steps
            if self.progress_callback:
                self.progress_callback(current_steps)


class ChatSession:
    def __init__(
        self,
        tools: Sequence[ToolWrapper | FastMCP],
        client: DataHubClient,
        history: Optional[ChatHistory] = None,
        extra_instructions_override: Optional[str] = None,
        # Custom context reducers can be supported in future
    ):
        self.session_id = str(uuid.uuid4())  # TODO: use uuid7 in the future
        self.tools: List[ToolWrapper] = [
            tool
            for entry in tools
            for tool in (
                tools_from_fastmcp(entry) if isinstance(entry, FastMCP) else [entry]
            )
        ] + [_respond_to_user_tool]
        self.client = client
        self.extra_instructions_override = extra_instructions_override
        self.history: ChatHistory = history or ChatHistory()

        self.context_reducers: Iterable[ChatContextReducer] = (
            create_default_context_reducer_chain(
                self._get_model_id(), self._get_tools_config()
            )
        )

        # Create a dummy progress listener to start with.
        self._progress_listener = FilteredProgressListener(
            history=self.history, progress_callback=None
        )

        # This requires a model that supports prompt caching.
        # See https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html#prompt-caching-models
        self._use_prompt_caching = True

    @property
    def tool_map(self) -> Dict[str, ToolWrapper]:
        return {tool.name: tool for tool in self.tools}

    def _get_tools_config(self) -> dict:
        return {
            "tools": [tool.to_bedrock_spec() for tool in self.tools],
        }

    def _get_model_id(self) -> str:
        # Use the new model configuration for chat assistant
        return model_config.chat_assistant_ai.model

    @classmethod
    def is_respond_to_user(cls, message: Message) -> TypeGuard[ToolResult]:
        return (
            isinstance(message, ToolResult)
            and message.tool_request.tool_name == _respond_to_user_tool.name
        )

    def _add_message(self, message: Message) -> None:
        # Log messages for debugging purposes.
        if isinstance(message, ToolResult):
            logger.debug(
                f"Adding ToolResult for {message.tool_request.tool_name}: {truncate(str(message), max_length=1000, show_length=True)}"
            )
        elif isinstance(message, ToolResultError):
            logger.debug(
                f"Adding ToolResultError for {message.tool_request.tool_name}: {truncate(str(message), max_length=1000, show_length=True)}"
            )
        else:
            logger.debug(
                f"Adding {type(message).__name__} message: {truncate(str(message), max_length=400, show_length=True)}"
            )

        self.history.add_message(message)
        self._progress_listener._handle_history_updated()

    @contextlib.contextmanager
    def set_progress_callback(
        self, progress_callback: ProgressCallback
    ) -> Iterator[None]:
        prev_progress_listener = self._progress_listener
        self._progress_listener = FilteredProgressListener(
            history=self.history,
            progress_callback=progress_callback,
            start_offset=len(self.history.messages),
        )
        try:
            yield
        finally:
            self._progress_listener = prev_progress_listener

    def _get_system_messages(self) -> List["SystemContentBlockTypeDef"]:
        """
        Get the system messages for the LLM.

        Returns a list of system messages, including the base prompt and any
        optional extra instructions as separate messages.
        """
        system_messages: List["SystemContentBlockTypeDef"] = [{"text": _SYSTEM_PROMPT}]

        # Use override if provided, otherwise fall back to standard retrieval
        extra_instructions = (
            self.extra_instructions_override
            if self.extra_instructions_override is not None
            else _get_extra_llm_instructions(self.client)
        )

        if extra_instructions:
            # Add a concise header to indicate these are customer-specific requirements
            formatted_instructions = (
                f"CUSTOMER-SPECIFIC REQUIREMENTS - You must follow these in addition to base instructions:\n\n"
                f"{extra_instructions}"
            )
            system_messages.append({"text": formatted_instructions})

        return system_messages

    def _prepare_messages(self) -> list[dict]:
        # Message history will have something like this. Potential locations
        # for cache points are marked with <cachepoint>. In general, potential
        # locations are after any HumanMessage, AssistantMessage, or ToolResult{,Error}.
        #
        # - HumanMessage
        #    <cachepoint>
        # - ReasoningMessage #1
        # - ToolCallRequest  -> model returns
        # - ToolResult / ToolResultError
        #    <cachepoint>
        # - ReasoningMessage #2
        # - ToolCallRequest  -> model returns
        # - ToolResult / ToolResultError
        # - AssistantMessage
        #    <cachepoint>
        #
        # We want there to be at most 2 message cache points in each request to the model.
        # The first cache point should make the query fast, and the second cache point
        # sets us up to handle a subsequent request quickly. As long as a cache is used
        # once, prompt caching will also be cheaper.

        # Apply context reduction if configured
        for reducer in self.context_reducers:
            reducer.reduce(self.history)

        formatted_messages = [
            message.to_obj() for message in self.history.context_messages
        ]

        if self._use_prompt_caching:
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
            if len(potential_cache_point_indexes) > 2:
                potential_cache_point_indexes = potential_cache_point_indexes[-2:]
            for index in potential_cache_point_indexes:
                formatted_messages[index]["content"].append(
                    {"cachePoint": {"type": "default"}}
                )

        return formatted_messages

    def _generate_tool_call(self) -> None:
        bedrock_client = get_bedrock_client()

        messages = self._prepare_messages()

        tools = [tool.to_bedrock_spec() for tool in self.tools]
        if self._use_prompt_caching:
            tools.append({"cachePoint": {"type": "default"}})

        try:
            response = bedrock_client.converse(
                modelId=self._get_model_id(),
                system=self._get_system_messages(),
                messages=messages,  # type: ignore
                toolConfig={
                    "tools": tools,  # type: ignore
                },
                inferenceConfig={
                    "temperature": 0.5,
                    "maxTokens": 4096,
                },
            )
        except bedrock_client.exceptions.ValidationException as e:
            # Example error messages:
            # The model returned the following errors: Input is too long for requested model.
            # The model returned the following errors: input length and `max_tokens` exceed context limit
            if "Input is too long" in str(e) or "exceed context limit" in str(e):
                raise ChatSessionMaxTokensExceededError(str(e)) from e
            else:
                raise e

        log_tokens_usage(response["usage"])
        is_end_turn = False
        output = response["output"]
        stop_reason = response["stopReason"]
        if stop_reason == "max_tokens":
            raise ChatOutputMaxTokensExceededError(str(response))
        elif stop_reason == "tool_use":
            # Expected - we'll handle this below.
            pass
        elif stop_reason == "end_turn":
            is_end_turn = True
        else:
            raise ChatSessionError(f"Unknown stop reason {stop_reason}: {response}")

        message = output.get("message")
        if message is None:
            raise ChatSessionError(f"No message in response {response}")
        response_content = message["content"]
        for i, content_block in enumerate(response_content):
            is_last_block = i == len(response_content) - 1
            if "text" in content_block:
                self._handle_text_content(content_block, is_end_turn, is_last_block)
            elif "toolUse" in content_block:
                self._handle_tool_call_request(content_block)
            else:
                raise ChatSessionError(f"Unknown content block type {content_block}")

    def _handle_text_content(
        self,
        content_block: "ContentBlockOutputTypeDef",
        is_end_turn: bool,
        is_last_block: bool,
    ) -> None:
        is_final_response = is_last_block and is_end_turn
        if is_final_response:
            # This is a fallback case where LLM outputs text without using respond_to_user tool
            # We log this to track when it happens (unexpected behavior)
            response_text = content_block["text"]
            self._add_message(AssistantMessage(text=response_text))

            # Log final response as MLflow span to track unexpected direct responses
            attributes = {
                "response_length": len(response_text),
                "message_index": len(self.history.messages),
                "is_unexpected_direct_response": True,  # Flag that this bypassed respond_to_user
            }

            # Use message count as span suffix. This is safe because:
            # 1. history.messages is append-only (even with context reduction)
            # 2. Each generate_next_message() call creates a new trace
            # 3. Message count provides useful debugging context
            with mlflow.start_span(
                f"assistant_message_{len(self.history.messages)}",
                span_type=mlflow.entities.SpanType.LLM,
                attributes=attributes,
            ) as span:
                span.set_inputs(
                    {"context": "Direct LLM response (bypassed respond_to_user tool)"}
                )
                span.set_outputs({"response": response_text})
        else:
            reasoning_text = content_block["text"]
            self._add_message(ReasoningMessage(text=reasoning_text))

            # Log reasoning as MLflow span (full XML preserved in outputs)
            attributes = {
                "reasoning_length": len(reasoning_text),
                "message_index": len(self.history.messages),
            }

            # Use message count as span suffix. This is safe because:
            # 1. history.messages is append-only (even with context reduction)
            # 2. Each generate_next_message() call creates a new trace
            # 3. Message count provides useful debugging context
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
            error = f"{type(e).__name__}: {e}"
            self._add_message(
                ToolResultError(
                    tool_request=tool_request,
                    error=error,
                    # raw_error=e,
                )
            )
        else:
            self._add_message(ToolResult(tool_request=tool_request, result=result))

        track_saas_event(
            ChatbotToolCallEvent(
                chat_session_id=self.session_id,
                tool_name=tool_name,
                tool_execution_duration_sec=timer.elapsed_seconds(),
                tool_result_length=len(str(result)) if result else None,
                tool_result_is_error=error is not None,
                tool_error=error,
            )
        )

    @mlflow.trace
    def generate_next_message(self) -> NextMessage:
        if is_mlflow_enabled():
            mlflow.update_current_trace(tags={"session_id": self.session_id})

        logger.info(
            f"Generating next message for session {self.session_id}, currently have {len(self.history.messages)} messages/tool calls in chat history"
        )
        for i in range(MAX_TOOL_CALLS):
            logger.info(f"Generating tool call {i} for session {self.session_id}")
            self._generate_tool_call()

            if not self.history.messages:
                raise ChatSessionError("No messages in chat history")
            last_message = self.history.messages[-1]
            if self.is_respond_to_user(last_message):
                logger.info(
                    f"Respond to user call received for session {self.session_id}"
                )
                return NextMessage.model_validate(last_message.result)
            elif isinstance(last_message, AssistantMessage):
                logger.info(f"End turn message received for session {self.session_id}")
                return NextMessage(
                    text=last_message.text,
                    suggestions=[],
                )

        raise ChatMaxToolCallsExceededError(
            f"Failed to generate next message after {MAX_TOOL_CALLS} tool calls"
        )


def create_default_context_reducer_chain(
    model_id: str,
    tools_config: dict,
) -> Iterable[ChatContextReducer]:
    estimator = TokenCountEstimator(model_id)

    config = ContextReducerConfig(
        llm_token_limit=CLAUDE_TOKEN_LIMIT if "claude" in model_id else int(100e3),
        safety_buffer=int(CLAUDE_TOKEN_LIMIT * 0.1),
        system_message_tokens=estimator.estimate_tokens(_SYSTEM_PROMPT),
        tool_config_tokens=estimator.estimate_tokens(json.dumps(tools_config)),
    )

    # Return iterable of reducers: ConversationSummarizer first, then SlidingWindowReducer
    return [
        ConversationSummarizer(
            estimator,
            config,
            num_recent_messages_to_keep=5,
            summarization_model=model_config.chat_assistant_ai.summary_model,
        ),
        SlidingWindowReducer(estimator, config, max_messages=10),
    ]


def log_tokens_usage(response: "TokenUsageTypeDef") -> None:
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


if __name__ == "__main__":
    from pprint import pprint as print

    chat = ChatSession(
        tools=[mcp],
        client=DataHubClient.from_env(),
        history=ChatHistory(
            messages=[
                HumanMessage(text="What datasets should I look at for pet profiles?")
            ]
        ),
    )
    response = chat.generate_next_message()
    print(response)
