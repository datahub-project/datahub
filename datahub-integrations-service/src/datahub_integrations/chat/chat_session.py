from datahub_integrations.gen_ai.mlflow_init import MLFLOW_ENABLED, MLFLOW_INITIALIZED

import contextlib
import re
import uuid
from typing import (
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    TypeGuard,
)

import mlflow
import mlflow.entities
import mlflow.tracing
from datahub.sdk.main_client import DataHubClient
from fastmcp import FastMCP
from loguru import logger
from pydantic import BaseModel

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    Message,
    ReasoningMessage,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.gen_ai.bedrock import (
    BedrockModel,
    get_bedrock_client,
    get_bedrock_model_env_variable,
)
from datahub_integrations.mcp.mcp_server import mcp, with_datahub_client
from datahub_integrations.mcp.tool import ToolWrapper, tools_from_fastmcp
from datahub_integrations.slack.utils.string import truncate

assert MLFLOW_INITIALIZED
MAX_TOOL_CALLS = 20
MESSAGE_LENGTH_SOFT_LIMIT = 2000

_CHATBOT_MODEL = get_bedrock_model_env_variable(
    "CHATBOT_MODEL", BedrockModel.CLAUDE_37_SONNET
)

ProgressCallback = Callable[[List[str]], None]


class ChatSessionMaxTokensExceededError(Exception):
    pass


class ChatSessionError(Exception):
    pass


class NextMessage(BaseModel):
    text: str
    suggestions: List[str]


def respond_to_user(next_message: NextMessage) -> NextMessage:
    assert isinstance(next_message, NextMessage)
    return next_message


_respond_to_user_tool = ToolWrapper.from_function(
    fn=respond_to_user,
    name="respond_to_user",
    description=f"""\
Respond to the user with a message formatted using Markdown. \
However, do not use any headers (e.g. #, ##, ###, etc.) or tables, as these are not supported.

The first reference to each entity must be formatted as a link to the entity in DataHub.

IMPORTANT: Keep your response concise and under {MESSAGE_LENGTH_SOFT_LIMIT} characters. \
If you need to provide more information, focus on the most relevant points and summarize the rest. \
Break down complex information into bullet points for better readability.""",
)

_SYSTEM_PROMPT = """\
The assistant is DataHub AI, created by Acryl Data.

DataHub AI is a helpful assistant that can answer questions and help with tasks relating to \
metadata management, data discovery, data governance, and data quality.

DataHub AI provides thorough responses to more complex and open-ended questions or to anything where a long response is requested, but concise responses to simpler questions and tasks.

DataHub AI makes use of the available tools in order to effectively answer the person's question. DataHub AI will typically make multiple tool calls in order to answer a single question, and will stop asking for more tool calls once it has enough information to answer the question.
DataHub AI will not make more than 10 tool calls in a single response.

DataHub AI provides the shortest answer it can to the person’s message, while respecting any stated length and comprehensiveness preferences given by the person. DataHub AI addresses the specific query or task at hand, avoiding tangential information unless absolutely critical for completing the request.

DataHub AI avoids writing lists, but if it does need to write a list, DataHub AI focuses on key info instead of trying to be comprehensive. If DataHub AI can answer the human in 1-3 sentences or a short paragraph, it does. If DataHub AI can write a natural language list of a few comma separated items instead of a numbered or bullet-pointed list, it does so. DataHub AI tries to stay focused and share fewer, high quality examples or ideas rather than many.

For any progress or reasoning updates, always use short, user-friendly, and non-technical statements. Keep each step under 75 characters if possible, and never exceed 300 characters. If a step is too long, provide a concise summary suitable for a progress bar or status update.

DataHub AI is now being connected with a person.
"""


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
                sanitized_text = cls._sanitize_progress_step(message.text)
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
        self.history: ChatHistory = history or ChatHistory()

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
                f"Adding ToolResult for {message.tool_request.tool_name}: {truncate(str(message), max_length=1000)}"
            )
        elif isinstance(message, ToolResultError):
            logger.debug(
                f"Adding ToolResultError for {message.tool_request.tool_name}: {truncate(str(message), max_length=1000)}"
            )
        else:
            logger.debug(
                f"Adding {type(message).__name__} message: {truncate(str(message), max_length=400)}"
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

        messages = self.history.messages
        formatted_messages = [message.to_obj() for message in messages]

        if self._use_prompt_caching:
            potential_cache_point_indexes = [
                i
                for i, message in enumerate(messages)
                if isinstance(
                    message,
                    (HumanMessage, AssistantMessage, ToolResult, ToolResultError),
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

        response = bedrock_client.converse(
            modelId=(
                _CHATBOT_MODEL.value
                if isinstance(_CHATBOT_MODEL, BedrockModel)
                else _CHATBOT_MODEL
            ),
            system=[
                {"text": _SYSTEM_PROMPT},
            ],
            messages=messages,  # type: ignore
            toolConfig={
                "tools": tools,  # type: ignore
            },
            inferenceConfig={
                "temperature": 0.7,
                "maxTokens": 2048,
            },
        )

        is_end_turn = False
        output = response["output"]
        stop_reason = response["stopReason"]
        if stop_reason == "max_tokens":
            raise ChatSessionMaxTokensExceededError(str(response))
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
            if "text" in content_block:
                is_last_block = i == len(response_content) - 1
                is_final_response = is_last_block and is_end_turn
                if is_final_response:
                    # TODO: Do we want to force another loop to ensure a tool call?
                    self._add_message(AssistantMessage(text=content_block["text"]))
                else:
                    self._add_message(ReasoningMessage(text=content_block["text"]))
            elif "toolUse" in content_block:
                tool_use = content_block["toolUse"]
                tool_name = tool_use["name"]

                tool = self.tool_map[tool_name]
                tool_request = ToolCallRequest(
                    tool_use_id=tool_use["toolUseId"],
                    tool_name=tool_name,
                    tool_input=tool_use["input"],
                )
                self._add_message(tool_request)

                try:
                    with with_datahub_client(self.client):
                        result = tool.run(arguments=tool_request.tool_input)
                except Exception as e:
                    self._add_message(
                        ToolResultError(
                            tool_request=tool_request,
                            error=f"{type(e).__name__}: {e}",
                            # raw_error=e,
                        )
                    )
                else:
                    self._add_message(
                        ToolResult(tool_request=tool_request, result=result)
                    )
            else:
                raise ChatSessionError(f"Unknown content block type {content_block}")

    @mlflow.trace
    def generate_next_message(self) -> NextMessage:
        if MLFLOW_ENABLED:
            mlflow.update_current_trace(tags={"session_id": self.session_id})

        logger.info(
            f"Generating next message for session {self.session_id}, currently have {len(self.history.messages)} messages/tool calls in chat history"
        )
        for i in range(MAX_TOOL_CALLS):
            logger.info(f"Generating tool call {i}")
            self._generate_tool_call()

            last_message = self.history.messages[-1]
            if self.is_respond_to_user(last_message):
                logger.info("Respond to user call received")
                return NextMessage.model_validate(last_message.result)
            elif isinstance(last_message, AssistantMessage):
                logger.info("End turn message received")
                return NextMessage(
                    text=last_message.text,
                    suggestions=[],
                )

        raise ChatSessionError(
            f"Failed to generate next message after {MAX_TOOL_CALLS} tool calls"
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
