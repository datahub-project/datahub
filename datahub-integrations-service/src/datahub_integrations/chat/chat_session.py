from datahub_integrations.gen_ai.mlflow_init import MLFLOW_INITIALIZED

import contextlib
import uuid
from typing import (
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    TypeGuard,
)

import mlflow
import mlflow.entities
import mlflow.tracing
from datahub.sdk.main_client import DataHubClient
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
from datahub_integrations.chat.mcp_server import mcp, with_client
from datahub_integrations.chat.tool import Tool
from datahub_integrations.gen_ai.bedrock import BedrockModel, get_bedrock_client

assert MLFLOW_INITIALIZED
MAX_TOOL_CALLS = 12
MESSAGE_LENGTH_SOFT_LIMIT = 2000

# Mapping of tool names to user-friendly progress messages
PROGRESS_MESSAGES = {
    "get_entity": "Looking up details about this data asset...",
    "search": "Searching through DataHub's catalog...",
    "get_lineage": "Analyzing data relationships and dependencies...",
    "get_dataset_queries": "Retrieving query history and usage patterns...",
    "get_datahub_coordinates": "Finding the exact location in DataHub...",
    "respond_to_user": "Preparing your response...",
}

# Default progress message if tool not found in mapping
DEFAULT_PROGRESS_MESSAGE = "Processing your request..."

ProgressCallback = Callable[[str], None]


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


_respond_to_user_tool = Tool(
    fn=respond_to_user,
    name="respond_to_user",
    description=f"""\
Respond to the user with a message formatted using Slack mrkdwn. \
Create headers, lists, and other formatting elements using the appropriate Slack mrkdwn syntax. \
Always use full URNs without any additional formatting when referring to entities, since urns will be automatically converted into links.

IMPORTANT: Keep your response concise and under {MESSAGE_LENGTH_SOFT_LIMIT} characters. \
If you need to provide more information, focus on the most relevant points and summarize the rest. \
Break down complex information into bullet points for better readability.""",
)


class ChatSession:
    def __init__(
        self,
        tools: List[Tool],
        client: DataHubClient,
        history: Optional[ChatHistory] = None,
        progress_callback: Optional[ProgressCallback] = None,
    ):
        self.session_id = str(uuid.uuid4())  # TODO: use uuid7 in the future
        self.tools = tools + [_respond_to_user_tool]
        self.client = client
        self.history: ChatHistory = history or ChatHistory()
        self._progress_callback = progress_callback
        self._progress_messages: List[str] = []  # Store progress messages

    @property
    def tool_map(self) -> Dict[str, Tool]:
        return {tool.name: tool for tool in self.tools}

    @classmethod
    def is_respond_to_user(cls, message: Message) -> TypeGuard[ToolResult]:
        return (
            isinstance(message, ToolResult)
            and message.tool_request.tool_name == _respond_to_user_tool.name
        )

    def _add_message(self, message: Message) -> None:
        logger.info(f"Adding message: {message}")
        self.history.add_message(message)

        # Add internal messages to progress display
        if isinstance(message, ReasoningMessage):
            if message.text not in self._progress_messages:  # Avoid duplicates
                self._progress_messages.append(message.text)
                self._report_progress("")  # Trigger progress update

    @contextlib.contextmanager
    def set_progress_callback(
        self, progress_callback: ProgressCallback
    ) -> Iterator[None]:
        prev_progress_callback = self._progress_callback
        self._progress_callback = progress_callback
        try:
            yield
        finally:
            self._progress_callback = prev_progress_callback

    def _report_progress(self, message: str) -> None:
        """Report progress if callback is configured"""
        if self._progress_callback:
            if message == "Thinking...":
                # For "Thinking...", append it to existing messages
                messages_to_show = self._progress_messages + [message]
            elif message:  # Only add non-empty messages
                # For other messages, add to the list and show with "Thinking..."
                if message not in self._progress_messages:  # Avoid duplicates
                    self._progress_messages.append(message)
                messages_to_show = self._progress_messages + ["Thinking..."]
            else:
                # For empty messages, just show current progress with "Thinking..."
                messages_to_show = self._progress_messages + ["Thinking..."]

            # Join all messages with newlines
            full_message = "\n".join(messages_to_show)
            self._progress_callback(full_message)

    def _get_progress_message(self, tool_name: str) -> str:
        """Get user-friendly progress message for a tool"""
        return PROGRESS_MESSAGES.get(tool_name, DEFAULT_PROGRESS_MESSAGE)

    def _generate_tool_call(self) -> None:
        bedrock_client = get_bedrock_client()

        self._report_progress("Thinking...")

        # TODO: Add smart truncation / removal of messages if the history is too long.
        messages = [message.to_obj() for message in self.history.messages]

        tools = [tool.to_bedrock_spec() for tool in self.tools]
        response = bedrock_client.converse(
            modelId=BedrockModel.CLAUDE_37_SONNET.value,
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

                # Report progress before tool execution
                self._report_progress(self._get_progress_message(tool_name))

                tool = self.tool_map[tool_name]
                tool_request = ToolCallRequest(
                    tool_use_id=tool_use["toolUseId"],
                    tool_name=tool_name,
                    tool_input=tool_use["input"],
                )
                self._add_message(tool_request)

                try:
                    with with_client(self.client):
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
        mlflow.update_current_trace(tags={"session_id": self.session_id})

        for i in range(MAX_TOOL_CALLS):
            logger.info(f"Generating tool call {i}")
            self._generate_tool_call()

            last_message = self.history.messages[-1]
            if self.is_respond_to_user(last_message):
                logger.info("Respond to user call received")
                return last_message.result
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
        tools=mcp.get_all_tools(),
        client=DataHubClient.from_env(),
        history=ChatHistory(
            messages=[
                HumanMessage(text="What datasets should I look at for pet profiles?")
            ]
        ),
    )
    response = chat.generate_next_message()
    print(response)
