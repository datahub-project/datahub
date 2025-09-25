import json
import pathlib
from typing import (
    Annotated,
    Any,
    Literal,
    Optional,
    Union,
)

from pydantic import BaseModel, Field

# This is similar to the base classes from LangChain. The main difference
# is that LangChain's AIMessage was split into AssistantMessage and
# ReasoningMessage for user-facing and internal messages, respectively.


class _BaseMessage(BaseModel):
    def to_obj(self) -> dict:
        # TODO: This to_obj is very specific to the Bedrock Converse API.
        raise NotImplementedError("Subclasses must implement this method")


class HumanMessage(_BaseMessage):
    type: Literal["human"] = "human"
    text: str

    def to_obj(self) -> dict:
        return {
            "role": "user",
            "content": [{"text": self.text}],
        }


class AssistantMessage(_BaseMessage):
    type: Literal["assistant"] = "assistant"
    text: str

    def to_obj(self) -> dict:
        return {
            "role": "assistant",
            "content": [{"text": self.text}],
        }


class ReasoningMessage(_BaseMessage):
    type: Literal["internal"] = "internal"
    text: str

    def to_obj(self) -> dict:
        return {
            "role": "assistant",
            "content": [{"text": self.text}],
        }


class ToolCallRequest(_BaseMessage):
    type: Literal["tool_call"] = "tool_call"
    tool_use_id: str
    tool_name: str
    tool_input: dict

    def to_obj(self) -> dict:
        return {
            "role": "assistant",
            "content": [
                {
                    "toolUse": {
                        "toolUseId": self.tool_use_id,
                        "name": self.tool_name,
                        "input": self.tool_input,
                    },
                }
            ],
        }


class ToolResult(_BaseMessage):
    type: Literal["tool_result"] = "tool_result"
    tool_request: ToolCallRequest
    result: dict | str

    def to_obj(self) -> dict:
        content: dict[str, Any]
        if isinstance(self.result, dict):
            # Ensure it's JSON-serializable - this is probably redundant.
            jsonify = json.loads(json.dumps(self.result))
            content = {"json": jsonify}
        else:
            content = {"text": str(self.result)}

        return {
            "role": "user",
            "content": [
                {
                    "toolResult": {
                        "toolUseId": self.tool_request.tool_use_id,
                        "content": [content],
                    },
                }
            ],
        }


class ToolResultError(_BaseMessage):
    type: Literal["tool_result_error"] = "tool_result_error"
    tool_request: ToolCallRequest
    error: str
    # raw_error: Exception = pydantic.Field(exclude=True)

    def to_obj(self) -> dict:
        return {
            "role": "user",
            "content": [
                {
                    "toolResult": {
                        "toolUseId": self.tool_request.tool_use_id,
                        "content": [{"text": self.error}],
                        "status": "error",
                    },
                }
            ],
        }


class SummaryMessage(_BaseMessage):
    type: Literal["summary"] = "summary"
    text: str

    def to_obj(self) -> dict:
        return {
            "role": "assistant",
            "content": [{"text": f"Summary of the conversation so far:\n {self.text}"}],
        }


Message = Annotated[
    Union[
        HumanMessage,
        AssistantMessage,
        ReasoningMessage,
        ToolCallRequest,
        ToolResult,
        ToolResultError,
        SummaryMessage,
    ],
    Field(discriminator="type"),
]


class ChatHistory(BaseModel):
    messages: list[Message] = []  # store for all original messages
    extra_properties: dict = {}

    # Maybe make this private by adding _ prefix
    reduced_history: Optional[list[Message]] = None

    def add_message(self, message: Message) -> None:
        self.messages.append(message)
        if self.reduced_history:
            self.reduced_history.append(message)

    def json(self, **kwargs: Any) -> str:
        kwargs.setdefault("indent", 2)
        return self.model_dump_json(**kwargs)

    def save_file(self, path: pathlib.Path) -> None:
        path.write_text(self.json())

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
        self.reduced_history = reduced_history
        self.extra_properties.setdefault("reducers", [])
        self.extra_properties["reducers"].append(reducer_metadata)

    @classmethod
    def load_file(cls, path: pathlib.Path) -> "ChatHistory":
        return cls.model_validate_json(path.read_text())

    @property
    def num_tool_calls(self) -> int:
        new_tool_calls = 0
        for message in reversed(self.messages):
            if isinstance(message, HumanMessage):
                # This makes sure to measure only the tool calls for the last question
                break
            if isinstance(message, ToolCallRequest):
                new_tool_calls += 1
        return new_tool_calls

    @property
    def num_tool_results(self) -> int:
        new_tool_results = 0
        for message in reversed(self.messages):
            if isinstance(message, HumanMessage):
                # This makes sure to measure only the tool results for the last question
                break
            if isinstance(message, ToolResult):
                new_tool_results += 1
        return new_tool_results

    @property
    def num_tool_call_errors(self) -> int:
        new_tool_call_errors = 0
        for message in reversed(self.messages):
            if isinstance(message, HumanMessage):
                # This makes sure to measure only the tool errors for the last question
                break
            if isinstance(message, ToolResultError):
                new_tool_call_errors += 1
        return new_tool_call_errors

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
