import json
import pathlib
from typing import (
    Any,
    Literal,
    TypeAlias,
)

from pydantic import BaseModel

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
    result: Any = None

    def to_obj(self) -> dict:
        content: dict[str, Any]
        if isinstance(self.result, BaseModel):
            jsonify = self.result.dict()
            content = {"json": jsonify}
        elif isinstance(self.result, dict):
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


Message: TypeAlias = (
    HumanMessage
    | AssistantMessage
    | ReasoningMessage
    | ToolCallRequest
    | ToolResult
    | ToolResultError
)


class ChatHistory(BaseModel):
    messages: list[Message] = []
    extra_properties: dict = {}

    def add_message(self, message: Message) -> None:
        self.messages.append(message)

    def json(self, **kwargs: Any) -> str:
        kwargs.setdefault("indent", 2)
        return self.model_dump_json(**kwargs)

    def save_file(self, path: pathlib.Path) -> None:
        path.write_text(self.json())
