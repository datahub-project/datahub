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
    """
    Internal reasoning message from the LLM.

    Can optionally include plan coordination fields to track progress
    through multi-step plans created by the planning tools.
    """

    type: Literal["internal"] = "internal"
    text: str

    # Optional plan coordination fields
    plan_id: Optional[str] = Field(
        default=None,
        description="ID of the plan being executed (e.g., 'plan_abc123')",
    )
    plan_step: Optional[str] = Field(
        default=None,
        description="Current step ID being worked on (e.g., 's0', 's1')",
    )
    step_status: Optional[Literal["started", "in_progress", "completed", "failed"]] = (
        Field(
            default=None,
            description="Status of the current step",
        )
    )
    plan_status: Optional[Literal["active", "completed", "failed", "revised"]] = Field(
        default=None,
        description="Overall status of the plan",
    )

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

    def _count_messages_of_type(self, message_type: type) -> int:
        """Count messages of a specific type since the last HumanMessage.

        This ensures we only measure messages for the last question.
        """
        count = 0
        for message in reversed(self.messages):
            if isinstance(message, HumanMessage):
                break
            if isinstance(message, message_type):
                count += 1
        return count

    @property
    def num_tool_calls(self) -> int:
        return self._count_messages_of_type(ToolCallRequest)

    @property
    def num_tool_results(self) -> int:
        return self._count_messages_of_type(ToolResult)

    @property
    def num_tool_call_errors(self) -> int:
        return self._count_messages_of_type(ToolResultError)

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

    @property
    def is_followup_datahub_ask_question(self) -> Optional[bool]:
        """Whether the current question is a follow-up to a prior DataHub response.

        Returns:
            Optional[bool]: True if a prior assistant reply exists, False if none exist
            in the available history, or None when history is incomplete.
        """
        if not self.messages:
            return None

        prior_messages = self.messages[:-1]
        has_prior_assistant = any(
            isinstance(message, AssistantMessage) for message in prior_messages
        )

        if has_prior_assistant:
            return True

        is_limited_history = bool(self.extra_properties.get("is_limited_history"))
        if not prior_messages:
            return None if is_limited_history else False

        return None if is_limited_history else False
