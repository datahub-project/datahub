import json
from typing import TYPE_CHECKING, List, Optional

import pydantic
from diskcache import Cache
from mlflow.metrics.genai.prompt_template import PromptTemplate

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.experimentation.chatbot.chatbot import ExpectedToolCall
from datahub_integrations.gen_ai.bedrock import call_bedrock_llm
from datahub_integrations.gen_ai.model_config import BedrockModel

JUDGE_CACHE_ENABLED = True
if JUDGE_CACHE_ENABLED and not TYPE_CHECKING:
    _cache = Cache("judge_cache")

    call_bedrock_llm = _cache.memoize()(call_bedrock_llm)

CHATBOT_AI_JUDGE_MODEL = BedrockModel.CLAUDE_35_SONNET_V2

# Derived from https://github.com/braintrustdata/autoevals/blob/main/templates/closed_q_a.yaml
LLM_JUDGE_PROMPT = PromptTemplate(
    [
        """\
You are assessing a submitted response to a message based on criteria. Here is the data:

<message>
{message}
</message>

<response>
{response}
</response>

<criteria>
{guidelines}
</criteria>

Does the response meet the criteria?

You must return your response as a JSON object with the following fields:
{{
    "choice": true/false,
    "justification": "a critique of the response, plus a detailed explanation of why it passes or fails"
}}
"""
    ]
)

# You must return the following fields in your response in two lines, one below the other:
# score: Your numerical score based on the rubric
# justification: Your reasoning for giving this score

# Do not add additional new lines. Do not add any other fields."""


class LLMJudgeResponse(pydantic.BaseModel):
    choice: Optional[bool] = None
    justification: str


def chatbot_llm_judge_evaluation(
    message: str, response: str, guidelines: str
) -> LLMJudgeResponse:
    """Evaluate a single message/response pair using an LLM judge."""

    prompt_text = LLM_JUDGE_PROMPT.format(
        message=message,
        response=response,
        guidelines=guidelines,
    )

    raw_response = call_bedrock_llm(
        prompt=prompt_text, model=CHATBOT_AI_JUDGE_MODEL, max_tokens=500
    )

    try:
        judge_response = LLMJudgeResponse.model_validate_json(raw_response)
        assert judge_response.choice is not None
        return judge_response
    except (pydantic.ValidationError, json.JSONDecodeError, AssertionError):
        # TODO Add second call for json coercion in case of error
        return LLMJudgeResponse(
            choice=None,
            justification=f"Failed to parse LLM judge response: {raw_response}",
        )


class ToolCallValidationResult(pydantic.BaseModel):
    """Result of validating expected tool calls against chat history."""

    is_valid: bool
    justification: str


def _arguments_match(expected_args: dict, actual_args: dict) -> bool:
    """
    Check if actual arguments match expected arguments.

    Expected arguments can use "*" as a wildcard to indicate that
    the argument can be any value or can be absent.

    Args:
        expected_args: Expected arguments, may contain "*" wildcards
        actual_args: Actual arguments from tool call

    Returns:
        True if arguments match, False otherwise
    """
    for key, expected_value in expected_args.items():
        if expected_value == "*":
            # Wildcard - any value or absence is acceptable
            continue

        if key not in actual_args:
            # Required argument is missing
            return False

        if actual_args[key] != expected_value:
            # Argument value doesn't match
            return False

    # All expected arguments matched (wildcards or exact matches)
    return True


# TOOD: support wildcard / regex for string arguments in tool calls - particularly useful for siblings, search
def validate_expected_tool_calls(
    chat_history: ChatHistory, expected_tool_calls: List[ExpectedToolCall]
) -> ToolCallValidationResult:
    """
    Validate that all expected tool calls are present in the chat history.

    Uses subset matching - all expected tool calls must be present, but additional
    tool calls are allowed. Tool arguments are matched exactly as key-value pairs,
    with dict comparison ignoring key order.

    Args:
        chat_history: The chat history containing actual tool calls
        expected_tool_calls: List of expected tool calls to validate

    Returns:
        ToolCallValidationResult with validation outcome and detailed justification
    """
    if not expected_tool_calls:
        return ToolCallValidationResult(
            is_valid=True, justification="No expected tool calls to validate"
        )

    # Extract actual tool calls from chat history
    actual_tool_requests: List[ToolCallRequest] = []
    for message in chat_history.messages:
        if isinstance(message, (ToolResult, ToolResultError)):
            actual_tool_requests.append(message.tool_request)

    if not actual_tool_requests:
        missing_calls = [
            f"{call.tool_name}({call.tool_input})" for call in expected_tool_calls
        ]
        return ToolCallValidationResult(
            is_valid=False,
            justification=f"No tool calls found in chat history. Expected: {missing_calls}",
        )

    # Check each expected tool call
    missing_tool_calls = []
    found_tool_calls = []

    for expected_call in expected_tool_calls:
        found_match = False

        for actual_call in actual_tool_requests:
            if actual_call.tool_name == expected_call.tool_name:
                # Check if arguments match, allowing "*" as wildcard
                if _arguments_match(expected_call.tool_input, actual_call.tool_input):
                    found_match = True
                    found_tool_calls.append(
                        f"{expected_call.tool_name}({expected_call.tool_input})"
                    )
                    break

        if not found_match:
            missing_tool_calls.append(
                f"{expected_call.tool_name}({expected_call.tool_input})"
            )

    # Build detailed justification
    if missing_tool_calls:
        justification_parts = [f"Missing expected tool calls: {missing_tool_calls}"]

        if found_tool_calls:
            justification_parts.append(f"Found expected tool calls: {found_tool_calls}")

        # Add details about what was actually called
        actual_calls_summary = [
            f"{call.tool_name}({call.tool_input})" for call in actual_tool_requests
        ]
        justification_parts.append(
            f"Actual tool calls in history: {actual_calls_summary}"
        )

        return ToolCallValidationResult(
            is_valid=False, justification=". ".join(justification_parts)
        )

    return ToolCallValidationResult(
        is_valid=True,
        justification=f"All {len(expected_tool_calls)} expected tool calls found: {found_tool_calls}",
    )
