import re
import urllib.parse
from functools import lru_cache
from typing import List

from loguru import logger

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    ToolResult,
)


def _extract_urns_from_dict(data: dict) -> List[str]:
    """Recursively extract URNs from a dictionary."""
    urns = []
    for _, value in data.items():
        if isinstance(value, str) and value.startswith("urn:li:"):
            urns.append(value)
        elif isinstance(value, dict):
            urns.extend(_extract_urns_from_dict(value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.startswith("urn:li:"):
                    urns.append(item)
                elif isinstance(item, dict):
                    urns.extend(_extract_urns_from_dict(item))
    return urns


def extract_urns_from_history(history: ChatHistory) -> List[str]:
    """Extract all URNs from tool call results in chat history."""
    if not history:
        return []

    try:
        urns = []

        for message in history.messages:
            if (
                isinstance(message, ToolResult)
                and message.tool_request.tool_name != "respond_to_user"
            ):
                result = message.result
                if isinstance(result, dict):
                    # Look for URNs in the result data
                    urns.extend(_extract_urns_from_dict(result))
                elif isinstance(result, str):
                    # Look for URNs in string format
                    # TODO: AI docs may have incorrect links
                    urns.extend(re.findall(r'urn:li:[^"\s,\]]+', result))

        return list(set(urns))  # Remove duplicates
    except Exception as e:
        logger.warning(f"Error parsing chat history: {e}")
        return []


def extract_datahub_links_from_response(response: str) -> List[str]:
    """Extract DataHub links from response in format https://xxx.acryl.io/<entity>/<urn>."""
    if not response:
        return []

    # Pattern to match DataHub links with URNs, assuming URNs are url encoded
    pattern = r"https://[^/]+\.acryl\.io/[^/]+/([^\)]+)"
    matches = re.findall(pattern, response)
    return [urllib.parse.unquote(urn.strip("\\/")) for urn in matches]


def extract_response_from_history(history: ChatHistory) -> str | None:
    """Extract the response to user from chat history."""

    try:
        # Look for the last ToolResult with tool_name "respond_to_user"
        for message in reversed(history.messages):
            if (
                isinstance(message, ToolResult)
                and message.tool_request.tool_name == "respond_to_user"
            ):
                return message.tool_request.tool_input["response"]
            elif len(history.messages) == 2 and isinstance(message, AssistantMessage):
                return message.text
        return None
    except Exception:
        return None


@lru_cache(maxsize=1000)
def extract_response_from_history_json(history_json: str) -> str | None:
    if not history_json:
        return None
    history = ChatHistory.model_validate_json(history_json)
    return extract_response_from_history(history)
