import re
import urllib.parse
from functools import lru_cache
from typing import List, Tuple

import tiktoken
from datahub.ingestion.graph.links import make_url_for_urn
from loguru import logger

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    ToolResult,
)
from datahub_integrations.experimentation.docs_generation.metrics import extract_links


def get_token_count(text: str) -> int:
    """Count tokens in text and return abbreviated count with 'tokens' suffix."""
    # This is just an approximation since different models have different tokenizers.
    encoding = tiktoken.encoding_for_model("gpt-4o")
    token_count = len(encoding.encode(text))
    return token_count


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


def extract_valid_urns_from_history(history: ChatHistory) -> List[str]:
    """Extract all URNs from tool call results in chat history."""

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


def extract_full_datahub_links_from_response(response: str) -> List[Tuple[str, str]]:
    """Extract DataHub links from response in format https://xxx.acryl.io/<entity>/<urn>."""
    if not response:
        return []

    # Pattern to match DataHub links with URNs, assuming URNs are url encoded
    pattern = r"\[([^\]]+)\]\((https://[^/]+\.acryl\.io/[^/]+/[^\)]+)\)"
    matches = re.findall(pattern, response)
    return [(text, urllib.parse.unquote(link.strip("\\/"))) for text, link in matches]


def extract_response_from_history(history: ChatHistory) -> str | None:
    """Extract the response to user from chat history."""

    try:
        # Look for the last ToolResult with tool_name "respond_to_user"
        for message in reversed(history.messages):
            if (
                isinstance(message, ToolResult)
                and message.tool_request.tool_name == "respond_to_user"
                and isinstance(message.result, dict)
                and "text" in message.result
            ):
                return message.result["text"]

        # Fallback: Look for the last AssistantMessage
        # This handles cases where LLM outputs directly without calling respond_to_user
        for message in reversed(history.messages):
            if isinstance(message, AssistantMessage):
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


def check_for_invalid_links(
    response: str, valid_urns: List[str], frontend_base_url: str
) -> List[Tuple[str, str]]:
    """Check if the response contains invalid links and return the invalid links."""
    response_links = extract_full_datahub_links_from_response(response)

    response_links += extract_links(response)

    invalid_links = []

    for response_link in response_links:
        urn = _extract_urn_from_link(response_link[1])
        if urn not in valid_urns:
            invalid_links.append(response_link)
            continue

        valid_link = make_url_for_urn(frontend_base_url, urn)
        if urllib.parse.unquote(valid_link.strip("\\/")) != response_link[1]:
            invalid_links.append(response_link)
            continue

    return invalid_links


def _extract_urn_from_link(full_link: str) -> str:
    return full_link[full_link.find("urn:li:") :]
