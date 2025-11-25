"""
System prompt builder protocol and base implementations.

This module defines the interface for building system prompts for agents,
allowing each agent to customize its behavior while maintaining a consistent interface.
"""

from typing import TYPE_CHECKING, List, Protocol

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient
    from mypy_boto3_bedrock_runtime.type_defs import SystemContentBlockTypeDef


class SystemPromptBuilder(Protocol):
    """
    Protocol for building system prompts for agents.

    Implementations should define how system messages are constructed,
    potentially incorporating dynamic content from DataHub or other sources.
    """

    def build_system_messages(
        self, client: "DataHubClient"
    ) -> List["SystemContentBlockTypeDef"]:
        """
        Build system messages for the agent.

        Args:
            client: DataHub client for fetching dynamic configuration

        Returns:
            List of system content blocks (typically text blocks with prompts)
        """
        ...


class StaticPromptBuilder:
    """
    Simple implementation that uses a static prompt string.

    This is the most common case where the system prompt doesn't
    need any dynamic content from DataHub.
    """

    def __init__(self, prompt: str):
        """
        Initialize with a static prompt.

        Args:
            prompt: The system prompt text to use
        """
        self.prompt = prompt

    def build_system_messages(
        self, client: "DataHubClient"
    ) -> List["SystemContentBlockTypeDef"]:
        """Build system messages from the static prompt."""
        return [{"text": self.prompt}]


class CallablePromptBuilder:
    """
    Implementation that uses a callable to generate prompts dynamically.

    Useful when you need to generate prompts based on runtime state
    without creating a full class.
    """

    def __init__(self, prompt_fn):
        """
        Initialize with a callable that generates the prompt.

        Args:
            prompt_fn: Callable[[DataHubClient], str] that returns prompt text
        """
        self.prompt_fn = prompt_fn

    def build_system_messages(
        self, client: "DataHubClient"
    ) -> List["SystemContentBlockTypeDef"]:
        """Build system messages by calling the prompt function."""
        prompt = self.prompt_fn(client)
        return [{"text": prompt}]
