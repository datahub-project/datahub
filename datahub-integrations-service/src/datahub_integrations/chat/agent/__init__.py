"""
Reusable agent infrastructure for DataHub.

This module provides composable infrastructure for building agents and subagents
without duplicating the core agentic loop, tool execution, and message handling.

Key exports:
- AgentRunner: Core infrastructure for running agentic loops
- AgentConfig: Configuration for agent behavior via composition
- SystemPromptBuilder: Protocol for building system prompts
- ProgressTracker: Progress tracking utilities
- Tool composition utilities

Example:
    ```python
    from datahub_integrations.chat.agent import (
        AgentRunner,
        AgentConfig,
        StaticPromptBuilder,
    )

    config = AgentConfig(
        model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
        system_prompt_builder=StaticPromptBuilder("You are a helpful assistant"),
        tools=[mcp],
    )

    runner = AgentRunner(config=config, client=client)
    runner.history.add_message(HumanMessage(text="Hello"))
    response = runner.generate_next_message()
    ```
"""

from datahub_integrations.chat.agent.agent_config import AgentConfig
from datahub_integrations.chat.agent.agent_runner import (
    AgentError,
    AgentMaxTokensExceededError,
    AgentMaxToolCallsExceededError,
    AgentOutputMaxTokensExceededError,
    AgentRunner,
)
from datahub_integrations.chat.agent.conversational_parser import (
    ConversationalParser,
    PlainTextParser,
    XmlReasoningParser,
)
from datahub_integrations.chat.agent.progress_tracker import (
    ProgressCallback,
    ProgressTracker,
    ProgressUpdate,
)
from datahub_integrations.chat.agent.system_prompt_builder import (
    CallablePromptBuilder,
    StaticPromptBuilder,
    SystemPromptBuilder,
)
from datahub_integrations.chat.agent.tool_composition import (
    exclude_tools_by_names,
    filter_tools_by_names,
    flatten_tools,
    tools_from_fastmcp,
)

__all__ = [
    # Core classes
    "AgentRunner",
    "AgentConfig",
    # System prompt builders
    "SystemPromptBuilder",
    "StaticPromptBuilder",
    "CallablePromptBuilder",
    # Conversational parsers
    "ConversationalParser",
    "PlainTextParser",
    "XmlReasoningParser",
    # Progress tracking
    "ProgressTracker",
    "ProgressUpdate",
    "ProgressCallback",
    # Tool composition
    "flatten_tools",
    "tools_from_fastmcp",
    "filter_tools_by_names",
    "exclude_tools_by_names",
    # Exceptions
    "AgentError",
    "AgentMaxTokensExceededError",
    "AgentOutputMaxTokensExceededError",
    "AgentMaxToolCallsExceededError",
]
