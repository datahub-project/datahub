"""
Agent configuration via composition.

This module defines AgentConfig, which specifies all aspects of an agent's
behavior through composition of pluggable components.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Optional

if TYPE_CHECKING:
    from datahub_integrations.chat.agent.conversational_parser import (
        ConversationalParser,
    )
    from datahub_integrations.chat.agent.system_prompt_builder import (
        SystemPromptBuilder,
    )
    from datahub_integrations.chat.context_reducer import ChatContextReducer
    from datahub_integrations.mcp_integration.tool import ToolWrapper


@dataclass
class AgentConfig:
    """
    Configuration for agent behavior via composition.

    This dataclass defines all aspects of an agent's behavior by composing
    pluggable components rather than using inheritance. This makes it easy
    to create new agents by mixing and matching existing components.

    Example:
        ```python
        tools = flatten_tools([mcp])
        tools.append(respond_to_user_tool)

        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=StaticPromptBuilder("You are a helpful assistant"),
            tools=tools,
            temperature=0.5,
        )
        ```
    """

    # Core LLM settings
    model_id: str
    """The LLM model identifier (e.g., 'anthropic.claude-3-5-sonnet-20241022-v2:0')"""

    system_prompt_builder: "SystemPromptBuilder"
    """Builder that constructs system messages for this agent"""

    # Tools
    tools: List["ToolWrapper"]
    """
    All tools available to the agent.
    
    This includes both:
    - Plannable tools (search, get_entities, etc.) - used in execution plans
    - Internal tools (respond_to_user, planning tools) - control flow
    
    Example:
        ```python
        plannable_tools = flatten_tools([mcp])
        internal_tools = [respond_to_user_tool]
        all_tools = plannable_tools + internal_tools
        
        config = AgentConfig(
            tools=all_tools,
            plannable_tools=plannable_tools,  # Subset used for planning
        )
        ```
    """

    plannable_tools: List["ToolWrapper"]
    """
    Subset of tools that can be used in execution plans.
    
    Typically excludes internal/control-flow tools like respond_to_user
    and planning tools themselves (to avoid meta-planning).
    
    If your agent doesn't use planning, this can be the same as tools.
    """

    # Context management
    context_reducers: Optional[Iterable["ChatContextReducer"]] = None
    """
    Optional chain of context reducers for managing token limits.
    
    If None, defaults will be created based on model_id.
    """

    # Conversational message parsing
    conversational_parser: Optional["ConversationalParser"] = None
    """
    Optional parser for converting LLM reasoning messages to user-visible text.
    
    Different agents may use different formats (XML, JSON, plain text).
    If None, defaults to PlainTextParser (returns text as-is).
    
    Example:
        ```python
        from datahub_integrations.chat.agent.conversational_parser import XmlReasoningParser
        
        config = AgentConfig(
            ...
            conversational_parser=XmlReasoningParser(),  # For DataHub's XML format
        )
        ```
    """

    # LLM inference settings
    use_prompt_caching: bool = True
    """Whether to use prompt caching for faster responses and lower costs"""

    max_tool_calls: int = 30
    """Maximum number of tool calls before stopping generation"""

    temperature: float = 0.5
    """LLM temperature (0.0-1.0, lower is more deterministic)"""

    max_tokens: int = 4096
    """Maximum tokens to generate in a single response"""

    # Additional metadata
    agent_name: str = "Agent"
    """Human-readable name for this agent (used in logging)"""

    agent_description: str = ""
    """Optional description of what this agent does"""


# Import here to avoid circular imports
if TYPE_CHECKING:
    pass
