"""
Agent configuration via composition.

This module defines AgentConfig, which specifies all aspects of an agent's
behavior through composition of pluggable components.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Iterable, List, Optional

if TYPE_CHECKING:
    from datahub_integrations.chat.agent.conversational_parser import (
        ConversationalParser,
    )
    from datahub_integrations.chat.agent.system_prompt_builder import (
        SystemPromptBuilder,
    )
    from datahub_integrations.chat.chat_history import Message
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

    # Response formatting
    response_formatter: Optional[Callable[["Message", Any], Any]] = None
    """
    Optional formatter to convert the final Message to a domain-specific response format.
    
    This allows agents to return custom response types (e.g., NextMessage for DataHub chat)
    without the core AgentRunner knowing about those types.
    
    The formatter receives:
    - message: The final Message from generate_next_message()
    - agent: The AgentRunner instance (for accessing config, history, etc.)
    
    Returns: Any custom response type (or Message if None)
    
    Example:
        ```python
        def format_response(message: Message, agent: AgentRunner) -> NextMessage:
            if is_respond_to_user(message):
                return NextMessage.model_validate(message.result)
            return NextMessage(text=message.text, suggestions=[])
        
        config = AgentConfig(
            ...
            response_formatter=format_response,
        )
        ```
    """

    # Completion check
    completion_check: Optional[Callable[["Message"], bool]] = None
    """
    Optional function to determine when the agent should stop generating.
    
    If None, defaults to stopping when an AssistantMessage is generated.
    Different agents may have different completion conditions (e.g., DataCatalog
    Explorer stops when respond_to_user tool is called).
    
    Function signature: (Message) -> bool
    Returns True if the agent should stop, False to continue.
    
    Example:
        ```python
        def my_completion_check(message: Message) -> bool:
            return isinstance(message, ToolResult) and message.tool_request.tool_name == "done"
        
        config = AgentConfig(
            ...
            completion_check=my_completion_check,
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
