"""
DataCatalog Explorer agent factory and implementation.

This module provides the factory function for creating DataCatalog Explorer agents,
which is the default agent for DataHub chat (formerly ChatSession).
"""

from typing import TYPE_CHECKING, Callable, List, Optional, Sequence, TypeGuard

from datahub.sdk.main_client import DataHubClient
from fastmcp import FastMCP
from loguru import logger

if TYPE_CHECKING:
    from datahub_integrations.observability.bot_metrics import BotPlatform

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentError,
    AgentRunner,
    XmlReasoningParser,
)
from datahub_integrations.chat.agents.data_catalog_prompts import (
    DataHubSystemPromptBuilder,
)
from datahub_integrations.chat.agents.data_catalog_tools import (
    _respond_to_user_tool,
    get_data_catalog_internal_tools,
    is_smart_search_enabled,
)
from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    Message,
    ToolResult,
)
from datahub_integrations.chat.chat_session_formatter import format_message
from datahub_integrations.chat.sql_generator.tools import generate_sql
from datahub_integrations.chat.types import ChatType, NextMessage
from datahub_integrations.gen_ai.linkify import auto_fix_chat_links
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.mcp.mcp_server import (
    ToolType,
    register_all_tools,
)
from datahub_integrations.mcp_integration.tool import (
    ToolWrapper,
    async_background,
    tools_from_fastmcp,
)
from datahub_integrations.smart_search.smart_search import smart_search

register_all_tools(is_oss=False)


MAX_TOOL_CALLS = 30
register_all_tools(is_oss=False)


def _is_respond_to_user_result(message: Message) -> TypeGuard[ToolResult]:
    """Check if message is a ToolResult from respond_to_user tool."""
    return (
        isinstance(message, ToolResult)
        and message.tool_request.tool_name == _respond_to_user_tool.name
    )


def create_response_formatter(
    chat_type: ChatType,
    client: DataHubClient,
) -> Callable[[Message, AgentRunner], NextMessage]:
    """
    Create a response formatter closure that captures the chat_type and client.

    This factory function creates a formatter bound to a specific chat_type and client,
    avoiding the need to store these as dynamic attributes on AgentConfig.

    The formatter applies chat_type-specific formatting to responses from both:
    - respond_to_user tool calls (which return pre-processed text)
    - Direct AssistantMessage responses (fallback case)

    Args:
        chat_type: The chat context type (UI, Slack, Teams, etc.)
        client: DataHub client for accessing frontend URL for link fixing

    Returns:
        A formatter function suitable for AgentConfig.response_formatter
    """
    # Capture frontend_base_url from client at closure creation time
    frontend_base_url = client._graph.frontend_base_url

    def formatter(message: Message, agent: AgentRunner) -> NextMessage:
        """
        Format the final Message into a NextMessage for DataCatalog Explorer.

        This formatter handles two cases uniformly:
        1. respond_to_user tool was called - extract text and suggestions from tool result
        2. Direct AssistantMessage (fallback) - extract text, no suggestions

        Both cases then go through the same processing pipeline:
        - Fix DataHub entity links
        - Apply chat_type-specific formatting

        Args:
            message: The final Message from generate_next_message()
            agent: The AgentRunner instance

        Returns:
            NextMessage with formatted text and optional suggestions

        Raises:
            AgentError: If message type is unexpected
        """
        # Extract text and suggestions from either source
        # Note: reasoning tags already stripped in both cases:
        #   - respond_to_user tool strips them from LLM's tool argument
        #   - agent_runner._handle_text_content strips them for AssistantMessage
        if _is_respond_to_user_result(message):
            logger.info(f"Respond to user call received for session {agent.session_id}")
            pre_formatted = NextMessage.model_validate(message.result)
            text = pre_formatted.text
            suggestions = pre_formatted.suggestions
        elif isinstance(message, AssistantMessage):
            logger.info(f"End turn message received for session {agent.session_id}")
            text = message.text
            suggestions = []
        else:
            raise AgentError(f"Unexpected message type: {type(message)}")

        # Common processing for both cases
        text = auto_fix_chat_links(text, frontend_base_url)
        text = format_message(text, chat_type)
        return NextMessage(text=text, suggestions=suggestions)

    return formatter


def data_catalog_completion_check(message: Message) -> bool:
    """
    Check if DataCatalog Explorer agent should stop generating.

    Stops when:
    - respond_to_user tool is called, OR
    - Agent generates a direct AssistantMessage (fallback)

    Args:
        message: The latest message in history

    Returns:
        True if agent should stop, False to continue
    """
    return _is_respond_to_user_result(message) or isinstance(message, AssistantMessage)


def create_data_catalog_explorer_agent_fast(
    client: DataHubClient,
    history: Optional[ChatHistory] = None,
    extra_instructions_override: Optional[str] = None,
    chat_type: ChatType = ChatType.DEFAULT,
    tools: Optional[Sequence[ToolWrapper | FastMCP]] = None,
    context: Optional[str] = None,
    platform: Optional["BotPlatform"] = None,
) -> AgentRunner:
    """
    Create a fast DataCatalog Explorer agent optimized for speed.

    Fast mode characteristics:
    - Uses faster model (Haiku)
    - No planning mode
    - No mutation tools
    - No smart search

    Args:
        client: DataHub client for tool execution and GraphQL queries
        history: Optional existing chat history to continue from
        extra_instructions_override: Optional override for extra instructions
        chat_type: Type of chat (UI, Slack, Teams, etc.)
        tools: Optional tools to use (defaults to [mcp])
        context: Optional natural language context about what the user is working on
        platform: Optional bot platform (SLACK or TEAMS) for observability tracking

    Returns:
        Configured AgentRunner instance optimized for speed
    """
    model_id = model_config.chat_assistant_ai.agent_mode_to_model.get(
        "AskDataHubFast", model_config.chat_assistant_ai.model
    )

    return create_data_catalog_explorer_agent_base(
        client=client,
        history=history,
        extra_instructions_override=extra_instructions_override,
        chat_type=chat_type,
        model_id=model_id,
        tools=tools,
        context=context,
        platform=platform,
        agent_name="AskDataHubFast",
        include_mutation_tools=False,
        smart_search_enabled=False,
        is_planning_enabled=False,
    )


def create_data_catalog_explorer_agent_research(
    client: DataHubClient,
    history: Optional[ChatHistory] = None,
    extra_instructions_override: Optional[str] = None,
    chat_type: ChatType = ChatType.DEFAULT,
    tools: Optional[Sequence[ToolWrapper | FastMCP]] = None,
    context: Optional[str] = None,
    platform: Optional["BotPlatform"] = None,
) -> AgentRunner:
    """
    Create a research DataCatalog Explorer agent optimized for thoroughness.

    Research mode characteristics:
    - Uses standard model (Sonnet)
    - Planning mode enabled
    - Includes mutation tools
    - Includes smart search

    Args:
        client: DataHub client for tool execution and GraphQL queries
        history: Optional existing chat history to continue from
        extra_instructions_override: Optional override for extra instructions
        chat_type: Type of chat (UI, Slack, Teams, etc.)
        tools: Optional tools to use (defaults to [mcp])
        context: Optional natural language context about what the user is working on
        platform: Optional bot platform (SLACK or TEAMS) for observability tracking

    Returns:
        Configured AgentRunner instance optimized for thoroughness
    """
    model_id = model_config.chat_assistant_ai.agent_mode_to_model.get(
        "AskDataHubResearch", model_config.chat_assistant_ai.model
    )

    return create_data_catalog_explorer_agent_base(
        client=client,
        history=history,
        extra_instructions_override=extra_instructions_override,
        chat_type=chat_type,
        model_id=model_id,
        tools=tools,
        context=context,
        platform=platform,
        agent_name="AskDataHubResearch",
        include_mutation_tools=True,
        smart_search_enabled=is_smart_search_enabled(),
        is_planning_enabled=True,
    )


def create_data_catalog_explorer_agent(
    client: DataHubClient,
    history: Optional[ChatHistory] = None,
    extra_instructions_override: Optional[str] = None,
    chat_type: ChatType = ChatType.DEFAULT,
    tools: Optional[Sequence[ToolWrapper | FastMCP]] = None,
    context: Optional[str] = None,
    platform: Optional["BotPlatform"] = None,
) -> AgentRunner:
    """
    Create an auto DataCatalog Explorer agent with balanced settings.

    Auto mode characteristics:
    - Uses standard model (Sonnet)
    - No planning mode
    - Includes mutation tools
    - Includes smart search

    Args:
        client: DataHub client for tool execution and GraphQL queries
        history: Optional existing chat history to continue from
        extra_instructions_override: Optional override for extra instructions
        chat_type: Type of chat (UI, Slack, Teams, etc.)
        tools: Optional tools to use (defaults to [mcp])
        context: Optional natural language context about what the user is working on
        platform: Optional bot platform (SLACK or TEAMS) for observability tracking

    Returns:
        Configured AgentRunner instance with balanced settings
    """
    model_id = model_config.chat_assistant_ai.agent_mode_to_model.get(
        "AskDataHubAuto", model_config.chat_assistant_ai.model
    )

    return create_data_catalog_explorer_agent_base(
        client=client,
        history=history,
        extra_instructions_override=extra_instructions_override,
        chat_type=chat_type,
        model_id=model_id,
        tools=tools,
        context=context,
        platform=platform,
        agent_name="AskDataHubAuto",
        include_mutation_tools=True,
        smart_search_enabled=is_smart_search_enabled(),
        is_planning_enabled=False,
    )


def build_data_catalog_agent_tools(
    client: DataHubClient,
    chat_type: ChatType,
    tools: Optional[Sequence[ToolWrapper | FastMCP]],
    include_mutation_tools: bool,
    smart_search_enabled: bool,
) -> List[ToolWrapper]:
    """
    Build tool lists for DataCatalog Explorer agent.

    Args:
        client: DataHub client for tool execution
        chat_type: Type of chat (UI, Slack, Teams, etc.)
        tools: Optional tools to use (defaults to [mcp])
        include_mutation_tools: Whether to include MUTATION-tagged tools
        smart_search_enabled: Whether to include smart_search tool
        is_planning_enabled: Whether planning mode is enabled
        agent: Optional AgentRunner for internal tools (only needed if building internal tools)

    Returns:
        Tuple of (tools, plannable_tools, internal_tools)
    """
    # Default to MCP server if no tools provided
    if tools is None:
        from datahub_integrations.mcp.mcp_server import mcp

        tools = [mcp]

    # Filter function for Slack/Teams to exclude USER-tagged tools and FAST mode to exclude MUTATION tools
    def filter_tools(tool):
        # Exclude USER-tagged tools in Slack/Teams
        if chat_type in {ChatType.SLACK, ChatType.TEAMS}:
            if ToolType.USER.value in (tool.tags or set()):
                return False

        # Exclude MUTATION tools if not included
        if not include_mutation_tools:
            if ToolType.MUTATION.value in (tool.tags or set()):
                return False

        return True

    # Prepare plannable tools (public tools from MCP)
    # tools_from_fastmcp applies both tag filtering and document tools filtering
    plannable_tools: List[ToolWrapper] = [
        tool
        for entry in tools
        for tool in (
            tools_from_fastmcp(entry, client, filter_fn=filter_tools)
            if isinstance(entry, FastMCP)
            else ([entry] if filter_tools(entry) else [])
        )
    ]

    # Add smart_search if enabled
    if smart_search_enabled:
        plannable_tools.append(
            ToolWrapper.from_function(
                fn=async_background(smart_search),
                name="smart_search",
                description=smart_search.__doc__ or "Smart search with AI reranking",
            )
        )

    # Add generate_sql tool for text-to-SQL generation
    plannable_tools.append(
        ToolWrapper.from_function(
            fn=async_background(generate_sql),
            name="generate_sql",
            description=generate_sql.__doc__ or "Generate SQL from natural language",
        )
    )

    return plannable_tools


def create_data_catalog_explorer_agent_base(
    client: DataHubClient,
    history: Optional[ChatHistory] = None,
    extra_instructions_override: Optional[str] = None,
    chat_type: ChatType = ChatType.DEFAULT,
    model_id: Optional[str] = None,
    tools: Optional[Sequence[ToolWrapper | FastMCP]] = None,
    context: Optional[str] = None,
    platform: Optional["BotPlatform"] = None,
    agent_name: str = "DataCatalogExplorer",
    include_mutation_tools: bool = True,
    smart_search_enabled: bool = True,
    is_planning_enabled: bool = False,
) -> AgentRunner:
    """
    Create a DataCatalog Explorer agent (formerly ChatSession).

    This factory creates a configured AgentRunner that:
    - Uses DataHub-specific system prompts
    - Has access to MCP tools + smart_search (if enabled)
    - Includes internal tools (respond_to_user, planning tools)
    - Formats responses as NextMessage
    - Uses XML reasoning format

    Args:
        client: DataHub client for tool execution and GraphQL queries
        history: Optional existing chat history to continue from
        extra_instructions_override: Optional override for extra instructions
        chat_type: Type of chat (UI, Slack, Teams, etc.)
        model_id: Optional model ID to use for the agent
        tools: Optional tools to use (defaults to [mcp])
        context: Optional natural language context about what the user is working on
        platform: Optional bot platform (SLACK or TEAMS) for observability tracking
        agent_name: Agent name string (e.g., "DataCatalogExplorer")
        include_mutation_tools: Whether to include MUTATION-tagged tools
        smart_search_enabled: Whether to include smart_search tool
        is_planning_enabled: Whether planning mode is enabled

    Returns:
        Configured AgentRunner instance

    Example:
        ```python
        from datahub.sdk.main_client import DataHubClient
        from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
        from datahub_integrations.chat.chat_history import HumanMessage

        client = DataHubClient.from_env()
        agent = create_data_catalog_explorer_agent(client)
        agent.add_message(HumanMessage(text="What datasets do we have?"))
        response = agent.generate_formatted_message()  # Returns NextMessage
        print(response.text)
        ```
    """

    plannable_tools = build_data_catalog_agent_tools(
        client=client,
        chat_type=chat_type,
        tools=tools,
        include_mutation_tools=include_mutation_tools,
        smart_search_enabled=smart_search_enabled,
    )

    if model_id is None:
        model_id = model_config.chat_assistant_ai.model

    # Create agent configuration (internal tools will be added after AgentRunner creation)
    config = AgentConfig(
        model_id=model_id,
        system_prompt_builder=DataHubSystemPromptBuilder(
            extra_instructions_override, context, is_planning_enabled
        ),
        tools=plannable_tools.copy(),
        plannable_tools=plannable_tools,  # Subset for planning (excludes internal)
        context_reducers=None,  # Will use defaults
        conversational_parser=XmlReasoningParser(),  # DataHub's XML reasoning format
        use_prompt_caching=True,
        max_llm_turns=MAX_TOOL_CALLS,
        temperature=0.5,
        max_tokens=4096,
        agent_name=agent_name,
        response_formatter=create_response_formatter(chat_type, client),
        completion_check=data_catalog_completion_check,
        platform=platform,
    )

    # Create the agent runner
    agent = AgentRunner(
        config=config,
        client=client,
        history=history,
    )

    # Add internal tools that require agent reference
    internal_tools = get_data_catalog_internal_tools(agent, is_planning_enabled)
    agent.tools.extend(internal_tools)

    logger.info(
        f"Created DataCatalog Explorer agent (session={agent.session_id}) "
        f"with {len(agent.tools)} tools ({len(plannable_tools)} plannable, {len(internal_tools)} internal)"
    )

    return agent
