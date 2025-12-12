"""
Ingestion Troubleshooting agent factory and implementation.

This module provides the factory function for creating Ingestion Troubleshooting agents,
which is the default agent for chats from the Ingestion page in the UI.
"""

from typing import List, Optional, Sequence

from datahub.sdk.main_client import DataHubClient
from fastmcp import FastMCP
from loguru import logger

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentRunner,
    XmlReasoningParser,
)
from datahub_integrations.chat.agents.data_catalog_agent import (
    create_response_formatter,
    data_catalog_completion_check,
)
from datahub_integrations.chat.agents.data_catalog_prompts import (
    DataHubSystemPromptBuilder,
)
from datahub_integrations.chat.agents.data_catalog_tools import (
    get_data_catalog_internal_tools,
    is_smart_search_enabled,
)
from datahub_integrations.chat.agents.tools.ingestion import (
    get_ingestion_execution_logs,
    get_ingestion_execution_request,
    get_ingestion_source,
)
from datahub_integrations.chat.agents.tools.troubleshoot import (
    is_troubleshoot_available,
    troubleshoot,
)
from datahub_integrations.chat.chat_history import (
    ChatHistory,
)
from datahub_integrations.chat.types import ChatType
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.mcp.mcp_server import register_all_tools
from datahub_integrations.mcp_integration.tool import (
    ToolWrapper,
    async_background,
    tools_from_fastmcp,
)
from datahub_integrations.smart_search.smart_search import smart_search

MAX_TOOL_CALLS = 30

# Register MCP tools (thread-safe, idempotent)
register_all_tools(is_oss=False)


def create_ingestion_troubleshooting_agent(
    client: DataHubClient,
    history: Optional[ChatHistory] = None,
    extra_instructions_override: Optional[str] = None,
    chat_type: ChatType = ChatType.DEFAULT,
    tools: Optional[Sequence[ToolWrapper | FastMCP]] = None,
    context: Optional[str] = None,
) -> AgentRunner:
    """
    Create a Ingestion Troubleshooting agent.

    This factory creates a configured AgentRunner that:
    - Uses DataHub-specific system prompts
    - Has access to MCP tools + smart_search (if enabled)
    - Includes internal tools (respond_to_user, planning tools)
    - Has access to the troubleshooting tool which queries another agent trained on DataHub documentation and common issues
    - Formats responses as NextMessage
    - Uses XML reasoning format

    Args:
        client: DataHub client for tool execution and GraphQL queries
        history: Optional existing chat history to continue from
        extra_instructions_override: Optional override for extra instructions
        chat_type: Type of chat (UI, Slack, Teams, etc.)
        tools: Optional tools to use (defaults to [mcp])
        context: Optional natural language context about what the user is working on

    Returns:
        Configured AgentRunner instance
    """
    # Use default MCP tools if not provided
    if tools is None:
        from datahub_integrations.mcp.mcp_server import mcp

        tools = [mcp]

    # Prepare plannable tools (public tools from MCP)
    plannable_tools: List[ToolWrapper] = [
        tool
        for entry in tools
        for tool in (
            tools_from_fastmcp(entry) if isinstance(entry, FastMCP) else [entry]
        )
    ]

    plannable_tools.extend(
        [
            ToolWrapper.from_function(
                fn=async_background(get_ingestion_source),
                name="get_ingestion_source",
                description=get_ingestion_source.__doc__
                or "Get ingestion source information",
            ),
            ToolWrapper.from_function(
                fn=async_background(get_ingestion_execution_request),
                name="get_ingestion_execution_request",
                description=get_ingestion_execution_request.__doc__
                or "Get ingestion execution request details",
            ),
            ToolWrapper.from_function(
                fn=async_background(get_ingestion_execution_logs),
                name="get_ingestion_execution_logs",
                description=get_ingestion_execution_logs.__doc__
                or "Get ingestion execution logs",
            ),
        ]
    )

    # Add smart_search if enabled
    if is_smart_search_enabled():
        plannable_tools.append(
            ToolWrapper.from_function(
                fn=async_background(smart_search),
                name="smart_search",
                description=smart_search.__doc__ or "Smart search with AI reranking",
            )
        )

    # Add troubleshoot tool if a provider is configured
    if is_troubleshoot_available():
        plannable_tools.append(
            ToolWrapper.from_function(
                fn=async_background(troubleshoot),
                name="troubleshoot",
                description=troubleshoot.__doc__
                or "Search DataHub documentation and troubleshoot issues",
            )
        )

    # Create agent configuration (internal tools will be added after AgentRunner creation)
    config = AgentConfig(
        model_id=model_config.chat_assistant_ai.model,
        system_prompt_builder=DataHubSystemPromptBuilder(
            extra_instructions_override, context
        ),
        tools=plannable_tools.copy(),  # Will be extended with internal tools
        plannable_tools=plannable_tools,  # Subset for planning (excludes internal)
        context_reducers=None,  # Will use defaults
        conversational_parser=XmlReasoningParser(),  # DataHub's XML reasoning format
        use_prompt_caching=True,
        max_tool_calls=MAX_TOOL_CALLS,
        temperature=0.5,
        max_tokens=4096,
        agent_name="Ingestion Troubleshooting",
        response_formatter=create_response_formatter(chat_type, client),
        completion_check=data_catalog_completion_check,
    )

    # Create the agent runner
    agent = AgentRunner(
        config=config,
        client=client,
        history=history,
    )

    # Add internal tools after AgentRunner is created (they need the runner instance)
    internal_tools = get_data_catalog_internal_tools(agent)
    agent.tools.extend(internal_tools)

    logger.info(
        f"Created Ingestion Troubleshooting agent (session={agent.session_id}) "
        f"with {len(agent.tools)} tools ({len(plannable_tools)} plannable, {len(internal_tools)} internal)"
    )

    return agent
