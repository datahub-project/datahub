"""
Tools and utilities for the DataCatalog Explorer agent.

This module contains the respond_to_user tool and helper functions for
setting up the DataCatalog Explorer agent's toolset.
"""

import functools
import os
from typing import TYPE_CHECKING, List, Optional

from datahub.cli.env_utils import get_boolean_env_variable
from loguru import logger

from datahub_integrations.chat.agent.agent_runner import _strip_reasoning_tag
from datahub_integrations.chat.agents.data_catalog_prompts import (
    _MAX_SUGGESTIONS,
    MESSAGE_LENGTH_SOFT_LIMIT,
)
from datahub_integrations.chat.types import NextMessage
from datahub_integrations.gen_ai.bedrock import get_bedrock_client
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.mcp_integration.tool import ToolWrapper

if TYPE_CHECKING:
    from datahub_integrations.chat.agent.agent_runner import AgentRunner


# Planning tools feature flag
PLANNING_TOOLS_ENABLED = model_config.chat_assistant_ai.planning_mode_enabled


def respond_to_user(
    response: str,
    follow_up_suggestions: Optional[List[str]] = None,
) -> NextMessage:
    """
    Respond to the user with a pre-processed message.

    This tool only strips reasoning tags. All other processing (link fixing,
    chat_type-specific formatting) is applied uniformly by the response_formatter
    in the agent configuration, which has access to the correct context.

    Args:
        response: The response text to send to the user
        follow_up_suggestions: Optional list of follow-up question suggestions

    Returns:
        NextMessage with pre-processed text (ready for final formatting)
    """
    # Strip any <reasoning> tags that the LLM might have included in tool argument
    response = _strip_reasoning_tag(response)
    # Note: link fixing and chat_type formatting applied by response_formatter
    return NextMessage(
        text=response,
        suggestions=follow_up_suggestions or [],
    )


_respond_to_user_tool = ToolWrapper.from_function(
    fn=respond_to_user,
    name="respond_to_user",
    description=f"""\
CRITICAL: This tool generates the ACTUAL MESSAGE that will be displayed directly to the user. \
Write your response AS IF you are speaking directly to the user, NOT as instructions or meta-commentary.

Format your response using MARKDOWN ONLY. Do NOT use XML, HTML, or any other markup language. \
However, do not use any headers (e.g. #, ##, ###, etc.) or tables, as these are not supported in the chat interface.

The first reference to each entity must be formatted as a link to the entity in DataHub.
CRITICAL: When you have the exact identifier for an entity, include it in your response for clarity and to avoid ambiguity.
Use the human-readable identifier for each entity type:
- For Datasets: qualifiedName (e.g., "prod.finance.customer_transactions")
- For Dashboards: dashboardId
- For Charts: chartId
- For DataJobs: jobId
- For Users: username

Example: "I found [customer_transactions](link) with qualified name: prod.finance.customer_transactions"
This helps users confirm you found the right entity, especially when there are multiple similar entities.

State only FACTUAL INFORMATION. Do not try to infer connection without proof, do not make assumptions.

CRITICAL - VALIDATION BEFORE CALLING THIS TOOL:
Before calling this tool, you MUST validate that your response accurately matches what the user requested.

CRITICAL: For fully-qualified entity names (database.schema.table format), the database, schema, AND table name must ALL match exactly. \
If even one part differs (e.g., different database or schema), it is a DIFFERENT entity, not the same entity.

If the response might not match the user request state this clearly and use conditional/uncertain tone THROUGHOUT \
your entire response 
    EXAMPLES: 
    - User asked for "PROD_DB.FINANCE.revenue_report" but you only found "reporting.finance.revenue_report" 
        → "I couldn't find PROD_DB.FINANCE.revenue_report. I found reporting.finance.revenue_report which \
has a similar name but different database. If this is what you meant, here's what I found..."
    - User asked for "Customer Metrics" dashboard but you found "Customer Analytics" dashboard 
        → "I couldn't find a dashboard named 'Customer Metrics'. I found 'Customer Analytics' which may be \
related. If this is what you're looking for, it shows..."
    - User asked for deprecated datasets but you found deprecated dashboards 
        → "I couldn't find deprecated datasets, but I did find deprecated dashboards. If you're interested \
in these instead, here are..."

When stating that the entities are related always provide proof for that, if you don't have proof call them SIMILAR.

You may also provide up to {_MAX_SUGGESTIONS} suggestions for questions the user may want to ask as a follow \
up, but this is not required. If there is uncertainty that we correctly answered the user's request, \
clarify this in the suggestions.

IMPORTANT: Keep your response concise and under {MESSAGE_LENGTH_SOFT_LIMIT} characters. \
If you need to provide more information, focus on the most relevant points and summarize the rest. \
Break down complex information into bullet points for better readability.""",
)


def get_data_catalog_internal_tools(agent: "AgentRunner") -> List[ToolWrapper]:
    """
    Get internal tools for the DataCatalog Explorer agent.

    These include:
    - respond_to_user: For responding to the user
    - Planning tools: create_plan, revise_plan, report_step_progress (if planning enabled)

    These tools are NOT exposed on the customer-facing MCP server.

    Args:
        agent: The AgentRunner instance to bind to planning tools
    """
    tools = [_respond_to_user_tool]

    if PLANNING_TOOLS_ENABLED:
        # Import inline to avoid circular dependency
        from datahub_integrations.chat.planner.tools import get_planning_tool_wrappers

        tools.extend(get_planning_tool_wrappers(agent))

    return tools


@functools.cache
def is_smart_search_enabled() -> bool:
    """
    Lazily determine if smart search should be enabled.

    Checks in order:
    1. If CHATBOT_SMART_SEARCH_ENABLED is explicitly set, use that value
    2. Otherwise, check if Bedrock client's region supports Cohere rerank models

    This is lazily evaluated so we don't initialize the Bedrock client at module import time.
    The cache decorator ensures thread-safe singleton behavior.
    """
    # Regions where Cohere rerank models are available in Bedrock
    cohere_rerank_supported_regions = {"us-west-2", "eu-central-1"}

    # Check if explicitly set via environment variable
    env_value = os.environ.get("CHATBOT_SMART_SEARCH_ENABLED")
    if env_value is not None:
        enabled = get_boolean_env_variable("CHATBOT_SMART_SEARCH_ENABLED")
        logger.info(
            f"Smart search tool {'ENABLED' if enabled else 'DISABLED'} for DataCatalog agent (explicit env var)"
        )
        return enabled

    # Otherwise, check if the Bedrock region supports Cohere rerank
    try:
        bedrock_client = get_bedrock_client()
        region = bedrock_client.meta.region_name
        enabled = region in cohere_rerank_supported_regions
        logger.info(
            f"Smart search tool {'ENABLED' if enabled else 'DISABLED'} for DataCatalog agent "
            f"(auto-detected from Bedrock region: {region})"
        )
        return enabled
    except Exception as e:
        logger.warning(
            f"Failed to check Bedrock region for smart search: {e}. Defaulting to DISABLED."
        )
        return False
