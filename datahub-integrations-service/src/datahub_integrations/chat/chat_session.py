from datahub_integrations.gen_ai.mlflow_init import initialize_mlflow

import contextlib
import functools
import os
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    TypeGuard,
)

import cachetools
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.sdk.main_client import DataHubClient
from fastmcp import FastMCP
from loguru import logger
from pydantic import BaseModel, field_validator

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentError,
    AgentMaxTokensExceededError,
    AgentMaxToolCallsExceededError,
    AgentOutputMaxTokensExceededError,
    AgentRunner,
)
from datahub_integrations.chat.agent.agent_runner import _strip_reasoning_tag
from datahub_integrations.chat.agent.conversational_parser import (
    XmlReasoningParser,
)
from datahub_integrations.chat.agent.progress_tracker import (
    ProgressCallback,
    ProgressUpdate,
)
from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    Message,
    ReasoningMessage,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.chat.chat_session_formatter import format_message
from datahub_integrations.chat.context_reducer import (
    ChatContextReducer,
)
from datahub_integrations.chat.types import ChatType
from datahub_integrations.chat.utils import parse_reasoning_message
from datahub_integrations.gen_ai.bedrock import (
    get_bedrock_client,
)
from datahub_integrations.gen_ai.linkify import auto_fix_chat_links
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.mcp.mcp_server import (
    get_datahub_client,
    mcp,
    register_all_tools,
)
from datahub_integrations.mcp_integration.tool import (
    ToolWrapper,
    async_background,
    tools_from_fastmcp,
)
from datahub_integrations.slack.utils.string import truncate
from datahub_integrations.smart_search.smart_search import smart_search

# Register MCP tools with Cloud features (thread-safe, idempotent)
register_all_tools(is_oss=False)

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import (
        SystemContentBlockTypeDef,
    )

# Initialize MLflow for @mlflow.trace decorators in this module
initialize_mlflow()
# Planning tools feature flag
PLANNING_TOOLS_ENABLED = model_config.chat_assistant_ai.planning_mode_enabled

if PLANNING_TOOLS_ENABLED:
    logger.info("Planning tools ENABLED for ChatSession")
else:
    logger.info("Planning tools DISABLED for ChatSession")


@functools.cache
def _is_smart_search_enabled() -> bool:
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
            f"Smart search tool {'ENABLED' if enabled else 'DISABLED'} for ChatSession (explicit env var)"
        )
        return enabled

    # Otherwise, check if the Bedrock region supports Cohere rerank
    try:
        bedrock_client = get_bedrock_client()
        region = bedrock_client.meta.region_name
        enabled = region in cohere_rerank_supported_regions
        logger.info(
            f"Smart search tool {'ENABLED' if enabled else 'DISABLED'} for ChatSession "
            f"(auto-detected from Bedrock region: {region})"
        )
        return enabled
    except Exception as e:
        logger.warning(
            f"Failed to check Bedrock region for smart search: {e}. Defaulting to DISABLED."
        )
        return False


MAX_TOOL_CALLS = 30

# The soft limit is passed to the LLM's prompt, in an effort
# to have it be concise.
MESSAGE_LENGTH_SOFT_LIMIT = 1500
# The hard limit is derived from Slack's limits, where the Slack "section"
# block's text field has a limit of 3000 characters.
# See https://api.slack.com/reference/block-kit/blocks#section
MESSAGE_LENGTH_HARD_LIMIT = 3000 - 100  # 100 is a buffer
assert MESSAGE_LENGTH_HARD_LIMIT >= 1.5 * MESSAGE_LENGTH_SOFT_LIMIT

_MAX_SUGGESTIONS = 4

CLAUDE_TOKEN_LIMIT = int(200e3)


# Backward compatible exception aliases (simple references to agent exceptions)
# These allow existing code (Slack, Teams, tests) to continue using ChatSession-prefixed names
ChatSessionMaxTokensExceededError = AgentMaxTokensExceededError
ChatOutputMaxTokensExceededError = AgentOutputMaxTokensExceededError
ChatSessionError = AgentError
ChatMaxToolCallsExceededError = AgentMaxToolCallsExceededError


class NextMessage(BaseModel):
    text: str
    suggestions: List[str] = []

    @field_validator("suggestions", mode="after")
    @classmethod
    def validate_suggestions(cls, v: List[str]) -> List[str]:
        """
        Validate and warn about suggestion count limits.

        Args:
            v: List of suggestion strings

        Returns:
            Validated suggestions list
        """
        if len(v) > _MAX_SUGGESTIONS:
            logger.warning(
                f"Model provided {len(v)} suggestions, but only {_MAX_SUGGESTIONS} are allowed. Truncating to {_MAX_SUGGESTIONS}."
            )
            return v[:_MAX_SUGGESTIONS]

        return v


def respond_to_user(
    response: str,
    follow_up_suggestions: Optional[List[str]] = None,
    chat_type: ChatType = ChatType.DEFAULT,
) -> NextMessage:
    """Respond to the user with a formatted message."""
    client = get_datahub_client()
    # Strip any <reasoning> tags that the LLM might have included
    response = _strip_reasoning_tag(response)
    response = auto_fix_chat_links(response, client._graph.frontend_base_url)
    formatted_text = format_message(response, chat_type)
    return NextMessage(
        text=formatted_text,
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


def _get_internal_chatbot_tools(agent: AgentRunner) -> List[ToolWrapper]:
    """
    Get internal chatbot tools that are always available.

    These include:
    - respond_to_user: For responding to the user
    - Planning tools: create_plan, revise_plan, report_step_progress (if CHATBOT_PLANNING_ENABLED)

    These tools are NOT exposed on the customer-facing MCP server.

    Args:
        agent: The AgentRunner instance to bind to planning tools
    """
    tools = [_respond_to_user_tool]

    if PLANNING_TOOLS_ENABLED:
        # Import inline to avoid circular dependency
        # (planner.tools imports from chat_session for get_extra_llm_instructions)
        from datahub_integrations.chat.planner.tools import get_planning_tool_wrappers

        tools.extend(get_planning_tool_wrappers(agent))

    return tools


_SYSTEM_PROMPT = f"""\
The assistant is DataHub AI, created by Acryl Data.

DataHub AI is a helpful assistant that can answer questions relating to \
metadata management, data discovery, data governance, and data quality within the organization.

DataHub AI provides thorough responses to more complex and open-ended questions or to anything where \
a long response is requested, but concise responses to simpler questions and tasks.

UNDERSTANDING DATAHUB'S METADATA MODEL:
DataHub organizes metadata into entity types with specific relationships. When searching, DataHub AI considers these relationships:
- Dataset: Tables with rows and columns that can be joined and queried
- Dashboard: Visualizations of metrics and dimensions over time that contain Charts
- Chart: Individual visualization panels within a Dashboard
- Data Flow: Multi-step DAGs (pipelines) that contain Data Jobs
- Data Job: Individual steps within a Data Flow or pipeline
- Container: Collections of other assets (e.g., databases, schemas)
- Tag: Freeform labels for organizing data assets
- Glossary Term: Governed, hierarchical business concepts and definitions
- Domain: Business areas for organizing data, typically hierarchical

When users ask about Dashboards, DataHub AI proactively searches for both DASHBOARD and CHART entity types, \
since dashboards contain charts and users typically want to see both. Similarly, when users ask about \
pipelines or data flows, DataHub AI searches for both DATA_FLOW and DATA_JOB entity types.

DataHub AI makes use of the available tools in order to effectively answer the person's question. \
DataHub AI will typically make multiple tool calls in order to answer a single question, and will stop asking for more tool calls once it has enough information to answer the question.
DataHub AI will not make more than 10 tool calls in a single response.

{
    "DataHub AI SHOULD use create_plan for complex tasks that require 3 or more tool calls, especially for impact analysis, dependency analysis, or tasks requiring iterative refinement. Simple 1-2 tool call tasks can be executed directly without planning."
    if PLANNING_TOOLS_ENABLED
    else ""
}

DataHub AI can also answer very basic questions about DataHub itself using its built-in knowledge. \
For more complex questions about DataHub's features and best practices (e.g. "how do I set up a business \
glossary?" or "can I download search results as a CSV?"), it suggests asking the \
DataHub team and checking out the DataHub documentation at https://docs.datahub.com/. \
When referencing the DataHub documentation, it only links to the docs homepage as it does not know specific page URLs.

DataHub AI provides the shortest answer it can to the person's message, while respecting any stated length and comprehensiveness preferences given by the person. DataHub AI addresses the specific query or task at hand, avoiding tangential information unless absolutely critical for completing the request.

DataHub AI avoids writing lists, but if it does need to write a list, DataHub AI focuses on key info instead of trying to be comprehensive. If DataHub AI can answer the human in 1-3 sentences or a short paragraph, it does. If DataHub AI can write a natural language list of a few comma separated items instead of a numbered or bullet-pointed list, it does so. DataHub AI tries to stay focused and share fewer, high quality examples or ideas rather than many.

IMPORTANT: Before each tool call, DataHub AI outputs structured reasoning in XML format explaining what it's about to do and why. \
The reasoning MUST be wrapped in <reasoning></reasoning> tags and include these fields:

<reasoning>
  <action>Brief description of the tool call about to be made</action>
  <rationale>Why this tool call is needed</rationale>
  <justification>REQUIRED when making choices: When selecting from multiple options \
explicitly justify the choice. Include: \
(1) What alternatives were available (e.g., "Search returned 10 results, examining result #2"), \
(2) Why this specific option was chosen over others, \
(3) How search ranking was considered.</justification>
  <user_requested>What the user originally asked for (if applicable)</user_requested>
  <what_found>What was actually found (if different from user request)</what_found>
  <exact_match>true/false - Does what was found exactly match what the user requested?</exact_match>
  <discrepancies>If exact_match is false, list specific differences (database, schema, name, type, etc.)</discrepancies>
  <proof_of_relation>If claiming entities are related, provide specific proof. If no proof, state "SIMILAR names only"</proof_of_relation>
  <confidence>high/medium/low</confidence>
  <warning>Any important caveats or warnings about this action</warning>
{
    '''  <plan_id>OPTIONAL: If executing a plan created by create_plan, include the plan_id here (e.g., "plan_abc123")</plan_id>
  <plan_step>OPTIONAL: If working on a specific step, include the step ID (e.g., "s0", "s1")</plan_step>
  <done_criteria_met>OPTIONAL: Check actual results against the step's done_when condition. Example: done_when="Search returned exactly 1 result": total=1→true, total=9→FALSE. Always check actual values!</done_criteria_met>
  <failed_criteria_met>OPTIONAL: Check actual results against the step's failed_when condition. Example: failed_when="Search returned more than 1 result": total=9→TRUE, total=1→false</failed_criteria_met>
  <return_to_user_criteria_met>OPTIONAL: Check actual results against the step's return_to_user_when condition. If the step has a return_to_user_when field, evaluate whether that condition is met. Example: return_to_user_when="No PII metadata exists OR search returned 0 results": if step0 found no metadata OR total=0 → TRUE, otherwise → FALSE. If no return_to_user_when field exists, this should be omitted or false.</return_to_user_criteria_met>
  <step_status>OPTIONAL: returned_to_user (if return_to_user_criteria_met=true), failed (if failed_criteria_met=true but not returned_to_user), completed (if done_criteria_met=true), in_progress, started</step_status>
  <plan_status>OPTIONAL: If step_status=returned_to_user: plan_status=returned_to_user, call respond_to_user, do NOT call revise_plan</plan_status>
  <next_action>REQUIRED if step_status=returned_to_user: Your next and ONLY action must be respond_to_user. Do NOT call any other tools. Do NOT call revise_plan. Do NOT continue execution.</next_action>
'''
    if PLANNING_TOOLS_ENABLED
    else ""
}
</reasoning>

{
    '''  CRITICAL DISTINCTION - returned_to_user vs failed:
- step_status="failed": Technical failure that CAN be fixed by revising the plan or retrying (e.g., timeout, API error)
  → Action: Can call revise_plan to try different approach
- step_status="returned_to_user": Fundamental blocker that CANNOT be fixed by replanning (e.g., no metadata exists, missing required data, ambiguous search results requiring user choice)
  → Action: MUST call respond_to_user with explanation, MUST NOT call revise_plan, MUST NOT continue execution

'''
    if PLANNING_TOOLS_ENABLED
    else ""
}For tool calls where entity matching is not relevant (e.g., initial searches), you can omit the matching fields.
The plan fields are OPTIONAL and should only be included when you are executing a multi-step plan created by the create_plan tool.

CRITICAL: For fully-qualified entity names (database.schema.table format), the database, schema, AND table name must ALL match exactly. If even one part differs, it is a DIFFERENT entity, not the same entity.
- State only FACTUAL INFORMATION. Do not try to infer connection without proof, do not make assumptions.


DataHub AI is now being connected with a person."""


@cachetools.cached(cache=cachetools.TTLCache(maxsize=1, ttl=60 * 5))
def get_extra_llm_instructions(client: DataHubClient) -> Optional[str]:
    """
    Retrieve optional extra LLM instructions from GraphQL API.

    Instructions are cached for 5 minutes via TTLCache decorator.
    Fetches the most recent ACTIVE GENERAL_CONTEXT instruction from global AI assistant settings.

    Args:
        client: DataHub client instance to use for GraphQL queries

    Returns:
        Optional extra instructions string, or None if not configured.

    Raises:
        Exception: If GraphQL call fails
    """

    query = """
    query getGlobalSettings {
        globalSettings {
            aiAssistant {
                instructions {
                    id
                    type
                    state
                    instruction
                    lastModified {
                        time
                        actor
                    }
                }
            }
        }
    }
    """

    try:
        response = client._graph.execute_graphql(query)

        # Extract AI assistant instructions
        global_settings = response.get("globalSettings")
        if not global_settings:
            return None

        ai_assistant = global_settings.get("aiAssistant")
        if not ai_assistant:
            return None

        instructions = ai_assistant.get("instructions", [])

        # Filter for GENERAL_CONTEXT and ACTIVE instructions
        valid_instructions = [
            instr
            for instr in instructions
            if (
                instr.get("type") == "GENERAL_CONTEXT"
                and instr.get("state") == "ACTIVE"
            )
        ]

        if not valid_instructions:
            return None

        # Use the last item from the array as the most recent instruction
        latest_instruction = valid_instructions[-1]
        instruction_text = latest_instruction.get("instruction")

        if instruction_text and instruction_text.strip():
            return instruction_text.strip()
        else:
            return None

    except Exception as e:
        # Failed to fetch AI instructions from GraphQL.
        # This is typically because the GMS instance doesn't have the aiAssistant
        # field in GlobalSettings yet (feature not deployed to this instance).
        # We log a warning and return None (no extra instructions) rather than
        # crashing the chat session.
        #
        # NOTE: This catches ALL exceptions, which means we might accidentally
        # suppress other GraphQL errors. This is a temporary solution until all
        # GMS instances are upgraded to support AI assistant settings (expected
        # within a couple of months). After that, any errors here would be
        # unexpected and should be investigated.
        logger.warning(
            f"Failed to fetch AI assistant instructions from GraphQL: {e}. "
            "This is expected if the GMS instance hasn't been upgraded to support "
            "this feature yet. Proceeding without additional instructions."
        )
        return None


class DataHubSystemPromptBuilder:
    """
    System prompt builder for DataHub ChatSession.

    Builds the DataHub-specific system prompt including:
    - Base DataHub AI assistant prompt
    - Optional extra instructions from GraphQL API
    """

    def __init__(self, extra_instructions_override: Optional[str] = None):
        """
        Initialize DataHub system prompt builder.

        Args:
            extra_instructions_override: Optional override for extra instructions
                                        (skips GraphQL fetch if provided)
        """
        self.extra_instructions_override = extra_instructions_override

    def build_system_messages(
        self, client: DataHubClient
    ) -> List["SystemContentBlockTypeDef"]:
        """Build system messages for DataHub ChatSession."""
        system_messages: List["SystemContentBlockTypeDef"] = [{"text": _SYSTEM_PROMPT}]

        # Use override if provided, otherwise fetch from GraphQL
        extra_instructions = (
            self.extra_instructions_override
            if self.extra_instructions_override is not None
            else get_extra_llm_instructions(client)
        )

        if extra_instructions:
            formatted_instructions = (
                f"CUSTOMER-SPECIFIC REQUIREMENTS - You must follow these in addition to base instructions:\n\n"
                f"{extra_instructions}"
            )
            system_messages.append({"text": formatted_instructions})

        return system_messages


class FilteredProgressListener:
    # Not super happy with the naming of this. But the purpose is to
    # 1. encapsulate the history -> progress message logic
    # 2. ensure that the progress callback is only called when things change
    def __init__(
        self,
        history: ChatHistory,
        progress_callback: Optional[ProgressCallback],
        agent: Optional[AgentRunner] = None,
        start_offset: int = 0,
    ):
        self.history = history
        self.progress_callback = progress_callback
        self.agent = agent
        self.start_offset = start_offset

        self._last_progress_updates: Optional[List[ProgressUpdate]] = None

    @classmethod
    def _sanitize_progress_step(cls, step: str) -> str:
        """Replace trailing colon (with optional whitespace) with a period"""
        return re.sub(r":\s*$", ".", step).strip()

    @classmethod
    def get_progress_updates(
        cls,
        history: ChatHistory,
        *,
        start_offset: int,
        agent: Optional[AgentRunner] = None,
    ) -> List[ProgressUpdate]:
        """Get current progress updates derived from chat history with type information"""
        updates = []

        for message in history.messages[start_offset:]:
            # Determine message type
            message_type: Literal["THINKING", "TOOL_CALL", "TOOL_RESULT", "TEXT"]

            if isinstance(message, ReasoningMessage):
                message_type = "THINKING"
                # Parse the reasoning message to extract user-friendly text
                parsed = parse_reasoning_message(message.text)
                user_visible_text = parsed.to_user_visible_message(session=agent)

                # Sanitize and truncate progress messages
                # Max 1000 chars per step: generous buffer since parsed messages are
                # typically 50-200 chars. Even with 10 steps (10K chars total), this
                # stays well within Slack's 3K recommended limit and Teams' 28KB limit.
                sanitized_text = cls._sanitize_progress_step(user_visible_text)
                text = truncate(sanitized_text, max_length=1000)

                updates.append(ProgressUpdate(text=text, message_type=message_type))

            elif isinstance(message, ToolCallRequest):
                message_type = "TOOL_CALL"
                # Could add tool call details here if needed
                # For now
                # updates.append(ProgressUpdate(text=f"Calling tool: {message.tool_name}", message_type=message_type))
                pass

            elif isinstance(message, (ToolResult, ToolResultError)):
                message_type = "TOOL_RESULT"
                # Could add tool result details here if needed
                # For now, skip
                # updates.append(ProgressUpdate(text="Tool completed", message_type=message_type))
                pass

        return updates

    def _handle_history_updated(self) -> None:
        current_updates = self.get_progress_updates(
            self.history, start_offset=self.start_offset, agent=self.agent
        )
        if current_updates != self._last_progress_updates:
            self._last_progress_updates = current_updates
            if self.progress_callback:
                self.progress_callback(current_updates)


class ChatSession:
    """
    DataHub-specific chat session that uses AgentRunner infrastructure.

    This class maintains backward compatibility with existing code while
    using the new composable agent infrastructure internally.
    """

    def __init__(
        self,
        tools: Sequence[ToolWrapper | FastMCP],
        client: DataHubClient,
        history: Optional[ChatHistory] = None,
        extra_instructions_override: Optional[str] = None,
        chat_type: ChatType = ChatType.DEFAULT,
        # Custom context reducers can be supported in future
    ):
        """
        Initialize ChatSession with DataHub-specific configuration.

        Args:
            tools: Base tools to provide to the agent (typically [mcp])
            client: DataHub client for tool execution and GraphQL queries
            history: Optional existing chat history to continue from
            extra_instructions_override: Optional override for extra instructions
            chat_type: Type of chat (UI, Slack, Teams, etc.)
        """
        # Store ChatSession-specific attributes
        self.client = client
        self.extra_instructions_override = extra_instructions_override
        self.chat_type = chat_type

        # Prepare plannable tools (public tools from MCP)
        plannable_tools: List[ToolWrapper] = [
            tool
            for entry in tools
            for tool in (
                tools_from_fastmcp(entry) if isinstance(entry, FastMCP) else [entry]
            )
        ]

        # Add smart_search if enabled (BEFORE storing to _plannable_tools)
        if _is_smart_search_enabled():
            plannable_tools.append(
                ToolWrapper.from_function(
                    fn=async_background(smart_search),
                    name="smart_search",
                    description=smart_search.__doc__
                    or "Smart search with AI reranking",
                )
            )

        # Store for get_plannable_tools() - includes smart_search if enabled
        self._plannable_tools = plannable_tools.copy()

        # Note: Internal tools will be created by AgentRunner after initialization
        # We'll add them via a post-init step since they need the runner instance
        internal_tools: List[ToolWrapper] = []

        # Combine all tools
        all_tools = plannable_tools + internal_tools

        # Create agent configuration
        config = AgentConfig(
            model_id=model_config.chat_assistant_ai.model,
            system_prompt_builder=DataHubSystemPromptBuilder(
                extra_instructions_override
            ),
            tools=all_tools,
            plannable_tools=plannable_tools,  # Subset for planning (excludes internal)
            context_reducers=None,  # Will use defaults
            conversational_parser=XmlReasoningParser(),  # DataHub's XML reasoning format
            use_prompt_caching=True,
            max_tool_calls=MAX_TOOL_CALLS,
            temperature=0.5,
            max_tokens=4096,
            agent_name="DataHub ChatSession",
        )

        # Create the underlying agent runner
        self._agent_runner = AgentRunner(
            config=config,
            client=client,
            history=history,
        )

        # Add internal tools after AgentRunner is created (they need the runner instance)
        internal_tools_to_add = _get_internal_chatbot_tools(self._agent_runner)
        self._agent_runner.tools.extend(internal_tools_to_add)

        # Create a dummy progress listener to start with (for backward compat)
        self._progress_listener = FilteredProgressListener(
            history=self._agent_runner.history,
            progress_callback=None,
            agent=self._agent_runner,
        )

    # Properties that delegate to AgentRunner
    @property
    def session_id(self) -> str:
        """Get session identifier from underlying agent runner."""
        return self._agent_runner.session_id

    @property
    def history(self) -> ChatHistory:
        """Get chat history from underlying agent runner."""
        return self._agent_runner.history

    @history.setter
    def history(self, value: ChatHistory) -> None:
        """Set chat history on underlying agent runner."""
        self._agent_runner.history = value
        # Update progress listener to track new history
        self._progress_listener = FilteredProgressListener(
            history=self._agent_runner.history,
            progress_callback=None,
            agent=self._agent_runner,
        )

    @property
    def tools(self) -> List[ToolWrapper]:
        """Get all tools (plannable + internal) from underlying agent runner."""
        return self._agent_runner.tools

    @property
    def tool_map(self) -> Dict[str, ToolWrapper]:
        """Get mapping of tool names to tool instances."""
        return self._agent_runner.tool_map

    @property
    def context_reducers(self) -> Iterable[ChatContextReducer]:
        """Get context reducers from underlying agent runner."""
        return self._agent_runner.context_reducers

    @property
    def plan_cache(self) -> Dict[str, Dict[str, Any]]:
        """Get plan cache from underlying agent runner (backward compatibility)."""
        return self._agent_runner.plan_cache

    # ChatSession-specific methods
    def get_plannable_tools(self) -> List[ToolWrapper]:
        """
        Get tools that can be used in execution plans.

        Delegates to AgentRunner which stores the plannable tools
        configured at initialization.

        Returns:
            List of ToolWrapper objects suitable for planning
        """
        return self._agent_runner.get_plannable_tools()

    def _get_tools_config(self) -> dict:
        """Get tools configuration for token estimation (backward compat)."""
        return {
            "tools": [tool.to_bedrock_spec() for tool in self.tools],
        }

    def _get_model_id(self) -> str:
        """Get model ID (backward compat)."""
        return model_config.chat_assistant_ai.model

    @classmethod
    def is_respond_to_user(cls, message: Message) -> TypeGuard[ToolResult]:
        return (
            isinstance(message, ToolResult)
            and message.tool_request.tool_name == _respond_to_user_tool.name
        )

    def _add_message(self, message: Message) -> None:
        """
        Add a message to history (backward compatibility method).

        This delegates to the underlying AgentRunner's _add_message method.
        Kept for backward compatibility with existing code that calls this directly.

        Args:
            message: Message to add to history
        """
        self._agent_runner._add_message(message)

    @contextlib.contextmanager
    def set_progress_callback(
        self, progress_callback: ProgressCallback
    ) -> Iterator[None]:
        """
        Set a callback for progress updates during generation.

        This delegates to the underlying AgentRunner while maintaining
        the FilteredProgressListener for backward compatibility.
        """
        prev_progress_listener = self._progress_listener
        self._progress_listener = FilteredProgressListener(
            history=self.history,
            progress_callback=progress_callback,
            agent=self._agent_runner,
            start_offset=len(self.history.messages),
        )
        try:
            # Also set on the agent runner
            with self._agent_runner.set_progress_callback(progress_callback):
                yield
        finally:
            self._progress_listener = prev_progress_listener

    def generate_next_message(self) -> NextMessage:
        """
        Generate the next message via agentic loop.

        This delegates to AgentRunner for the core agentic loop while
        maintaining ChatSession-specific logic for respond_to_user detection
        and chat_type formatting.

        Returns:
            NextMessage with formatted text and optional suggestions
        """

        # Define completion check for DataHub ChatSession
        def is_complete(message: Message) -> bool:
            # Complete if we got respond_to_user or direct AssistantMessage
            return self.is_respond_to_user(message) or isinstance(
                message, AssistantMessage
            )

        # Delegate to AgentRunner with custom completion check
        last_message = self._agent_runner.generate_next_message(
            completion_check=is_complete
        )

        # Handle respond_to_user tool result
        if self.is_respond_to_user(last_message):
            logger.info(f"Respond to user call received for session {self.session_id}")
            return NextMessage.model_validate(last_message.result)

        # Handle direct AssistantMessage (fallback case)
        elif isinstance(last_message, AssistantMessage):
            logger.info(f"End turn message received for session {self.session_id}")
            formatted_text = format_message(last_message.text, self.chat_type)
            return NextMessage(
                text=formatted_text,
                suggestions=[],
            )

        # Should not reach here due to completion check
        raise ChatSessionError(f"Unexpected message type: {type(last_message)}")


if __name__ == "__main__":
    from pprint import pprint as print

    chat = ChatSession(
        tools=[mcp],
        client=DataHubClient.from_env(),
        history=ChatHistory(
            messages=[
                HumanMessage(text="What datasets should I look at for pet profiles?")
            ]
        ),
    )
    response = chat.generate_next_message()
    print(response)
