"""
System prompts and prompt builders for the DataCatalog Explorer agent.

This module contains the DataHub-specific system prompts and the builder
that fetches dynamic instructions from the GraphQL API.
"""

from enum import Enum
from typing import TYPE_CHECKING, List, Optional, Sequence

import cachetools
from loguru import logger

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient
    from mypy_boto3_bedrock_runtime.type_defs import SystemContentBlockTypeDef

# Feature flag for planning tools
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.mcp_integration.external_mcp_manager import (
    ExternalToolWrapper,
)
from datahub_integrations.mcp_integration.tool import Tool

PLANNING_TOOLS_ENABLED = model_config.chat_assistant_ai.planning_mode_enabled


class PlanningMode(str, Enum):
    """Planning mode for the DataCatalog Explorer agent."""

    STRICT = "strict"
    """Planning is required - MUST call create_plan as the FIRST tool for every user question."""

    AUTO = "auto"
    """Planning is recommended for complex tasks - SHOULD use create_plan for SQL generation or 3+ tool call tasks."""

    DISABLED = "disabled"
    """Planning is disabled - no planning tools available."""


# Message length limits
MESSAGE_LENGTH_SOFT_LIMIT = 1500
MESSAGE_LENGTH_HARD_LIMIT = 3000 - 100  # 100 is a buffer
_MAX_SUGGESTIONS = 4


def get_tool_instructions(
    tools: Sequence[Tool],
) -> List[str]:
    """Get unique prefixed instructions from a list of tools.

    External MCP tools (like GitHub) can provide server-specific instructions
    that guide the LLM on how to use them correctly (e.g., "always include
    repo:owner/repo in search queries").

    Instructions are prefixed with the tool's prefix (e.g., "[github]") to make
    it clear which plugin each instruction applies to when multiple plugins
    are configured.

    Args:
        tools: List of ToolWrapper (internal MCP) or ExternalToolWrapper (external MCP)

    Returns:
        List of unique instruction strings from tools that have them, prefixed
        with the tool prefix. Only ExternalToolWrapper has instructions.
    """
    seen: set[str] = set()
    instructions: List[str] = []
    for tool in tools:
        # Only ExternalToolWrapper has instructions
        if not isinstance(tool, ExternalToolWrapper):
            continue
        if tool.instructions and tool.instructions not in seen:
            seen.add(tool.instructions)
            # Add prefix for clarity when multiple plugins are configured
            if tool.tool_prefix:
                instructions.append(f"[{tool.tool_prefix}] {tool.instructions}")
            else:
                instructions.append(tool.instructions)
    return instructions


def _get_system_prompt(planning_mode: PlanningMode) -> str:
    """
    Generate the system prompt based on the planning mode.

    Args:
        planning_mode: The planning mode (STRICT, AUTO, or DISABLED)

    Returns:
        System prompt string with optional planning instructions
    """
    planning_enabled = planning_mode != PlanningMode.DISABLED

    return f"""\
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
DataHub AI will not make more than 20 tool calls in a single response.

{
        {
            PlanningMode.STRICT: "DataHub AI MUST call create_plan as the FIRST tool for every user question.",
            PlanningMode.AUTO: (
                "DataHub AI SHOULD use create_plan for any SQL generation or complex tasks that require "
                "3 or more tool calls, especially for impact analysis, dependency analysis, or tasks "
                "requiring iterative refinement. Simple 1-2 tool call tasks can be executed directly "
                "without planning."
            ),
            PlanningMode.DISABLED: "",
        }.get(planning_mode, "")
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
        if planning_enabled
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
        if planning_enabled
        else ""
    }For tool calls where entity matching is not relevant (e.g., initial searches), you can omit the matching fields.
The plan fields are OPTIONAL and should only be included when you are executing a multi-step plan created by the create_plan tool.

CRITICAL: For fully-qualified entity names (database.schema.table format), the database, schema, AND table name must ALL match exactly. If even one part differs, it is a DIFFERENT entity, not the same entity.
- State only FACTUAL INFORMATION. Do not try to infer connection without proof, do not make assumptions.


DataHub AI is now being connected with a person."""


@cachetools.cached(cache=cachetools.TTLCache(maxsize=1, ttl=60 * 5))
def get_extra_llm_instructions(client: "DataHubClient") -> Optional[str]:
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
    - Optional conversation context
    - Optional extra instructions from GraphQL API
    """

    def __init__(
        self,
        extra_instructions_override: Optional[str] = None,
        context: Optional[str] = None,
        planning_mode: PlanningMode = PlanningMode.DISABLED,
        tools: Optional[Sequence] = None,
    ):
        """
        Initialize DataHub system prompt builder.

        Args:
            extra_instructions_override: Optional override for extra instructions
                                        (skips GraphQL fetch if provided)
            context: Optional natural language context about what the user is working on
            planning_mode: Planning mode (STRICT, AUTO, or DISABLED)
            tools: Optional sequence of tools to extract plugin-specific instructions from
        """
        self.planning_mode = planning_mode
        self.extra_instructions_override = extra_instructions_override
        self.context = context
        self.tools = tools or []

    def build_system_messages(
        self, client: "DataHubClient"
    ) -> List["SystemContentBlockTypeDef"]:
        """Build system messages for DataHub ChatSession."""

        system_messages: List["SystemContentBlockTypeDef"] = [
            {"text": _get_system_prompt(self.planning_mode)}
        ]

        # Add context if provided
        if self.context:
            context_message = (
                f"The following context is provided from our UI in order to give information about what the user is doing or seeing when they send a message:\n\n{self.context}\n\n"
                f"Use this context to better understand what the user is working on and provide more relevant assistance when formulating a response."
            )
            system_messages.append({"text": context_message})

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

        # Add plugin-specific instructions from external MCP servers
        if self.tools:
            plugin_instructions = get_tool_instructions(self.tools)
            if plugin_instructions:
                formatted_plugin_instructions = "\n\n".join(plugin_instructions)
                system_messages.append(
                    {
                        "text": f"TOOL-SPECIFIC INSTRUCTIONS:\n\n{formatted_plugin_instructions}"
                    }
                )

        return system_messages
