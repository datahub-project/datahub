"""
Example: Schema Comparison Agent

This module demonstrates how to build a specialized subagent using the
reusable agent infrastructure. This agent focuses on comparing dataset schemas
and identifying differences.

Key features demonstrated:
- Custom system prompt via SystemPromptBuilder
- Specialized internal tools
- Reusing existing MCP tools
- Configuring inference parameters for the task
"""

from typing import Any, Dict, List, Optional

from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentRunner,
    StaticPromptBuilder,
    flatten_tools,
)
from datahub_integrations.chat.chat_history import ChatHistory, HumanMessage
from datahub_integrations.mcp.mcp_server import mcp
from datahub_integrations.mcp_integration.tool import ToolWrapper

# System prompt for the schema comparison agent
SCHEMA_COMPARISON_PROMPT = """
You are a Schema Comparison Specialist for DataHub.

Your expertise is comparing dataset schemas to identify:
- Field additions and removals
- Type changes and incompatibilities
- Column renames and migrations
- Breaking vs non-breaking changes

Always present schema differences in a structured format with:
1. Summary of changes (added/removed/modified fields)
2. Impact assessment (breaking vs non-breaking)
3. Recommended migration strategy

Be concise and focus on actionable insights. Use technical language appropriate
for data engineers.

When comparing schemas:
- First fetch both schemas using datahub__get_dataset
- Analyze field-by-field differences
- Categorize changes by severity
- Use report_schema_differences to present findings
"""


def report_schema_differences(
    dataset_a_urn: str,
    dataset_b_urn: str,
    added_fields: List[str],
    removed_fields: List[str],
    modified_fields: List[Dict[str, Any]],
    breaking_changes: bool,
) -> Dict[str, Any]:
    """
    Report schema differences between two datasets.

    This is a specialized tool that formats schema comparison results
    in a structured way for the Schema Comparison Agent.

    Args:
        dataset_a_urn: URN of the first dataset
        dataset_b_urn: URN of the second dataset
        added_fields: List of field names added in dataset B
        removed_fields: List of field names removed from dataset A
        modified_fields: List of field modifications with details
        breaking_changes: Whether any changes are breaking

    Returns:
        Structured report of schema differences
    """
    # Generate recommendations based on changes with explicit type annotation
    recommendations: List[str] = []
    if removed_fields:
        recommendations.append(
            "Review downstream queries/dashboards that reference removed fields"
        )
    if breaking_changes:
        recommendations.append("Plan staged rollout with backward compatibility period")
    if modified_fields:
        recommendations.append(
            "Update data contracts and documentation to reflect type changes"
        )

    summary = {
        "comparison": {
            "dataset_a": dataset_a_urn,
            "dataset_b": dataset_b_urn,
        },
        "changes": {
            "added_fields": added_fields,
            "removed_fields": removed_fields,
            "modified_fields": modified_fields,
        },
        "impact": {
            "breaking_changes": breaking_changes,
            "severity": "HIGH"
            if breaking_changes
            else "MEDIUM"
            if modified_fields
            else "LOW",
        },
        "recommendations": recommendations,
    }

    return summary


def respond_with_schema_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    Final response tool for schema comparison agent.

    Formats the schema comparison report as a user-friendly message.
    """
    summary = report

    # Format the response
    response = f"""## Schema Comparison Results

**Comparing:**
- Dataset A: {summary["comparison"]["dataset_a"]}
- Dataset B: {summary["comparison"]["dataset_b"]}

**Changes:**
- Added Fields: {len(summary["changes"]["added_fields"])}
- Removed Fields: {len(summary["changes"]["removed_fields"])}
- Modified Fields: {len(summary["changes"]["modified_fields"])}

**Impact:** {summary["impact"]["severity"]}
{"⚠️ **Breaking changes detected**" if summary["impact"]["breaking_changes"] else ""}

**Recommendations:**
"""
    for rec in summary["recommendations"]:
        response += f"- {rec}\n"

    return {"text": response, "suggestions": []}


def create_schema_comparison_agent(
    client: DataHubClient, history: Optional[ChatHistory] = None
) -> AgentRunner:
    """
    Create a schema comparison agent.

    This agent specializes in comparing dataset schemas and identifying
    differences. It demonstrates how to build a focused subagent with:
    - Custom system prompt
    - Specialized internal tools
    - Reuse of existing MCP tools
    - Task-appropriate inference parameters

    Args:
        client: DataHub client for tool execution
        history: Optional existing chat history

    Returns:
        AgentRunner configured for schema comparison

    Example:
        ```python
        agent = create_schema_comparison_agent(client)
        agent.history.add_message(
            HumanMessage(text="Compare schemas of prod.users and staging.users")
        )
        response = agent.generate_next_message()
        print(response.text)
        ```
    """

    # Prepare tools
    plannable_tools = flatten_tools([mcp])  # Reuse DataHub MCP tools
    plannable_tools.append(
        ToolWrapper.from_function(
            fn=report_schema_differences,
            name="report_schema_differences",
            description="Report structured schema differences between two datasets",
        )
    )

    internal_tools = [
        ToolWrapper.from_function(
            fn=respond_with_schema_report,
            name="respond_to_user",
            description="Send final schema comparison report to user",
        ),
    ]

    all_tools = plannable_tools + internal_tools

    # Create agent configuration
    config = AgentConfig(
        model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
        system_prompt_builder=StaticPromptBuilder(SCHEMA_COMPARISON_PROMPT),
        tools=all_tools,
        plannable_tools=plannable_tools,  # Excludes respond_to_user
        temperature=0.3,  # Lower temperature for consistent analysis
        max_tool_calls=15,  # Fewer calls needed for focused task
        agent_name="Schema Comparison Agent",
        agent_description="Specialized agent for comparing dataset schemas",
    )

    return AgentRunner(config=config, client=client, history=history)


# Example usage
if __name__ == "__main__":
    from datahub.sdk.main_client import DataHubClient

    # Create client
    client = DataHubClient.from_env()

    # Create agent
    agent = create_schema_comparison_agent(client)

    # Add user query
    agent.history.add_message(
        HumanMessage(
            text="Compare the schemas of urn:li:dataset:(urn:li:dataPlatform:hive,prod.users,PROD) "
            "and urn:li:dataset:(urn:li:dataPlatform:hive,staging.users,PROD)"
        )
    )

    # Generate response
    response = agent.generate_next_message()
    print(f"Agent: {response}")
