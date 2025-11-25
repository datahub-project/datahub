"""
Example: Ingestion Troubleshooting Agent

This module demonstrates how to build a proactive agent that automatically explores
and diagnoses DataHub ingestion issues. This is an advanced example showing:

1. **Proactive behavior**: Agent takes initiative to investigate issues
2. **Multi-step diagnosis**: Combines multiple tools to reach conclusions
3. **Placeholder tools**: Shows how to design tools before implementation
4. **Helpful guide personality**: Explains findings clearly to users

Key features demonstrated:
- Custom system prompt for troubleshooting domain
- Mix of existing MCP tools + new placeholder tools
- Proactive multi-tool orchestration
- Clear user communication

ARCHITECTURE:
- Uses AgentRunner infrastructure (reusable agentic loop)
- Configured via AgentConfig composition
- Public tools: MCP search/get + ingestion-specific placeholders
- Internal tools: respond_to_user with troubleshooting context
"""

import os

from datahub_integrations.gen_ai.model_config import BedrockModel

# Set environment variables before imports to avoid external connections
# This allows the example to run standalone

# Avoid connecting to DataHub at import time
if "DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL" not in os.environ:
    os.environ["DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL"] = "https://www.example.com"


from typing import Any, Dict, List, Optional

from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentRunner,
    StaticPromptBuilder,
    flatten_tools,
)
from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.mcp.mcp_server import mcp, register_all_tools
from datahub_integrations.mcp_integration.tool import ToolWrapper

# Register MCP tools (thread-safe, idempotent)
register_all_tools(is_oss=False)

# ============================================================================
# SYSTEM PROMPT
# ============================================================================

INGESTION_TROUBLESHOOTING_PROMPT = """
You are a DataHub Ingestion Troubleshooting Specialist.

Your mission is to help users diagnose and resolve data ingestion issues in DataHub.
You are a helpful guide who explains findings clearly and suggests actionable fixes.

PROACTIVE BEHAVIOR:
When a user reports an issue (e.g., "my table isn't showing up"), you should:
1. 🔍 **Investigate automatically** - don't just ask questions, use tools to gather evidence
2. 🧩 **Check multiple angles** - search for entities, check ingestion runs, look for errors
3. 💡 **Explain findings** - clearly state what you found and what it means
4. 🛠️ **Suggest fixes** - provide actionable next steps

COMMON TROUBLESHOOTING PATTERNS:

1. **Missing Entity**:
   - Search for similar entities (check for typos, case differences, schema variations)
   - Find relevant ingestion sources (by platform/type)
   - Check recent run history (failures, last success time)
   - Examine error logs from failed runs
   - Check if entity exists in source system (if possible)

2. **Stale Metadata**:
   - Check entity's last modified timestamp
   - Find the ingestion source responsible
   - Check run frequency/schedule
   - Verify recent runs succeeded

3. **Failed Ingestion**:
   - Get recent execution requests
   - Parse error logs for root cause
   - Identify common failure patterns (auth, permissions, config, connectivity)
   - Check structured report for entity-level errors

4. **Schema Differences**:
   - Compare current schema with expected
   - Check when schema was last updated
   - Look for schema evolution in run logs

COMMUNICATION STYLE:
- Be conversational and friendly, not robotic
- Explain technical details simply
- Use bullet points for clarity
- Always provide next steps
- If you find multiple possible causes, list them in order of likelihood

IMPORTANT:
- Use multiple tools in sequence to build a complete picture
- Don't jump to conclusions - gather evidence first
- Clearly distinguish between what you found vs what you're inferring
- If something is unclear, say so and explain what additional info is needed

You have access to:
- Existing MCP tools: search, get_entities, list_schema_fields
- Ingestion-specific tools: get_ingestion_runs, get_run_logs, diagnose_missing_entity, check_entity_freshness
"""


# ============================================================================
# PLACEHOLDER INGESTION TOOLS
# These demonstrate the tool design without full implementation
# ============================================================================


def get_ingestion_runs(
    source_urn: str,
    count: int = 10,
    status_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get recent ingestion run history for a specific ingestion source.

    This tool helps track ingestion execution history and identify patterns
    in failures or performance issues.

    Args:
        source_urn: URN of the ingestion source (e.g., "urn:li:dataHubIngestionSource:abc123")
        count: Number of recent runs to return (default: 10, max: 50)
        status_filter: Optional filter by status - "SUCCESS", "FAILURE", or "RUNNING"

    Returns:
        Dictionary containing:
        - source_urn: The ingestion source URN
        - source_name: Human-readable name of the source
        - source_type: Platform type (e.g., "snowflake", "bigquery")
        - total_runs: Total number of runs in history
        - runs: List of recent runs with:
            - execution_request_urn: URN of the execution request
            - status: SUCCESS, FAILURE, RUNNING, CANCELLED
            - start_time_ms: When the run started (epoch milliseconds)
            - duration_ms: How long it took (null if still running)
            - entities_written: Number of entities successfully written
            - errors_count: Number of errors encountered
            - warnings_count: Number of warnings
        - success_rate: Percentage of successful runs in this set
        - average_duration_ms: Average runtime for successful runs

    TODO: Implement with GraphQL query:
    ```graphql
    query GetIngestionRuns($urn: String!, $start: Int!, $count: Int!) {
      ingestionSource(urn: $urn) {
        urn
        name
        type
        executions(start: $start, count: $count) {
          total
          executionRequests {
            urn
            result {
              status
              startTimeMs
              durationMs
              structuredReport {
                serializedValue  # JSON with entity counts, errors
                contentType
              }
            }
          }
        }
      }
    }
    ```

    Example output:
        get_ingestion_runs("urn:li:dataHubIngestionSource:snowflake_prod")
        # Returns:
        {
            "source_urn": "urn:li:dataHubIngestionSource:snowflake_prod",
            "source_name": "Snowflake Production",
            "source_type": "snowflake",
            "total_runs": 145,
            "runs": [
                {
                    "execution_request_urn": "urn:li:executionRequest:abc123",
                    "status": "FAILURE",
                    "start_time_ms": 1700000000000,
                    "duration_ms": 45000,
                    "entities_written": 0,
                    "errors_count": 1,
                    "warnings_count": 0
                },
                # ... more runs
            ],
            "success_rate": 87.5,
            "average_duration_ms": 120000
        }
    """
    # Placeholder implementation - returns empty data
    # This demonstrates the tool design without requiring GraphQL implementation
    return {
        "source_urn": source_urn,
        "source_name": "Placeholder Source",
        "source_type": "unknown",
        "total_runs": 0,
        "runs": [],
        "success_rate": 0.0,
        "average_duration_ms": 0,
        "_note": "PLACEHOLDER: Implement GraphQL query shown in docstring",
    }


def get_run_logs(execution_request_urn: str) -> Dict[str, Any]:
    """
    Get detailed logs and structured error information from an ingestion run.

    This is essential for understanding WHY an ingestion run failed and
    what specific errors occurred.

    Args:
        execution_request_urn: URN of the execution request (e.g., "urn:li:executionRequest:abc123")

    Returns:
        Dictionary containing:
        - execution_request_urn: The execution request URN
        - status: SUCCESS, FAILURE, RUNNING, CANCELLED
        - start_time_ms: When the run started
        - duration_ms: How long it took
        - raw_logs: Full text logs from the run
        - structured_report: Parsed structured report with:
            - entities_written: Count of successfully written entities
            - entities_read: Count of entities read from source
            - warnings: List of warning messages
            - errors: List of error messages with details
            - failure_reason: Primary reason for failure (if failed)
        - common_errors: Identified common error patterns:
            - "PERMISSION_DENIED": Credential/permission issues
            - "CONNECTION_TIMEOUT": Network/connectivity issues
            - "INVALID_CONFIG": Configuration problems
            - "SCHEMA_MISMATCH": Schema validation errors
            - "RATE_LIMIT": API rate limiting

    TODO: Implement with GraphQL query:
    ```graphql
    query GetExecutionRequestDetails($urn: String!) {
      executionRequest(urn: $urn) {
        urn
        result {
          status
          startTimeMs
          durationMs
          report  # Full text logs
          structuredReport {
            type
            serializedValue  # JSON string to parse
            contentType
          }
        }
        input {
          arguments {
            key
            value
          }
        }
      }
    }
    ```

    Then parse structuredReport.serializedValue (JSON) to extract:
    - entities_written, entities_read (from ingestion report)
    - warnings[], failures[] (from ingestion report)
    - Classify errors into common patterns

    Example output:
        get_run_logs("urn:li:executionRequest:abc123")
        # Returns:
        {
            "execution_request_urn": "urn:li:executionRequest:abc123",
            "status": "FAILURE",
            "start_time_ms": 1700000000000,
            "duration_ms": 45000,
            "raw_logs": "...",
            "structured_report": {
                "entities_written": 0,
                "entities_read": 150,
                "warnings": ["Schema field 'deprecated_col' ignored"],
                "errors": ["Permission denied on schema 'prod'"],
                "failure_reason": "Permission denied accessing source database"
            },
            "common_errors": ["PERMISSION_DENIED"]
        }
    """
    # Placeholder implementation
    return {
        "execution_request_urn": execution_request_urn,
        "status": "UNKNOWN",
        "start_time_ms": 0,
        "duration_ms": 0,
        "raw_logs": "Placeholder logs - no real data available",
        "structured_report": {
            "entities_written": 0,
            "entities_read": 0,
            "warnings": [],
            "errors": [],
            "failure_reason": None,
        },
        "common_errors": [],
        "_note": "PLACEHOLDER: Implement GraphQL query shown in docstring",
    }


def diagnose_missing_entity(
    expected_qualified_name: str,
    platform: str,
    env: str = "PROD",
) -> Dict[str, Any]:
    """
    Automatically diagnose why an expected entity is missing from DataHub.

    This is a higher-level diagnostic tool that orchestrates multiple checks
    to identify the most likely cause of a missing entity issue.

    The tool performs these checks:
    1. Search for entities with similar names (fuzzy matching)
    2. Search for the ingestion source that should ingest this entity
    3. Get recent run history for that source
    4. Check for relevant errors in failed runs
    5. Check if entity was soft-deleted

    Args:
        expected_qualified_name: The qualified name of the expected entity
                                (e.g., "prod.sales.orders", "my_schema.my_table")
        platform: The data platform (e.g., "snowflake", "bigquery", "postgres")
        env: Environment (default: "PROD")

    Returns:
        Dictionary containing:
        - expected_entity: What was expected (qualified name, platform, env)
        - entity_exists: Boolean - was exact entity found?
        - similar_entities: List of entities with similar names, each with:
            - urn: Entity URN
            - qualified_name: The qualified name
            - similarity_score: How similar (0.0-1.0)
            - reason: Why it's similar (e.g., "Same name, different schema")
        - relevant_sources: Ingestion sources that might ingest this entity:
            - source_urn: Ingestion source URN
            - source_name: Human-readable name
            - source_type: Platform type
            - last_run_status: Status of most recent run
            - last_run_time_ms: When last run occurred
        - recent_errors: Errors from recent runs that might explain the issue:
            - error_message: The error text
            - error_type: Classified error type
            - execution_request_urn: Which run had this error
        - diagnosis: Overall assessment with:
            - likely_cause: Most probable reason (string)
            - confidence: HIGH, MEDIUM, or LOW
            - evidence: List of supporting evidence
            - recommendations: List of suggested fixes
        - soft_deleted: Boolean - was entity previously ingested but deleted?

    TODO: Implement by combining:
    1. Use search() MCP tool to find similar entities
    2. Call get_ingestion_runs() for relevant sources
    3. Call get_run_logs() to examine errors
    4. Use get_entities() to check for soft-deleted entities
    5. Apply heuristics to classify likely cause

    Example output:
        diagnose_missing_entity("prod.sales.orders", "snowflake")
        # Returns:
        {
            "expected_entity": {
                "qualified_name": "prod.sales.orders",
                "platform": "snowflake",
                "env": "PROD"
            },
            "entity_exists": False,
            "similar_entities": [
                {
                    "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.order,PROD)",
                    "qualified_name": "prod.sales.order",
                    "similarity_score": 0.95,
                    "reason": "Same database and schema, singular vs plural table name"
                }
            ],
            "relevant_sources": [
                {
                    "source_urn": "urn:li:dataHubIngestionSource:snowflake_prod",
                    "source_name": "Snowflake Production",
                    "source_type": "snowflake",
                    "last_run_status": "FAILURE",
                    "last_run_time_ms": 1700000000000
                }
            ],
            "recent_errors": [
                {
                    "error_message": "Permission denied on schema 'prod'",
                    "error_type": "PERMISSION_DENIED",
                    "execution_request_urn": "urn:li:executionRequest:abc123"
                }
            ],
            "diagnosis": {
                "likely_cause": "Permission denied on source database schema",
                "confidence": "HIGH",
                "evidence": [
                    "Recent ingestion runs failed with permission errors",
                    "Last successful run was 2 days ago",
                    "Found similar entity with singular name"
                ],
                "recommendations": [
                    "Grant SELECT permissions on schema 'prod' to DataHub service account",
                    "Verify the table name (found 'order' instead of 'orders')",
                    "Check ingestion source configuration for schema inclusion patterns"
                ]
            },
            "soft_deleted": False
        }
    """
    # Placeholder implementation - demonstrates the response structure
    # A real implementation would combine multiple GraphQL queries and MCP tool calls
    return {
        "expected_entity": {
            "qualified_name": expected_qualified_name,
            "platform": platform,
            "env": env,
        },
        "entity_exists": False,
        "similar_entities": [],
        "relevant_sources": [],
        "recent_errors": [],
        "diagnosis": {
            "likely_cause": "Unable to diagnose - tool not yet implemented",
            "confidence": "LOW",
            "evidence": [],
            "recommendations": [
                "Check if the entity exists in the source system",
                "Verify ingestion source is configured for this database/schema",
                "Check recent ingestion run logs for errors",
            ],
        },
        "soft_deleted": False,
        "_note": "PLACEHOLDER: Implement multi-step diagnosis as described in docstring",
    }


def check_entity_freshness(urn: str) -> Dict[str, Any]:
    """
    Check when an entity was last updated by ingestion.

    This helps identify stale metadata issues where ingestion hasn't run
    recently or hasn't picked up changes from the source system.

    Args:
        urn: URN of the entity to check

    Returns:
        Dictionary containing:
        - entity_urn: The entity URN
        - entity_name: Human-readable name
        - last_modified_ms: Timestamp when entity was last modified (epoch milliseconds)
        - hours_since_update: Hours since last modification
        - is_stale: Boolean - true if not updated in 24+ hours
        - ingestion_source: The source responsible for this entity:
            - source_urn: Ingestion source URN
            - source_name: Source name
            - last_run_time_ms: When source last ran
        - freshness_assessment: Overall assessment:
            - status: "FRESH", "AGING", or "STALE"
            - message: Human-readable explanation
            - recommendation: What to do if stale

    TODO: Implement using:
    1. get_entities(urn) to fetch lastModified from aspects
    2. ingestionSourceForEntity(urn) GraphQL query to find responsible source
    3. Calculate staleness based on entity type and update patterns

    GraphQL query needed:
    ```graphql
    query GetEntityFreshness($urn: String!) {
      # Get the entity
      # (use MCP get_entities tool for this)

      # Get the ingestion source for this entity
      ingestionSourceForEntity(urn: $urn) {
        urn
        name
        type
        latestSuccessfulExecution {
          result {
            startTimeMs
          }
        }
      }
    }
    ```

    Example output:
        check_entity_freshness("urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)")
        # Returns:
        {
            "entity_urn": "urn:li:dataset:(...)",
            "entity_name": "prod.sales.orders",
            "last_modified_ms": 1699000000000,
            "hours_since_update": 48,
            "is_stale": True,
            "ingestion_source": {
                "source_urn": "urn:li:dataHubIngestionSource:snowflake_prod",
                "source_name": "Snowflake Production",
                "last_run_time_ms": 1699900000000
            },
            "freshness_assessment": {
                "status": "STALE",
                "message": "Entity hasn't been updated in 48 hours. Ingestion source ran recently but didn't update this entity.",
                "recommendation": "Check if table still exists in source system or if ingestion filters exclude it"
            }
        }
    """
    # Placeholder implementation
    return {
        "entity_urn": urn,
        "entity_name": "unknown",
        "last_modified_ms": None,
        "hours_since_update": None,
        "is_stale": None,
        "ingestion_source": None,
        "freshness_assessment": {
            "status": "UNKNOWN",
            "message": "Unable to check freshness - tool not yet implemented",
            "recommendation": "Manually check entity's last modified timestamp in DataHub UI",
        },
        "_note": "PLACEHOLDER: Implement using get_entities + ingestionSourceForEntity GraphQL",
    }


def find_ingestion_sources(
    platform: Optional[str] = None,
    name_pattern: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Find ingestion sources matching criteria.

    Helps identify which ingestion source(s) should be ingesting specific entities.

    Args:
        platform: Filter by platform type (e.g., "snowflake", "bigquery")
        name_pattern: Filter by name pattern (substring match)

    Returns:
        Dictionary containing:
        - total: Total number of matching sources
        - sources: List of matching ingestion sources:
            - urn: Source URN
            - name: Human-readable name
            - type: Platform type
            - schedule: Cron schedule if scheduled (or null)
            - last_run_status: Status of most recent run
            - last_run_time_ms: When last run occurred
            - last_success_time_ms: When last successful run occurred
            - config_summary: Key config details (database names, schemas, etc.)

    TODO: Implement with GraphQL query:
    ```graphql
    query ListIngestionSources($start: Int!, $count: Int!, $query: String) {
      listIngestionSources(
        input: {
          start: $start
          count: $count
          query: $query
        }
      ) {
        total
        ingestionSources {
          urn
          name
          type
          schedule { interval, timezone }
          executions(start: 0, count: 1) {
            executionRequests {
              urn
              result { status, startTimeMs }
            }
          }
          latestSuccessfulExecution {
            result { startTimeMs }
          }
          config {
            recipe  # Parse to extract database/schema patterns
          }
        }
      }
    }
    ```

    Example output:
        find_ingestion_sources(platform="snowflake")
        # Returns:
        {
            "total": 3,
            "sources": [
                {
                    "urn": "urn:li:dataHubIngestionSource:snowflake_prod",
                    "name": "Snowflake Production",
                    "type": "snowflake",
                    "schedule": "0 */6 * * *",  # Every 6 hours
                    "last_run_status": "SUCCESS",
                    "last_run_time_ms": 1700000000000,
                    "last_success_time_ms": 1700000000000,
                    "config_summary": {
                        "database": "PROD_DB",
                        "schema_pattern": "sales.*"
                    }
                },
                # ... more sources
            ]
        }
    """
    # Placeholder implementation
    return {
        "total": 0,
        "sources": [],
        "_note": "PLACEHOLDER: Implement GraphQL listIngestionSources query",
    }


# ============================================================================
# AGENT CONFIGURATION
# ============================================================================


def create_ingestion_troubleshooting_agent(
    client: DataHubClient, history: Optional[ChatHistory] = None
) -> AgentRunner:
    """
    Create an Ingestion Troubleshooting Agent for DataHub.

    This agent specializes in diagnosing and resolving data ingestion issues.
    It demonstrates PROACTIVE behavior where the agent automatically explores
    and investigates issues rather than just answering questions.

    AGENT BEHAVIOR:
    When a user reports an issue like "my table isn't showing up", the agent will:
    1. Search for the entity and similar entities (check for typos)
    2. Find relevant ingestion sources for that platform
    3. Check recent ingestion run history
    4. Examine error logs from failed runs
    5. Synthesize findings into a clear diagnosis with recommended fixes

    TOOLS AVAILABLE:
    - **From MCP** (existing, fully functional):
        - search: Find entities, check if they exist
        - get_entities: Get detailed entity metadata
        - list_schema_fields: Check schema details

    - **Ingestion-specific** (placeholder implementations):
        - get_ingestion_runs: Get run history for a source
        - get_run_logs: Get detailed logs and errors from a run
        - diagnose_missing_entity: Automated multi-step diagnosis
        - check_entity_freshness: Check entity staleness
        - find_ingestion_sources: Find sources by platform/name

    Args:
        client: DataHub client for tool execution
        history: Optional existing chat history

    Returns:
        AgentRunner configured for ingestion troubleshooting

    Example Usage:
        ```python
        # Create the agent
        agent = create_ingestion_troubleshooting_agent(client)

        # User reports an issue
        agent.history.add_message(
            HumanMessage(text="My table snowflake.prod.sales_orders isn't showing up in DataHub")
        )

        # Agent will proactively:
        # 1. Search for similar entities
        # 2. Find Snowflake ingestion sources
        # 3. Check recent run history
        # 4. Examine error logs
        # 5. Provide diagnosis with recommendations

        response = agent.generate_next_message()
        print(response.text)
        ```

    Integration with ChatSessionManager:
        ```python
        # Extend manager to support troubleshooting agent
        class TroubleshootingManager(ChatSessionManager):
            def create_troubleshooting_session(self):
                return create_ingestion_troubleshooting_agent(self.tools_client)

        # Use in your API
        manager = TroubleshootingManager(system_client, tools_client)
        agent = manager.create_troubleshooting_session()
        ```
    """

    # ========================================================================
    # PREPARE TOOLS
    # Combine existing MCP tools + ingestion-specific placeholder tools
    # ========================================================================

    # Start with all MCP tools (search, get_entities, list_schema_fields, etc.)
    public_tools = flatten_tools([mcp])

    # Add ingestion-specific placeholder tools
    public_tools.extend(
        [
            ToolWrapper.from_function(
                fn=get_ingestion_runs,
                name="get_ingestion_runs",
                description=get_ingestion_runs.__doc__ or "Get ingestion run history",
            ),
            ToolWrapper.from_function(
                fn=get_run_logs,
                name="get_run_logs",
                description=get_run_logs.__doc__ or "Get run logs and errors",
            ),
            ToolWrapper.from_function(
                fn=diagnose_missing_entity,
                name="diagnose_missing_entity",
                description=diagnose_missing_entity.__doc__
                or "Diagnose missing entity",
            ),
            ToolWrapper.from_function(
                fn=check_entity_freshness,
                name="check_entity_freshness",
                description=check_entity_freshness.__doc__ or "Check entity freshness",
            ),
            ToolWrapper.from_function(
                fn=find_ingestion_sources,
                name="find_ingestion_sources",
                description=find_ingestion_sources.__doc__ or "Find ingestion sources",
            ),
        ]
    )

    # ========================================================================
    # PREPARE INTERNAL TOOLS
    # Creates respond_to_user with troubleshooting context
    # ========================================================================

    def respond_with_troubleshooting_context(
        diagnosis: str,
        likely_cause: Optional[str] = None,
        recommendations: Optional[List[str]] = None,
        evidence: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Respond to user with troubleshooting findings.

        This is a specialized version of respond_to_user that formats
        troubleshooting information in a clear, actionable way.

        Args:
            diagnosis: Main explanation of the findings
            likely_cause: The most probable root cause (if identified)
            recommendations: List of suggested fixes
            evidence: List of supporting evidence for the diagnosis

        Returns:
            Dictionary with text and suggestions for NextMessage
        """
        response = diagnosis

        # Add likely cause if identified
        if likely_cause:
            response += f"\n\n**Likely Cause**: {likely_cause}"

        # Add evidence section
        if evidence:
            response += "\n\n**Evidence**:"
            for item in evidence:
                response += f"\n- {item}"

        # Add recommendations section
        if recommendations:
            response += "\n\n**Recommended Actions**:"
            for i, rec in enumerate(recommendations, 1):
                response += f"\n{i}. {rec}"

        # Generate follow-up suggestions
        suggestions = []
        if recommendations:
            # Suggest taking the first action
            suggestions.append(f"Help me with: {recommendations[0][:60]}...")
        suggestions.extend(
            [
                "Check another entity",
                "Show me recent ingestion failures",
            ]
        )

        return {"text": response, "suggestions": suggestions[:4]}

    internal_tools = [
        ToolWrapper.from_function(
            fn=respond_with_troubleshooting_context,
            name="respond_to_user",
            description="Respond to user with troubleshooting diagnosis and recommendations",
        )
    ]

    # ========================================================================
    # AGENT CONFIGURATION
    # Composes all components into a working agent
    # ========================================================================
    # Combine all tools
    plannable_tools = public_tools  # All public tools are plannable
    all_tools = public_tools + internal_tools

    config = AgentConfig(
        model_id=str(BedrockModel.CLAUDE_45_SONNET),
        system_prompt_builder=StaticPromptBuilder(INGESTION_TROUBLESHOOTING_PROMPT),
        tools=all_tools,
        plannable_tools=plannable_tools,  # Excludes respond_to_user
        temperature=0.4,  # Balanced between creative investigation and focused diagnosis
        max_tool_calls=20,  # Proactive agents may need multiple investigation steps
        agent_name="Ingestion Troubleshooting Agent",
        agent_description="Proactive agent for diagnosing DataHub ingestion issues",
    )

    return AgentRunner(config=config, client=client, history=history)


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    """
    Example usage showing how the agent would handle common troubleshooting scenarios.

    Run this to see the agent in action (requires real DataHub instance).
    """
    from datahub_integrations.gen_ai.mlflow_init import initialize_mlflow

    import os

    from datahub.sdk.main_client import DataHubClient
    from dotenv import load_dotenv

    from datahub_integrations.chat.agent import ProgressUpdate
    from datahub_integrations.chat.chat_history import HumanMessage

    # Load environment variables for AWS credentials
    load_dotenv()

    # Initialize MLflow for agent tracing
    initialize_mlflow()

    # ========================================================================
    # PROGRESS LISTENER - Shows agent's thinking in real-time
    # ========================================================================
    def demo_progress_listener(updates: List[ProgressUpdate]) -> None:
        """
        Progress callback that prints agent's thinking and actions.

        This demonstrates how to stream agent progress for UI updates.
        In a real application, this could send updates via SSE, WebSocket, etc.

        Args:
            updates: List of ProgressUpdate objects with text and message_type
        """
        # Only print the latest update (new ones added to list)
        if not updates:
            return

        latest = updates[-1]

        # Format based on message type for clarity
        if latest.message_type == "THINKING":
            # Agent's internal reasoning
            print(f"  💭 {latest.text}")
        elif latest.message_type == "TOOL_CALL":
            # Tool being called (usually not shown, but could be)
            print("  🔧 Calling tool...")
        elif latest.message_type == "TOOL_RESULT":
            # Tool completed (usually not shown)
            pass
        elif latest.message_type == "TEXT":
            # Final response text
            print(f"  💬 {latest.text}")

    # Create client and agent
    client = DataHubClient.from_env()
    agent = create_ingestion_troubleshooting_agent(client)

    # ========================================================================
    # SCENARIO 1: Missing Entity
    # ========================================================================
    print("=" * 80)
    print("SCENARIO 1: User reports missing entity")
    print("=" * 80)

    agent.history.add_message(
        HumanMessage(
            text="My table snowflake.prod.sales_orders isn't showing up in DataHub"
        )
    )

    # Agent will proactively:
    # 1. Search for similar entities (check for typos, schema differences)
    # 2. Find Snowflake ingestion sources
    # 3. Check recent run history for those sources
    # 4. Examine error logs if runs failed
    # 5. Synthesize findings into diagnosis with recommendations

    print("\n🔍 Agent is investigating (with real-time progress)...\n")

    # Run with LLM - shows real agent behavior!
    with agent.set_progress_callback(demo_progress_listener):
        response = agent.generate_next_message()

    print("\n📝 Final Response:")
    print(f"{response.text}")

    if hasattr(response, "suggestions") and response.suggestions:
        print("\n💡 Follow-up Suggestions:")
        for i, suggestion in enumerate(response.suggestions, 1):
            print(f"  {i}. {suggestion}")

    print("\n" + "=" * 80)
    print("DEMO COMPLETE")
    print("=" * 80)
    print(
        "\nThis example demonstrates the agent infrastructure with placeholder tools."
    )
    print(
        "To make it fully functional, implement the TODO GraphQL queries in the tool functions."
    )
