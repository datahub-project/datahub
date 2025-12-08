"""
MCP tools for text-to-SQL generation.

This module provides the generate_sql tool that converts natural language
queries to SQL using DataHub's semantic understanding.
"""

import json
import re
from typing import Any, Dict, List, Literal, Optional, Set

from datahub.metadata.urns import DatasetUrn, Urn
from json_repair import repair_json
from loguru import logger

from datahub_integrations.chat.sql_generator.models import (
    GenerateSqlResponse,
    SemanticModel,
    SemanticModelSummary,
)
from datahub_integrations.chat.sql_generator.semantic_model_builder import (
    SemanticModelBuilder,
)
from datahub_integrations.gen_ai.llm.factory import get_llm_client
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.mcp.mcp_server import get_datahub_client
from datahub_integrations.mcp_integration.tool import ToolWrapper

# Supported SQL platforms
_SUPPORTED_SQL_PLATFORMS: Set[str] = {
    "snowflake",
    "bigquery",
    "postgres",
    "postgresql",
    "redshift",
    "mysql",
    "mssql",
    "sqlserver",
    "oracle",
    "databricks",
    "trino",
    "presto",
    "athena",
    "hive",
    "impala",
    "clickhouse",
    "dbt",
}

# SQL validation patterns - block dangerous operations
_DANGEROUS_SQL_PATTERNS = [
    r"\bINSERT\s+INTO\b",
    r"\bUPDATE\s+\w+\s+SET\b",
    r"\bDELETE\s+FROM\b",
    r"\bDROP\s+(TABLE|DATABASE|SCHEMA|INDEX|VIEW)\b",
    r"\bCREATE\s+(TABLE|DATABASE|SCHEMA|INDEX|VIEW)\b",
    r"\bALTER\s+(TABLE|DATABASE|SCHEMA)\b",
    r"\bTRUNCATE\s+TABLE\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
]

# System prompts for SQL generation
_SQL_GENERATION_SYSTEM_PROMPT = """\
You are an expert SQL analyst. Your task is to generate SQL queries from natural language descriptions.

You will be given:
1. A semantic model describing available tables, their columns (dimensions, facts, time dimensions), and relationships
2. A natural language query describing what data is needed
3. The target SQL platform/dialect
4. Optional additional context

Your response should include:
- The SQL query that answers the user's question
- An explanation of what the query does
- Your confidence level (high/medium/low)
- Any assumptions you made
- Any ambiguities in the request
- Suggested clarifying questions if the query could be improved with more information

IMPORTANT GUIDELINES:
- Generate SELECT queries only - never generate INSERT, UPDATE, DELETE, or DDL
- Use the table and column names exactly as specified in the semantic model
- Follow the join patterns from the relationships in the semantic model
- Use appropriate aggregations for facts/measures
- Apply appropriate date/time functions for time dimensions
- Consider the target platform for dialect-specific syntax
"""

_SQL_GENERATION_USER_PROMPT = """\
Generate a SQL query for the following request:

<request>
{query}
</request>

<semantic_model>
{semantic_model}
</semantic_model>

<platform>
{platform}
</platform>

{additional_context}

Please generate a SQL query that answers this request. Use the relationships and column information from the semantic model to construct proper JOINs and select appropriate columns.
"""


# =============================================================================
# LLM Tool Definition (using FastMCP for schema generation)
# =============================================================================


def _generate_sql_response(
    sql: str,
    explanation: str,
    confidence: Literal["high", "medium", "low"],
    assumptions: List[str],
    ambiguities: List[str],
    suggested_clarifications: List[str],
) -> None:
    """Returns the generated SQL query with metadata.

    Args:
        sql: The generated SQL query.
        explanation: Explanation of what the query does.
        confidence: Confidence level in the generated SQL (high, medium, or low).
        assumptions: Assumptions made during SQL generation.
        ambiguities: Ambiguities in the request that could affect results.
        suggested_clarifications: Questions to ask for better results.
    """
    pass  # Placeholder - LLM provides the response


# Generate tool spec using FastMCP (single source of truth from function signature)
_sql_generation_tool = ToolWrapper.from_function(
    _generate_sql_response,
    name="generate_sql_response",
    description=_generate_sql_response.__doc__ or "",
)


def _extract_tool_response(response: dict) -> dict:
    """
    Extract the structured response from an LLM tool use response.

    Args:
        response: The full LLM response from converse()

    Returns:
        The input dict from the tool use block

    Raises:
        ValueError: If no tool use block is found
    """
    output = response.get("output", {})
    message = output.get("message", {})
    content = message.get("content", [])

    for block in content:
        if "toolUse" in block:
            return block["toolUse"].get("input", {})

    # Fallback: try to extract from text response
    for block in content:
        if "text" in block:
            text = block["text"]
            # Try to parse JSON, using json_repair for malformed output
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                logger.warning("Malformed JSON in LLM response, attempting repair")
                try:
                    repaired = repair_json(text)
                    return json.loads(repaired)
                except Exception:
                    pass

    raise ValueError("No tool use block found in LLM response")


def _validate_sql_safety(sql: str) -> List[str]:
    """
    Validate that SQL is safe (SELECT only, no DDL/DML).

    Args:
        sql: The SQL query to validate

    Returns:
        List of warnings if any dangerous patterns found, empty list if safe
    """
    warnings = []
    sql_upper = sql.upper()

    for pattern in _DANGEROUS_SQL_PATTERNS:
        if re.search(pattern, sql_upper, re.IGNORECASE):
            warnings.append(
                f"SQL contains potentially dangerous pattern: {pattern}. "
                "Only SELECT queries are allowed."
            )

    return warnings


def _get_platform_dialect_hints(platform: str) -> str:
    """
    Get platform-specific SQL dialect hints.

    Args:
        platform: The target SQL platform

    Returns:
        Hints for the LLM about platform-specific syntax
    """
    hints = {
        "snowflake": """
Platform-specific notes for Snowflake:
- Use ILIKE for case-insensitive string matching
- Use DATE_TRUNC('day', column) for date truncation
- Use :: for type casting (e.g., column::VARCHAR)
- Identifiers are case-insensitive but preserve case when quoted
- Use FLATTEN() for JSON/ARRAY expansion
""",
        "bigquery": """
Platform-specific notes for BigQuery:
- Use backticks for project.dataset.table references
- Use SAFE_CAST for type conversions that might fail
- Use DATE_TRUNC(column, DAY) for date truncation
- Arrays are first-class types, use UNNEST() to expand
- Use EXCEPT/REPLACE in SELECT * to exclude/rename columns
""",
        "postgres": """
Platform-specific notes for PostgreSQL:
- Use ILIKE for case-insensitive matching
- Use DATE_TRUNC('day', column) for date truncation
- Use :: for type casting
- Use jsonb operators for JSON data (->, ->>, @>, etc.)
- CTEs are materialized by default
""",
        "generic": """
Use standard SQL syntax that should work across most databases.
Avoid database-specific functions when possible.
""",
    }
    return hints.get(platform, hints["generic"])


def generate_sql(
    natural_language_query: str,
    table_urns: List[str],
    platform: Literal["snowflake", "bigquery", "postgres", "generic"] = "generic",
    additional_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generate SQL from natural language using DataHub semantic understanding.

    This tool converts natural language queries to SQL by:
    1. Building a semantic model from the specified tables (schema, relationships, query patterns)
    2. Using an LLM to generate SQL that answers the natural language query
    3. Returning the SQL with confidence scores and clarification suggestions

    WHEN TO USE:
    Use this tool when you need to generate SQL queries based on user questions about data.
    The tool works best when you've already identified the relevant tables using search/lineage tools.

    WORKFLOW:
    1. Use search or lineage tools to find relevant tables
    2. Pass the table URNs to generate_sql with the user's question
    3. Review the generated SQL, confidence, and suggestions
    4. If confidence is low or ambiguities exist, ask the user for clarification

    Args:
        natural_language_query: The user's question in natural language.
            Example: "Show me the top 10 customers by total order value last month"

        table_urns: List of dataset URNs to include in the semantic model.
            These should be the tables relevant to answering the query.
            Example: ["urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)",
                     "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.customers,PROD)"]

        platform: Target SQL dialect. Options:
            - "snowflake": Snowflake SQL syntax
            - "bigquery": BigQuery SQL syntax
            - "postgres": PostgreSQL syntax
            - "generic": Standard SQL (default)

        additional_context: Optional dict with additional context from the calling agent.
            Can include structured information like:
            - filters: {"date_range": "last 30 days", "region": "US"}
            - aggregation: "daily" or "monthly"
            - specific_columns: ["customer_id", "order_total"]
            - business_context: "We need this for the quarterly report"

    Returns:
        Dict with:
        - sql: The generated SQL query
        - explanation: What the query does
        - platform: Target platform
        - confidence: "high", "medium", or "low"
        - tables_used: List of table URNs used
        - assumptions: List of assumptions made
        - ambiguities: List of ambiguities that could affect results
        - suggested_clarifications: Questions to ask for better results
        - semantic_model_summary: Stats about the semantic model used

    Examples:
        # Simple aggregation query
        generate_sql(
            natural_language_query="What were total sales by region last quarter?",
            table_urns=["urn:li:dataset:(...):sales_orders"],
            platform="snowflake"
        )

        # Multi-table query with context
        generate_sql(
            natural_language_query="Which customers haven't ordered in 90 days?",
            table_urns=[
                "urn:li:dataset:(...):customers",
                "urn:li:dataset:(...):orders"
            ],
            platform="bigquery",
            additional_context={"threshold_date": "2024-01-01"}
        )
    """
    logger.bind(
        table_count=len(table_urns),
        platform=platform,
    ).info("Generating SQL for query: {}...", natural_language_query[:100])

    # Validate table URNs
    parsed_urns: List[DatasetUrn] = []
    for urn_str in table_urns:
        try:
            urn = Urn.from_string(urn_str)
            if not isinstance(urn, DatasetUrn):
                raise ValueError(f"URN {urn_str} is not a dataset URN")
            parsed_urns.append(urn)
        except Exception as e:
            raise ValueError(f"Invalid URN: {urn_str}. Error: {e}") from e

    if not parsed_urns:
        raise ValueError("At least one table URN must be provided")

    # Validate that all tables are from SQL-compatible platforms
    unsupported_platforms = []
    for urn in parsed_urns:
        platform_name = urn.get_data_platform_urn().platform_name.lower()
        if platform_name not in _SUPPORTED_SQL_PLATFORMS:
            unsupported_platforms.append((str(urn), platform_name))

    if unsupported_platforms:
        platform_list = ", ".join(
            f"{platform} ({urn})" for urn, platform in unsupported_platforms
        )
        raise ValueError(
            f"SQL generation is only supported for SQL databases. "
            f"Unsupported platforms found: {platform_list}. "
            f"Supported platforms: {', '.join(sorted(_SUPPORTED_SQL_PLATFORMS))}"
        )

    # Get DataHub client
    client = get_datahub_client()

    # Build semantic model
    logger.info("Building semantic model from {} tables", len(parsed_urns))
    builder = SemanticModelBuilder(
        client=client,
        max_tables=min(len(parsed_urns) * 2, 10),  # Allow some related tables
        start_urns=parsed_urns,
    )
    semantic_model = builder.build()

    logger.info(
        "Built semantic model with {} tables and {} relationships",
        len(semantic_model.tables),
        len(semantic_model.relationships) if semantic_model.relationships else 0,
    )

    # Count query patterns analyzed
    query_patterns_count = sum(len(joins) for joins in builder._joins.values())

    # Generate SQL using LLM
    llm_response = _generate_sql_from_context(
        query=natural_language_query,
        semantic_model=semantic_model,
        platform=platform,
        additional_context=additional_context,
    )

    # Validate SQL safety
    sql = llm_response.get("sql", "")
    safety_warnings = _validate_sql_safety(sql)

    if safety_warnings:
        logger.warning("SQL safety warnings: {}", safety_warnings)
        llm_response["ambiguities"] = (
            llm_response.get("ambiguities", []) + safety_warnings
        )
        llm_response["confidence"] = "low"

    # Build response
    response = GenerateSqlResponse(
        sql=sql,
        explanation=llm_response.get("explanation", ""),
        platform=platform,
        confidence=llm_response.get("confidence", "medium"),
        tables_used=table_urns,
        assumptions=llm_response.get("assumptions", []),
        ambiguities=llm_response.get("ambiguities", []),
        suggested_clarifications=llm_response.get("suggested_clarifications", []),
        semantic_model_summary=SemanticModelSummary(
            tables_analyzed=len(semantic_model.tables),
            relationships_found=len(semantic_model.relationships)
            if semantic_model.relationships
            else 0,
            query_patterns_analyzed=query_patterns_count,
        ),
    )

    logger.bind(
        confidence=response.confidence,
        assumptions_count=len(response.assumptions),
        ambiguities_count=len(response.ambiguities),
    ).info("Generated SQL")

    return response.model_dump()


def _generate_sql_from_context(
    query: str,
    semantic_model: SemanticModel,
    platform: str,
    additional_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate SQL using LLM with semantic model context.

    Args:
        query: The natural language query
        semantic_model: The semantic model with table and relationship info
        platform: Target SQL platform
        additional_context: Optional additional context

    Returns:
        Dict with sql, explanation, confidence, assumptions, ambiguities, suggested_clarifications
    """
    # Serialize semantic model for LLM
    semantic_model_str = semantic_model.model_dump_json(indent=2, exclude_none=True)

    # Format additional context
    context_str = ""
    if additional_context:
        context_str = f"""
<additional_context>
{json.dumps(additional_context, indent=2)}
</additional_context>
"""

    # Get platform-specific hints
    platform_hints = _get_platform_dialect_hints(platform)

    # Prepare the user prompt
    user_prompt = _SQL_GENERATION_USER_PROMPT.format(
        query=query,
        semantic_model=semantic_model_str,
        platform=f"{platform}\n\n{platform_hints}",
        additional_context=context_str,
    )

    try:
        llm_client = get_llm_client(model_config.chat_assistant_ai.model)

        response = llm_client.converse(
            system=[{"text": _SQL_GENERATION_SYSTEM_PROMPT}],
            messages=[
                {
                    "role": "user",
                    "content": [{"text": user_prompt}],
                }
            ],
            toolConfig={"tools": [_sql_generation_tool.to_bedrock_spec()]},
            inferenceConfig={"temperature": 0.3, "maxTokens": 4096},
        )

        result = _extract_tool_response(response)  # type: ignore[arg-type]

        # Ensure all required fields are present
        return {
            "sql": result.get("sql", ""),
            "explanation": result.get("explanation", ""),
            "confidence": result.get("confidence", "medium"),
            "assumptions": result.get("assumptions", []),
            "ambiguities": result.get("ambiguities", []),
            "suggested_clarifications": result.get("suggested_clarifications", []),
        }

    except Exception as e:
        logger.error(f"Failed to generate SQL: {e}")
        return {
            "sql": "",
            "explanation": f"Error generating SQL: {str(e)}",
            "confidence": "low",
            "assumptions": [],
            "ambiguities": [f"SQL generation failed: {str(e)}"],
            "suggested_clarifications": [
                "Please try rephrasing your question or providing more specific table names"
            ],
        }
