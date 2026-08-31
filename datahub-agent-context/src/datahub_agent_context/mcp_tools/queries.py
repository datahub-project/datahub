"""Tools for getting dataset queries."""

import contextlib
import logging
import pathlib
from typing import Literal, Optional

from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import FilterDsl
from datahub.utilities.ordered_set import OrderedSet
from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import clean_gql_response, execute_graphql
from datahub_agent_context.mcp_tools.helpers import (
    maybe_convert_to_schema_field_urn,
    truncate_query,
)

logger = logging.getLogger(__name__)

# Load GraphQL query
queries_gql = (pathlib.Path(__file__).parent / "gql/queries.gql").read_text()


def _deduplicate_subjects(subjects: list[dict]) -> list[str]:
    """Deduplicate subjects to unique dataset URNs.

    The "subjects" field returns every dataset and schema field associated with the query.
    While this is useful for our backend to have, it's not useful here because
    we can just look at the query directly. So we'll narrow it down to the unique
    list of dataset urns.

    Args:
        subjects: List of subject dicts with dataset/schemaField info

    Returns:
        List of unique dataset URNs
    """
    updated_subjects: OrderedSet[str] = OrderedSet()
    for subject in subjects:
        with contextlib.suppress(KeyError):
            updated_subjects.add(subject["dataset"]["urn"])
    return list(updated_subjects)


def get_dataset_queries(
    urn: str,
    column: Optional[str] = None,
    source: Optional[Literal["MANUAL", "SYSTEM"]] = None,
    start: int = 0,
    count: int = 10,
) -> dict:
    """Get SQL queries associated with a dataset or column to understand usage patterns.

    This tool retrieves actual SQL queries that reference a specific dataset or column.
    Useful for understanding how data is used, common JOIN patterns, typical filters,
    and aggregation logic.

    Args:
        urn: Dataset URN
        column: Optional column name to filter queries
        source: Filter by query origin:
                - "MANUAL": Queries written by users in query editors (real SQL patterns)
                - "SYSTEM": Queries extracted from BI tools/dashboards (production usage)
                - None: Return both types (default)
        start: Starting offset for pagination (default: 0)
        count: Number of queries to return (default: 10)

    Returns:
        Dictionary with:
        - total: Total number of queries matching criteria
        - start: Starting offset
        - count: Number of results returned
        - queries: Array of query objects with:
          - urn: Query identifier
          - properties.statement.value: The actual SQL text
          - properties.statement.language: Query language (SQL, etc.)
          - properties.source: MANUAL or SYSTEM
          - properties.name: Optional query name
          - platform: Source platform
          - subjects: Referenced datasets/columns (deduplicated to dataset URNs)

    COMMON USE CASES:

    1. SQL Generation - Learn real query patterns:
       get_dataset_queries(graph, urn, source="MANUAL", count=5-10)
       → See how users actually write SQL against this table
       → Discover common JOINs, aggregations, filters
       → Match organizational SQL conventions and patterns

    2. Production usage analysis:
       get_dataset_queries(graph, urn, source="SYSTEM", count=20)
       → See how dashboards and reports query this data
       → Understand which queries run in production
       → Identify critical query patterns

    3. Column usage patterns:
       get_dataset_queries(graph, urn, column="customer_id", source="MANUAL", count=5)
       → See how a specific column is used in queries
       → Learn filtering and grouping patterns for that column
       → Discover relationships via JOIN patterns

    4. General usage exploration:
       get_dataset_queries(graph, urn, count=10)
       → Get mix of manual and system queries
       → Understand overall table usage

    EXAMPLES:

    - Get manual queries for SQL generation:
      get_dataset_queries(
          urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)",
          source="MANUAL",
          count=10
      )

    - Get dashboard queries (production usage):
      get_dataset_queries(
          urn="urn:li:dataset:(...)",
          source="SYSTEM",
          count=20
      )

    - Column-specific query patterns:
      get_dataset_queries(
          urn="urn:li:dataset:(...)",
          column="created_at",
          source="MANUAL",
          count=5
      )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = get_dataset_queries(urn="urn:li:dataset:(...)", source="MANUAL")

    ANALYZING RETRIEVED QUERIES:
    Once you retrieve queries, examine the SQL statements to identify:
    - JOIN patterns: Which tables are joined? On what keys?
    - Aggregations: Common SUM, COUNT, AVG, GROUP BY patterns
    - Filters: Typical WHERE clauses, date range logic
    - Column usage: Which columns appear frequently vs rarely
    - CTEs and subqueries: Complex query structures

    BEST PRACTICES:
    - For SQL generation: Use source="MANUAL" (count=5-10) to see real user patterns
    - For production analysis: Use source="SYSTEM" to see dashboard/report queries
    - Start with moderate count (5-10) to avoid overwhelming context
    - If no queries found (total=0), proceed without query examples - not all tables have queries
    - Parse the SQL statements yourself to find patterns - they are not full-text searchable
    """
    graph = get_graph()
    urn = maybe_convert_to_schema_field_urn(urn, column)

    entities_filter = FilterDsl.custom_filter(
        field="entities", condition="EQUAL", values=[urn]
    )
    _, compiled_filters = compile_filters(entities_filter)

    # Set up variables for the query
    variables = {
        "input": {
            "start": start,
            "count": count,
            "orFilters": compiled_filters,
        }
    }

    # Add optional source filter
    if source is not None:
        variables["input"]["source"] = source

    # Execute the GraphQL query
    result = execute_graphql(
        graph,
        query=queries_gql,
        variables=variables,
        operation_name="listQueries",
    )["listQueries"]

    for query in result["queries"]:
        if query.get("subjects"):
            query["subjects"] = _deduplicate_subjects(query["subjects"])

        # Truncate long SQL queries to prevent context window issues
        if queryProperties := query.get("properties"):
            queryProperties["statement"]["value"] = truncate_query(
                queryProperties["statement"]["value"]
            )

    return clean_gql_response(result)
