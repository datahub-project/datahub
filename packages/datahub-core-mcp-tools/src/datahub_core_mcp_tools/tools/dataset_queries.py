"""Dataset queries: get_dataset_queries."""

from typing import Annotated, List, Literal, Optional

from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import FilterDsl
from pydantic import Field

from .. import arg_validation, entity_cleaning, graphql_helpers
from ..read_backend import JsonObject, get_read_backend, require_existing
from ..result_policy import annotate_items, attach_header, current_reader_decisions
from ..version_requirements import read_only


def _dataset_subjects(subjects: object) -> List[str]:
    """The distinct dataset URNs among a query's subjects, in order.

    `subjects` lists every dataset and schema field the query touches; the SQL
    itself shows the columns, so only the datasets are kept.
    """
    urns: List[str] = []
    for subject in subjects if isinstance(subjects, list) else []:
        dataset = subject.get("dataset") if isinstance(subject, dict) else None
        urn = dataset.get("urn") if isinstance(dataset, dict) else None
        if isinstance(urn, str) and urn not in urns:
            urns.append(urn)
    return urns


@read_only
def get_dataset_queries(
    urn: Annotated[str, Field(validation_alias=arg_validation.URN_ALIASES)],
    column: Optional[str] = None,
    source: Optional[Literal["MANUAL", "SYSTEM"]] = None,
    start: Annotated[int, Field(validation_alias=arg_validation.OFFSET_ALIASES)] = 0,
    count: Annotated[int, Field(validation_alias=arg_validation.SIZE_ALIASES)] = 10,
) -> dict:
    """Get SQL queries associated with a dataset or column to understand usage patterns.

    This tool retrieves actual SQL queries that reference a specific dataset or column.
    Useful for understanding how data is used, common JOIN patterns, typical filters,
    and aggregation logic.

    PARAMETERS:

    source - Filter by query origin:
    - "MANUAL": Queries written by users in query editors (real SQL patterns)
    - "SYSTEM": Queries extracted from BI tools/dashboards (production usage)
    - null: Return both types (default)

    COMMON USE CASES:

    1. SQL Generation - Learn real query patterns:
       get_dataset_queries(urn, source="MANUAL", count=5-10)
       → See how users actually write SQL against this table
       → Discover common JOINs, aggregations, filters
       → Match organizational SQL conventions and patterns

    2. Production usage analysis:
       get_dataset_queries(urn, source="SYSTEM", count=20)
       → See how dashboards and reports query this data
       → Understand which queries run in production
       → Identify critical query patterns

    3. Column usage patterns:
       get_dataset_queries(urn, column="customer_id", source="MANUAL", count=5)
       → See how a specific column is used in queries
       → Learn filtering and grouping patterns for that column
       → Discover relationships via JOIN patterns

    4. General usage exploration:
       get_dataset_queries(urn, count=10)
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

    RESPONSE STRUCTURE:
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
    if column in ("null", ""):
        column = None
    tool = "get_dataset_queries"
    urn = arg_validation.require_urn(urn, tool=tool)
    backend = get_read_backend()
    # listQueries returns 0 for a URN that does not exist, which reads as "this
    # table has no queries".
    require_existing(backend, urn)

    subject = arg_validation.schema_field_urn(urn, column, tool=tool)
    _, compiled_filters = compile_filters(
        FilterDsl.custom_filter(field="entities", condition="EQUAL", values=[subject])
    )
    variables: JsonObject = {
        "start": start,
        "count": count,
        "orFilters": compiled_filters,  # type: ignore[dict-item]
    }
    if source is not None:
        variables["source"] = source

    result = graphql_helpers.clean_gql_response(backend.list_queries(variables))
    queries: List[dict] = result.get("queries") or []
    for query in queries:
        if "subjects" in query:
            query["subjects"] = _dataset_subjects(query["subjects"])
        statement = (query.get("properties") or {}).get("statement")
        if isinstance(statement, dict) and isinstance(statement.get("value"), str):
            statement["value"] = entity_cleaning.truncate_query(statement["value"])

    urns = [query["urn"] for query in queries if query.get("urn")]
    decisions = current_reader_decisions(urns)
    if decisions is not None:
        queries, _ = annotate_items(
            queries, lambda item: item.get("urn"), decisions=decisions
        )
        attach_header(result, decisions, urns, reader_only=True)
    result["queries"] = queries
    # Descriptions and each customProperties value are capped as get_entities caps
    # them: a value can hold a whole config or compiled SQL.
    entity_cleaning.truncate_descriptions(result)
    return result
