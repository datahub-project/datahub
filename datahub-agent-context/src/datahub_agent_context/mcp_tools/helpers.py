"""Helper functions for MCP tools."""

import html
import logging
import os
import re
from typing import Any, Callable, Generator, Iterator, List, Optional, TypeVar

import jmespath  # type: ignore[import-untyped]

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from datahub_agent_context.mcp_tools._token_estimator import TokenCountEstimator
from datahub_agent_context.mcp_tools.base import _is_datahub_cloud

logger = logging.getLogger(__name__)

T = TypeVar("T")

DESCRIPTION_LENGTH_HARD_LIMIT = 1000
QUERY_LENGTH_HARD_LIMIT = 5000

# Maximum token count for tool responses to prevent context window issues
TOOL_RESPONSE_TOKEN_LIMIT = int(os.getenv("TOOL_RESPONSE_TOKEN_LIMIT", "80000"))

# Per-entity schema token budget for field truncation
ENTITY_SCHEMA_TOKEN_BUDGET = int(os.getenv("ENTITY_SCHEMA_TOKEN_BUDGET", "16000"))


def sanitize_html_content(text: str) -> str:
    """Remove HTML tags and decode HTML entities from text.

    Uses a bounded regex pattern to prevent ReDoS (Regular Expression Denial of Service)
    attacks. The pattern limits matching to tags with at most 100 characters between < and >,
    which prevents backtracking on malicious input like "<" followed by millions of characters
    without a closing ">".
    """
    if not text:
        return text

    # Use bounded regex to prevent ReDoS (max 100 chars between < and >)
    text = re.sub(r"<[^<>]{0,100}>", "", text)

    # Decode HTML entities
    text = html.unescape(text)

    return text.strip()


def truncate_with_ellipsis(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to max_length and add suffix if truncated."""
    if not text or len(text) <= max_length:
        return text

    # Account for suffix length
    actual_max = max_length - len(suffix)
    return text[:actual_max] + suffix


def sanitize_markdown_content(text: str) -> str:
    """Remove markdown-style embeds that contain encoded data from text, but preserve alt text."""
    if not text:
        return text

    # Remove markdown embeds with data URLs (base64 encoded content) but preserve alt text
    # Pattern: ![alt text](data:image/type;base64,encoded_data) -> alt text
    text = re.sub(r"!\[([^\]]*)\]\(data:[^)]+\)", r"\1", text)

    return text.strip()


def sanitize_and_truncate_description(text: str, max_length: int) -> str:
    """Sanitize HTML content and truncate to specified length."""
    if not text:
        return text

    try:
        # First sanitize HTML content
        sanitized = sanitize_html_content(text)

        # Then sanitize markdown content (preserving alt text)
        sanitized = sanitize_markdown_content(sanitized)

        # Then truncate if needed
        return truncate_with_ellipsis(sanitized, max_length)
    except Exception as e:
        logger.warning(f"Error sanitizing and truncating description: {e}")
        return text[:max_length] if len(text) > max_length else text


def truncate_descriptions(
    data: dict | list, max_length: int = DESCRIPTION_LENGTH_HARD_LIMIT
) -> None:
    """Recursively truncates values of keys named 'description' in a dictionary in place."""
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "description" and isinstance(value, str):
                data[key] = sanitize_and_truncate_description(value, max_length)
            elif isinstance(value, (dict, list)):
                truncate_descriptions(value)
    elif isinstance(data, list):
        for item in data:
            truncate_descriptions(item)


def truncate_query(query: str) -> str:
    """Truncate a SQL query if it exceeds the maximum length."""
    return truncate_with_ellipsis(
        query, QUERY_LENGTH_HARD_LIMIT, suffix="... [truncated]"
    )


def inject_urls_for_urns(
    graph: DataHubGraph, response: Any, json_paths: List[str]
) -> None:
    """Inject URLs for URNs in the response at specified JSON paths (in place).

    Only works for DataHub Cloud instances.

    Args:
        graph: DataHubGraph instance
        response: Response dict to modify in place
        json_paths: List of JMESPath expressions to find URNs
    """
    if not _is_datahub_cloud(graph):
        return

    for path in json_paths:
        for item in jmespath.search(path, response) if path else [response]:
            if isinstance(item, dict) and item.get("urn"):
                # Update item in place with url, ensuring that urn and url are first.
                new_item = {"urn": item["urn"], "url": graph.url_for(item["urn"])}
                new_item.update({k: v for k, v in item.items() if k != "urn"})
                item.clear()
                item.update(new_item)


def maybe_convert_to_schema_field_urn(urn: str, column: Optional[str]) -> str:
    """Convert a dataset URN to a schema field URN if column is provided.

    Args:
        urn: Dataset URN
        column: Optional column name

    Returns:
        SchemaField URN if column provided, otherwise original URN

    Raises:
        ValueError: If column provided but URN is not a dataset URN
    """
    if column:
        maybe_dataset_urn = Urn.from_string(urn)
        if not isinstance(maybe_dataset_urn, DatasetUrn):
            raise ValueError(
                f"Input urn should be a dataset urn if column is provided, but got {urn}."
            )
        urn = str(SchemaFieldUrn(maybe_dataset_urn, column))
    return urn


def _select_results_within_budget(
    results: Iterator[T],
    fetch_entity: Callable[[T], dict],
    max_results: int = 10,
    token_budget: Optional[int] = None,
) -> Generator[T, None, None]:
    """Generator that yields results within token budget.

    Generic helper that works for any result structure. Caller provides a function
    to extract/clean entity for token counting (can mutate the result).

    Yields results until:
    - max_results reached, OR
    - token_budget would be exceeded (and we have at least 1 result)

    Args:
        results: Iterator of result objects of any type T (memory efficient)
        fetch_entity: Function that extracts entity dict from result for token counting.
                   Can mutate the result to clean/update entity in place.
                   Signature: T -> dict (entity for token counting)
                   Example: lambda r: (r.__setitem__("entity", clean(r["entity"])), r["entity"])[1]
        max_results: Maximum number of results to return
        token_budget: Token budget (defaults to 90% of TOOL_RESPONSE_TOKEN_LIMIT)

    Yields:
        Original result objects of type T (possibly mutated by fetch_entity)
    """
    if token_budget is None:
        # Use 90% of limit as safety buffer:
        # - Token estimation is approximate, not exact
        # - Response wrapper adds overhead
        # - Better to return fewer results that fit than exceed limit
        token_budget = int(TOOL_RESPONSE_TOKEN_LIMIT * 0.9)

    total_tokens = 0
    results_count = 0

    # Consume iterator up to max_results
    for i, result in enumerate(results):
        if i >= max_results:
            break
        # Extract (and possibly clean) entity using caller's lambda
        # Note: fetch_entity may mutate result to clean/update entity in place
        entity = fetch_entity(result)

        # Estimate token cost
        entity_tokens = TokenCountEstimator.estimate_dict_tokens(entity)

        # Check if adding this entity would exceed budget
        if total_tokens + entity_tokens > token_budget:
            if results_count == 0:
                # Always yield at least 1 result
                logger.warning(
                    f"First result ({entity_tokens:,} tokens) exceeds budget ({token_budget:,}), "
                    "yielding it anyway"
                )
                yield result  # Yield original result structure
                results_count += 1
                total_tokens += entity_tokens
            else:
                # Have at least 1 result, stop here to stay within budget
                logger.info(
                    f"Stopping at {results_count} results (next would exceed {token_budget:,} token budget)"
                )
                break
        else:
            yield result  # Yield original result structure
            results_count += 1
            total_tokens += entity_tokens

    logger.info(
        f"Selected {results_count} results using {total_tokens:,} tokens "
        f"(budget: {token_budget:,})"
    )


def clean_get_entities_response(
    raw_response: dict,
    *,
    sort_fn: Optional[Callable[[List[dict]], Iterator[dict]]] = None,
    offset: int = 0,
    limit: Optional[int] = None,
) -> dict:
    """Clean and optimize entity responses for LLM consumption.

    Performs several transformations to reduce token usage while preserving essential information:

    1. **Clean GraphQL artifacts**: Removes __typename, null values, empty objects/arrays
       (via clean_gql_response)

    2. **Schema field processing** (if schemaMetadata.fields exists):
       - Sorts fields using sort_fn (defaults to _sort_fields_by_priority)
       - Cleans each field to keep only essential properties (fieldPath, type, description, etc.)
       - Merges editableSchemaMetadata into fields with "edited*" prefix (editedDescription,
         editedTags, editedGlossaryTerms) - only included when they differ from system values
       - Applies pagination (offset/limit) with token budget constraint
       - Field selection stops when EITHER limit is reached OR ENTITY_SCHEMA_TOKEN_BUDGET is exceeded
       - Adds schemaFieldsTruncated metadata when fields are cut

    3. **Remove duplicates**: Deletes editableSchemaMetadata after merging into schemaMetadata

    4. **Truncate view definitions**: Limits SQL view logic to QUERY_LENGTH_HARD_LIMIT

    The result is optimized for LLM tool responses: reduced token usage, no duplication,
    clear distinction between system-generated and user-curated content.

    Args:
        raw_response: Raw entity dict from GraphQL query
        sort_fn: Optional custom function to sort fields. If None, uses _sort_fields_by_priority.
                 Should take a list of field dicts and return an iterator of sorted fields.
        offset: Number of fields to skip after sorting (default: 0)
        limit: Maximum number of fields to include after offset (default: None = unlimited)

    Returns:
        Cleaned entity dict optimized for LLM consumption
    """
    from datahub_agent_context.mcp_tools.base import clean_gql_response

    response = clean_gql_response(raw_response)

    if response and (schema_metadata := response.get("schemaMetadata")):
        # Remove empty platformSchema to reduce response clutter
        if platform_schema := schema_metadata.get("platformSchema"):
            schema_value = platform_schema.get("schema")
            if not schema_value or schema_value == "":
                del schema_metadata["platformSchema"]

        # Process schema fields with sorting and budget constraint
        if fields := schema_metadata.get("fields"):
            # Use custom sort function if provided, otherwise sort by priority
            sorted_fields = iter(fields) if sort_fn is None else sort_fn(fields)

            # Apply offset/limit with token budget constraint
            selected_fields: list[dict] = []
            total_schema_tokens = 0
            fields_truncated = False

            for i, field in enumerate(sorted_fields):
                # Skip fields before offset
                if i < offset:
                    continue

                # Check limit
                if limit is not None and len(selected_fields) >= limit:
                    fields_truncated = True
                    break

                # Estimate tokens for this field
                field_tokens = TokenCountEstimator.estimate_dict_tokens(field)

                # Check token budget
                if total_schema_tokens + field_tokens > ENTITY_SCHEMA_TOKEN_BUDGET:
                    if len(selected_fields) == 0:
                        # Always include at least one field
                        logger.warning(
                            f"First field ({field_tokens:,} tokens) exceeds schema budget "
                            f"({ENTITY_SCHEMA_TOKEN_BUDGET:,}), including it anyway"
                        )
                        selected_fields.append(field)
                        total_schema_tokens += field_tokens
                    fields_truncated = True
                    break

                selected_fields.append(field)
                total_schema_tokens += field_tokens

            schema_metadata["fields"] = selected_fields

            if fields_truncated:
                schema_metadata["schemaFieldsTruncated"] = True
                logger.info(
                    f"Truncated schema fields: showing {len(selected_fields)} of {len(fields)} fields "
                    f"({total_schema_tokens:,} tokens)"
                )

    # Remove editableSchemaMetadata if present (already merged into fields)
    if response and "editableSchemaMetadata" in response:
        del response["editableSchemaMetadata"]

    # Truncate view definitions
    if response and (view_properties := response.get("viewProperties")):
        if logic := view_properties.get("logic"):
            view_properties["logic"] = truncate_query(logic)

    return response


def _extract_lineage_columns_from_paths(search_results: List[dict]) -> List[dict]:
    """Extract column information from paths field for column-level lineage results.

    When querying column-level lineage (e.g., get_lineage(urn, column="user_id")),
    the GraphQL response returns DATASET entities (not individual columns) with a
    'paths' field containing the column-level lineage chains.

    Each path shows the column flow, e.g.:
      source_table.user_id -> intermediate_table.uid -> target_table.customer_id

    The LAST entity in each path is a SchemaFieldEntity representing a column in the
    target dataset. This function extracts those column names into a 'lineageColumns' field.

    Args:
        search_results: List of lineage search results where entities are DATASET

    Returns:
        Same list with 'lineageColumns' field added to each result:
        - entity: Dataset entity (unchanged)
        - lineageColumns: List of unique column names (fieldPath) from path endpoints
        - degree: Degree value (unchanged)
        - paths: Removed to reduce response size (column info extracted to lineageColumns)

    Example transformation:
        Input: [
            {
                entity: {type: "DATASET", name: "target_table"},
                paths: [
                    {path: [
                        {type: "SCHEMA_FIELD", fieldPath: "user_id"},
                        {type: "SCHEMA_FIELD", fieldPath: "customer_id"}  # <- target column
                    ]},
                    {path: [
                        {type: "SCHEMA_FIELD", fieldPath: "user_id"},
                        {type: "SCHEMA_FIELD", fieldPath: "uid"}  # <- another target column
                    ]}
                ],
                degree: 1
            }
        ]
        Output: [
            {
                entity: {type: "DATASET", name: "target_table"},
                lineageColumns: ["customer_id", "uid"],
                degree: 1
            }
        ]
    """
    if not search_results:
        return search_results

    # Check if this is column-level lineage by looking for paths
    has_column_paths = any(
        result.get("paths") and len(result.get("paths", [])) > 0
        for result in search_results
    )

    if not has_column_paths:
        # Not column-level lineage (or no paths available), return as-is
        return search_results

    processed_results = []
    for result in search_results:
        paths = result.get("paths", [])

        if not paths:
            # No paths for this result, keep as-is
            processed_results.append(result)
            continue

        # Extract column names from the LAST entity in each path
        lineage_columns = []
        for path_obj in paths:
            path = path_obj.get("path", [])
            if not path:
                continue

            # Get the last entity in the path (target column)
            last_entity = path[-1]
            if last_entity.get("type") == "SCHEMA_FIELD":
                field_path = last_entity.get("fieldPath")
                if field_path and field_path not in lineage_columns:
                    lineage_columns.append(field_path)

        # Create new result with lineageColumns
        new_result = {
            "entity": result["entity"],
            "degree": result.get("degree", 0),
        }

        if lineage_columns:
            new_result["lineageColumns"] = lineage_columns

        # Keep other fields that might exist (explored, truncatedChildren, etc.)
        for key in ["explored", "truncatedChildren", "ignoredAsHop"]:
            if key in result:
                new_result[key] = result[key]

        processed_results.append(new_result)

    return processed_results
