"""Tools for getting entity information."""

import json
import logging
import pathlib
from typing import Iterator, List, Optional

from json_repair import repair_json

from datahub.errors import ItemNotFoundError
from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import execute_graphql
from datahub_agent_context.mcp_tools.helpers import (
    clean_get_entities_response,
    inject_urls_for_urns,
    truncate_descriptions,
)

logger = logging.getLogger(__name__)

# Load GraphQL queries
entity_details_fragment_gql = (
    pathlib.Path(__file__).parent / "gql/entity_details.gql"
).read_text()
query_entity_gql = (pathlib.Path(__file__).parent / "gql/query_entity.gql").read_text()


def get_entities(urns: List[str] | str) -> List[dict] | dict:
    """Get detailed information about one or more entities by their DataHub URNs.

    IMPORTANT: Pass an array of URNs to retrieve multiple entities in a single call - this is much
    more efficient than calling this tool multiple times. When examining search results, always pass
    an array with the top 3-10 result URNs to compare and find the best match.

    Accepts an array of URNs or a single URN. Supports all entity types including datasets,
    assertions, incidents, dashboards, charts, users, groups, and more. The response fields vary
    based on the entity type.

    Args:
        urns: List of URNs or a single URN string

    Returns:
        Single dict if single URN provided, list of dicts if multiple URNs provided.
        Each result contains entity details or error information.

    Raises:
        ItemNotFoundError: If single URN provided and entity not found

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = get_entities(urns=["urn:li:dataset:(...)"])
    """
    graph = get_graph()
    # Handle JSON-stringified arrays
    # Some MCP clients/LLMs pass arrays as JSON strings instead of proper lists
    if isinstance(urns, str):
        urns_str = urns.strip()  # Remove leading/trailing whitespace

        # Try to parse as JSON array first
        if urns_str.startswith("["):
            try:
                # Use json_repair to handle malformed JSON from LLMs
                urns = json.loads(repair_json(urns_str))
                return_single = False
            except (json.JSONDecodeError, Exception) as e:
                logger.warning(
                    f"Failed to parse URNs as JSON array: {e}. Treating as single URN."
                )
                # Not valid JSON, treat as single URN string
                urns = [urns_str]
                return_single = True
        else:
            # Single URN string
            urns = [urns_str]
            return_single = True
    else:
        return_single = False

    # Trim whitespace from each URN (defensive against string concatenation issues)
    urns = [urn.strip() for urn in urns]

    results = []
    for urn in urns:
        try:
            # Check if entity exists first
            if not graph.exists(urn):
                logger.warning(f"Entity not found during existence check: {urn}")
                if return_single:
                    raise ItemNotFoundError(f"Entity {urn} not found")
                results.append({"error": f"Entity {urn} not found", "urn": urn})
                continue

            # Special handling for Query entities (not part of Entity union type)
            is_query = urn.startswith("urn:li:query:")

            # Execute the appropriate GraphQL query
            variables = {"urn": urn}
            if is_query:
                result = execute_graphql(
                    graph,
                    query=query_entity_gql,
                    variables=variables,
                    operation_name="GetQueryEntity",
                )["entity"]
            else:
                result = execute_graphql(
                    graph,
                    query=entity_details_fragment_gql,
                    variables=variables,
                    operation_name="GetEntity",
                )["entity"]

            # Check if entity data was returned
            if result is None:
                raise ItemNotFoundError(
                    f"Entity {urn} exists but no data could be retrieved. "
                    f"This can happen if the entity has no aspects ingested yet, or if there's a permissions issue."
                )

            inject_urls_for_urns(graph, result, [""])
            truncate_descriptions(result)

            results.append(clean_get_entities_response(result))

        except Exception as e:
            logger.warning(f"Error fetching entity {urn}: {e}")
            if return_single:
                raise
            results.append({"error": str(e), "urn": urn})

    # Return single dict if single URN was passed, array otherwise
    return results[0] if return_single else results


def list_schema_fields(
    urn: str,
    keywords: Optional[List[str] | str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """List schema fields for a dataset, with optional keyword filtering and pagination.

    Useful when schema fields were truncated in search results (schemaFieldsTruncated present)
    and you need to explore specific columns. Supports pagination for large schemas.

    Args:
        urn: Dataset URN
        keywords: Optional keywords to filter schema fields (OR matching).
                 - Single string: Treated as one keyword (NOT split on whitespace). Use for field names or exact phrases.
                 - List of strings: Multiple keywords, matches any (OR logic).
                 - None or empty list: Returns all fields in priority order (same as get_entities).
                 Matches against fieldPath, description, label, tags, and glossary terms.
                 Matching fields are returned first, sorted by match count.
        limit: Maximum number of fields to return (default: 100)
        offset: Number of fields to skip for pagination (default: 0)

    Returns:
        Dictionary with:
        - urn: The dataset URN
        - fields: List of schema fields (paginated)
        - totalFields: Total number of fields in the schema
        - returned: Number of fields actually returned
        - remainingCount: Number of fields not included after offset (accounts for limit and token budget)
        - matchingCount: Number of fields that matched keywords (if keywords provided, None otherwise)
        - offset: The offset used

    Examples:
        # Single keyword (string) - search for exact field name or phrase
        list_schema_fields(urn="urn:li:dataset:(...)", keywords="user_email")
        # Returns fields matching "user_email" (like user_email_address, primary_user_email)

        # Multiple keywords (list) - OR matching
        list_schema_fields(urn="urn:li:dataset:(...)", keywords=["email", "user"])
        # Returns fields containing "email" OR "user" (user_email, contact_email, user_id, etc.)

        # Pagination through all fields
        list_schema_fields(urn="urn:li:dataset:(...)", limit=100, offset=0)   # First 100
        list_schema_fields(urn="urn:li:dataset:(...)", limit=100, offset=100) # Next 100

        # Combine filtering + pagination
        list_schema_fields(urn="urn:li:dataset:(...)", keywords=["user"], limit=50, offset=0)

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = list_schema_fields(urn="urn:li:dataset:(...)", keywords="email")

    Raises:
        ItemNotFoundError: If entity not found
    """
    graph = get_graph()
    # Normalize keywords to list (None means no filtering)
    keywords_lower = None
    if keywords is not None:
        if isinstance(keywords, str):
            keywords = [keywords]
        keywords_lower = [kw.lower() for kw in keywords]

    # Fetch entity
    if not graph.exists(urn):
        raise ItemNotFoundError(f"Entity {urn} not found")

    # Execute GraphQL query to get full schema
    variables = {"urn": urn}
    result = execute_graphql(
        graph,
        query=entity_details_fragment_gql,
        variables=variables,
        operation_name="GetEntity",
    )["entity"]

    # Check if entity data was returned
    if result is None:
        raise ItemNotFoundError(
            f"Entity {urn} exists but no data could be retrieved. "
            f"This can happen if the entity has no aspects ingested yet, or if there's a permissions issue."
        )

    # Apply same preprocessing as get_entities
    inject_urls_for_urns(graph, result, [""])
    truncate_descriptions(result)

    # Extract total field count before processing
    total_fields = len(result.get("schemaMetadata", {}).get("fields", []))

    if total_fields == 0:
        return {
            "urn": urn,
            "fields": [],
            "totalFields": 0,
            "returned": 0,
            "remainingCount": 0,
            "matchingCount": None,
            "offset": offset,
        }

    # Define custom sorting function for keyword matching
    sort_fn = None
    matching_count = None

    if keywords_lower:
        # Helper function to score a field by keyword matches
        def score_field_by_keywords(field: dict) -> int:
            """Score a field by counting keyword match coverage across its metadata.

            Scoring logic (OR matching):
            - Each keyword gets +1 if it appears in ANY searchable text (substring match)
            - Multiple occurrences of the same keyword in one text still count as +1
            - Higher score = more aspects of the field match the keywords

            Searchable texts (in order of priority):
            1. fieldPath (column name)
            2. description
            3. label
            4. tag names
            5. glossary term names

            Example:
                keywords = ["email", "user"]
                field = {
                    "fieldPath": "user_email",        # matches both
                    "description": "User's email",    # matches both
                    "tags": ["PII"]                   # matches neither
                }
                Score = 4 (email in fieldPath + email in desc + user in fieldPath + user in desc)

            Returns:
                Integer score (0 = no matches, higher = more coverage)
            """
            searchable_texts = [
                field.get("fieldPath", ""),
                field.get("description", ""),
                field.get("label", ""),
            ]

            # Add tag names
            if tags := field.get("tags"):
                if tag_list := tags.get("tags"):
                    searchable_texts.extend(
                        [
                            (t.get("tag", {}).get("properties") or {}).get("name", "")
                            for t in tag_list
                        ]
                    )

            # Add glossary term names
            if glossary_terms := field.get("glossaryTerms"):
                if terms_list := glossary_terms.get("terms"):
                    searchable_texts.extend(
                        [
                            (t.get("term", {}).get("properties") or {}).get("name", "")
                            for t in terms_list
                        ]
                    )

            # Count keyword coverage: +1 for each (keyword, text) pair that matches
            # Note: Substring matching, case-insensitive
            return sum(
                1
                for kw in keywords_lower
                for text in searchable_texts
                if text and kw in text.lower()
            )

        # Pre-compute matching count (need all fields for this)
        fields_for_counting = result.get("schemaMetadata", {}).get("fields", [])
        matching_count = sum(
            1 for field in fields_for_counting if score_field_by_keywords(field) > 0
        )

        # Define sort function for clean_get_entities_response
        def sort_by_keyword_match(fields: List[dict]) -> Iterator[dict]:
            """Sort fields by keyword match count (descending), then alphabetically."""
            scored_fields = [
                (score_field_by_keywords(field), field) for field in fields
            ]
            scored_fields.sort(key=lambda x: (-x[0], x[1].get("fieldPath", "")))
            return iter(field for _, field in scored_fields)

        sort_fn = sort_by_keyword_match

    # Use clean_get_entities_response for consistent processing
    cleaned_entity = clean_get_entities_response(
        result,
        sort_fn=sort_fn,
        offset=offset,
        limit=limit,
    )

    # Extract the cleaned fields and metadata
    schema_metadata = cleaned_entity.get("schemaMetadata", {})
    cleaned_fields = schema_metadata.get("fields", [])

    # Calculate how many fields remain after what we returned
    # This accounts for both pagination and token budget constraints
    remaining_count = total_fields - offset - len(cleaned_fields)

    return {
        "urn": urn,
        "fields": cleaned_fields,
        "totalFields": total_fields,
        "returned": len(cleaned_fields),
        "remainingCount": remaining_count,
        "matchingCount": matching_count,
        "offset": offset,
    }
