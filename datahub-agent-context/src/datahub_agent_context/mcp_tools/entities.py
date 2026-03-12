"""Tools for getting entity information."""

import logging
import pathlib
from typing import Iterator, List, Optional

from datahub.errors import ItemNotFoundError
from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import execute_graphql
from datahub_agent_context.mcp_tools.helpers import (
    clean_get_entities_response,
    clean_related_documents_response,
    inject_urls_for_urns,
    truncate_descriptions,
)

logger = logging.getLogger(__name__)

# Load GraphQL queries
entity_details_fragment_gql = (
    pathlib.Path(__file__).parent / "gql/entity_details.gql"
).read_text()
query_entity_gql = (pathlib.Path(__file__).parent / "gql/query_entity.gql").read_text()
related_documents_gql = (
    pathlib.Path(__file__).parent / "gql/related_documents.gql"
).read_text()


def get_entities(urns: List[str]) -> List[dict]:
    """Get detailed information about one or more entities by their DataHub URNs.

    IMPORTANT: Pass multiple URNs in a single call — this is much more efficient than
    calling this tool multiple times. When examining search results, always pass the top
    3-10 result URNs together to compare and find the best match.

    Supports all entity types including datasets, assertions, incidents, dashboards,
    charts, users, groups, and more. Response fields vary by entity type.

    Args:
        urns: List of URN strings

    Returns:
        List of dicts, one per URN. Each entry contains entity details or an "error" key
        if the entity was not found or could not be retrieved.

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            results = get_entities(urns=["urn:li:dataset:(...)"])
    """
    graph = get_graph()

    # Trim whitespace from each URN (defensive against string concatenation issues)
    urns = [urn.strip() for urn in urns]

    results = []
    for urn in urns:
        try:
            # Check if entity exists first
            if not graph.exists(urn):
                logger.warning(f"Entity not found during existence check: {urn}")
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

            # Fetch related documents for supported entity types
            try:
                related_docs_input = {"start": 0, "count": 10}
                related_docs_result = execute_graphql(
                    graph,
                    query=related_documents_gql,
                    variables={"urn": urn, "input": related_docs_input},
                    operation_name="getRelatedDocuments",
                )
                if (
                    related_docs_result
                    and related_docs_result.get("entity")
                    and related_docs_result["entity"].get("relatedDocuments")
                ):
                    result["relatedDocuments"] = clean_related_documents_response(
                        related_docs_result["entity"]["relatedDocuments"]
                    )
            except Exception as e:
                logger.warning(
                    f"Could not fetch related documents for {urn}: {e}. This entity type may not support related documents."
                )

            results.append(clean_get_entities_response(result))

        except Exception as e:
            logger.warning(f"Error fetching entity {urn}: {e}")
            results.append({"error": str(e), "urn": urn})

    return results


def list_schema_fields(
    urn: str,
    keywords: Optional[List[str]] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """List schema fields for a dataset, with optional keyword filtering and pagination.

    Useful when schema fields were truncated in search results (schemaFieldsTruncated present)
    and you need to explore specific columns. Supports pagination for large schemas.

    Args:
        urn: Dataset URN
        keywords: Optional list of keywords to filter schema fields (OR matching).
                 Each string is treated as one keyword (NOT split on whitespace).
                 None returns all fields in priority order (same as get_entities).
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
        # Single keyword - search for exact field name or phrase
        list_schema_fields(urn="urn:li:dataset:(...)", keywords=["user_email"])
        # Returns fields matching "user_email" (like user_email_address, primary_user_email)

        # Multiple keywords (OR matching)
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
    keywords_lower = [kw.lower() for kw in keywords] if keywords is not None else None

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
