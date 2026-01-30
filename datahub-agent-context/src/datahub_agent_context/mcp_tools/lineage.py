"""Tools for getting lineage information."""

import logging
import pathlib
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel

from datahub.errors import ItemNotFoundError
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, FilterDsl, load_filters
from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import clean_gql_response, execute_graphql
from datahub_agent_context.mcp_tools.helpers import (
    _extract_lineage_columns_from_paths,
    _select_results_within_budget,
    clean_get_entities_response,
    inject_urls_for_urns,
    maybe_convert_to_schema_field_urn,
    truncate_descriptions,
)

logger = logging.getLogger(__name__)

# Load GraphQL query
entity_details_fragment_gql = (
    pathlib.Path(__file__).parent / "gql/entity_details.gql"
).read_text()


class AssetLineageDirective(BaseModel):
    """Configuration for lineage query."""

    urn: str
    upstream: bool
    downstream: bool
    max_hops: int
    extra_filters: Optional[Filter]
    max_results: int


class AssetLineageAPI:
    """API for querying asset lineage."""

    def __init__(self) -> None:
        """Initialize lineage API."""
        self.graph = get_graph()

    def get_degree_filter(self, max_hops: int) -> Filter:
        """Get filter for lineage degree (hops).

        Args:
            max_hops: Maximum number of hops to search for lineage

        Returns:
            Filter for degree field

        Raises:
            ValueError: If max_hops is invalid
        """
        if max_hops == 1 or max_hops == 2:
            return FilterDsl.custom_filter(
                field="degree",
                condition="EQUAL",
                values=[str(i) for i in range(1, max_hops + 1)],
            )
        elif max_hops >= 3:
            return FilterDsl.custom_filter(
                field="degree",
                condition="EQUAL",
                values=["1", "2", "3+"],
            )
        else:
            raise ValueError(f"Invalid number of hops: {max_hops}")

    def get_lineage(
        self,
        asset_lineage_directive: AssetLineageDirective,
        query: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get lineage for an asset.

        Args:
            asset_lineage_directive: Lineage query configuration
            query: Optional search query to filter lineage results

        Returns:
            Dictionary with upstreams and/or downstreams fields
        """
        result: Dict[str, Any] = {}

        filter = self.get_degree_filter(asset_lineage_directive.max_hops)
        if asset_lineage_directive.extra_filters:
            filter = FilterDsl.and_(filter, asset_lineage_directive.extra_filters)
        types, compiled_filters = compile_filters(filter)
        variables = {
            "urn": asset_lineage_directive.urn,
            "query": query or "*",
            "start": 0,
            "count": asset_lineage_directive.max_results,
            "types": types,
            "orFilters": compiled_filters,
            "searchFlags": {"skipHighlighting": True, "maxAggValues": 3},
        }
        if asset_lineage_directive.upstream:
            result["upstreams"] = clean_gql_response(
                execute_graphql(
                    self.graph,
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "UPSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )["searchAcrossLineage"]
            )
        if asset_lineage_directive.downstream:
            result["downstreams"] = clean_gql_response(
                execute_graphql(
                    self.graph,
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "DOWNSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )["searchAcrossLineage"]
            )

        return result


def get_lineage(
    urn: str,
    column: Optional[str] = None,
    query: Optional[str] = None,
    filters: Optional[Filter | str] = None,
    upstream: bool = True,
    max_hops: int = 1,
    max_results: int = 30,
    offset: int = 0,
) -> dict:
    """Get upstream or downstream lineage for any entity.

    Set upstream to True for upstream lineage, False for downstream lineage.
    Set `column: null` to get lineage for entire dataset or for entity type other than dataset.
    Setting max_hops to 3 is equivalent to unlimited hops.
    Usage and format of filters is same as that in search tool.

    Args:
        urn: Entity URN
        column: Optional column name for column-level lineage
        query: Optional search query to filter lineage results
        filters: Optional filters to apply
        upstream: True for upstream, False for downstream
        max_hops: Maximum number of hops (1-3+)
        max_results: Maximum number of results to return
        offset: Pagination offset

    Returns:
        Dictionary with upstreams or downstreams field containing:
        - searchResults: List of lineage entities
        - facets: Aggregations
        - start: Starting offset
        - count: Number of results
        - total: Total number of entities
        - offset: Applied offset
        - returned: Number of entities returned
        - hasMore: Whether more results available
        - metadata: Additional metadata for column-level lineage

    PAGINATION:
    Use offset to paginate through large lineage graphs:
    - offset=0, max_results=30 → first 30 entities
    - offset=30, max_results=30 → next 30 entities

    Note: Token budget constraints may return fewer entities than max_results.
    Check the returned metadata (hasMore, returned, etc.) to understand truncation.

    QUERY PARAMETER - Search within lineage results:
    You can filter lineage results using the `query` parameter with same /q syntax as search tool:
    - /q workspace.growthgestaofin → find tables in specific schema
    - /q customer+transactions → find entities with both terms
    - /q looker OR tableau → find dashboards on either platform
    - /q * → get all lineage results (default)

    Examples:
    - Find specific table in 643 downstreams: query="workspace.growthgestaofin.qs_retention"
    - Find Looker dashboards in lineage: query="/q tag:looker"
    - Get all results: query="*" or omit parameter

    COUNT PARAMETER - Control result size:
    - Default: 30 results
    - For aggregation: count=30 is sufficient (facets computed on ALL items server-side)
    - For finding specific item: Increase count or use query to filter
    - Example: count=100 for larger result sets

    WHEN TO USE QUERY vs COUNT:
    - User asks "is X affected?" → Use query to filter for X specifically
    - Large lineage (>30 items) → Keep count=30, use facets for aggregation
    - Need complete list → Increase count only if total ≤100

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = get_lineage(urn="urn:li:dataset:(...)", upstream=True)
    """
    graph = get_graph()
    # Normalize column parameter: Some LLMs pass the string "null" instead of JSON null.
    # Note: This means columns literally named "null" cannot be queried.
    if column == "null" or column == "":
        column = None

    # Parse filters if provided as string
    if isinstance(filters, str):
        filters = load_filters(filters)

    lineage_api = AssetLineageAPI()

    urn = maybe_convert_to_schema_field_urn(urn, column)
    asset_lineage_directive = AssetLineageDirective(
        urn=urn,
        upstream=upstream,
        downstream=not upstream,
        max_hops=max_hops,
        extra_filters=filters,
        max_results=max_results,
    )
    lineage = lineage_api.get_lineage(asset_lineage_directive, query=query)
    inject_urls_for_urns(graph, lineage, ["*.searchResults[].entity"])
    truncate_descriptions(lineage)

    # Track if this is column-level lineage for metadata
    is_column_level_lineage = column is not None

    # Apply offset, entity-level truncation, and cleaning to upstreams/downstreams
    for direction in ["upstreams", "downstreams"]:
        if direction_results := lineage.get(direction):
            if search_results := direction_results.get("searchResults"):
                # Extract lineageColumns from paths for column-level lineage
                search_results = _extract_lineage_columns_from_paths(search_results)
                direction_results["searchResults"] = search_results

                total_available = len(search_results)

                # Apply offset (skip first N entities)
                if offset >= total_available:
                    direction_results["searchResults"] = []
                    direction_results["offset"] = offset
                    direction_results["returned"] = 0
                    direction_results["hasMore"] = False
                    continue

                # Skip offset and apply token budget using generic helper
                results_after_offset = search_results[offset:]

                # Lambda to clean entity in place and return it for token counting
                def get_cleaned_entity(result_item: dict) -> dict:
                    entity = result_item.get("entity", {})
                    cleaned = clean_get_entities_response(entity)
                    result_item["entity"] = cleaned  # Mutate in place
                    return cleaned  # Return for token counting

                # Get results within budget (entities cleaned in place, degree preserved)
                selected_results = list(
                    _select_results_within_budget(
                        results=iter(results_after_offset),
                        fetch_entity=get_cleaned_entity,
                        max_results=max_results,
                    )
                )

                # Update results and add metadata
                direction_results["searchResults"] = selected_results
                direction_results["offset"] = offset
                direction_results["returned"] = len(selected_results)
                direction_results["hasMore"] = (
                    offset + len(selected_results)
                ) < total_available

                if len(selected_results) < len(results_after_offset):
                    direction_results["truncatedDueToTokenBudget"] = True

                logger.info(
                    f"get_lineage {direction}: Returned {len(selected_results)}/{total_available} entities "
                    f"(offset={offset}, hasMore={direction_results['hasMore']})"
                )

    # Add metadata for column-level lineage responses
    if is_column_level_lineage:
        lineage["metadata"] = {
            "queryType": "column-level-lineage",
            "groupedBy": "dataset",
            "fields": {
                "lineageColumns": {
                    "description": "Columns in each dataset that have a lineage relationship with the source column",
                    "semantics": {
                        "downstream": "Columns derived from the source column",
                        "upstream": "Columns that the source column depends on",
                    },
                }
            },
        }

    return lineage


def _find_result_with_target_urn(
    search_results: List[dict],
    target_urn: str,
    is_column_level: bool,
) -> Optional[dict]:
    """Find the search result that contains the target URN.

    For column-level lineage: Searches paths to find one ending with target column URN
    For dataset-level lineage: Matches entity URN directly

    Args:
        search_results: List of lineage search results
        target_urn: URN to search for (dataset or schemaField)
        is_column_level: Whether this is column-level lineage

    Returns:
        The search result containing the target, or None if not found
    """
    for result in search_results:
        if is_column_level:
            # Column-level: Check if any path ends with target column URN
            paths = result.get("paths") or []
            for path_obj in paths:
                if not path_obj:
                    continue
                path = path_obj.get("path") or []
                if path and path[-1].get("urn") == target_urn:
                    return result
        else:
            # Dataset-level: Match entity URN directly
            if result.get("entity", {}).get("urn") == target_urn:
                return result

    return None


def _find_upstream_lineage_path(
    query_urn: str,
    search_for_urn: str,
    source_urn: str,
    target_urn: str,
    source_column: Optional[str],
    target_column: Optional[str],
    semantic_direction: Literal["upstream", "downstream"],
) -> dict:
    """Internal helper to find upstream lineage path.

    Always queries upstream lineage (more bounded than downstream).

    KEY INSIGHT: Lineage is isotropic (symmetric):
    - If B is in A's downstream, then A is in B's upstream
    - The path is the same, just viewed from different ends
    - Therefore, we can always query upstream and reverse the path for downstream queries

    This optimization significantly reduces response sizes:
    - Upstream: Typically 10-100 results (bounded by data sources)
    - Downstream: Can be 1000s of results (unlimited consumers)

    Args:
        query_urn: URN to query lineage from (could be source or target depending on semantic direction)
        search_for_urn: URN to search for in results
        source_urn: Original source URN (for response metadata)
        target_urn: Original target URN (for response metadata)
        source_column: Original source column (for response metadata)
        target_column: Original target column (for response metadata)
        semantic_direction: User's requested direction (for metadata, not query direction)

    Returns:
        Dictionary with paths and metadata

    Raises:
        ItemNotFoundError: If no lineage path found
    """
    graph = get_graph()
    # Get lineage with paths using the API directly (always upstream)
    lineage_api = AssetLineageAPI()
    asset_lineage_directive = AssetLineageDirective(
        urn=query_urn,
        upstream=True,  # Always upstream
        downstream=False,
        max_hops=10,  # Higher to ensure we find target
        extra_filters=None,
        max_results=100,  # Need enough results to find target
    )
    lineage = lineage_api.get_lineage(asset_lineage_directive, query="*")

    # Clean up the response
    inject_urls_for_urns(graph, lineage, ["*.searchResults[].entity"])
    truncate_descriptions(lineage)

    # Get upstream results (always querying upstream)
    search_results = lineage.get("upstreams", {}).get("searchResults", [])

    if not search_results:
        raise ItemNotFoundError(
            f"No lineage found from {source_urn}"
            + (f".{source_column}" if source_column else "")
        )

    # Find the result containing the target URN
    target_result = _find_result_with_target_urn(
        search_results=search_results,
        target_urn=search_for_urn,
        is_column_level=(target_column is not None),
    )

    if not target_result:
        raise ItemNotFoundError(
            f"No lineage path found from {source_urn}"
            + (f".{source_column}" if source_column else "")
            + f" to {target_urn}"
            + (f".{target_column}" if target_column else "")
        )

    # Extract paths array (with QUERY URNs as-is)
    paths = target_result.get("paths", [])
    if not paths:
        raise ValueError(
            "Target found but no path information available. "
            "This may indicate the entities are directly connected without intermediate steps."
        )

    # Clean the paths response
    cleaned_paths = clean_gql_response(paths)

    # Check if any paths contain QUERY entities (with safe null handling)
    has_queries = any(
        any(
            entity.get("type") == "QUERY"
            for entity in (path_obj.get("path") or [])
            if entity  # Skip None entities
        )
        for path_obj in cleaned_paths
        if path_obj  # Skip None path objects
    )

    # Build metadata
    paths_metadata: dict[str, str] = {
        "description": "Array of lineage paths showing transformation chains from source to target",
        "structure": "Each path contains alternating entities (SCHEMA_FIELD or DATASET) and optional transformation QUERY entities",
    }

    # Add query enrichment note only when queries are present
    if has_queries:
        paths_metadata["queryEntities"] = (
            "QUERY entities are returned as URNs only. "
            "Use get_entities(query_urn) to fetch SQL statement and other query details."
        )

    metadata = {
        "queryType": "lineage-path-trace",
        "direction": semantic_direction,
        "pathType": "column-level" if source_column else "dataset-level",
        "fields": {"paths": paths_metadata},
    }

    # Build response with metadata
    return {
        "metadata": metadata,
        "source": {
            "urn": source_urn,
            **({"column": source_column} if source_column else {}),
        },
        "target": {
            "urn": target_urn,
            **({"column": target_column} if target_column else {}),
        },
        "pathCount": len(cleaned_paths),
        "paths": cleaned_paths,
    }


def _find_lineage_path(
    query_urn: str,
    target_full_urn: str,
    source_urn: str,
    target_urn: str,
    source_column: Optional[str],
    target_column: Optional[str],
    direction: Literal["upstream", "downstream"],
) -> dict:
    """Internal helper to find lineage path in a specific direction.

    Always queries upstream internally (more efficient), but maintains the
    semantic direction in the API. For downstream queries, swaps source/target
    and reverses the path.

    Separated from main function to support auto-discovery logic.

    Args:
        query_urn: URN to query lineage from
        target_full_urn: Target URN to find
        source_urn: Source dataset URN
        target_urn: Target dataset URN
        source_column: Source column name
        target_column: Target column name
        direction: Direction to search ("upstream" or "downstream")

    Returns:
        Dictionary with paths and metadata

    Raises:
        ItemNotFoundError: If no lineage path found
    """
    if direction == "downstream":
        # User semantic: source flows TO target (source → target)
        # Implementation: Query target's upstream to find source
        # Then reverse the path to show source → target
        result = _find_upstream_lineage_path(
            query_urn=target_full_urn,  # Query from target
            search_for_urn=query_urn,  # Search for source
            source_urn=source_urn,
            target_urn=target_urn,
            source_column=source_column,
            target_column=target_column,
            semantic_direction=direction,
        )

        # Reverse paths to show source → target order
        # Structure: result["paths"] = [{"path": [entity1, entity2, ...]}, ...]
        # Each path is an array of entities (SCHEMA_FIELD/DATASET/QUERY)
        # Upstream query returns: [target, query, intermediate, query, source]
        # We reverse to: [source, query, intermediate, query, target]
        for path_obj in result.get("paths", []):
            if path_obj and "path" in path_obj:
                path_obj["path"] = list(reversed(path_obj["path"]))

        return result
    else:
        # User semantic: source depends ON target (target → source)
        # Implementation: Query source's upstream to find target
        # Path is already in correct order (target → source)
        return _find_upstream_lineage_path(
            query_urn=query_urn,  # Query from source
            search_for_urn=target_full_urn,  # Search for target
            source_urn=source_urn,
            target_urn=target_urn,
            source_column=source_column,
            target_column=target_column,
            semantic_direction=direction,
        )


def get_lineage_paths_between(
    source_urn: str,
    target_urn: str,
    source_column: Optional[str] = None,
    target_column: Optional[str] = None,
    direction: Optional[Literal["upstream", "downstream"]] = None,
) -> dict:
    """Get detailed lineage path(s) between two specific entities or columns.

    Returns the paths array from searchAcrossLineage, showing the exact transformation
    chain(s) including intermediate entities, columns, and transformation query URNs.

    Unlike get_lineage() which returns all lineage targets with compact lineageColumns,
    this tool focuses on ONE specific target and returns detailed path information.

    Args:
        source_urn: URN of the source dataset
        target_urn: URN of the target dataset
        source_column: Optional column name in source dataset
        target_column: Optional column name in target dataset (required if source_column provided)
        direction: Optional direction to search. If None (default), automatically discovers
                  the path by trying downstream first, then upstream. Specify "downstream" or
                  "upstream" explicitly for better performance if you know the direction.

    Returns:
        Dictionary with:
        - source: Source entity/column info
        - target: Target entity/column info
        - paths: Array of path objects from GraphQL (with QUERY URNs)
        - pathCount: Number of paths found
        - metadata: Query metadata including direction and path type

    Examples:
        # Column-level paths
        paths_result = get_lineage_paths_between(
            source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.base_table,PROD)",
            target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.final_table,PROD)",
            source_column="user_id",
            target_column="customer_id"
        )
        # Returns paths with QUERY URNs showing transformation chain

        # Fetch SQL for specific query of interest
        query_details = get_entities(paths_result["paths"][0]["path"][1]["urn"])

        # Dataset-level paths (auto-discover direction)
        get_lineage_paths_between(
            source_urn="urn:li:dataset:(...):base_table",
            target_urn="urn:li:dataset:(...):final_table"
        )

        # Explicit direction for better performance
        get_lineage_paths_between(
            source_urn="urn:li:dataset:(...):base_table",
            target_urn="urn:li:dataset:(...):final_table",
            direction="downstream"
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = get_lineage_paths_between(
                source_urn="urn:li:dataset:(...)",
                target_urn="urn:li:dataset:(...)"
            )

    Raises:
        ValueError: If column parameters are mismatched or invalid
        ItemNotFoundError: If no lineage path found
    """
    # Normalize column parameters
    if source_column == "null" or source_column == "":
        source_column = None
    if target_column == "null" or target_column == "":
        target_column = None

    # Validate: if either column is specified, must be column-level lineage
    if (source_column is None) != (target_column is None):
        raise ValueError(
            "Both source_column and target_column must be provided for column-level lineage, "
            "or both must be None for dataset-level lineage"
        )

    # Convert to schema field URN if column specified
    query_urn = maybe_convert_to_schema_field_urn(source_urn, source_column)
    target_full_urn = maybe_convert_to_schema_field_urn(target_urn, target_column)

    # Auto-discover direction if not specified
    if direction is None:
        # Try downstream first (more common use case)
        try:
            result = _find_lineage_path(
                query_urn=query_urn,
                target_full_urn=target_full_urn,
                source_urn=source_urn,
                target_urn=target_urn,
                source_column=source_column,
                target_column=target_column,
                direction="downstream",
            )
            result["metadata"]["direction"] = "auto-discovered-downstream"
            result["metadata"]["note"] = (
                "Direction was automatically discovered. Specify direction='downstream' or 'upstream' explicitly for better performance."
            )
            return result
        except ItemNotFoundError:
            # Try upstream as fallback
            try:
                result = _find_lineage_path(
                    query_urn=query_urn,
                    target_full_urn=target_full_urn,
                    source_urn=source_urn,
                    target_urn=target_urn,
                    source_column=source_column,
                    target_column=target_column,
                    direction="upstream",
                )
                result["metadata"]["direction"] = "auto-discovered-upstream"
                result["metadata"]["note"] = (
                    "Direction was automatically discovered. Specify direction='downstream' or 'upstream' explicitly for better performance."
                )
                return result
            except ItemNotFoundError:
                # Not found in either direction
                raise ItemNotFoundError(
                    f"No lineage path found between {source_urn}"
                    + (f".{source_column}" if source_column else "")
                    + f" and {target_urn}"
                    + (f".{target_column}" if target_column else "")
                    + " in either upstream or downstream direction"
                ) from None
    else:
        # User specified direction explicitly
        return _find_lineage_path(
            query_urn=query_urn,
            target_full_urn=target_full_urn,
            source_urn=source_urn,
            target_urn=target_urn,
            source_column=source_column,
            target_column=target_column,
            direction=direction,
        )
