"""
Search query building and execution for two-pass ersatz semantic search.
"""

from typing import Dict, List, Optional

from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import Filter
from loguru import logger

from datahub_integrations.mcp.mcp_server import (
    _search_implementation,
    with_datahub_client,
)
from datahub_integrations.smart_search.models import SearchConfig


def build_query_string(
    anchors: List[str],
    phrases: Optional[List[str]] = None,
    synonyms: Optional[List[str]] = None,
) -> str:
    """
    Build a /q query string from keywords using OR logic.

    Args:
        anchors: Core keywords to combine with OR
        phrases: Multi-word phrases to include with proximity matching
        synonyms: Synonym terms (used for Pass B, not Pass A)

    Returns:
        Query string like "/q anchor1 OR anchor2 OR \"phrase1\"~2"
    """
    query_parts = []

    # Add anchors with OR
    if anchors:
        query_parts.extend(anchors)

    # Add phrases with proximity matching (~2 means within 2 positions)
    if phrases:
        for phrase in phrases:
            query_parts.append(f'"{phrase}"~2')

    # Add synonyms (typically only used in Pass B)
    if synonyms:
        query_parts.extend(synonyms)

    # Join with OR and prepend /q
    if query_parts:
        query = " OR ".join(query_parts)
        return f"/q {query}"
    else:
        return "*"


def execute_two_pass_search(
    client: DataHubClient,
    anchors: List[str],
    phrases: Optional[List[str]],
    synonyms: Optional[List[str]],
    filters: Optional[Filter | str],
    config: SearchConfig,
) -> Dict:
    """
    Execute two-pass search: Pass A (anchors+phrases), Pass B (synonyms).

    Args:
        client: DataHub client to use for searches
        anchors: Core keywords for Pass A
        phrases: Multi-word expressions for Pass A
        synonyms: Alternative terms for Pass B
        filters: Optional search filters
        config: Search configuration with budgets

    Returns:
        Dictionary with merged results and metadata
    """
    results_a = []
    results_b = []

    # Use context manager to set the client for _search_implementation
    with with_datahub_client(client):
        # Pass A: Anchors + Phrases (high precision)
        if anchors or phrases:
            query_a = build_query_string(anchors=anchors, phrases=phrases)
            logger.info(f"Pass A query: {query_a}")

            response_a = _search_implementation(
                query=query_a,
                filters=filters,
                num_results=config.anchors_budget,
                search_strategy="ersatz_semantic",  # Use ersatz for rich entity details
            )

            results_a = response_a.get("searchResults", [])
            logger.info(f"Pass A returned {len(results_a)} results")

        # Pass B: Synonyms (adds recall)
        if synonyms:
            query_b = build_query_string(anchors=[], phrases=None, synonyms=synonyms)
            logger.info(f"Pass B query: {query_b}")

            response_b = _search_implementation(
                query=query_b,
                filters=filters,
                num_results=config.synonyms_budget,
                search_strategy="ersatz_semantic",  # Use ersatz for rich entity details
            )

            results_b = response_b.get("searchResults", [])
            logger.info(f"Pass B returned {len(results_b)} results")

    # Merge and deduplicate by URN
    merged_results = _merge_and_dedupe(results_a, results_b, config.max_candidates)

    return {
        "results": merged_results,
        "pass_a_count": len(results_a),
        "pass_b_count": len(results_b),
        "merged_count": len(merged_results),
    }


def _merge_and_dedupe(
    results_a: List[Dict],
    results_b: List[Dict],
    max_candidates: int,
) -> List[Dict]:
    """
    Merge results from both passes and deduplicate by URN.

    Pass A results are prioritized (appear first).

    Args:
        results_a: Results from Pass A (anchors + phrases)
        results_b: Results from Pass B (synonyms)
        max_candidates: Maximum number of candidates to return

    Returns:
        Merged and deduplicated list of results
    """
    seen_urns = set()
    merged = []

    # Add Pass A results first (higher priority)
    for result in results_a:
        urn = result.get("entity", {}).get("urn")
        if urn and urn not in seen_urns:
            seen_urns.add(urn)
            merged.append(result)
            if len(merged) >= max_candidates:
                break

    # Add Pass B results that aren't already in Pass A
    for result in results_b:
        if len(merged) >= max_candidates:
            break
        urn = result.get("entity", {}).get("urn")
        if urn and urn not in seen_urns:
            seen_urns.add(urn)
            merged.append(result)

    logger.info(f"Merged to {len(merged)} unique results (max: {max_candidates})")
    return merged
