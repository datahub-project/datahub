"""
Main semantic search implementation using keyword expansion and reranking.
"""

import re
from typing import Generator, List, Optional

from datahub.sdk.search_filters import Filter
from loguru import logger

from datahub_integrations.chat.context_reducer import TokenCountEstimator
from datahub_integrations.mcp.mcp_server import (
    _search_implementation,
    clean_get_entities_response,
    truncate_descriptions,
)
from datahub_integrations.mcp.tool import TOOL_RESPONSE_TOKEN_LIMIT
from datahub_integrations.smart_search.models import SmartSearchResponse
from datahub_integrations.smart_search.reranker import (
    RerankResult,
    create_reranker,
)


def _select_results_within_budget(
    rerank_results: List[RerankResult],
    candidates: List[dict],
    max_results: int = 10,
    token_budget: Optional[int] = None,
) -> Generator[dict, None, None]:
    """
    Generator that yields top reranked results within token budget.

    Yields entities until:
    - max_results reached, OR
    - token_budget would be exceeded (and we have at least 1 result)

    Args:
        rerank_results: Sorted rerank results
        candidates: Original candidates with entities
        max_results: Maximum number of results to return
        token_budget: Token budget (defaults to 90% of TOOL_RESPONSE_TOKEN_LIMIT)

    Yields:
        Cleaned entities that fit within budget
    """
    if token_budget is None:
        # Use 90% of limit as safety buffer:
        # - Token estimation is approximate, not exact
        # - Response wrapper ({"results": ..., "facets": ..., "total_candidates": N}) adds overhead
        # - Better to return fewer results that fit than exceed limit
        token_budget = int(TOOL_RESPONSE_TOKEN_LIMIT * 0.9)

    total_tokens = 0
    results_count = 0

    for rerank_result in rerank_results[:max_results]:
        candidate = candidates[rerank_result.index]
        raw_entity = candidate.get("entity", {})

        # Truncate all descriptions to prevent overly long content
        # (entity descriptions, tag descriptions, glossary terms, etc.)
        truncate_descriptions(raw_entity)

        entity = clean_get_entities_response(raw_entity)

        # Fast token estimation (no JSON serialization)
        entity_tokens = TokenCountEstimator.estimate_dict_tokens(entity)

        # Check if adding this entity would exceed budget
        if total_tokens + entity_tokens > token_budget:
            if results_count == 0:
                # Always yield at least 1 result
                logger.warning(
                    f"First result ({entity_tokens:,} tokens) exceeds budget ({token_budget:,}), "
                    "yielding it anyway"
                )
                yield entity
                results_count += 1
                total_tokens += entity_tokens
            else:
                # Have at least 1 result, stop here to stay within budget
                logger.info(
                    f"Stopping at {results_count} results (next would exceed {token_budget:,} token budget)"
                )
                break
        else:
            yield entity
            results_count += 1
            total_tokens += entity_tokens

    logger.info(
        f"Selected {results_count} results using {total_tokens:,} tokens "
        f"(budget: {token_budget:,})"
    )


def _extract_keywords_from_query(keyword_search_query: str) -> List[str]:
    """
    Extract individual keywords from a /q search query.

    Note: This intentionally splits quoted phrases like "premium plan" into
    separate keywords ["premium", "plan"]. This is correct because:
    - DataHub's search engine handles the actual phrase matching in the query
    - These keywords are only used to prioritize which entity fields to include
      in the text representation for reranking (via _sort_by_keyword_matches)
    - The reranker sees the full entity text and understands phrase context

    Example:
        "/q \"premium plan\" OR organization" -> ["premium", "plan", "organization"]

    Args:
        keyword_search_query: Query like "/q premium+plan OR organization+subscription"

    Returns:
        List of unique keywords: ["premium", "plan", "organization", "subscription"]
    """
    # Remove /q prefix
    query = keyword_search_query.replace("/q ", "").strip()

    # First extract quoted phrases (preserve multi-word phrases)
    phrases = re.findall(r'"([^"]+)"', query)
    # Remove quoted sections from query
    query = re.sub(r'"[^"]+"', "", query)

    # Split remaining by operators and special chars
    tokens = re.split(r"[\s\+\(\)]", query)

    keywords = []
    for token in tokens:
        token = token.strip().lower()
        # Skip operators and empty strings (using set for O(1) lookup)
        # Note: + is equivalent to AND in search syntax
        if not token or token in {"or", "and", "not", "+"}:
            continue

        # Handle field syntax (name:value -> extract value only)
        if ":" in token:
            token = token.split(":", 1)[1]

        # Remove proximity markers like ~2
        token = re.sub(r"~\d+", "", token)
        # Remove wildcards
        token = token.rstrip("*")

        if token:
            keywords.append(token)

    # Add phrase keywords (split into words)
    for phrase in phrases:
        keywords.extend(phrase.lower().split())

    # Return unique keywords (dedupe, preserve order)
    return list(dict.fromkeys(keywords))


def smart_search(
    semantic_query: str,
    keyword_search_query: str,
    filters: Optional[Filter | str] = None,
) -> SmartSearchResponse:
    """Smart search across DataHub entities with AI-powered relevance ranking.

    Performs keyword search (fetching ~100 candidates) and uses AI to identify and return
    the top 5 most relevant results with DETAILED, FULL entity information. Use this as your
    primary search tool.

    KEYWORD SEARCH QUERY SYNTAX:
    - **Always start queries with /q**
    - **Use + operator for AND** (strongly recommended for semantic relevance!)
    - Supports boolean logic: AND (+), OR, NOT, parentheses, wildcards
    - Examples:
      • /q customer+email → requires both terms (finds customer email data)
      • /q premium+plan+organization → requires all three (very focused, best results)
      • /q revenue+quarterly OR sales+quarterly → alternative combinations
      • /q pii+contact OR sensitive+personal → finds PII/sensitive contact data
      • /q user* → wildcard matching (user_profile, user_data, user_events)
      • /q "pricing plan" → exact phrase
      • /q (revenue OR sales) AND quarterly → complex combinations

    **RECOMMENDED: Use + for focused, semantically relevant results**
    - /q premium+pricing+plan → finds tables specifically about premium pricing plans ✅ GOOD
    - /q premium OR pricing OR plan → finds any table with any of these words ⚠️ TOO BROAD

    Using + ensures entities match multiple concepts, which are usually more relevant.
    The AI ranking works best when you provide focused keyword combinations.

    FILTERS:
    Same format as regular search:
    - {"entity_type": ["DATASET"]} → only tables
    - {"platform": ["snowflake"]} → specific platform
    - {"and": [{"entity_type": ["DATASET"]}, {"platform": ["snowflake"]}]} → combine filters

    EXAMPLES:

    Example 1 - Premium pricing plans:
    ```python
    smart_search(
        semantic_query="find organizations on premium pricing plan",
        keyword_search_query="/q premium+plan+organization OR premium+pricing OR organization+subscription",
        filters={"entity_type": ["DATASET"]}
    )
    ```

    Example 2 - Customer PII data:
    ```python
    smart_search(
        semantic_query="find customer email and phone data",
        keyword_search_query="/q customer+email OR customer+phone OR pii+contact",
        filters={"entity_type": ["DATASET"]}
    )
    ```

    Example 3 - Revenue dashboards:
    ```python
    smart_search(
        semantic_query="quarterly revenue dashboards",
        keyword_search_query="/q revenue+quarterly OR sales+quarterly",
        filters={"entity_type": ["DASHBOARD"]}
    )
    ```

    Returns:
        Dictionary with:
        - results: Array of DETAILED, FULL entity objects with all metadata.
            These entities have complete information - you do NOT need to call get_entities.
        - facets: Aggregated metadata (platforms, tags, domains, etc.) across all results
        - total_candidates: Number of candidates found before AI reranking

    IMPORTANT: This tool returns DETAILED and FULL entity information including schema fields.
    DO NOT call get_entities on these results - all detailed information is already included.

    Args:
        semantic_query: User's natural language query (used for AI semantic reranking)
        keyword_search_query: Keyword search query using /q syntax
        filters: Optional entity/platform/domain filters
    """
    # Internal constants (not exposed to agent)
    MAX_RESULTS = 5

    logger.info(
        f"smart_search called with semantic_query='{semantic_query}', "
        f"keyword_search_query='{keyword_search_query}', filters={filters}"
    )

    # Execute search using the provided keyword query
    # Note: Assumes DataHub client is already set in context by caller
    search_response = _search_implementation(
        query=keyword_search_query,
        filters=filters,
        num_results=100,  # Fetch candidates for AI reranking
        search_strategy="ersatz_semantic",
    )

    candidates = search_response.get("searchResults", [])
    facets = search_response.get("facets", [])

    if not candidates:
        logger.info("No candidates found, returning empty results")
        return SmartSearchResponse(
            results=[],
            facets=facets,
            total_candidates=0,
        )

    logger.info(f"Found {len(candidates)} candidates, reranking")

    # Extract entities for reranking
    entities = [candidate.get("entity", {}) for candidate in candidates]

    # Rerank - reranker handles text generation internally
    logger.info(f"Calling reranker for {len(entities)} entities")
    reranker = create_reranker()  # Uses env var to choose Cohere or LLM
    rerank_results = reranker.rerank(
        semantic_query=semantic_query,
        entities=entities,
        keyword_search_query=keyword_search_query,
    )

    # Sort candidates by rerank scores and take top MAX_RESULTS
    scored_candidates = []
    for rerank_result in rerank_results:
        candidate = candidates[rerank_result.index]
        scored_candidates.append(
            {
                **candidate,
                "score": rerank_result.score,
            }
        )

    # Already sorted by reranker, just take top MAX_RESULTS
    final_results = scored_candidates[:MAX_RESULTS]

    logger.info(
        f"Returning {len(final_results)} results "
        f"(from {len(candidates)} candidates, "
        f"top score: {rerank_results[0].score if rerank_results else 0:.4f})"
    )

    # Use generator to select results within token budget
    cleaned_results = list(
        _select_results_within_budget(
            rerank_results=rerank_results,
            candidates=candidates,
            max_results=MAX_RESULTS,
        )
    )

    return SmartSearchResponse(
        results=cleaned_results,
        facets=facets,
        total_candidates=len(candidates),
    )
