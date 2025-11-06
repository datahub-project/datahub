"""
Main semantic search implementation using keyword expansion and reranking.
"""

import re
from typing import List, Optional, TypeVar

from datahub.sdk.search_filters import Filter
from loguru import logger

from datahub_integrations.mcp.mcp_server import (
    _search_implementation,
    _select_results_within_budget,
    clean_get_entities_response,
    truncate_descriptions,
)
from datahub_integrations.smart_search.models import SmartSearchResponse
from datahub_integrations.smart_search.reranker import (
    RerankResult,
    create_reranker,
)

T = TypeVar("T")


def _select_entities_by_score_quality(
    rerank_results: List[RerankResult],
    initial_k: int = 4,
    max_entities: int = 30,
    relative_drop: float = 0.5,
    plateau_threshold: float = 0.05,
    plateau_length: int = 3,
) -> tuple[List[RerankResult], bool, Optional[str]]:
    """
    Select entities based on score quality with adaptive cutoff detection.

    Returns entities with quality signals to help LLM understand if more candidates exist.

    Strategy:
    1. Always include first initial_k entities (guaranteed minimum)
    2. Continue beyond initial_k if:
       - Haven't reached max_entities AND
       - No large score drop detected (relative_drop threshold) AND
       - No score plateau detected (flat tail)
    3. Signal hasMore=True only when we hit limits (not when we stopped due to poor quality)

    Args:
        rerank_results: Reranked results sorted by score (descending)
        initial_k: Minimum entities to return unconditionally (default: 4)
        max_entities: Maximum entities to return (default: 30)
        relative_drop: Max allowed drop from top score as fraction (0.5 = 50% drop)
        plateau_threshold: Max score variation to be considered plateau (0.05 = 5%)
        plateau_length: Consecutive similar scores indicating plateau (default: 3)

    Returns:
        Tuple of:
        - Selected rerank results
        - hasMore: True if more quality results exist beyond selection (only for limit-based cutoffs, not quality-based)
        - cutoffReason: Why we stopped (None if returned all, or reason string if cutoff triggered)
    """
    if not rerank_results:
        return [], False, None

    total_available = len(rerank_results)

    # Handle case where we have fewer than initial_k
    if total_available <= initial_k:
        return rerank_results, False, None

    top_score = rerank_results[0].score
    selected = []
    cutoff_reason = None

    for i, result in enumerate(rerank_results):
        # Always include first initial_k
        if i < initial_k:
            selected.append(result)
            continue

        # Stop if we've hit max_entities
        if len(selected) >= max_entities:
            cutoff_reason = "max_entities_reached"
            break

        # Check for large score drop (quality cliff)
        score_drop = top_score - result.score
        if score_drop > (top_score * relative_drop):
            cutoff_reason = "score_drop_detected"
            logger.info(
                f"Score drop detected at position {i}: "
                f"score={result.score:.4f}, drop from top={score_drop:.4f} "
                f"(>{relative_drop * 100:.0f}% of top score {top_score:.4f})"
            )
            break

        # Check for plateau (flat tail of low-quality results)
        if i >= initial_k + plateau_length - 1:
            # Look at last plateau_length scores including current
            window_start = i - plateau_length + 1
            window_scores = [
                rerank_results[j].score for j in range(window_start, i + 1)
            ]
            score_range = max(window_scores) - min(window_scores)

            if score_range <= plateau_threshold:
                cutoff_reason = "score_plateau_detected"
                logger.info(
                    f"Score plateau detected at position {i}: "
                    f"last {plateau_length} scores vary by only {score_range:.4f}"
                )
                break

        # Quality checks passed, include this entity
        selected.append(result)

    # Determine if more quality results are available
    # Only signal has_more when we hit limits (not when we stopped due to poor quality)
    has_more = (
        cutoff_reason in ("max_entities_reached", None)
        and len(selected) < total_available
    )

    return selected, has_more, cutoff_reason


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
    the most relevant results (typically 4-30 entities) with DETAILED, FULL entity information.
    Dynamically adjusts result count based on score quality - more results when quality is high,
    fewer when there's a clear quality drop. Use this as your primary search tool.

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
        - candidates_reviewed: Number of candidates found in initial keyword search and reviewed by AI
        - candidates_selected: Number selected after AI reranking and quality filtering
        - returned_count: Number of results actually returned (may be less due to token budget)
        - has_more_selected_results: True if more quality results exist beyond what was returned (only True for limit-based cutoffs like token_budget_exceeded or max_entities_reached, not for quality-based cutoffs like score_drop_detected or score_plateau_detected)
        - selection_cutoff_reason: Why candidate selection was limited (e.g., "score_drop_detected", "score_plateau_detected", "token_budget_exceeded", "max_entities_reached")

    IMPORTANT: This tool returns DETAILED and FULL entity information including schema fields.
    DO NOT call get_entities on these results - all detailed information is already included.

    If has_more_selected_results is True, there are additional quality results worth exploring.
    The candidates_reviewed field shows how many were initially found and evaluated.

    Args:
        semantic_query: User's natural language query (used for AI semantic reranking)
        keyword_search_query: Keyword search query using /q syntax
        filters: Optional entity/platform/domain filters
    """
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
            candidates_reviewed=0,
            candidates_selected=0,
            returned_count=0,
            has_more_selected_results=False,
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

    # Select entities based on score quality (adaptive cutoff)
    selected_rerank, has_more, selection_cutoff_reason = (
        _select_entities_by_score_quality(
            rerank_results=rerank_results,
            initial_k=4,  # Minimum entities to return
            max_entities=30,  # Upper limit
            relative_drop=0.5,  # 50% drop from top = quality cliff
            plateau_threshold=0.05,  # 5% variation = plateau
            plateau_length=3,  # 3 consecutive similar scores
        )
    )

    logger.info(
        f"Quality-based selection: {len(selected_rerank)}/{len(rerank_results)} entities "
        f"(top score: {rerank_results[0].score if rerank_results else 0:.4f}, "
        f"selection_cutoff_reason: {selection_cutoff_reason}, has_more: {has_more})"
    )

    # Lambda to clean entity in place and return it for token counting
    def get_cleaned_entity(rerank_result: RerankResult) -> dict:
        candidate = candidates[rerank_result.index]
        raw_entity = candidate.get("entity", {})
        truncate_descriptions(raw_entity)
        cleaned_entity = clean_get_entities_response(raw_entity)
        candidate["entity"] = cleaned_entity  # Store back to candidate
        return cleaned_entity  # Return for token counting

    # Select results within budget and extract cleaned entities
    # Entities are cleaned lazily only for results that fit in budget
    cleaned_results = [
        candidates[rr.index]["entity"]
        for rr in _select_results_within_budget(
            results=iter(selected_rerank),
            fetch_entity=get_cleaned_entity,
            max_results=len(selected_rerank),  # Use quality-based selection count
        )
    ]

    # If token budget caused further reduction, update has_more
    if len(cleaned_results) < len(selected_rerank):
        has_more = True
        if not selection_cutoff_reason:
            selection_cutoff_reason = "token_budget_exceeded"

    logger.info(
        f"Returning {len(cleaned_results)} results "
        f"(reviewed {len(candidates)} candidates, selected {len(selected_rerank)}, "
        f"has_more: {has_more}, selection_cutoff_reason: {selection_cutoff_reason})"
    )

    return SmartSearchResponse(
        results=cleaned_results,
        facets=facets,
        candidates_reviewed=len(candidates),
        candidates_selected=len(selected_rerank),
        returned_count=len(cleaned_results),
        has_more_selected_results=has_more,
        selection_cutoff_reason=selection_cutoff_reason,
    )
