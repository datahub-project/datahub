"""Document tools for DataHub MCP server."""

import pathlib
from typing import Any, Dict, List, Literal, Optional

import re2  # type: ignore[import-untyped]
from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from ..version_requirements import min_version

# Load GraphQL queries at module level (no circular dependency here)
document_search_gql = (
    pathlib.Path(__file__).parent.parent / "gql/document_search.gql"
).read_text()
document_semantic_search_gql = (
    pathlib.Path(__file__).parent.parent / "gql/document_semantic_search.gql"
).read_text()
document_content_gql = (
    pathlib.Path(__file__).parent.parent / "gql/document_content.gql"
).read_text()


def _merge_search_results(
    keyword_results: Optional[Dict[str, Any]],
    semantic_results: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge keyword and semantic search results with deduplication and ranking.

    Merge strategy:
    1. If semantic search returned empty results (but keyword has results), log warning
       and return keyword results only (empty semantic is suspicious)
    2. Position 1: Top keyword result (exact match priority)
    3. Position 2: Top semantic result (if score >= threshold)
    4. Position 3-N: Interleave remaining by score, deduplicated
    5. Results appearing in both searches get searchType="both"

    Args:
        keyword_results: Results from keyword search (may be None if search failed)
        semantic_results: Results from semantic search (may be None if unavailable)

    Returns:
        Merged results with searchType field on each result
    """
    # Handle edge cases
    if not keyword_results and not semantic_results:
        return {
            "searchResults": [],
            "total": 0,
            "count": 0,
        }

    if not semantic_results:
        # Semantic search unavailable or failed - return keyword results only
        if keyword_results:
            for result in keyword_results.get("searchResults", []):
                result["searchType"] = "keyword"
        return keyword_results or {"searchResults": [], "total": 0, "count": 0}

    if not keyword_results:
        # Only semantic results available
        for result in semantic_results.get("searchResults", []):
            result["searchType"] = "semantic"
        return semantic_results

    keyword_search_results = keyword_results.get("searchResults", [])
    semantic_search_results = semantic_results.get("searchResults", [])

    # Check for suspicious empty semantic results
    if not semantic_search_results and keyword_search_results:
        logger.warning(
            "Semantic search returned 0 results while keyword search found %d results. "
            "This may indicate an issue with semantic search indexing.",
            len(keyword_search_results),
        )
        for result in keyword_search_results:
            result["searchType"] = "keyword"
        return keyword_results

    # Build URN lookup for deduplication
    keyword_urns: Dict[str, Dict[str, Any]] = {}
    for result in keyword_search_results:
        entity = result.get("entity", {})
        urn = entity.get("urn")
        if urn:
            keyword_urns[urn] = result

    semantic_urns: Dict[str, Dict[str, Any]] = {}
    for result in semantic_search_results:
        entity = result.get("entity", {})
        urn = entity.get("urn")
        if urn:
            semantic_urns[urn] = result

    # Find URNs that appear in both searches
    both_urns = set(keyword_urns.keys()) & set(semantic_urns.keys())

    # Merge results with interleaving strategy
    merged_results: List[Dict[str, Any]] = []
    seen_urns: set = set()

    # Position 1: Top keyword result (exact match priority)
    if keyword_search_results:
        top_keyword = keyword_search_results[0].copy()
        urn = top_keyword.get("entity", {}).get("urn")
        if urn:
            top_keyword["searchType"] = "both" if urn in both_urns else "keyword"
            merged_results.append(top_keyword)
            seen_urns.add(urn)

    # Position 2: Top semantic result (if not already added)
    if semantic_search_results:
        top_semantic = semantic_search_results[0].copy()
        urn = top_semantic.get("entity", {}).get("urn")
        if urn and urn not in seen_urns:
            top_semantic["searchType"] = "both" if urn in both_urns else "semantic"
            merged_results.append(top_semantic)
            seen_urns.add(urn)

    # Remaining results: interleave keyword and semantic, deduplicated
    keyword_remaining = [
        r
        for r in keyword_search_results[1:]
        if r.get("entity", {}).get("urn") not in seen_urns
    ]
    semantic_remaining = [
        r
        for r in semantic_search_results[1:]
        if r.get("entity", {}).get("urn") not in seen_urns
    ]

    # Interleave remaining results
    ki, si = 0, 0
    while ki < len(keyword_remaining) or si < len(semantic_remaining):
        # Alternate between keyword and semantic
        if ki < len(keyword_remaining):
            result = keyword_remaining[ki].copy()
            urn = result.get("entity", {}).get("urn")
            if urn and urn not in seen_urns:
                result["searchType"] = "both" if urn in both_urns else "keyword"
                merged_results.append(result)
                seen_urns.add(urn)
            ki += 1

        if si < len(semantic_remaining):
            result = semantic_remaining[si].copy()
            urn = result.get("entity", {}).get("urn")
            if urn and urn not in seen_urns:
                result["searchType"] = "both" if urn in both_urns else "semantic"
                merged_results.append(result)
                seen_urns.add(urn)
            si += 1

    # Build merged response, preserving facets from keyword search
    merged_response: Dict[str, Any] = {
        "searchResults": merged_results,
        "total": len(merged_results),
        "count": len(merged_results),
    }

    # Include facets from keyword search (more reliable for filtering)
    if "facets" in keyword_results:
        merged_response["facets"] = keyword_results["facets"]

    # Preserve start/offset if present
    if "start" in keyword_results:
        merged_response["start"] = keyword_results["start"]

    return merged_response


# Maximum number of results to fetch for hybrid search before applying offset
# This ensures consistent merge behavior across pagination
MAX_HYBRID_FETCH_RESULTS = 100


def _hybrid_search_documents(
    keyword_query: str,
    semantic_query: str,
    platforms: Optional[List[str]] = None,
    domains: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    glossary_terms: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
    num_results: int = 10,
    offset: int = 0,
) -> dict:
    """Execute keyword and semantic searches in parallel and merge results.

    This function runs both searches concurrently for better performance,
    then merges the results using _merge_search_results().

    Pagination strategy: To ensure consistent merge behavior, we fetch up to
    (offset + num_results) results from both searches (capped at MAX_HYBRID_FETCH_RESULTS),
    merge them, then apply the offset to the final merged results.

    If semantic search fails (e.g., on older DataHub deployments), it gracefully
    falls back to keyword-only results.

    Args:
        keyword_query: Query for keyword search
        semantic_query: Query for semantic search
        platforms: Filter by source platforms
        domains: Filter by business domains
        tags: Filter by tags
        glossary_terms: Filter by glossary terms
        owners: Filter by owners
        num_results: Number of results per page (max: 50)
        offset: Starting position for pagination

    Returns:
        Merged search results with searchType field on each result
    """
    # Late imports to avoid circular dependency
    from ..mcp_server import clean_gql_response

    keyword_results: Optional[Dict[str, Any]] = None
    semantic_results: Optional[Dict[str, Any]] = None

    # Calculate how many results to fetch from each search
    # We need enough to cover offset + num_results after deduplication
    fetch_count = min(offset + num_results, MAX_HYBRID_FETCH_RESULTS)
    hit_fetch_limit = (offset + num_results) > MAX_HYBRID_FETCH_RESULTS

    def run_keyword_search() -> Dict[str, Any]:
        return _search_documents_impl(
            query=keyword_query,
            search_strategy="keyword",
            platforms=platforms,
            domains=domains,
            tags=tags,
            glossary_terms=glossary_terms,
            owners=owners,
            num_results=fetch_count,
            offset=0,  # Always fetch from beginning for consistent merge
        )

    def run_semantic_search() -> Optional[Dict[str, Any]]:
        try:
            return _search_documents_impl(
                query=semantic_query,
                search_strategy="semantic",
                platforms=platforms,
                domains=domains,
                tags=tags,
                glossary_terms=glossary_terms,
                owners=owners,
                num_results=fetch_count,
                offset=0,
            )
        except Exception as e:
            # Semantic search may not be available on older DataHub deployments
            logger.warning(
                "Semantic search not available, falling back to keyword-only: %s",
                e,
                exc_info=True,
            )
            return None

    # Run both searches sequentially
    keyword_results = run_keyword_search()
    semantic_results = run_semantic_search()

    # Merge all results
    merged = _merge_search_results(keyword_results, semantic_results)

    # Apply pagination to merged results
    all_results = merged.get("searchResults", [])
    total_merged = len(all_results)

    # Slice to get the requested page
    paginated_results = all_results[offset : offset + num_results]

    # Build final response
    merged["searchResults"] = paginated_results
    merged["start"] = offset
    merged["count"] = len(paginated_results)
    merged["total"] = total_merged

    # Add metadata if we hit the fetch limit
    if hit_fetch_limit:
        merged["_hybridSearchLimitReached"] = True
        merged["_hybridSearchMaxResults"] = MAX_HYBRID_FETCH_RESULTS
        logger.info(
            "Hybrid search pagination limit reached: requested offset=%d + num_results=%d "
            "exceeds max fetch of %d results",
            offset,
            num_results,
            MAX_HYBRID_FETCH_RESULTS,
        )

    # Clean the merged response
    return clean_gql_response(merged)


@min_version(cloud="0.3.16", oss="1.4.0")
def search_documents(
    query: str = "*",
    semantic_query: Optional[str] = None,
    platforms: Optional[List[str]] = None,
    domains: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    glossary_terms: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
    num_results: int = 10,
    offset: int = 0,
) -> dict:
    """Search for documents stored in the customer's DataHub deployment.

    These are the organization's own documents (runbooks, FAQs, knowledge articles)
    ingested from sources like Notion, Confluence, etc. - not DataHub documentation.

    Returns document metadata WITHOUT content to keep responses concise.
    Use get_entities() with a document URN to retrieve full content when needed.

    HYBRID SEARCH (recommended for natural language queries):
    When both query and semantic_query are provided, runs keyword and semantic
    searches in parallel and merges results intelligently:
    - Results are deduplicated by URN
    - Top keyword result appears first (exact match priority)
    - Each result includes searchType: "keyword", "semantic", or "both"
    - Results appearing in both searches are high-confidence matches

    Example: search_documents(
        query="kubernetes deployment",
        semantic_query="how do I deploy applications to kubernetes cluster"
    )

    KEYWORD SEARCH (query parameter):
    - Full-text search with boolean logic
    - Use /q prefix for structured queries
    - Best for: exact terms, known keywords, specific phrases
    - Examples:
      • /q deployment guide → documents containing both terms
      • /q kubernetes OR k8s → documents with either term
      • /q "production deployment" → exact phrase match

    SEMANTIC SEARCH (semantic_query parameter):
    - Uses AI embeddings to find conceptually related documents
    - Best for: natural language questions, finding related topics
    - Only use when the query expresses intent/meaning, not for keyword lookups
    - Example: "how to deploy" finds deployment guides, CI/CD docs, release runbooks

    FILTERS - Narrow results by metadata:

    platforms: Filter by source platform (use full URN)
    - Examples: ["urn:li:dataPlatform:notion"], ["urn:li:dataPlatform:confluence"]

    domains: Filter by business domain (use full URN)
    - Examples: ["urn:li:domain:engineering"], ["urn:li:domain:data-platform"]

    tags: Filter by tags (use full URN)
    - Examples: ["urn:li:tag:critical"], ["urn:li:tag:deprecated"]

    glossary_terms: Filter by glossary terms (use full URN)
    - Examples: ["urn:li:glossaryTerm:pii"], ["urn:li:glossaryTerm:gdpr"]

    owners: Filter by document owners (use full URN)
    - Examples: ["urn:li:corpuser:alice"], ["urn:li:corpGroup:platform-team"]

    PAGINATION:
    - num_results: Number of results per page (max: 50)
    - offset: Starting position (default: 0)

    FACET DISCOVERY:
    - Set num_results=0 to get ONLY facets (no results)
    - Useful for discovering what platforms, domains exist

    EXAMPLE WORKFLOWS:

    1. Hybrid search for deployment docs:
       search_documents(
           query="kubernetes deployment",
           semantic_query="how to deploy applications to production"
       )

    2. Keyword-only search (when you know exact terms):
       search_documents(query="deployment", platforms=["urn:li:dataPlatform:notion"])

    3. Discover document sources:
       search_documents(num_results=0)
       → Examine facets to see available platforms, domains

    4. Find engineering team's critical docs:
       search_documents(
           domains=["urn:li:domain:engineering"],
           tags=["urn:li:tag:critical"]
       )
    """
    with PerfTimer() as timer:
        # If semantic_query is provided, run hybrid search
        if semantic_query:
            result = _hybrid_search_documents(
                keyword_query=query,
                semantic_query=semantic_query,
                platforms=platforms,
                domains=domains,
                tags=tags,
                glossary_terms=glossary_terms,
                owners=owners,
                num_results=num_results,
                offset=offset,
            )
            logger.info(
                "Hybrid document search completed in %.3fs (keyword=%r, semantic=%r, results=%d)",
                timer.elapsed_seconds(),
                query,
                semantic_query,
                len(result.get("searchResults", [])),
            )
            return result

        # Otherwise, run keyword-only search
        result = _search_documents_impl(
            query=query,
            search_strategy="keyword",
            platforms=platforms,
            domains=domains,
            tags=tags,
            glossary_terms=glossary_terms,
            owners=owners,
            num_results=num_results,
            offset=offset,
        )
        logger.info(
            "Keyword document search completed in %.3fs (query=%r, results=%d)",
            timer.elapsed_seconds(),
            query,
            len(result.get("searchResults", [])),
        )
        return result


def _search_documents_impl(
    query: str = "*",
    search_strategy: Optional[Literal["semantic", "keyword"]] = None,
    sub_types: Optional[List[str]] = None,
    platforms: Optional[List[str]] = None,
    domains: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    glossary_terms: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
    num_results: int = 10,
    offset: int = 0,
) -> dict:
    """Search for documents stored in the customer's DataHub deployment.

    These are the organization's own documents (runbooks, FAQs, knowledge articles)
    ingested from sources like Notion, Confluence, etc. - not DataHub documentation.

    Returns document metadata WITHOUT content to keep responses concise.
    Use get_entities() with a document URN to retrieve full content when needed.

    SEARCH STRATEGIES:

    SEMANTIC SEARCH (search_strategy="semantic"):
    - Uses AI embeddings to find conceptually related documents
    - Best for: natural language queries, finding related topics
    - Example: "how to deploy" finds deployment guides, CI/CD docs, release runbooks

    KEYWORD SEARCH (search_strategy="keyword" or default):
    - Full-text search with boolean logic
    - Use /q prefix for structured queries
    - Examples:
      • /q deployment guide → documents containing both terms
      • /q kubernetes OR k8s → documents with either term
      • /q "production deployment" → exact phrase match

    FILTERS - Narrow results by metadata:

    sub_types: Filter by document type
    - Examples: ["Runbook"], ["FAQ", "Tutorial"], ["Reference"]

    platforms: Filter by source platform (use full URN)
    - Examples: ["urn:li:dataPlatform:notion"], ["urn:li:dataPlatform:confluence"]

    domains: Filter by business domain (use full URN)
    - Examples: ["urn:li:domain:engineering"], ["urn:li:domain:data-platform"]

    tags: Filter by tags (use full URN)
    - Examples: ["urn:li:tag:critical"], ["urn:li:tag:deprecated"]

    glossary_terms: Filter by glossary terms (use full URN)
    - Examples: ["urn:li:glossaryTerm:pii"], ["urn:li:glossaryTerm:gdpr"]

    owners: Filter by document owners (use full URN)
    - Examples: ["urn:li:corpuser:alice"], ["urn:li:corpGroup:platform-team"]

    PAGINATION:
    - num_results: Number of results per page (max: 50)
    - offset: Starting position (default: 0)

    FACET DISCOVERY:
    - Set num_results=0 to get ONLY facets (no results)
    - Useful for discovering what sub_types, platforms, domains exist

    EXAMPLE WORKFLOWS:

    1. Find all runbooks:
       search_documents(sub_types=["Runbook"])

    2. Find Notion docs about deployment:
       search_documents(query="deployment", platforms=["urn:li:dataPlatform:notion"])

    3. Discover document types:
       search_documents(num_results=0)
       → Examine facets to see available subTypes, platforms, domains

    4. Find engineering team's critical docs:
       search_documents(
           domains=["urn:li:domain:engineering"],
           tags=["urn:li:tag:critical"]
       )
    """
    # Late imports to avoid circular dependency
    from ..mcp_server import (
        clean_gql_response,
        execute_graphql,
        fetch_global_default_view,
        get_datahub_client,
    )

    client = get_datahub_client()

    # Cap num_results at 50
    num_results = min(num_results, 50)

    # Build orFilters from the simple filter parameters
    # Each filter type is ANDed together, values within a filter are ORed
    and_filters: List[Dict[str, Any]] = []

    if sub_types:
        and_filters.append({"field": "subTypes", "values": sub_types})
    if platforms:
        and_filters.append({"field": "platform", "values": platforms})
    if domains:
        and_filters.append({"field": "domains", "values": domains})
    if tags:
        and_filters.append({"field": "tags", "values": tags})
    if glossary_terms:
        and_filters.append({"field": "glossaryTerms", "values": glossary_terms})
    if owners:
        and_filters.append({"field": "owners", "values": owners})

    # Wrap in orFilters format (list of AND groups)
    or_filters = [{"and": and_filters}] if and_filters else []

    # Fetch and apply default view
    view_urn = fetch_global_default_view(client._graph)

    # Choose search strategy
    if search_strategy == "semantic":
        gql_query = document_semantic_search_gql
        operation_name = "documentSemanticSearch"
        response_key = "semanticSearchAcrossEntities"
        variables: Dict[str, Any] = {
            "query": query,
            "orFilters": or_filters,
            "count": max(num_results, 1),
            "viewUrn": view_urn,
        }
    else:
        # Default: keyword search
        gql_query = document_search_gql
        operation_name = "documentSearch"
        response_key = "searchAcrossEntities"
        variables = {
            "query": query,
            "orFilters": or_filters,
            "count": max(num_results, 1),
            "start": offset,
            "viewUrn": view_urn,
        }

    response = execute_graphql(
        client._graph,
        query=gql_query,
        variables=variables,
        operation_name=operation_name,
    )[response_key]

    if num_results == 0 and isinstance(response, dict):
        # Support num_results=0 for facet-only queries
        response.pop("searchResults", None)
        response.pop("count", None)

    return clean_gql_response(response)


@min_version(cloud="0.3.16", oss="1.4.0")
def grep_documents(
    urns: List[str],
    pattern: str,
    context_chars: int = 200,
    max_matches_per_doc: int = 5,
    start_offset: int = 0,
) -> dict:
    """Search within document content using regex patterns.

    Similar to ripgrep/grep - finds matching excerpts within documents.
    Use search_documents() first to find relevant document URNs, then use this
    tool to search within their content.

    PATTERN SYNTAX (RE2 regex):
    - Simple text: "deploy" matches the word deploy
    - Case insensitive: "(?i)deploy" matches Deploy, DEPLOY, deploy
    - Word boundaries: r"\\bword\\b" matches whole word only
    - Alternatives: "deploy|release" matches either term
    - Wildcards: "deploy.*prod" matches deploy followed by prod
    - Character classes: "[Dd]eploy" matches Deploy or deploy

    PARAMETERS:

    urns: List of document URNs to search within
    - Get these from search_documents() results
    - Example: ["urn:li:document:doc1", "urn:li:document:doc2"]

    pattern: Regex pattern to search for
    - Examples: "kubernetes", "(?i)deploy.*production", "error|warning"
    - Use ".*" to get raw content (for continuing after truncation)

    context_chars: Characters to show before/after each match (default: 200)
    - Higher values show more surrounding context
    - When reading raw content (pattern=".*"), use higher values (e.g., 8000)

    max_matches_per_doc: Maximum matches to return per document (default: 5)
    - Limits output size for documents with many matches

    start_offset: Character offset to start searching from (default: 0)
    - Use this to continue reading after get_entities() truncation
    - When get_entities() returns _truncatedAtChar=8000, use start_offset=8000
      to continue reading from where it left off

    EXAMPLE WORKFLOWS:

    1. Find deployment instructions:
       docs = search_documents(query="deployment", sub_types=["Runbook"])
       urns = [r["entity"]["urn"] for r in docs["searchResults"]]
       grep_documents(urns, pattern="kubectl apply", context_chars=300)

    2. Find all error handling sections (case insensitive):
       grep_documents(urns, pattern="(?i)error|exception|failure")

    3. Find specific configuration values:
       grep_documents(urns, pattern=r"timeout.*=.*\\d+")

    4. Continue reading after truncation (when get_entities returns _truncatedAtChar):
       # After get_entities() shows: _truncatedAtChar=8000, _originalLengthChars=15000
       grep_documents(urns=[doc_urn], pattern=".*", context_chars=8000, start_offset=8000)
       # Returns content from char 8000 onwards

    RETURNS:
    - results: List of documents with matches, each containing:
      - urn: Document URN
      - title: Document title
      - matches: List of excerpts with position info (positions are absolute)
      - total_matches: Total matches found (may exceed max_matches_per_doc)
      - content_length: Total length of document content (when start_offset is used)
    - total_matches: Total matches across all documents
    - documents_with_matches: Number of documents containing matches
    """
    # Late imports to avoid circular dependency
    from ..mcp_server import (
        execute_graphql,
        get_datahub_client,
    )

    client = get_datahub_client()

    if not urns:
        return {
            "results": [],
            "total_matches": 0,
            "documents_with_matches": 0,
        }

    # Fetch document content via GraphQL
    response = execute_graphql(
        client._graph,
        query=document_content_gql,
        variables={"urns": urns},
        operation_name="documentContent",
    )

    entities = response.get("entities", [])

    # Compile regex pattern using RE2 (safe against ReDoS attacks)
    # RE2 guarantees linear-time matching, preventing pathological backtracking
    try:
        regex = re2.compile(pattern)
    except re2.error as e:
        return {
            "error": f"Invalid regex pattern: {e}",
            "results": [],
            "total_matches": 0,
            "documents_with_matches": 0,
        }

    results = []
    total_matches = 0
    documents_with_matches = 0

    for entity in entities:
        if not entity:
            continue

        urn = entity.get("urn", "")
        info = entity.get("info", {})
        title = info.get("title", "Untitled")
        contents = info.get("contents", {})
        text = contents.get("text", "") if contents else ""

        if not text:
            continue

        # Store original length before applying offset
        full_content_length = len(text)

        # Apply start_offset - skip first N characters
        if start_offset > 0:
            if start_offset >= len(text):
                # Offset is beyond document length, skip this document
                continue
            text = text[start_offset:]

        # Iterate through matches - only store excerpts for first max_matches_per_doc,
        # but count all matches without keeping them in memory
        excerpts: List[Dict[str, Any]] = []
        doc_total_matches = 0

        for match in regex.finditer(text):
            doc_total_matches += 1

            # Only extract excerpts for first max_matches_per_doc matches
            if len(excerpts) < max_matches_per_doc:
                start_pos = max(0, match.start() - context_chars)
                end_pos = min(len(text), match.end() + context_chars)

                # Extract excerpt
                excerpt = text[start_pos:end_pos]

                # Add ellipsis if truncated
                if start_pos > 0:
                    excerpt = "..." + excerpt
                if end_pos < len(text):
                    excerpt = excerpt + "..."

                # Report absolute position (accounting for start_offset)
                absolute_position = match.start() + start_offset

                excerpts.append(
                    {
                        "excerpt": excerpt,
                        "position": absolute_position,
                    }
                )

        if doc_total_matches == 0:
            continue

        documents_with_matches += 1
        total_matches += doc_total_matches

        result_entry: Dict[str, Any] = {
            "urn": urn,
            "title": title,
            "matches": excerpts,
            "total_matches": doc_total_matches,
        }

        # Include content_length when using start_offset to help with pagination
        if start_offset > 0:
            result_entry["content_length"] = full_content_length

        results.append(result_entry)

    return {
        "results": results,
        "total_matches": total_matches,
        "documents_with_matches": documents_with_matches,
    }
