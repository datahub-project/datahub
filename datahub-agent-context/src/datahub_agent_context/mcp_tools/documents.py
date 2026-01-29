"""Document tools for DataHub MCP server."""

import pathlib
from typing import Any, Dict, List, Literal, Optional

import re2  # type: ignore[import-untyped]

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import (
    clean_gql_response,
    execute_graphql,
    fetch_global_default_view,
)

# Load GraphQL queries at module level
document_search_gql = (
    pathlib.Path(__file__).parent / "gql/document_search.gql"
).read_text()
document_semantic_search_gql = (
    pathlib.Path(__file__).parent / "gql/document_semantic_search.gql"
).read_text()
read_documents_gql = (
    pathlib.Path(__file__).parent / "gql/read_documents.gql"
).read_text()


def search_documents(
    query: str = "*",
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
    ingested from sources like Notion, Confluence, etc.

    Returns document metadata WITHOUT content to keep responses concise.
    Use get_entities() with a document URN to retrieve full content when needed.

    KEYWORD SEARCH:
    - Full-text search with boolean logic
    - Use /q prefix for structured queries
    - Examples:
      • /q deployment guide → documents containing both terms
      • /q kubernetes OR k8s → documents with either term
      • /q "production deployment" → exact phrase match

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

    1. Find Notion docs about deployment:
       search_documents(query="deployment", platforms=["urn:li:dataPlatform:notion"])

    2. Discover document sources:
       search_documents(num_results=0)
       → Examine facets to see available platforms, domains

    3. Find engineering team's critical docs:
       search_documents(
           domains=["urn:li:domain:engineering"],
           tags=["urn:li:tag:critical"]
       )

    Args:
        query: Search query string
        platforms: List of platform URNs to filter by
        domains: List of domain URNs to filter by
        tags: List of tag URNs to filter by
        glossary_terms: List of glossary term URNs to filter by
        owners: List of owner URNs to filter by
        num_results: Number of results to return (max 50)
        offset: Starting position for pagination

    Returns:
        Dictionary with search results and facets

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = search_documents(query="deployment")
    """
    graph = get_graph()
    return _search_documents_impl(
        graph,
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


def _search_documents_impl(
    graph,
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
    view_urn = fetch_global_default_view(graph)

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
        graph,
        query=gql_query,
        variables=variables,
        operation_name=operation_name,
    )[response_key]

    if num_results == 0 and isinstance(response, dict):
        # Support num_results=0 for facet-only queries
        response.pop("searchResults", None)
        response.pop("count", None)

    return clean_gql_response(response)


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

    Args:
        urns: List of document URNs to search within
        pattern: Regex pattern to search for
        context_chars: Characters to show before/after each match
        max_matches_per_doc: Maximum matches to return per document
        start_offset: Character offset to start searching from

    Returns:
        Dictionary with search results

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = grep_documents(urns=["urn:li:document:doc1"], pattern="kubernetes")
    """
    graph = get_graph()
    if not urns:
        return {
            "results": [],
            "total_matches": 0,
            "documents_with_matches": 0,
        }

    # Fetch document content via GraphQL
    response = execute_graphql(
        graph,
        query=read_documents_gql,
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
