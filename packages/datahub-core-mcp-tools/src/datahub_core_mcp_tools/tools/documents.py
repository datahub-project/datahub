"""Document tools: search the organization's own documents."""

import inspect
import string
from typing import Annotated, Any, Dict, List, Optional, TypedDict, cast

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.sdk.search_client import compile_filters
from datahub.utilities.perf_timer import PerfTimer
from fastmcp.exceptions import ToolError
from loguru import logger
from pydantic import Field

from .. import arg_validation, graphql_helpers
from ..freshness import with_last_modified_dates
from ..read_backend import JsonObject, get_read_backend
from ..result_policy import exclude_configuration, label_search_response, trust_labeled
from ..search_filter_parser import parse_filter_string
from ..search_flags import request_search_flags
from ..version_requirements import min_version, read_only
from ..view_preference import resolve_view_urn

# Document-side counterpart to search_filter_parser.FILTER_DOCS: same parser, but a
# different field set (documents have no env/status/columnCount, and their values are
# platforms like notion/confluence rather than warehouses).
#
# Inlined into the tool description, so it is resent on every LLM turn: keep it terse.
# Worked examples of the SQL operators measurably cost accuracy on the dataset-side
# block; what must stay is what cannot be inferred: the field list, the full-URN
# requirement, WITHIN versus =, and the quoting rule.
DOCUMENT_FILTER_DOCS = """\
FILTER SYNTAX: SQL-like WHERE clause over the fields below. Supports AND, OR, NOT,
parentheses, IN (a, b), and WITHIN.
  e.g. platform IN (notion, confluence) AND tag = urn:li:tag:critical

SUPPORTED FILTER FIELDS:
- subtype (or entity_subtype): values vary by deployment and are
  CASE-SENSITIVE; enumerate them with the facet call below, don't guess
- platform: notion, confluence, datahub, etc.
- domain, container, tag, glossary_term, owner, parent_document: full URN required

IMPORTANT: domain, container, tag, glossary_term, owner, and parent_document
require full URN format (urn:li:...). Use the facet discovery call below to find
valid URNs, then use the exact URN from the results.

HIERARCHY (WITHIN): include nested children, not only the exact node. = stays exact.
One URN, or WITHIN (urn1, urn2).
- domain WITHIN urn:li:domain:eng — docs in that domain or any child domain.
  domain = matches only the exact assigned domain.
- container WITHIN urn:li:container:... — that container or any nested container.
- parent_document WITHIN urn:li:document:... — docs nested under that document,
  any depth. parent_document = matches only a direct child.

Double-quote any value containing spaces, =, or parentheses:
  tag = "urn:li:tag:my tag"
  subtype IN ("Two Words", OneWord)\
"""

SEMANTIC_SEARCH_ENABLED_ENV_VAR = "SEMANTIC_SEARCH_ENABLED"
MAX_NUM_RESULTS = 50
# How many results each leg of a hybrid search fetches, at most. Both legs fetch from
# the start and the merged list is paged, so every page merges the same pools.
MAX_HYBRID_FETCH_RESULTS = 100
# The searchFlags every document search sends, before a host's SearchFlagsOverride.
DEFAULT_DOCUMENT_SEARCH_FLAGS: JsonObject = {
    "skipHighlighting": True,
    "maxAggValues": 10,
}

# Queries that mean "match everything" rather than a real keyword search. GMS
# treats an empty string the same as "*" (see SearchUtils.applyDefaultSearchFlags).
_MATCH_ALL_QUERIES = frozenset({"*", ""})
# Query shapes that should NOT be auto-upgraded to a hybrid search. "/q" is the
# structured-query prefix (boolean/field syntax that is meaningless as an embedding
# query); identifier prefixes indicate an exact-match lookup.
_STRUCTURED_QUERY_PREFIX = "/q"
_IDENTIFIER_PREFIXES = ("urn:", "s3://", "gs://", "hdfs://", "abfss://")

_SYSTEM_EXCLUSION = "sourceType != SYSTEM"
_BRIDGE_DOCUMENT_EXCLUSION = 'subtype != "Bridge Document"'


class SearchResult(TypedDict, total=False):
    """One hit in a document search response. ``entity`` is the raw GraphQL entity
    (urn, info, subType, ...). ``searchType`` is set by ``_merge_search_results``
    (keyword / semantic / both)."""

    entity: Dict[str, Any]
    matchedFields: List[Dict[str, Any]]
    searchType: str


class SearchResponse(TypedDict, total=False):
    """A document search response: the GraphQL search envelope. ``searchResults`` and
    ``count`` are dropped in facet-only (num_results=0) mode, and the ``_hybrid*``
    markers appear only when the hybrid fetch cap was hit."""

    searchResults: List[SearchResult]
    total: int
    count: int
    start: int
    facets: List[Any]
    _hybridSearchLimitReached: bool
    _hybridSearchMaxResults: int


def semantic_search_enabled() -> bool:
    """Whether this deployment runs the semantic leg of search_documents.

    Off by default: semantic search needs an embedding index that GMS only has when
    it is configured for it. Read per call so tests can switch it.
    """
    return get_boolean_env_variable(SEMANTIC_SEARCH_ENABLED_ENV_VAR, default=False)


def _urn_of(result: SearchResult) -> Optional[str]:
    return result.get("entity", {}).get("urn")


def _merge_semantic_matched_fields(
    result: SearchResult, semantic_result: Optional[SearchResult]
) -> None:
    """Copy semantic-only matchedFields onto a deduped keyword result."""
    semantic_fields = (semantic_result or {}).get("matchedFields") or []
    if not semantic_fields:
        return
    fields = list(result.get("matchedFields") or [])
    seen = {(f.get("name"), f.get("value")) for f in fields if isinstance(f, dict)}
    for field in semantic_fields:
        if not isinstance(field, dict):
            continue
        key = (field.get("name"), field.get("value"))
        if key not in seen:
            fields.append(field.copy())
            seen.add(key)
    result["matchedFields"] = fields


def _copy_result_for_merge(
    result: SearchResult,
    search_type: str,
    semantic_by_urn: Dict[str, SearchResult],
) -> SearchResult:
    merged = result.copy()
    merged["searchType"] = search_type
    urn = _urn_of(merged)
    if search_type == "both" and urn:
        _merge_semantic_matched_fields(merged, semantic_by_urn.get(urn))
    return merged


def _merge_search_results(
    keyword_results: Optional[SearchResponse],
    semantic_results: Optional[SearchResponse],
) -> SearchResponse:
    """Merge keyword and semantic results, deduplicated by URN.

    1. Empty semantic results next to keyword hits are suspicious (an indexing
       problem, not "nothing related"): log it and return the keyword results.
    2. Position 1: the top keyword result (exact match priority).
    3. Position 2: the top semantic result.
    4. Then keyword and semantic results alternate.
    Each result gets searchType "keyword", "semantic", or "both".
    """
    if not keyword_results and not semantic_results:
        return {"searchResults": [], "total": 0, "count": 0}

    if not semantic_results:
        if keyword_results:
            for result in keyword_results.get("searchResults", []):
                result["searchType"] = "keyword"
        return keyword_results or {"searchResults": [], "total": 0, "count": 0}

    if not keyword_results:
        for result in semantic_results.get("searchResults", []):
            result["searchType"] = "semantic"
        return semantic_results

    keyword_hits = keyword_results.get("searchResults", [])
    semantic_hits = semantic_results.get("searchResults", [])

    if not semantic_hits and keyword_hits:
        logger.warning(
            "Semantic search returned 0 results while keyword search found {}. "
            "This may indicate an issue with semantic search indexing.",
            len(keyword_hits),
        )
        for result in keyword_hits:
            result["searchType"] = "keyword"
        return keyword_results

    keyword_urns = {urn for r in keyword_hits if (urn := _urn_of(r))}
    semantic_by_urn = {urn: r for r in semantic_hits if (urn := _urn_of(r))}
    both_urns = keyword_urns & set(semantic_by_urn)

    merged: List[SearchResult] = []
    seen: set[str] = set()

    def add(result: SearchResult, single_type: str) -> None:
        urn = _urn_of(result)
        if urn and urn not in seen:
            search_type = "both" if urn in both_urns else single_type
            merged.append(_copy_result_for_merge(result, search_type, semantic_by_urn))
            seen.add(urn)

    if keyword_hits:
        add(keyword_hits[0], "keyword")
    if semantic_hits:
        add(semantic_hits[0], "semantic")
    keyword_rest, semantic_rest = keyword_hits[1:], semantic_hits[1:]
    for i in range(max(len(keyword_rest), len(semantic_rest))):
        if i < len(keyword_rest):
            add(keyword_rest[i], "keyword")
        if i < len(semantic_rest):
            add(semantic_rest[i], "semantic")

    response: SearchResponse = {
        "searchResults": merged,
        "total": len(merged),
        "count": len(merged),
    }
    # Facets come from the keyword leg, which is the one filters are written against.
    if "facets" in keyword_results:
        response["facets"] = keyword_results["facets"]
    if "start" in keyword_results:
        response["start"] = keyword_results["start"]
    return response


def _hybrid_search_documents(
    keyword_query: str,
    semantic_query: str,
    filter: Optional[str],
    num_results: int,
    offset: int,
) -> SearchResponse:
    """Run the keyword and semantic legs and merge them.

    Both legs fetch ``offset + num_results`` results from the start (at most
    MAX_HYBRID_FETCH_RESULTS) and the merged list is sliced, so pages stay
    consistent. A failing semantic leg (an older or unconfigured GMS) falls back to
    keyword-only results.
    """
    fetch_count = min(offset + num_results, MAX_HYBRID_FETCH_RESULTS)
    hit_fetch_limit = offset + num_results > MAX_HYBRID_FETCH_RESULTS

    keyword_results = _search_documents_impl(
        keyword_query,
        semantic=False,
        filter=filter,
        num_results=fetch_count,
        offset=0,
        max_num_results=MAX_HYBRID_FETCH_RESULTS,
    )
    semantic_results: Optional[SearchResponse]
    try:
        semantic_results = _search_documents_impl(
            semantic_query,
            semantic=True,
            filter=filter,
            num_results=fetch_count,
            offset=0,
            max_num_results=MAX_HYBRID_FETCH_RESULTS,
        )
    except Exception as e:
        logger.warning(
            "Semantic search not available, falling back to keyword-only: {}", e
        )
        semantic_results = None

    merged = _merge_search_results(keyword_results, semantic_results)
    all_results = merged.get("searchResults", [])
    page = all_results[offset : offset + num_results]
    merged["searchResults"] = page
    merged["start"] = offset
    merged["count"] = len(page)
    merged["total"] = len(all_results)
    if hit_fetch_limit:
        merged["_hybridSearchLimitReached"] = True
        merged["_hybridSearchMaxResults"] = MAX_HYBRID_FETCH_RESULTS
    return with_last_modified_dates(graphql_helpers.clean_gql_response(merged))


def _validate_search_query(query: str, semantic_query: Optional[str]) -> None:
    """``"*"`` (match-all) is a valid keyword-only browse query, for facet discovery
    and filter-only listing, but it cannot anchor a hybrid search: the merge promotes
    the top keyword hit to position 1 as the exact match, and a match-all query has
    no meaningful top hit."""
    if semantic_query is not None and query.strip() in _MATCH_ALL_QUERIES:
        raise ToolError(
            "search_documents: a specific keyword `query` is required when "
            "`semantic_query` is set. `query='*'` (match-all) is only valid for "
            "keyword-only browsing or facet discovery — it can't be combined with "
            "semantic search. Provide a real keyword query, or omit `semantic_query`."
        )


def _looks_like_natural_language(query: str) -> bool:
    """Whether a keyword query reads like a natural-language phrase.

    Deliberately conservative: only multi-word free text qualifies, so structured
    ``/q`` queries, identifier lookups, browse-all, and single-token keyword lookups
    stay keyword-only.
    """
    trimmed = query.strip()
    if trimmed in _MATCH_ALL_QUERIES or trimmed.startswith(_STRUCTURED_QUERY_PREFIX):
        return False
    if trimmed.lower().startswith(_IDENTIFIER_PREFIXES):
        return False
    return len(trimmed.split()) >= 2


def _resolve_semantic_query(query: str, semantic_query: Optional[str]) -> Optional[str]:
    """The semantic query to run, or None for keyword-only.

    Where semantic search is off, never: the call would fail and fall back anyway.
    Otherwise an explicit ``semantic_query`` wins, and a natural-language ``query``
    is reused as one, since agents reliably set the keyword argument but often
    forget the semantic one.
    """
    if not semantic_search_enabled():
        return None
    if semantic_query is None and _looks_like_natural_language(query):
        return query
    return semantic_query


def default_document_filter(filter: Optional[str]) -> str:
    """Add the default exclusions to a caller's document filter.

    SYSTEM documents are platform-managed product guides; a caller opts in with
    ``filter="sourceType = SYSTEM"``. Bridge documents are hidden copies of datasets
    that DataHub Cloud generates for semantic search: they are NATIVE, so the SYSTEM
    exclusion misses them. A filter that names either kind opts back in to it.
    """
    wrapped = graphql_helpers.parenthesized_filter(filter)
    clauses = [wrapped] if wrapped else []
    if not (filter and "sourceType" in filter):
        clauses.append(_SYSTEM_EXCLUSION)
    if not (filter and "bridge document" in filter.lower()):
        clauses.append(_BRIDGE_DOCUMENT_EXCLUSION)
    return exclude_configuration(" AND ".join(clauses))


def _search_documents_impl(
    query: str,
    *,
    semantic: bool,
    filter: Optional[str],
    num_results: int,
    offset: int,
    max_num_results: int = MAX_NUM_RESULTS,
) -> SearchResponse:
    """One leg of a document search, through the request's ReadBackend.

    ``max_num_results`` caps ``num_results``: the tool's advertised page size, which
    the hybrid search raises to fetch its merge pools.
    """
    try:
        _, compiled_filters = compile_filters(
            parse_filter_string(default_document_filter(filter))
        )
    except ValueError as e:
        raise ToolError(f"Invalid filter {filter!r}: {e}") from e

    num_results = min(num_results, max_num_results)
    # Facets are for discovery: a facet-only call (num_results=0) or a browse-all
    # listing. Other searches skip them: they were most of a typical response, and
    # the hybrid merge discards the semantic leg's facets anyway.
    flags = dict(DEFAULT_DOCUMENT_SEARCH_FLAGS)
    if num_results > 0 and query.strip() not in _MATCH_ALL_QUERIES:
        flags["skipAggregates"] = True

    variables: JsonObject = {
        "query": query,
        "orFilters": compiled_filters,  # type: ignore[dict-item]
        "count": max(num_results, 1),  # 0 is not a valid value for count.
        "viewUrn": resolve_view_urn(graphql_helpers.get_datahub_client()._graph),
        "searchFlags": request_search_flags(flags),
    }
    if not semantic:
        variables["start"] = offset

    response = dict(get_read_backend().search_documents(variables, semantic=semantic))
    if num_results == 0:
        response.pop("searchResults", None)
        response.pop("count", None)
    return cast(
        SearchResponse,
        with_last_modified_dates(graphql_helpers.clean_gql_response(response)),
    )


def _search_documents_core(
    query: str,
    *,
    semantic_query: Optional[str],
    filter: Optional[str],
    num_results: int,
    offset: int,
) -> SearchResponse:
    """Validate, resolve the semantic leg, then run a hybrid or keyword-only search."""
    _validate_search_query(query, semantic_query)
    semantic_query = _resolve_semantic_query(query, semantic_query)

    with PerfTimer() as timer:
        if semantic_query:
            result = _hybrid_search_documents(
                keyword_query=query,
                semantic_query=semantic_query,
                filter=filter,
                num_results=num_results,
                offset=offset,
            )
        else:
            result = _search_documents_impl(
                query,
                semantic=False,
                filter=filter,
                num_results=num_results,
                offset=offset,
            )
    # Queries stay out of the log: they can carry user data.
    logger.info(
        "{} document search completed in {:.3f}s ({} results)",
        "Hybrid" if semantic_query else "Keyword",
        timer.elapsed_seconds(),
        len(result.get("searchResults", [])),
    )
    return result


@read_only
@trust_labeled
@min_version(cloud="0.3.16", oss="1.4.0")
def search_documents(
    query: str,
    semantic_query: Optional[str] = None,
    filter: Annotated[
        Optional[str], Field(validation_alias=arg_validation.FILTER_ALIASES)
    ] = None,
    num_results: Annotated[
        int, Field(validation_alias=arg_validation.SIZE_ALIASES)
    ] = 10,
    offset: Annotated[int, Field(validation_alias=arg_validation.OFFSET_ALIASES)] = 0,
) -> dict:
    """Search for documents stored in the customer's DataHub deployment.

    These are the organization's own documents (runbooks, FAQs, knowledge articles)
    ingested from sources like Notion, Confluence, etc. - not DataHub documentation.

    Returns document metadata and concise matched excerpts when semantic search
    finds them. To read a matched document's full body, pass its URN to
    grep_documents().

    HYBRID SEARCH (recommended for natural language queries):
    When both query and semantic_query are provided, runs keyword and semantic
    searches and merges their results:
    - Results are deduplicated by URN
    - Top keyword result appears first (exact match priority)
    - Each result includes searchType: "keyword", "semantic", or "both"
    - Results appearing in both searches are high-confidence matches

    Example: search_documents(
        query="/q kubernetes+deployment",
        semantic_query="how do I deploy applications to kubernetes cluster"
    )

    KEYWORD SEARCH (query parameter):
    - Start with /q: + requires all terms, OR allows either, - excludes terms,
      () groups, quotes match exact phrases, and term* matches prefixes.
    - Bare queries match one phrase; * browses all (keyword-only).

    SEMANTIC SEARCH (semantic_query parameter):
    - Uses AI embeddings to find conceptually related documents
    - Best for: natural language questions, finding related topics
    - Example: "how to deploy" finds deployment guides, CI/CD docs, release runbooks

    $DOCUMENT_FILTER_DOCS

    PAGINATION:
    - num_results: Number of results per page (max: 50)
    - offset: Starting position (default: 0)

    FACET DISCOVERY:
    - Facets are returned only with num_results=0 or query="*"; other searches
      return results without them
    - Set num_results=0 to get ONLY facets (no results)
    - Pass query="*" (optionally with a filter) to enumerate the values that
      actually exist: the "Sub Type" facet (field typeNames) lists valid subtypes,
      alongside platform, domain and tags
    - Do this before filtering on subtype. A guessed subtype is a valid filter that
      matches nothing, so it returns zero results rather than an error

    EXAMPLE WORKFLOWS:

    1. Hybrid search for deployment docs:
       search_documents(
           query="/q kubernetes+deployment",
           semantic_query="how to deploy applications to production"
       )

    2. Keyword-only search filtered by platform:
       search_documents(query="/q deployment", filter="platform = notion")

    3. Discover valid subtypes / platforms before filtering:
       search_documents(query="*", num_results=0)
       → typeNames facet lists the subtypes that exist, with counts

    4. Find engineering team's critical docs in that exact domain:
       search_documents(query="*", filter="domain = urn:li:domain:engineering AND tag = urn:li:tag:critical")

    5. Find all docs from Notion or Confluence:
       search_documents(query="*", filter="platform IN (notion, confluence)")

    6. Docs in a domain, including nested child domains:
       search_documents(query="*", filter="domain WITHIN urn:li:domain:engineering")

    7. Docs nested under a parent document, any depth:
       search_documents(query="*", filter="parent_document WITHIN urn:li:document:runbooks")
    """
    return label_search_response(
        cast(
            Dict[str, Any],
            _search_documents_core(
                query,
                semantic_query=semantic_query,
                filter=filter,
                num_results=num_results,
                offset=offset,
            ),
        )
    )


assert search_documents.__doc__ is not None
# Dedent before substituting: DOCUMENT_FILTER_DOCS is flush-left, so interpolating it
# into an indented placeholder would indent only its first line.
search_documents.__doc__ = string.Template(
    inspect.cleandoc(search_documents.__doc__)
).substitute(DOCUMENT_FILTER_DOCS=DOCUMENT_FILTER_DOCS)
