"""Document tools: search the organization's own documents and read their bodies."""

import inspect
import re
import string
from typing import Annotated, Any, Dict, List, Optional, TypedDict, cast

import re2  # type: ignore[import-untyped]
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.sdk.search_client import compile_filters
from datahub.utilities.perf_timer import PerfTimer
from fastmcp.exceptions import ToolError
from loguru import logger
from pydantic import BeforeValidator, Field

from .. import arg_validation, graphql_helpers
from ..freshness import with_last_modified_dates
from ..read_backend import JsonObject, get_read_backend
from ..result_policy import (
    annotate_items,
    attach_header,
    configuration_response,
    current_decisions,
    exclude_configuration,
    is_configuration,
    label_search_response,
    trust_labeled,
)
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
    num_results = min(num_results, MAX_NUM_RESULTS)
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

    # Each leg's total counts its matches, not just the ones fetched, and the union
    # of the legs matches at least as many as either. The merged pool only holds the
    # fetch window, so its size alone would end paging after the first page.
    leg_totals = [
        leg.get("total", 0) for leg in (keyword_results, semantic_results) if leg
    ]
    merged = _merge_search_results(keyword_results, semantic_results)
    all_results = merged.get("searchResults", [])
    page = all_results[offset : offset + num_results]
    merged["searchResults"] = page
    merged["start"] = offset
    merged["count"] = len(page)
    merged["total"] = max([len(all_results), *leg_totals])
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


# RE2 rejects bounded repetitions above this count ("invalid repetition size"). Agents
# reach for large quantifiers like ``.{0,3000}`` to grab a context window, for which
# ``context_chars`` is the right tool, so the count is clamped rather than the call
# bounced.
RE2_MAX_REPEAT = 1000
_REPETITION_RE = re.compile(r"\{(\d+)(,)?(\d*)\}")


def clamp_repetitions(pattern: str) -> str:
    """Clamp ``{m,n}`` / ``{n}`` / ``{n,}`` counts down to RE2's max, so a pattern that
    only fails on an oversized repetition compiles. The subject is the pattern string
    and the matcher a fixed safe regex, so this is not itself a ReDoS surface."""

    def _repl(m: re.Match) -> str:
        lo = min(int(m.group(1)), RE2_MAX_REPEAT)
        if not m.group(2):  # {n}
            return "{%d}" % lo
        if m.group(3) == "":  # {n,}
            return "{%d,}" % lo
        hi = min(int(m.group(3)), RE2_MAX_REPEAT)  # {m,n}
        return "{%d,%d}" % (lo, hi)

    return _REPETITION_RE.sub(_repl, pattern)


def re2_error_text(error: "re2.error") -> str:
    """The reason in an ``re2.error`` as plain text. RE2 passes it as bytes, so
    ``str(error)`` reads ``b'missing ): (unclosed'``."""
    reason = error.args[0] if error.args else ""
    if isinstance(reason, bytes):
        return reason.decode("utf-8", errors="replace")
    return str(reason)


def _compile_document_pattern(pattern: str) -> tuple["re2._Regexp", Optional[str]]:
    """The compiled pattern, plus a note when an oversized repetition was clamped.

    RE2 matches in linear time, so an agent's pattern cannot cause catastrophic
    backtracking.
    """
    try:
        return re2.compile(pattern), None
    except re2.error as exc:
        err = exc
    clamped = clamp_repetitions(pattern)
    if clamped != pattern:
        try:
            return re2.compile(clamped), (
                f"A repetition exceeded RE2's max ({RE2_MAX_REPEAT}); the pattern was "
                f"clamped to {clamped!r}. To capture a large window around a match, pass "
                "the context_chars argument instead of a {0,N} quantifier in the pattern."
            )
        except re2.error as exc:
            err = exc
    raise ToolError(
        f"Invalid regex pattern: {re2_error_text(err)}. Patterns run on RE2 — "
        f"bounded repetition {{m,n}} is capped at {RE2_MAX_REPEAT}. To capture "
        "text around a match, use the context_chars argument, not a large {0,N} "
        "quantifier."
    ) from err


@read_only
@trust_labeled
@min_version(cloud="0.3.16", oss="1.4.0")
def grep_documents(
    urns: Annotated[
        List[str],
        BeforeValidator(arg_validation.coerce_str_list),
        Field(validation_alias=arg_validation.DOCUMENT_URNS_ALIASES),
    ],
    pattern: str,
    context_chars: int = 200,
    # No SIZE_ALIASES: the limit is per document, not a result count.
    max_matches_per_doc: int = 5,
    start_offset: Annotated[
        int, Field(validation_alias=arg_validation.START_OFFSET_ALIASES)
    ] = 0,
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
    - Runs on RE2: bounded repetition {m,n} is capped at 1000. Do NOT bake a large
      window into the pattern (e.g. "phrase[\\s\\S]{0,3000}") — use context_chars for
      surrounding text. Oversized repetitions are auto-clamped to 1000 with a note.

    context_chars: Characters to show before/after each match (default: 200)
    - Higher values show more surrounding context
    - When reading raw content (pattern=".*"), use higher values (e.g., 8000)
    - This is the right way to capture text around a match — not a {0,N} quantifier.

    max_matches_per_doc: Maximum excerpts to return per document (default: 5)
    - Limits output size for documents with many matches
    - A match inside an earlier excerpt is counted but not shown again, and
      excerpts never repeat text, so nearby matches share one excerpt

    start_offset: Character offset to start searching from (default: 0)
    - Use this to page through a long document: read one window, then call again
      with a higher start_offset to continue where you left off
    - When start_offset is set, the result includes content_length (the document's
      total size) so you know when the whole body has been read

    EXAMPLE WORKFLOWS:

    1. Find deployment instructions:
       docs = search_documents(query="deployment", filter="platform = confluence")
       urns = [r["entity"]["urn"] for r in docs["searchResults"]]
       grep_documents(urns, pattern="kubectl apply", context_chars=300)

    2. Find all error handling sections (case insensitive):
       grep_documents(urns, pattern="(?i)error|exception|failure")

    3. Find specific configuration values:
       grep_documents(urns, pattern=r"timeout.*=.*\\d+")

    4. Read a long document's raw body in chunks:
       grep_documents(urns=[doc_urn], pattern=".*", context_chars=8000)
       # Returns consecutive excerpts from the start of the body, up to the output
       # limit. If the body is longer, the result's next_offset says where to continue:
       grep_documents(urns=[doc_urn], pattern=".*", context_chars=8000, start_offset=next_offset)
       # content_length reports the total

    OUTPUT LIMIT: One call returns at most about 60,000 characters of excerpt text,
    across all documents. Output stops at the limit, even mid-excerpt: the response
    then has truncated: true, and every document with more to show has a
    next_offset (documents not reached yet have matches: []).

    RETURNS:
    - results: List of documents with matches, each containing:
      - urn: Document URN
      - title: Document title
      - matches: List of excerpts with position info (positions are absolute). An
        excerpt cut by the output limit has truncated: true
      - total_matches: Total matches found (may exceed max_matches_per_doc)
      - content_length: Total length of document content (when start_offset is
        used, or the output limit cut this document)
      - next_offset: Present when this document has more to show (matches beyond
        max_matches_per_doc, or text past the output limit); pass it as
        start_offset to continue exactly there
    - total_matches: Total matches across all documents
    - documents_with_matches: Number of documents containing matches
    - truncated: Present (true) only when the output limit was reached.
    - truncation_note: Present with truncated; says how to continue.
    - skipped_urns: Present when an input was invalid, missing, or inaccessible. Each entry has
      `urn` (the rejected value) and `error` (why, and how to resolve it). The
      remaining URNs were still searched — resolve these with search_documents()
      and retry.
    - pattern_note: Present only when the pattern was rewritten to compile, explaining
      what changed.

    A pattern that cannot compile at all fails the call with a tool error.
    """
    if not urns:
        empty: dict = {"results": [], "total_matches": 0, "documents_with_matches": 0}
        attach_header(empty, current_decisions([]), [])
        return empty

    # A non-URN value fetches no content, which would read as "no matches". Skip
    # those entries and report them, so one bad URN does not discard the good ones.
    urns, skipped_urns = arg_validation.partition_urns(
        urns,
        tool="grep_documents",
        example=arg_validation.DOCUMENT_URN_EXAMPLE,
        resolve_with="search_documents",
    )
    if not urns:
        raise ToolError(
            arg_validation.no_usable_urns_error("grep_documents", skipped_urns)
        )

    # Before any read: a bad pattern fails the call either way.
    regex, pattern_note = _compile_document_pattern(pattern)

    decisions = current_decisions(urns)
    skipped_urns.extend(
        {"urn": urn, "error": configuration_response(urn)["error"]}
        for urn in urns
        if is_configuration(urn, decisions)
    )
    urns = [urn for urn in urns if not is_configuration(urn, decisions)]
    entities = [
        e for e in (get_read_backend().document_contents(urns) if urns else []) if e
    ]
    returned_urns = {entity.get("urn") for entity in entities}
    skipped_urns.extend(
        {"urn": urn, "error": "Document not found or inaccessible."}
        for urn in urns
        if urn not in returned_urns
    )

    results: List[Dict[str, Any]] = []
    total_matches = 0
    # Shared by every document in the response. Once it runs out, later documents are
    # still counted and listed, with a next_offset, but return no excerpts.
    budget_left = graphql_helpers.GREP_OUTPUT_CHAR_BUDGET
    budget_reached = False

    for entity in entities:
        urn = str(entity.get("urn", ""))
        if is_configuration(urn, decisions, entity):
            skipped_urns.append(
                {"urn": urn, "error": configuration_response(urn)["error"]}
            )
            continue
        info = cast(Dict[str, Any], entity.get("info") or {})
        title = info.get("title", "Untitled")
        text = (info.get("contents") or {}).get("text", "")
        if not text:
            continue

        full_content_length = len(text)
        if start_offset > 0:
            if start_offset >= len(text):
                continue
            text = text[start_offset:]

        # Excerpts are kept only for the first max_matches_per_doc matches; the rest
        # are counted without holding them in memory.
        excerpts: List[Dict[str, Any]] = []
        doc_total_matches = 0
        # Dense matches (".*" matches every line) would otherwise return the same
        # window up to max_matches_per_doc times. A match that ends inside an earlier
        # excerpt is counted but not repeated, and each excerpt starts no earlier than
        # where the previous one ended.
        shown_until = 0
        unshown_matches = False
        # Set when the budget stopped this document's output. next_offset then points
        # at the first character not shown, so the next call continues with no gap.
        cut_by_budget = False

        for match in regex.finditer(text):
            doc_total_matches += 1
            if excerpts and match.end() <= shown_until:
                continue
            if cut_by_budget or len(excerpts) >= max_matches_per_doc:
                unshown_matches = True
                continue
            if budget_left <= 0:
                cut_by_budget = True
                continue

            start_pos = max(0, match.start() - context_chars, shown_until)
            end_pos = min(len(text), match.end() + context_chars)
            truncated = end_pos - start_pos > budget_left
            if truncated:
                end_pos = start_pos + budget_left
                budget_left = 0
                cut_by_budget = True
                # A cut before the match begins would return only leading context.
                # Leave the whole excerpt to the next call, which starts at shown_until.
                if end_pos <= match.start():
                    continue
            else:
                budget_left -= end_pos - start_pos
            shown_until = end_pos

            excerpt = text[start_pos:end_pos]
            if start_pos > 0:
                excerpt = "..." + excerpt
            if end_pos < len(text):
                excerpt = excerpt + "..."
            entry: Dict[str, Any] = {
                "excerpt": excerpt,
                "position": match.start() + start_offset,
            }
            if truncated:
                entry["truncated"] = True
            excerpts.append(entry)

        if doc_total_matches == 0:
            continue

        total_matches += doc_total_matches
        budget_reached = budget_reached or cut_by_budget
        result_entry: Dict[str, Any] = {
            "urn": urn,
            "title": title,
            "matches": excerpts,
            "total_matches": doc_total_matches,
        }
        if start_offset > 0 or cut_by_budget:
            result_entry["content_length"] = full_content_length
        if unshown_matches or cut_by_budget:
            result_entry["next_offset"] = shown_until + start_offset
        results.append(result_entry)

    out: Dict[str, Any] = {
        "results": results,
        "total_matches": total_matches,
        "documents_with_matches": len(results),
    }
    if budget_reached:
        out["truncated"] = True
        out["truncation_note"] = (
            f"Output reached the {graphql_helpers.GREP_OUTPUT_CHAR_BUDGET}-character "
            "limit for one call. Results with next_offset have more to show, including "
            "results whose matches list is empty. To continue a document, call again "
            "with urns=[its urn] and start_offset=its next_offset."
        )
    if skipped_urns:
        out["skipped_urns"] = skipped_urns
    if pattern_note:
        out["pattern_note"] = pattern_note
    out["results"], decisions = annotate_items(
        out["results"], lambda item: item.get("urn"), decisions=decisions
    )
    attach_header(out, decisions, [item["urn"] for item in out["results"]])
    return out
