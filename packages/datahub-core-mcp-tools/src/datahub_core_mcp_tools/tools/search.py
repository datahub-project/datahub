"""The search tool: keyword search across DataHub entities."""

import inspect
import re
import string
import time
from dataclasses import dataclass
from typing import Annotated, Callable, List, Literal, Optional

from datahub.sdk.search_client import compile_filters
from fastmcp.exceptions import ToolError
from loguru import logger
from pydantic import Field, JsonValue

from .. import arg_validation, graphql_helpers
from ..freshness import with_last_modified_dates
from ..read_backend import JsonObject, get_read_backend
from ..result_policy import exclude_configuration, label_search_response, trust_labeled
from ..search_filter_parser import FILTER_DOCS, parse_filter_string
from ..search_filter_parser import _tokenize as _tokenize_filter
from ..search_filter_parser import _TokenType as _FilterToken
from ..search_flags import SearchFlagsOverride
from ..version_requirements import read_only
from ..view_preference import resolve_view_urn

# The searchFlags every search sends, before a host's SearchFlagsOverride.
DEFAULT_SEARCH_FLAGS: JsonObject = {"skipHighlighting": True, "maxAggValues": 5}
MAX_NUM_RESULTS = 50

# A quoted phrase (kept as-is) or a bare term; whitespace and parentheses are left
# untouched between matches.
_QUERY_TOKEN = re.compile(r'"[^"]*"|[^\s()"]+')


def _rewrite_plus_joined_wildcards(query: str) -> str:
    """Rewrite ``a+b*`` terms in a /q query into ``(a AND b*)``.

    /q queries go to Elasticsearch's query_string parser, where "+" between two
    words is not an operator: ``customer+churn`` is a single term that matches only
    because the index analyzer splits it into ``customer`` and ``churn``. Wildcard
    terms skip the analyzer, so ``customer+churn*`` would look for text starting
    with the literal ``customer+churn`` and never match anything. The tool docs
    recommend "+" for AND, so make it compose with "*".
    """
    if not query.lstrip().startswith("/q "):
        return query

    def rewrite(match: re.Match[str]) -> str:
        term = match.group(0)
        # Leave phrases, fielded, boosted, fuzzy, escaped and range terms alone.
        if "*" not in term or any(char in term for char in '":^~\\[]{}'):
            return term
        body = term.lstrip("+-!")
        prefix = term[: len(term) - len(body)]
        parts = [part for part in body.split("+") if part]
        # An operator word as a part (user*+NOT+id) would turn into a real operator.
        if len(parts) < 2 or any(part in _QUERY_OPERATORS for part in parts):
            return term
        return f"{prefix}({' AND '.join(parts)})"

    rewritten = _QUERY_TOKEN.sub(rewrite, query)
    if rewritten != query:
        logger.debug("Rewrote +-joined wildcard terms: {!r} -> {!r}", query, rewritten)
    return rewritten


_QUERY_OPERATORS = frozenset({"AND", "OR", "NOT", "&&", "||"})
# The search index drops tokens shorter than 3 characters, so a word of 1-2 letters or
# digits can't match inside a longer value such as device_os_version. As the first word
# of a "+" group it makes the whole group match nothing.
_SHORT_WORD = re.compile(r"[A-Za-z0-9]{1,2}")
# The only /q queries the short-word retry and the word-level checks rewrite: plain
# words (letters, digits, "_", ".", inner "-") joined by a single "+" or spaces. Every
# other query, with Boolean operators, groups, prefixes, fields, ranges, wildcards,
# escapes or repeated separators, is sent as written; none of it is rewritten in part.
_SIMPLE_WORD = r"[\w.][\w.\-]*"
_SIMPLE_QUERY = re.compile(rf"{_SIMPLE_WORD}(?:(?:\+|[ \t]+){_SIMPLE_WORD})*")
_SIMPLE_SEPARATOR = re.compile(r"(\+|[ \t]+)")


def _simple_words(body: str) -> Optional[List[str]]:
    """The words of a simple /q query body, or None if the body isn't one."""
    if not _SIMPLE_QUERY.fullmatch(body):
        return None
    words = _SIMPLE_SEPARATOR.split(body)[::2]
    if any(word in _QUERY_OPERATORS for word in words):
        return None
    return words


@dataclass
class SearchOutcome:
    """What ``_search_implementation`` did besides the search, for callers that
    explain an empty result: only a zero that was actually measured gets explained."""

    retry_failed: bool = False


@dataclass(frozen=True)
class ShortWordRemoval:
    query: str
    removed: List[str]

    def note(self, original: str) -> Optional[str]:
        if not self.removed:
            return None
        words = ", ".join(f"'{word}'" for word in self.removed)
        return (
            f"No results for {original.strip()}. Searched again without {words}: "
            f"words of 1-2 letters or digits don't match inside longer names. "
            f"Searched for: {self.query}"
        )


def drop_short_words(query: str) -> ShortWordRemoval:
    """Remove 1-2 character words from a simple /q query that also has longer words.

    Used to search again when such a query returns nothing. Words joined by "+" or a
    space must all match in one field, and the index can't match a short word inside a
    longer value. A short word on its own (``/q os``) still matches a value that is
    exactly that word, such as a column named ``os``, so it is kept. Queries that do
    return results are sent unchanged, because there the short word can still narrow
    the results. Only simple queries (see ``_SIMPLE_QUERY``) are rewritten.
    """
    unchanged = ShortWordRemoval(query, [])
    stripped = query.strip()
    if not stripped.startswith("/q "):
        return unchanged
    body = stripped[3:].strip()
    words = _simple_words(body)
    if words is None:
        return unchanged
    removed = [word for word in words if _SHORT_WORD.fullmatch(word)]
    if not removed or len(removed) == len(words):
        return unchanged

    # Keep the separators as written. Through a dropped word, two kept words stay
    # "+"-joined only if both separators were "+". GMS treats a space like "+" (both
    # make the words match in one field), so this keeps the query as the caller wrote
    # it rather than changing what it matches.
    parts = _SIMPLE_SEPARATOR.split(body)
    kept: List[str] = []
    separator: Optional[str] = None
    for i, part in enumerate(parts):
        if i % 2:
            separator = part if separator is None or separator == part == "+" else " "
        elif not _SHORT_WORD.fullmatch(part):
            kept.extend([separator, part] if kept and separator is not None else [part])
            separator = None
    rewritten = f"/q {''.join(kept)}"
    logger.debug("Dropped short words {}: {!r} -> {!r}", removed, query, rewritten)
    return ShortWordRemoval(rewritten, removed)


def _search_flags(skip_aggregates: bool) -> JsonObject:
    flags: JsonObject = dict(DEFAULT_SEARCH_FLAGS)
    if skip_aggregates:
        flags["skipAggregates"] = True
    try:
        override = graphql_helpers.get_mcp_context().tool_context.get(
            SearchFlagsOverride
        )
    except LookupError:
        override = None
    return override.apply(flags) if override is not None else flags


def _search_implementation(
    query: str,
    filter: Optional[str],
    num_results: int,
    sort_by: Optional[str] = None,
    sort_order: Optional[Literal["asc", "desc"]] = "desc",
    offset: int = 0,
    *,
    retry_short_words: bool = True,
    skip_aggregates: bool = False,
    outcome: Optional[SearchOutcome] = None,
) -> dict:
    """Run one keyword search through the request's ReadBackend.

    With ``retry_short_words``, a /q search that returns nothing is searched again
    without the 1-2 letter words joined to longer ones (see ``drop_short_words``).
    ``skip_aggregates`` leaves out the facets, for count-only searches. ``outcome``,
    when given, records whether that retry failed.
    """
    try:
        types, compiled_filters = compile_filters(
            graphql_helpers.parse_filter_input(exclude_configuration(filter))
        )
    except ValueError as e:
        raise ToolError(f"Invalid filter {filter!r}: {e}") from e

    num_results = min(num_results, MAX_NUM_RESULTS)

    # "relevance" is the default ranking, not a field any search index can sort on.
    # Sent as a sort, GMS fails the whole search (all_shards_failed); a caller asking
    # for it means "leave the default ranking alone".
    if sort_by is not None and sort_by.strip().lower() == "relevance":
        sort_by = None

    view_urn = resolve_view_urn(graphql_helpers.get_datahub_client()._graph)
    logger.debug("View: {}", view_urn or "no view")

    variables: JsonObject = {
        "query": _rewrite_plus_joined_wildcards(query),
        "types": list(types) if types else None,
        "orFilters": compiled_filters,  # type: ignore[dict-item]
        "count": max(num_results, 1),  # 0 is not a valid value for count.
        "start": offset,
        "viewUrn": view_urn,
        "searchFlags": _search_flags(skip_aggregates),
    }
    if sort_by is not None:
        variables["sortInput"] = {
            "sortCriteria": [
                {
                    "field": sort_by,
                    "sortOrder": "ASCENDING" if sort_order == "asc" else "DESCENDING",
                }
            ]
        }

    backend = get_read_backend()
    response = backend.search_entities(variables)

    note = None
    if retry_short_words and response.get("total") == 0:
        short_words = drop_short_words(query)
        note = short_words.note(query)
        if note is not None:
            variables["query"] = _rewrite_plus_joined_wildcards(short_words.query)
            try:
                response = backend.search_entities(variables)
            except Exception as e:
                # Keep the original empty result; the note says the retry failed.
                logger.opt(exception=True).warning("Short-word retry failed: {}", e)
                note = (
                    f"No results for {query.strip()}. Couldn't search again without"
                    f" {', '.join(repr(w) for w in short_words.removed)}: {str(e)[:200]}"
                )
                if outcome is not None:
                    outcome.retry_failed = True

    # Hack to support num_results=0 without support for it in the backend.
    if num_results == 0:
        response.pop("searchResults", None)
        response.pop("count", None)

    cleaned = _drop_redundant_dataset_key_name(
        with_last_modified_dates(graphql_helpers.clean_gql_response(response))
    )
    if note is not None:
        cleaned["search_notes"] = [note]
    return cleaned


_MAX_FILTER_CHECKS = 4
_MAX_WORD_CHECKS = 4
# Tokens after which an AND is a value (``tag = AND``), not a conjunction.
_FILTER_VALUE_CONTEXT = frozenset(
    {
        _FilterToken.EQ,
        _FilterToken.NEQ,
        _FilterToken.GT,
        _FilterToken.GTE,
        _FilterToken.LT,
        _FilterToken.LTE,
        _FilterToken.IN,
        _FilterToken.WITHIN,
        _FilterToken.COMMA,
        _FilterToken.LPAREN,
        _FilterToken.AND,
        _FilterToken.OR,
        _FilterToken.NOT,
    }
)


def _top_level_and_clauses(filter: str) -> List[str]:
    """The filter's top-level AND conditions, split with the filter parser's own
    tokenizer, so quoted values, escapes and keywords used as values stay whole.
    Returns the whole filter as one condition unless every condition parses alone,
    or if it has a top-level OR (``a OR b AND c`` is not a list of conditions that
    must all match)."""
    text = filter.strip()
    try:
        tokens = _tokenize_filter(text)
    except ValueError:
        return [text]
    cuts = []
    depth = 0
    previous = None
    for token in tokens:
        if token.type == _FilterToken.LPAREN:
            depth += 1
        elif token.type == _FilterToken.RPAREN:
            depth -= 1
        elif (
            token.type in (_FilterToken.AND, _FilterToken.OR)
            and depth == 0
            and previous is not None
            and previous.type not in _FILTER_VALUE_CONTEXT
        ):
            if token.type == _FilterToken.OR:
                return [text]
            cuts.append(token)
        previous = token
    if not cuts:
        return [text]
    clauses = []
    start = 0
    for cut in cuts:
        clauses.append(text[start : cut.pos].strip())
        start = cut.pos + len(cut.value)
    clauses.append(text[start:].strip())
    try:
        for clause in clauses:
            parse_filter_string(clause)
    except ValueError:
        return [text]
    return clauses


# _search_implementation, or the same function as patched or wrapped by a caller.
SearchFn = Callable[..., dict]


def _count_matches(search_fn: SearchFn, query: str, filter: Optional[str]) -> int:
    # Without the retry, so each count measures exactly the query the note names.
    response = search_fn(
        query, filter, 0, retry_short_words=False, skip_aggregates=True
    )
    return int(response.get("total", 0))


def _filter_clause_counts(
    search_fn: SearchFn, clauses: List[str]
) -> List[tuple[str, int]]:
    return [(clause, _count_matches(search_fn, "*", clause)) for clause in clauses]


def _empty_clauses_note(counts: List[tuple[str, int]]) -> Optional[str]:
    empty = [clause for clause, count in counts if count == 0]
    if not empty:
        return None
    return "Matches nothing on its own: " + "; ".join(f"`{c}`" for c in empty) + "."


def explain_zero_results(
    query: str,
    filter: Optional[str],
    search_fn: Optional[SearchFn] = None,
) -> List[str]:
    """Explain an empty search with facts measured by count-only searches.

    Each note says what matched, never that something doesn't exist. Checks stop at
    the first cause found, in this order: the quotes, the filter, then each word on its
    own. The quote and word checks run only on simple /q queries (see
    ``_SIMPLE_QUERY``); other queries get the filter checks only. ``search_fn`` runs
    the counts; pass the function that ran the empty search so both use the same view
    and backend.

    The search itself succeeded, so a failing count query doesn't replace its empty
    result: the failure is logged and reported as the note instead. Callers explain
    only a zero that was measured: not after a failed short-word retry, whose query
    this would describe (see ``SearchOutcome``).
    """
    run: SearchFn = search_fn or _search_implementation
    count_queries = 0

    def counted(*args: object, **kwargs: object) -> dict:
        nonlocal count_queries
        count_queries += 1
        return run(*args, **kwargs)

    started = time.monotonic()
    try:
        notes = _explain_zero_results(query, filter, counted)
    except Exception as e:
        # The notes are advisory; keep the empty result and say the check failed.
        # The query and filter stay out of the log: they can carry user data.
        logger.opt(exception=True).warning("Could not explain an empty search: {}", e)
        notes = [f"Couldn't check why the search is empty: {str(e)[:200]}"]
    logger.bind(
        count_queries=count_queries,
        elapsed_seconds=round(time.monotonic() - started, 3),
        explained=bool(notes),
    ).info("Explained an empty search")
    return notes


def _explain_zero_results(
    query: str, filter: Optional[str], search_fn: SearchFn
) -> List[str]:
    query = drop_short_words(query).query
    stripped = query.strip()
    is_structured = stripped.startswith("/q ")
    body = stripped[3:].strip() if is_structured else stripped
    clauses = _top_level_and_clauses(filter) if filter else []

    if body in ("", "*"):
        if len(clauses) < 2:
            return ["No entity matches the filter."] if filter else []
        counts = _filter_clause_counts(search_fn, clauses[:_MAX_FILTER_CHECKS])
        empty_note = _empty_clauses_note(counts)
        if empty_note is not None:
            return [empty_note]
        listed = ", ".join(f"`{clause}`: {count}" for clause, count in counts)
        unchecked = len(clauses) - len(counts)
        if unchecked:
            return [
                f"Each of these filter conditions matches on its own: {listed}."
                f" {_not_checked(unchecked, 'condition')}"
            ]
        return [
            f"Each filter condition matches on its own ({listed}), but no entity"
            " matches all of them."
        ]

    # One quoted phrase of simple words: try the words without the quotes.
    if is_structured and body.startswith('"') and body.endswith('"') and len(body) > 2:
        if _simple_words(body[1:-1]) is not None:
            count = _count_matches(search_fn, f"/q {body[1:-1]}", filter)
            if count:
                return [
                    "Nothing matches the quoted phrase exactly; without the quotes,"
                    f" {count} results match."
                ]

    if filter:
        unfiltered = _count_matches(search_fn, query, None)
        if unfiltered:
            note = f"{unfiltered} results match without the filter, so the filter removes all of them."
            if len(clauses) > 1:
                counts = _filter_clause_counts(search_fn, clauses[:_MAX_FILTER_CHECKS])
                empty_note = _empty_clauses_note(counts)
                if empty_note is not None:
                    note += " " + empty_note
            return [note]

    words = _simple_words(body) if is_structured else None
    if words is None:
        return []
    distinct = list(dict.fromkeys(words))
    checked = distinct[:_MAX_WORD_CHECKS]
    unchecked = len(distinct) - len(checked)
    counts = [
        (word, _count_matches(search_fn, f"/q {word}", filter)) for word in checked
    ]
    # The word counts keep the filter, so the notes say so: a word may exist outside it.
    scope = " with the filter" if filter else ""
    unmatched = [word for word, count in counts if count == 0]
    if unmatched:
        return [f"Matches nothing on its own{scope}: {', '.join(unmatched)}."]
    if len(counts) < 2 and not unchecked:
        return []
    listed = ", ".join(f"{word}: {count}" for word, count in counts)
    if not unchecked:
        return [
            f"Each word matches on its own{scope} ({listed}), but never all together"
            " in one field."
        ]
    return [
        f"Each of these words matches on its own{scope}: {listed}."
        f" {_not_checked(unchecked, 'word')}"
    ]


def _not_checked(count: int, noun: str) -> str:
    return (
        f"1 more {noun} wasn't checked."
        if count == 1
        else f"{count} more {noun}s weren't checked."
    )


def _drop_redundant_dataset_key_name(response: dict) -> dict:
    """Drop Dataset.name when properties.name is already set.

    The key name is the fallback for a dataset that never set properties.name.
    When both are present it repeats the URN's name component on every hit.
    """
    for item in response.get("searchResults") or []:
        entity = item.get("entity") if isinstance(item, dict) else None
        if not isinstance(entity, dict):
            continue
        urn = entity.get("urn")
        if not isinstance(urn, str) or not urn.startswith("urn:li:dataset:"):
            continue
        properties = entity.get("properties")
        if isinstance(properties, dict) and properties.get("name") and "name" in entity:
            del entity["name"]
    return response


@read_only
@trust_labeled
def search(
    query: str = "*",
    filter: Annotated[
        Optional[str], Field(validation_alias=arg_validation.FILTER_ALIASES)
    ] = None,
    num_results: Annotated[
        int, Field(validation_alias=arg_validation.SIZE_ALIASES)
    ] = 10,
    sort_by: Optional[str] = None,
    sort_order: Optional[Literal["asc", "desc"]] = "desc",
    offset: Annotated[int, Field(validation_alias=arg_validation.OFFSET_ALIASES)] = 0,
) -> dict[str, JsonValue]:
    """Search across DataHub entities using structured full-text search.
    Results are ordered by relevance and importance - examine top results first.

    SEARCH SYNTAX:
    - Always start keyword queries with /q.
    - Prefer + for names with punctuation; use quotes sparingly.
    - Names are split into words at _ . and -, so plain terms already match parts of
      names: /q revenue finds revenue_2023 and monthly_revenue
    - /q "exact table name" -> exact phrase matching.
    - /q point+sale+app -> requires all terms (matches point_of_sale_app_usage).
    - /q wizard OR pet -> entities containing either term.
    - /q (sales OR revenue)+quarterly -> requires quarterly and either sales or revenue.
    - /q revenue* -> wildcard prefix matching (revenue_2023, revenue_monthly, etc.).
    - /q sales -order -> requires sales and excludes order.
    - Wildcards: term* only matches names or words that START with the term (not
      "contains"), and wildcard matches are not ranked by relevance. /q metadata_aspect*
      finds metadata_aspect_v2 but not fct_metadata_aspects. Prefer plain terms; use *
      only to match a known name prefix.

    PAGINATION:
    - num_results: Number of results to return per page (max: 50)
    - offset: Starting position in results (default: 0)
    - Examples:
      * First page: offset=0, num_results=10
      * Second page: offset=10, num_results=10
      * Third page: offset=20, num_results=10

    FACET EXPLORATION - Discover metadata without returning results:
    - Set num_results=0 to get ONLY facets (no search results)
    - Facets show ALL tags, glossaryTerms, platforms, domains used in the catalog
    - Example: search(query="*", filter="entity_type = dataset", num_results=0)
      -> Returns facets showing all tags/glossaryTerms applied to datasets
    - Use this to discover what metadata exists before doing filtered searches

    TYPICAL WORKFLOW:
    1. Facet exploration: search(query="*", filter="entity_type = dataset", num_results=0)
       -> Examine tags/glossaryTerms facets to see what metadata exists
    2. Filtered search: search(query="*", filter="tag = urn:li:tag:pii", num_results=30)
       -> Get entities with specific tag using URN from step 1
    3. Get details: Use get_entities() on specific results

    $FILTER_DOCS

    SEARCH STRATEGY EXAMPLES:
    - /q customer+behavior -> finds tables with both terms (works with customer_behavior fields)
    - /q customer OR user -> finds tables with either term
    - /q (financial OR revenue)+metrics -> complex boolean logic

    SORTING - Order results by specific fields:
    - sort_by: Field name to sort by (optional)
    - sort_order: "desc" (default) or "asc"

    $SORTING_FIELDS_DOCS

    Note: If sort_by is not provided, search results use default ranking by relevance and
    importance. When using sort_by, results are strictly ordered by that field.
    """
    outcome = SearchOutcome()
    response = _search_implementation(
        query, filter, num_results, sort_by, sort_order, offset, outcome=outcome
    )
    # After a failed short-word retry the zero for the rewritten query was never
    # measured, so there is nothing to explain; the retry's note says it failed.
    if response.get("total") == 0 and offset == 0 and not outcome.retry_failed:
        notes = explain_zero_results(query, filter, search_fn=_search_implementation)
        if notes:
            response["search_notes"] = [*response.get("search_notes", []), *notes]
    return label_search_response(response)


_CLOUD_SORTING_DOCS = """
Available sort fields:
- queryCountLast30DaysFeature: Number of queries in last 30 days
- rowCountFeature: Table row count
- sizeInBytesFeature: Table size in bytes
- writeCountLast30DaysFeature: Number of writes/updates in last 30 days
- lastOperationTime: Time of the last write seen in the source's audit logs
- lastModifiedAt: When the asset was last changed in the source system
- createdAt: When the asset was created in the source system

Sorting examples:
- Most queried datasets:
  search(query="*", filter="entity_type = dataset", sort_by="queryCountLast30DaysFeature", num_results=10)
- Largest tables:
  search(query="*", filter="entity_type = dataset", sort_by="sizeInBytesFeature", num_results=10)
- Smallest tables first:
  search(query="*", filter="entity_type = dataset", sort_by="sizeInBytesFeature", sort_order="asc", num_results=10)
- Most recently updated:
  search(query="*", filter="entity_type = dataset", sort_by="lastOperationTime", sort_order="desc", num_results=10)
- Newest dashboards:
  search(query="*", filter="entity_type = dashboard", sort_by="createdAt", sort_order="desc", num_results=10)
"""

_OSS_SORTING_DOCS = """
Available sort fields:
- lastOperationTime: Time of the last write seen in the source's audit logs
- lastModifiedAt: When the asset was last changed in the source system
- createdAt: When the asset was created in the source system

Sorting examples:
- Most recently updated:
  search(query="*", filter="entity_type = dataset", sort_by="lastOperationTime", sort_order="desc", num_results=10)
- Newest dashboards:
  search(query="*", filter="entity_type = dashboard", sort_by="createdAt", sort_order="desc", num_results=10)
"""


def search_description(*, is_oss: bool) -> str:
    """The search tool's description; DataHub Cloud has more sortable fields."""
    assert search.__doc__ is not None
    # Dedent first: a multi-line value substituted into an indented placeholder
    # indents only its first line.
    return string.Template(inspect.cleandoc(search.__doc__)).substitute(
        FILTER_DOCS=FILTER_DOCS,
        SORTING_FIELDS_DOCS=(
            _OSS_SORTING_DOCS if is_oss else _CLOUD_SORTING_DOCS
        ).strip(),
    )
