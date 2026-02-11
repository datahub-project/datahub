"""Redshift SQL preprocessing for malformed queries.

This module fixes SQL parsing issues caused by:
1. Redshift internal RTRIM bug - when populating stl_query.querytxt, Redshift
   internally reconstructs queries from STL_QUERYTEXT segments using RTRIM on
   each 200-char segment, which removes meaningful trailing spaces at boundaries.
   Example: "CASE " at segment end -> "CASE" -> concatenated with "WHEN" -> "CASEWHEN"
2. AWS DMS - password redaction merges column names, UPDATE queries lack FROM clause
"""

import logging
import re
from typing import Callable, List, Tuple, Union

logger = logging.getLogger(__name__)

# Type alias for trigger: either a substring tuple or a callable
TriggerType = Union[Tuple[str, ...], Callable[[str], bool]]

# =============================================================================
# CONSTANTS
# =============================================================================

# Identifier length limit (prevents ReDoS on malformed input)
_MAX_IDENTIFIER_LEN = 100
_IDENTIFIER_PATTERN = rf"[a-z][a-z0-9_]{{1,{_MAX_IDENTIFIER_LEN}}}"
_IDENTIFIER_PATTERN_OPT = rf"[a-z][a-z0-9_]{{0,{_MAX_IDENTIFIER_LEN}}}"

# Sigma alias pattern: q + digits (q1, q11) or t + digits (t1, t12)
_SIGMA_ALIAS_PATTERN = r"(q\d{1,3}|t\d{1,3})"

# SQL data types for pattern matching
_SQL_TYPES = (
    r"timestamp(?:tz)?|date|time(?:tz)?|int(?:eger)?|bigint|smallint|"
    r"float|real|double|numeric|decimal|varchar|char|text|boolean|bool"
)

# Safe word suffixes - words that look like malformations but aren't
_SAFE_KEYWORD_SUFFIXES: dict[str, tuple[str, ...]] = {
    "when": ("ever", "ce"),  # whenever, whence
    "then": ("ce",),  # thence
    "else": ("where",),  # elsewhere
    "and": ("roid", "erson", "ersen", "rew", "re", "y", "es", "ora", "romeda"),
    "select": ("ion", "or", "ive", "ivity", "ed", "ing"),
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _has_keyword_followed_by_letter(query_lower: str, keyword: str) -> bool:
    """Check if keyword is directly followed by a letter (potential malformation)."""
    safe_suffixes = _SAFE_KEYWORD_SUFFIXES.get(keyword, ())

    idx = query_lower.find(keyword)
    while idx != -1:
        end = idx + len(keyword)
        if end < len(query_lower) and query_lower[end].isalpha():
            remaining = query_lower[end:]
            is_safe = safe_suffixes and any(
                remaining.startswith(s) for s in safe_suffixes
            )
            if not is_safe:
                return True
        idx = query_lower.find(keyword, end)
    return False


def _should_apply_trigger(trigger: TriggerType, query_lower: str) -> bool:
    """Check if a pattern's trigger condition is met."""
    if callable(trigger):
        return trigger(query_lower)
    return any(t in query_lower for t in trigger)


# =============================================================================
# REDSHIFT RTRIM BUG PATTERN DEFINITIONS
# =============================================================================

# --- Type Cast Patterns ---
_TYPE_CAST_PATTERNS: List[Tuple[TriggerType, re.Pattern[str], str]] = [
    # ::TYPEcast_ -> ::TYPE cast_
    (
        ("cast_",),
        re.compile(rf"::({_SQL_TYPES})(cast_)", re.IGNORECASE),
        r"::\1 \2",
    ),
    # ::TYPEis -> ::TYPE is (e.g., ::timestamptzis null)
    (
        ("tzis", "teis", "olis"),
        re.compile(rf"::({_SQL_TYPES})(is)\b", re.IGNORECASE),
        r"::\1 \2",
    ),
    # ASTYPE -> AS TYPE (e.g., ASINT4 -> AS INT4)
    (
        (
            "asint",
            "asbigint",
            "assmallint",
            "asfloat",
            "asreal",
            "asdouble",
            "asnumeric",
            "asdecimal",
            "asvarchar",
            "aschar",
            "astext",
            "asboolean",
            "asbool",
            "astimestamp",
            "asdate",
            "astime",
        ),
        re.compile(
            r"\bAS(INT[248]?|INTEGER|BIGINT|SMALLINT|FLOAT|REAL|DOUBLE|NUMERIC|"
            r"DECIMAL|VARCHAR|CHAR|TEXT|BOOLEAN|BOOL|TIMESTAMP(?:TZ)?|DATE|TIME(?:TZ)?)\b",
            re.IGNORECASE,
        ),
        r"AS \1",
    ),
]

# --- Compound Keyword Patterns ---
_COMPOUND_KEYWORD_PATTERNS: List[Tuple[TriggerType, re.Pattern[str], str]] = [
    (("casewhen",), re.compile(r"\bcasewhen\b", re.IGNORECASE), "case when"),
    (
        ("distinctcase",),
        re.compile(r"\bdistinctcase\b", re.IGNORECASE),
        "distinct case",
    ),
    (("elsenull",), re.compile(r"\belsenull\b", re.IGNORECASE), "else null"),
    (("notnull",), re.compile(r"\bnotnull\b", re.IGNORECASE), "not null"),
    (("nulland",), re.compile(r"\bnulland\b", re.IGNORECASE), "null and"),
    (("nullor",), re.compile(r"\bnullor\b", re.IGNORECASE), "null or"),
    (("nullgroup",), re.compile(r"\bnullgroup\b", re.IGNORECASE), "null group"),
    (("nullend",), re.compile(r"\bnullend\b", re.IGNORECASE), "null end"),
    (("groupby",), re.compile(r"\bgroupby\b", re.IGNORECASE), "group by"),
    (("orderby",), re.compile(r"\borderby\b", re.IGNORECASE), "order by"),
    (("thencase",), re.compile(r"\bend\s+thencase\b", re.IGNORECASE), "end then case"),
    # is null followed by and/or without space
    (("nulland",), re.compile(r"\bis\s+nulland\b", re.IGNORECASE), "is null and"),
    (("nullor",), re.compile(r"\bis\s+nullor\b", re.IGNORECASE), "is null or"),
    # UNI on ALL -> UNION ALL (Redshift truncates long queries)
    (("uni on all",), re.compile(r"\bUNI\s+on\s+ALL\b", re.IGNORECASE), "UNION ALL"),
    # on ON -> on (double ON from malformed SQL)
    (("on on",), re.compile(r"\bon\s+ON\b", re.IGNORECASE), "on"),
    # )AND -> ) AND, )OR -> ) OR (missing space after closing paren)
    ((")and",), re.compile(r"\)(AND)\b", re.IGNORECASE), r") \1"),
    ((")or",), re.compile(r"\)(OR)\b", re.IGNORECASE), r") \1"),
    # FALSEAS -> FALSE AS, TRUEAS -> TRUE AS
    (("falseas",), re.compile(r"\bFALSE(AS)\b", re.IGNORECASE), r"FALSE \1"),
    (("trueas",), re.compile(r"\bTRUE(AS)\b", re.IGNORECASE), r"TRUE \1"),
    # NULLAS -> NULL AS
    (("nullas",), re.compile(r"\bNULL(AS)\b", re.IGNORECASE), r"NULL \1"),
]

# --- Number + AS Patterns ---
# Handles cases like 0AS, 1AS, 123AS -> 0 AS, 1 AS, 123 AS
_NUMBER_AS_PATTERN: List[Tuple[TriggerType, re.Pattern[str], str]] = [
    # <number>AS -> <number> AS (e.g., CAST(0AS INT8) -> CAST(0 AS INT8))
    # Trigger on any digit followed by 'as' - the quick indicators cover 0as-9as
    (
        ("0as", "1as", "2as", "3as", "4as", "5as", "6as", "7as", "8as", "9as"),
        re.compile(r"(\d)(AS)\b", re.IGNORECASE),
        r"\1 \2",
    ),
]

# --- Sigma Alias Patterns ---
_SIGMA_ALIAS_PATTERNS: List[Tuple[TriggerType, re.Pattern[str], str]] = [
    # case when<alias>. -> case when <alias>.
    (
        ("whenq", "whent"),
        re.compile(rf"\bcase\s+when{_SIGMA_ALIAS_PATTERN}\.", re.IGNORECASE),
        r"case when \1.",
    ),
    # when<alias>. -> when <alias>.
    (
        ("whenq", "whent"),
        re.compile(rf"\bwhen{_SIGMA_ALIAS_PATTERN}\.", re.IGNORECASE),
        r"when \1.",
    ),
    # then<alias>. -> then <alias>.
    (
        ("thenq", "thent"),
        re.compile(rf"\bthen{_SIGMA_ALIAS_PATTERN}\.", re.IGNORECASE),
        r"then \1.",
    ),
    # else<alias>. -> else <alias>.
    (
        ("elseq", "elset"),
        re.compile(rf"\belse{_SIGMA_ALIAS_PATTERN}\.", re.IGNORECASE),
        r"else \1.",
    ),
    # on<alias>. -> on <alias>.
    (
        ("onq", "ont"),
        re.compile(rf"\bon{_SIGMA_ALIAS_PATTERN}\.([a-z])", re.IGNORECASE),
        r"on \1.\2",
    ),
    # ) <alias>on -> ) <alias> on
    (
        lambda q: "on " in q,
        re.compile(
            rf"\)(\s*)({_IDENTIFIER_PATTERN_OPT})on\s+(coalesce|q\d+\.|[a-z])",
            re.IGNORECASE,
        ),
        r")\1\2 on \3",
    ),
]

# --- Identifier Patterns ---
_IDENTIFIER_PATTERNS: List[Tuple[TriggerType, re.Pattern[str], str]] = [
    # case when<identifier><operator> -> case when <identifier> <operator>
    (
        lambda q: _has_keyword_followed_by_letter(q, "when"),
        re.compile(rf"\bcase\s+when({_IDENTIFIER_PATTERN})(>|<|=|!)", re.IGNORECASE),
        r"case when \1 \2",
    ),
    # case when<identifier> <keyword> -> case when <identifier> <keyword>
    (
        lambda q: _has_keyword_followed_by_letter(q, "when"),
        re.compile(
            rf"\bcase\s+when({_IDENTIFIER_PATTERN})\s+(then|and|or|is\b|in\b|not\b|between\b)",
            re.IGNORECASE,
        ),
        r"case when \1 \2",
    ),
    # when<identifier> <keyword> -> when <identifier> <keyword>
    (
        lambda q: _has_keyword_followed_by_letter(q, "when"),
        re.compile(
            rf"\bwhen(?!ever|ce\b)({_IDENTIFIER_PATTERN})\s+(then|and|or|>|<|=|!)",
            re.IGNORECASE,
        ),
        r"when \1 \2",
    ),
    # when<identifier><operator> -> when <identifier> <operator>
    (
        lambda q: _has_keyword_followed_by_letter(q, "when"),
        re.compile(
            rf"\bwhen(?!ever|ce\b)({_IDENTIFIER_PATTERN})(>|<|=|!)", re.IGNORECASE
        ),
        r"when \1 \2",
    ),
    # and<identifier> <operator> -> and <identifier> <operator>
    (
        lambda q: _has_keyword_followed_by_letter(q, "and"),
        re.compile(
            rf"\band(?!roid|erson|ersen|rew|re\b|y\b|es\b|ora|romeda)({_IDENTIFIER_PATTERN})\s*(>|<|=|!|is\b|in\b)",
            re.IGNORECASE,
        ),
        r"and \1 \2",
    ),
    # select<identifier>, -> select <identifier>,
    (
        lambda q: _has_keyword_followed_by_letter(q, "select"),
        re.compile(
            rf"\bselect(?!ion|or\b|ive|ivity|ed\b|ing\b)({_IDENTIFIER_PATTERN_OPT})\s*,",
            re.IGNORECASE,
        ),
        r"select \1,",
    ),
    # from<schema>.<table> -> from <schema>.<table>
    (
        lambda q: _has_keyword_followed_by_letter(q, "from"),
        re.compile(rf"\bfrom({_IDENTIFIER_PATTERN_OPT})\.([a-z])", re.IGNORECASE),
        r"from \1.\2",
    ),
    # <identifier>from " -> <identifier> from "
    (
        lambda q: _has_keyword_followed_by_letter(q, "from")
        or 'from"' in q
        or "from'" in q,
        re.compile(r"([a-z0-9_])from\s*\"", re.IGNORECASE),
        r"\1 from \"",
    ),
    # group by<identifier> -> group by <identifier>
    (
        lambda q: _has_keyword_followed_by_letter(q, "by"),
        re.compile(rf"\bgroup\s+by({_IDENTIFIER_PATTERN})", re.IGNORECASE),
        r"group by \1",
    ),
    # order by<identifier> -> order by <identifier>
    (
        lambda q: _has_keyword_followed_by_letter(q, "by"),
        re.compile(rf"\border\s+by({_IDENTIFIER_PATTERN})", re.IGNORECASE),
        r"order by \1",
    ),
    # end then<identifier> -> end then <identifier>
    (
        lambda q: _has_keyword_followed_by_letter(q, "then"),
        re.compile(rf"\bend\s+then({_IDENTIFIER_PATTERN})", re.IGNORECASE),
        r"end then \1",
    ),
    # null<identifier> -> null <identifier> (e.g., nullnull_eq_)
    (("nullnull_",), re.compile(r"\bnull(null_[a-z0-9_]+)", re.IGNORECASE), r"null \1"),
    # end<identifier> -> end <identifier> (e.g., endif_)
    (("endif_",), re.compile(r"\bend(if_[a-z0-9_]+)", re.IGNORECASE), r"end \1"),
    # or<identifier> -> or <identifier> (e.g., orif_542)
    (("orif_",), re.compile(r"\bor(if_[a-z0-9_]+)", re.IGNORECASE), r"or \1"),
    # selectstatus -> select status
    (
        ("selectstatus",),
        re.compile(r"\bselectstatus\b", re.IGNORECASE),
        "select status",
    ),
]

# --- Function Patterns ---
_FUNCTION_PATTERNS: List[Tuple[TriggerType, re.Pattern[str], str]] = [
    # or<aggregate>( -> or <aggregate>(
    (
        ("ormin", "ormax", "orsum", "orcount", "oravg"),
        re.compile(r"\bor(min|max|sum|count|avg)\s*\(", re.IGNORECASE),
        r"or \1(",
    ),
    # and<aggregate>( -> and <aggregate>(
    (
        ("andmin", "andmax", "andsum", "andcount", "andavg"),
        re.compile(r"\band(min|max|sum|count|avg)\s*\(", re.IGNORECASE),
        r"and \1(",
    ),
    # or<function>( -> or <function>(
    (
        ("ordateadd", "ordatediff", "ordate_trunc", "orcoalesce", "ornullif", "orcast"),
        re.compile(
            r"\bor(dateadd|datediff|date_trunc|coalesce|nullif|cast)\s*\(",
            re.IGNORECASE,
        ),
        r"or \1(",
    ),
    # end else<function> -> end else <function>
    (
        (
            "elsedateadd",
            "elsedatediff",
            "elsedate_trunc",
            "elsecoalesce",
            "elsenullif",
            "elsecast",
            "elsecase",
        ),
        re.compile(
            r"\bend\s+else(dateadd|datediff|date_trunc|coalesce|nullif|cast|case)\b",
            re.IGNORECASE,
        ),
        r"end else \1",
    ),
]

# --- Combined Pattern List ---
_REDSHIFT_RTRIM_PATTERNS: List[Tuple[TriggerType, re.Pattern[str], str]] = (
    _TYPE_CAST_PATTERNS
    + _COMPOUND_KEYWORD_PATTERNS
    + _NUMBER_AS_PATTERN
    + _SIGMA_ALIAS_PATTERNS
    + _IDENTIFIER_PATTERNS
    + _FUNCTION_PATTERNS
)

# Quick indicators for early exit optimization
_QUICK_MALFORMATION_INDICATORS = (
    # Compound keywords
    "casewhen",
    "groupby",
    "orderby",
    "elsenull",
    "notnull",
    "nulland",
    "nullor",
    "nullend",
    "distinctcase",
    "nullgroup",
    "thencase",
    "uni on all",
    "on on",
    # Missing space after ) before keyword
    ")and",
    ")or",
    # Missing space before AS
    "falseas",
    "trueas",
    "nullas",
    "0as",
    "1as",
    "2as",
    "3as",
    "4as",
    "5as",
    "6as",
    "7as",
    "8as",
    "9as",
    # Type cast triggers
    "cast_",
    "tzis",
    "teis",
    "olis",
    "asint",
    "asbigint",
    "asfloat",
    "asvarchar",
    "astext",
    "asbool",
    "astimestamp",
    "asdate",
    # Identifier triggers
    "nullnull_",
    "endif_",
    "orif_",
    "selectstatus",
    # Function triggers
    "ormin",
    "ormax",
    "andmin",
    "andmax",
    "ordateadd",
    "elsedateadd",
    # Sigma alias triggers
    "whenq",
    "whent",
    "thenq",
    "thent",
    "elseq",
    "elset",
    "onq",
    "ont",
)


# =============================================================================
# REDSHIFT RTRIM BUG PREPROCESSING
# =============================================================================


def _is_preprocessing_required(query_lower: str) -> bool:
    """Quick check if query may contain RTRIM malformations."""
    # Check obvious compound keywords and triggers
    for indicator in _QUICK_MALFORMATION_INDICATORS:
        if indicator in query_lower:
            return True

    # Check identifier patterns (blacklist with safe word filtering)
    for keyword in ("when", "then", "else", "from", "and", "select", "by"):
        if _has_keyword_followed_by_letter(query_lower, keyword):
            return True

    return False


def preprocess_redshift_query(query: str) -> str:
    """Preprocess query to fix Redshift's internal RTRIM bug.

    Redshift stores queries in STL_QUERYTEXT as 200-char segments. When
    reconstructing the full query for stl_query.querytxt, Redshift applies
    RTRIM to each segment, which removes meaningful trailing spaces at
    segment boundaries. This causes keywords like "CASE WHEN" to become
    "CASEWHEN" when the space falls at a 200-char boundary.

    This affects ANY query, regardless of the tool that generated it.
    Long queries (like those from BI tools) are more likely to be affected
    simply because they have more segment boundaries.

    Known malformations fixed:
    - ::timestamptzcast_ -> ::timestamptz cast_
    - ::timestamptzis -> ::timestamptz is
    - ASINT4 -> AS INT4 (and other types)
    - casewhen -> case when
    - distinctcase -> distinct case
    - elsenull -> else null
    - notnull -> not null
    - nulland -> null and
    - nullor -> null or
    - nullgroup -> null group
    - nullend -> null end
    - groupby -> group by
    - orderby -> order by
    - end thencase -> end then case
    - is nulland -> is null and
    - is nullor -> is null or
    - UNI on ALL -> UNION ALL
    - on ON -> on
    - )AND -> ) AND, )OR -> ) OR
    - FALSEAS -> FALSE AS, TRUEAS -> TRUE AS
    - 0AS, 1AS, <num>AS -> 0 AS, 1 AS, <num> AS
    - whenq11. -> when q11. (Sigma aliases)
    - thenq11. -> then q11.
    - whene_xxx then -> when e_xxx then
    - fromsigma.t_ -> from sigma.t_
    - ormin( -> or min(
    - andmax( -> and max(
    - ordateadd( -> or dateadd(
    - end elsedateadd -> end else dateadd

    Args:
        query: The SQL query to preprocess

    Returns:
        The preprocessed query with fixed spacing
    """
    query_lower = query.lower()

    # Early exit if no malformation indicators
    if not _is_preprocessing_required(query_lower):
        return query

    # Debug logging for UNI on ALL investigation
    has_uni_on_all = "uni on all" in query_lower
    if has_uni_on_all:
        logger.debug("Preprocessing query with 'UNI on ALL' pattern")

    # Apply only patterns whose triggers match
    result = query
    for trigger, pattern, replacement in _REDSHIFT_RTRIM_PATTERNS:
        if _should_apply_trigger(trigger, query_lower):
            result = pattern.sub(replacement, result)

    # Verify UNI on ALL was fixed
    if has_uni_on_all:
        if "uni on all" in result.lower():
            logger.warning(
                "UNI on ALL pattern was NOT fixed. Query snippet: %s",
                query[:200] if len(query) > 200 else query,
            )
        else:
            logger.debug("UNI on ALL pattern was successfully fixed")

    return result


# Backward compatibility alias
preprocess_query_for_sigma = preprocess_redshift_query


# =============================================================================
# DMS PREPROCESSING
# =============================================================================

# DMS staging table patterns
_DMS_STAGING_TABLE_PATTERN_QUOTED = re.compile(
    r'"([^"]+)"\.\"(awsdms_[^"]+)\"', re.IGNORECASE
)
_DMS_STAGING_TABLE_PATTERN_UNQUOTED = re.compile(
    r"\b([a-z_][a-z0-9_]*)\.(awsdms_[a-z0-9_]+)\b", re.IGNORECASE
)

# DMS password redaction pattern
_DMS_PASSWORD_REDACTION_PATTERN = re.compile(r" '\*\*\*'([a-zA-Z])")


def preprocess_dms_update_query(query: str) -> str:
    """Preprocess AWS DMS UPDATE queries to add missing FROM clause.

    DMS generates UPDATE queries that reference staging tables in SET
    expressions without a FROM clause, which sqlglot can't parse for CLL.
    """
    if not query.strip().upper().startswith("UPDATE"):
        return query

    if re.search(r"\bFROM\b", query, re.IGNORECASE):
        return query

    # Find DMS staging table references
    matches_quoted = _DMS_STAGING_TABLE_PATTERN_QUOTED.findall(query)
    matches_unquoted = _DMS_STAGING_TABLE_PATTERN_UNQUOTED.findall(query)

    staging_tables: set[tuple[str, str]] = set()
    for schema, table in matches_quoted + matches_unquoted:
        if table.lower().startswith("awsdms_"):
            staging_tables.add((schema, table))

    if not staging_tables:
        return query

    # Build and insert FROM clause
    from_parts = [f'"{schema}"."{table}"' for schema, table in staging_tables]
    from_clause = " FROM " + ", ".join(from_parts)

    where_match = re.search(r"\bWHERE\b", query, re.IGNORECASE)
    if where_match:
        insert_pos = where_match.start()
        result = query[:insert_pos] + from_clause + " " + query[insert_pos:]
    else:
        result = query + from_clause

    logger.debug(f"Injected FROM clause for DMS UPDATE: {from_clause}")
    return result


def preprocess_dms_password_redaction(query: str) -> str:
    """Fix DMS password redaction that merges column names.

    DMS redacts passwords with '***' which can merge with the next column:
        "password '***'next_col" -> "password", "next_col"
    """
    return _DMS_PASSWORD_REDACTION_PATTERN.sub(r'", "\1', query)
