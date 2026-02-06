import logging
import re
from typing import Callable, List, Tuple, Union

logger = logging.getLogger(__name__)

# Type alias for trigger: either a substring tuple or a callable
TriggerType = Union[Tuple[str, ...], Callable[[str], bool]]

# =============================================================================
# SIGMA ALIAS PATTERNS (Whitelist Approach)
# =============================================================================
# Sigma generates predictable table aliases in its SQL:
#   - q + digits: q1, q2, q11, q123 (query subselects)
#   - t + digits: t1, t2, t12 (temp tables)
#
# We use a WHITELIST approach for alias-based patterns because:
#   1. Sigma aliases are predictable and limited
#   2. Blacklisting English words (once, only, android...) is impossible to complete
#   3. Zero false positives - we only match what Sigma actually generates
#
# Conservative pattern: Only q + digits and t + digits
# We intentionally exclude single-letter aliases (a, b, c) because:
#   - They're less common in Sigma output
#   - Patterns like "ona.", "whenb." could theoretically match edge cases
#
# Example matches: whenq11., onq3., thent2.
# Example non-matches: once., only., onto. (safe!)
_SIGMA_ALIAS_PATTERN = r"(q\d{1,3}|t\d{1,3})"

# =============================================================================
# IDENTIFIER LENGTH LIMITS (ReDoS Prevention)
# =============================================================================
# All identifier patterns use {1,100} length limits to prevent catastrophic
# backtracking (ReDoS) on malformed input with very long identifiers.
# 100 chars is generous for any real SQL identifier while preventing DoS.
_MAX_IDENTIFIER_LEN = 100
_IDENTIFIER_PATTERN = rf"[a-z][a-z0-9_]{{1,{_MAX_IDENTIFIER_LEN}}}"
_IDENTIFIER_PATTERN_OPT = (
    rf"[a-z][a-z0-9_]{{0,{_MAX_IDENTIFIER_LEN}}}"  # Optional length
)

# =============================================================================
# SAFE WORD FILTERING (Blacklist Approach - for column/identifier patterns)
# =============================================================================
# For patterns involving arbitrary column names (not aliases), we use safe word
# filtering as a fallback. This is less robust than whitelist, but column names
# can be anything so we can't whitelist them.
#
# Known safe suffixes for each keyword - these are legitimate words, not malformations
# Must match the negative lookaheads in the regex patterns
_SAFE_KEYWORD_SUFFIXES: dict[str, tuple[str, ...]] = {
    # "when" + suffix -> whenever, whence (not "when e_col")
    "when": ("ever", "ce"),
    # "then" + suffix -> thence (not "then ce_col")
    "then": ("ce",),
    # "else" + suffix -> elsewhere (not "else where_col")
    "else": ("where",),
    # "and" + suffix -> android, anderson, andre, etc. (not "and roid_col")
    "and": (
        "roid",
        "erson",
        "ersen",
        "rew",
        "re",
        "y",
        "es",
        "ora",
        "romeda",
    ),
    # "select" + suffix -> selection, selector, etc. (not "select ion_col")
    "select": ("ion", "or", "ive", "ivity", "ed", "ing"),
}


def _has_keyword_followed_by_letter(
    query_lower: str, keyword: str, exclude_safe_words: bool = True
) -> bool:
    """Check if keyword is directly followed by a letter (potential malformation).

    Args:
        query_lower: Lowercase query string
        keyword: Keyword to search for (e.g., "when", "and")
        exclude_safe_words: If True, ignore known safe words (e.g., "whenever" for "when")
    """
    safe_suffixes = (
        _SAFE_KEYWORD_SUFFIXES.get(keyword, ()) if exclude_safe_words else ()
    )
    idx = 0
    keyword_len = len(keyword)
    while True:
        idx = query_lower.find(keyword, idx)
        if idx == -1:
            return False
        end = idx + keyword_len
        if end < len(query_lower) and query_lower[end].isalpha():
            # Check if this is a known safe word
            if safe_suffixes:
                remaining = query_lower[end:]
                is_safe = any(remaining.startswith(suffix) for suffix in safe_suffixes)
                if not is_safe:
                    return True
            else:
                return True
        idx = end
    return False


_SIGMA_SQL_FIX_PATTERNS: List[Tuple[TriggerType, re.Pattern[str], str]] = [
    # ::TYPEcast_ -> ::TYPE cast_
    # e.g., "::timestamptzcast_date_to_timestamp" -> "::timestamptz cast_date_to_timestamp"
    (
        ("cast_",),
        re.compile(
            r"::(timestamp(?:tz)?|date|time(?:tz)?|int(?:eger)?|bigint|smallint|"
            r"float|real|double|numeric|decimal|varchar|char|text|boolean|bool)"
            r"(cast_)",
            re.IGNORECASE,
        ),
        r"::\1 \2",
    ),
    # ::TYPEis -> ::TYPE is (e.g., ::timestamptzis null -> ::timestamptz is null)
    (
        (
            "tzis",
            "teis",
            "olis",
        ),  # timestamptzis, dateis, boolis
        re.compile(
            r"::(timestamp(?:tz)?|date|time(?:tz)?|int(?:eger)?|bigint|smallint|"
            r"float|real|double|numeric|decimal|varchar|char|text|boolean|bool)"
            r"(is)\b",
            re.IGNORECASE,
        ),
        r"::\1 \2",
    ),
    # casewhen -> case when
    (
        ("casewhen",),
        re.compile(r"\bcasewhen\b", re.IGNORECASE),
        "case when",
    ),
    # case when<alias>. -> case when <alias>. (e.g., "case whenq11.col" -> "case when q11.col")
    # WHITELIST: Only matches known Sigma alias patterns (q1, t1, a, etc.)
    (
        ("whenq", "whent"),  # Quick trigger for Sigma aliases
        re.compile(rf"\bcase\s+when{_SIGMA_ALIAS_PATTERN}\.", re.IGNORECASE),
        r"case when \1.",
    ),
    # case when<identifier><operator> -> case when <identifier> <operator> (no space before operator)
    # e.g., "case whenarr_down>" -> "case when arr_down >"
    (
        lambda q: _has_keyword_followed_by_letter(q, "when"),
        re.compile(rf"\bcase\s+when({_IDENTIFIER_PATTERN})(>|<|=|!)", re.IGNORECASE),
        r"case when \1 \2",
    ),
    # case when<identifier> <keyword> -> case when <identifier> <keyword>
    # e.g., "case whenarr_down then" -> "case when arr_down then"
    (
        lambda q: _has_keyword_followed_by_letter(q, "when"),
        re.compile(
            rf"\bcase\s+when({_IDENTIFIER_PATTERN})\s+(then|and|or|is\b|in\b|not\b|between\b)",
            re.IGNORECASE,
        ),
        r"case when \1 \2",
    ),
    # distinctcase -> distinct case
    (
        ("distinctcase",),
        re.compile(r"\bdistinctcase\b", re.IGNORECASE),
        "distinct case",
    ),
    # elsenull -> else null
    (
        ("elsenull",),
        re.compile(r"\belsenull\b", re.IGNORECASE),
        "else null",
    ),
    # SQL keyword + aggregate without space: ormin(, andmax(, etc.
    (
        ("ormin", "ormax", "orsum", "orcount", "oravg"),
        re.compile(r"\bor(min|max|sum|count|avg)\s*\(", re.IGNORECASE),
        r"or \1(",
    ),
    (
        ("andmin", "andmax", "andsum", "andcount", "andavg"),
        re.compile(r"\band(min|max|sum|count|avg)\s*\(", re.IGNORECASE),
        r"and \1(",
    ),
    # notnull -> not null, nulland -> null and, nullor -> null or
    (("notnull",), re.compile(r"\bnotnull\b", re.IGNORECASE), "not null"),
    (("nulland",), re.compile(r"\bnulland\b", re.IGNORECASE), "null and"),
    (("nullor",), re.compile(r"\bnullor\b", re.IGNORECASE), "null or"),
    # nullgroup -> null group (for "is null group by")
    (("nullgroup",), re.compile(r"\bnullgroup\b", re.IGNORECASE), "null group"),
    # isnull followed by and/or without space: isnulland -> is null and
    (("nulland",), re.compile(r"\bis\s+nulland\b", re.IGNORECASE), "is null and"),
    (("nullor",), re.compile(r"\bis\s+nullor\b", re.IGNORECASE), "is null or"),
    # is nullnull_eq_ -> is null null_eq_ (null followed by identifier starting with null)
    (("nullnull_",), re.compile(r"\bnull(null_[a-z0-9_]+)", re.IGNORECASE), r"null \1"),
    # endif_ -> end if_ (end followed by identifier starting with if)
    (("endif_",), re.compile(r"\bend(if_[a-z0-9_]+)", re.IGNORECASE), r"end \1"),
    # when<alias>. -> when <alias>. (e.g., "whenq11.col" -> "when q11.col")
    # WHITELIST: Only matches known Sigma alias patterns (q1, t1, a, etc.)
    # Safe from false positives like "whenever", "whence" - they don't match the pattern
    (
        ("whenq", "whent"),  # Quick trigger for Sigma aliases
        re.compile(rf"\bwhen{_SIGMA_ALIAS_PATTERN}\.", re.IGNORECASE),
        r"when \1.",
    ),
    # then<alias>. -> then <alias>. (e.g., "thenq11.col" -> "then q11.col")
    # WHITELIST: Only matches known Sigma alias patterns
    (
        ("thenq", "thent"),  # Quick trigger for Sigma aliases
        re.compile(rf"\bthen{_SIGMA_ALIAS_PATTERN}\.", re.IGNORECASE),
        r"then \1.",
    ),
    # else<alias>. -> else <alias>. (e.g., "elseq11.col" -> "else q11.col")
    # WHITELIST: Only matches known Sigma alias patterns
    (
        ("elseq", "elset"),  # Quick trigger for Sigma aliases
        re.compile(rf"\belse{_SIGMA_ALIAS_PATTERN}\.", re.IGNORECASE),
        r"else \1.",
    ),
    # when<identifier> followed by space/operator -> when <identifier>
    # e.g., "whene_r6m06nuq then" -> "when e_r6m06nuq then"
    # Exclude common words: whenever, whence
    (
        lambda q: _has_keyword_followed_by_letter(q, "when"),
        re.compile(
            rf"\bwhen(?!ever|ce\b)({_IDENTIFIER_PATTERN})\s+(then|and|or|>|<|=|!)",
            re.IGNORECASE,
        ),
        r"when \1 \2",
    ),
    # when<identifier><operator> -> when <identifier> <operator> (no space before operator)
    # e.g., "whenarr_down>" -> "when arr_down >"
    # Exclude common words: whenever, whence
    (
        lambda q: _has_keyword_followed_by_letter(q, "when"),
        re.compile(
            rf"\bwhen(?!ever|ce\b)({_IDENTIFIER_PATTERN})(>|<|=|!)", re.IGNORECASE
        ),
        r"when \1 \2",
    ),
    # from<schema>.<table> or from<schema>.t_ (Sigma temp table pattern)
    (
        lambda q: _has_keyword_followed_by_letter(q, "from"),
        re.compile(rf"\bfrom({_IDENTIFIER_PATTERN_OPT})\.([a-z])", re.IGNORECASE),
        r"from \1.\2",
    ),
    # groupby -> group by
    (("groupby",), re.compile(r"\bgroupby\b", re.IGNORECASE), "group by"),
    # orderby -> order by
    (("orderby",), re.compile(r"\borderby\b", re.IGNORECASE), "order by"),
    # or<function>( -> or <function>( for common functions
    (
        ("ordateadd", "ordatediff", "ordate_trunc", "orcoalesce", "ornullif", "orcast"),
        re.compile(
            r"\bor(dateadd|datediff|date_trunc|coalesce|nullif|cast)\s*\(",
            re.IGNORECASE,
        ),
        r"or \1(",
    ),
    # and<identifier> followed by comparison operator -> and <identifier>
    # e.g., "andwdctsy87db > 0" -> "and wdctsy87db > 0"
    # Exclude common words: android, anderson, andersen, andrew, andre, andy, and others
    (
        lambda q: _has_keyword_followed_by_letter(q, "and"),
        re.compile(
            rf"\band(?!roid|erson|ersen|rew|re\b|y\b|es\b|ora|romeda)({_IDENTIFIER_PATTERN})\s*(>|<|=|!|is\b|in\b)",
            re.IGNORECASE,
        ),
        r"and \1 \2",
    ),
    # on<alias>. -> on <alias>. (e.g., "onq3.id" -> "on q3.id")
    # WHITELIST: Only matches known Sigma alias patterns (q1, t1, a, etc.)
    # Safe from false positives like "once.", "only.", "onto." - they don't match the pattern
    (
        ("onq", "ont"),  # Quick trigger for Sigma aliases (onq1., ont2.)
        re.compile(rf"\bon{_SIGMA_ALIAS_PATTERN}\.([a-z])", re.IGNORECASE),
        r"on \1.\2",
    ),
    # or<identifier starting with if_> -> or <identifier>
    # e.g., "orif_542 is null" -> "or if_542 is null"
    (("orif_",), re.compile(r"\bor(if_[a-z0-9_]+)", re.IGNORECASE), r"or \1"),
    # selectstatus -> select status (select followed by status without space)
    (
        ("selectstatus",),
        re.compile(r"\bselectstatus\b", re.IGNORECASE),
        "select status",
    ),
    # select<identifier> followed by comma -> select <identifier>
    # e.g., "selectcol," -> "select col,"
    # Exclude common words: selection, selector, selective, selectivity, selected, selecting
    (
        lambda q: _has_keyword_followed_by_letter(q, "select"),
        re.compile(
            rf"\bselect(?!ion|or\b|ive|ivity|ed\b|ing\b)({_IDENTIFIER_PATTERN_OPT})\s*,",
            re.IGNORECASE,
        ),
        r"select \1,",
    ),
    # end thencase -> end then case (CASE END followed by THEN CASE without spaces)
    (("thencase",), re.compile(r"\bend\s+thencase\b", re.IGNORECASE), "end then case"),
    # end then<identifier> -> end then <identifier> (e.g., "end thencase when")
    (
        lambda q: _has_keyword_followed_by_letter(q, "then"),
        re.compile(rf"\bend\s+then({_IDENTIFIER_PATTERN})", re.IGNORECASE),
        r"end then \1",
    ),
    # end else<function>( -> end else <function>( (e.g., "end elsedateadd(")
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
    # CAST(... AS<TYPE> -> CAST(... AS <TYPE> (missing space after AS in CAST)
    # e.g., "CAST(1 ASINT4)" -> "CAST(1 AS INT4)"
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
    # group by<identifier> -> group by <identifier> (missing space after BY)
    # e.g., "group byrep_name" -> "group by rep_name"
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
    # <alias>on <keyword> -> <alias> on <keyword> (alias followed directly by ON)
    # e.g., ") q6on coalesce" -> ") q6 on coalesce"
    (
        lambda q: "on " in q,  # looking for "aliaSON " pattern
        re.compile(
            rf"\)(\s*)({_IDENTIFIER_PATTERN_OPT})on\s+(coalesce|q\d+\.|[a-z])",
            re.IGNORECASE,
        ),
        r")\1\2 on \3",
    ),
    # UNI on ALL -> UNION ALL (truncated UNION in Redshift query logs)
    # Redshift truncates queries at ~4000 chars, sometimes cutting "UNION" to "UNI"
    (("uni on all",), re.compile(r"\bUNI\s+on\s+ALL\b", re.IGNORECASE), "UNION ALL"),
    # on ON -> on (double ON from malformed SQL: ") c on ON con.id")
    (("on on",), re.compile(r"\bon\s+ON\b", re.IGNORECASE), "on"),
    # <identifier>from " -> <identifier> from " (missing space before FROM after alias)
    # e.g., "cast_181from "public"" -> "cast_181 from "public""
    (
        lambda q: _has_keyword_followed_by_letter(q, "from")
        or 'from"' in q
        or "from'" in q,
        re.compile(r"([a-z0-9_])from\s*\"", re.IGNORECASE),
        r"\1 from \"",
    ),
]


def _should_apply_trigger(trigger: TriggerType, query_lower: str) -> bool:
    """Check if a pattern's trigger condition is met."""
    if callable(trigger):
        return trigger(query_lower)
    else:
        # Tuple of substrings - check if any is present
        return any(t in query_lower for t in trigger)


# Quick indicators for early exit - if none present, skip all processing
_QUICK_MALFORMATION_INDICATORS = (
    "casewhen",
    "groupby",
    "orderby",
    "elsenull",
    "notnull",
    "nulland",
    "nullor",
    "distinctcase",
    "nullgroup",
)


def _may_need_preprocessing(query_lower: str) -> bool:
    """Quick check if query may contain Sigma malformations.

    Returns True if query might need preprocessing, False for definite skip.
    This is a fast O(n) check to avoid running 40+ regex patterns on clean queries.

    Two categories of checks:
    1. Whitelist patterns (Sigma aliases): Check for "whenq", "onq", etc.
    2. Blacklist patterns (column names): Check for keyword+letter with safe word filtering
    """
    # Check for obvious compound keywords (most reliable indicators)
    for indicator in _QUICK_MALFORMATION_INDICATORS:
        if indicator in query_lower:
            return True

    # Check for Sigma alias patterns (whitelist approach - no false positives)
    # These are patterns like "whenq1.", "onq3.", "thent2." etc.
    for prefix in ("whenq", "whent", "thenq", "thent", "elseq", "elset", "onq", "ont"):
        if prefix in query_lower:
            return True

    # Check for column-name patterns that still need safe-word filtering (blacklist approach)
    # These patterns involve arbitrary identifiers, not predictable Sigma aliases
    for keyword in ("when", "then", "else", "from", "and", "select", "by"):
        if _has_keyword_followed_by_letter(query_lower, keyword):
            return True

    return False


def preprocess_query_for_sigma(query: str) -> str:
    """Preprocess query to fix Sigma Computing malformed SQL.

    Sigma generates SQL with missing spaces between keywords, operators,
    and function calls. This causes sqlglot parsing failures.

    Performance optimization:
    - Level 1: Quick check for malformation indicators (early exit for clean queries)
    - Level 2: Only apply patterns whose trigger conditions are met

    Known Sigma malformations:
    - ::timestamptzcast_ -> ::timestamptz cast_
    - ::timestamptzis -> ::timestamptz is
    - casewhen -> case when
    - distinctcase -> distinct case
    - elsenull -> else null
    - ormin( -> or min(
    - andmax( -> and max(
    - notnull -> not null
    - nulland -> null and
    - nullgroup -> null group
    - whenq11. -> when q11. (when + alias.column)
    - thenq11. -> then q11. (then + alias.column)
    - elseq11. -> else q11. (else + alias.column)
    - whene_xxx then -> when e_xxx then (when + identifier + keyword)
    - fromsigma.t_ -> from sigma.t_ (from + schema.table)
    - groupby -> group by
    - orderby -> order by
    - ordateadd( -> or dateadd( (or + function)
    - andxxx > 0 -> and xxx > 0 (and + identifier + operator)
    - onq3.id -> on q3.id (on + alias.column)
    - end thencase -> end then case (CASE END then CASE)
    - end elsedateadd -> end else dateadd (CASE END else function)
    - ASINT4 -> AS INT4 (missing space in CAST AS type)
    - group byrep_name -> group by rep_name (missing space after BY)
    - q6on coalesce -> q6 on coalesce (alias before ON)
    """
    query_lower = query.lower()

    # Level 1: Early exit if no malformation indicators found
    if not _may_need_preprocessing(query_lower):
        return query

    # Level 2: Apply only patterns whose triggers match
    result = query
    for trigger, pattern, replacement in _SIGMA_SQL_FIX_PATTERNS:
        if _should_apply_trigger(trigger, query_lower):
            result = pattern.sub(replacement, result)

    return result


# Patterns to find DMS staging table references in UPDATE SET expressions
# Quoted: "schema"."awsdms_*"
_DMS_STAGING_TABLE_PATTERN_QUOTED = re.compile(
    r'"([^"]+)"\.\"(awsdms_[^"]+)\"', re.IGNORECASE
)
# Unquoted: schema.awsdms_* (word boundaries to avoid partial matches)
_DMS_STAGING_TABLE_PATTERN_UNQUOTED = re.compile(
    r"\b([a-z_][a-z0-9_]*)\.(awsdms_[a-z0-9_]+)\b", re.IGNORECASE
)


def preprocess_dms_update_query(query: str) -> str:
    """Preprocess AWS DMS UPDATE queries to add missing FROM clause.

    AWS DMS CDC generates UPDATE queries that reference the staging table
    directly in SET expressions without a FROM clause:

        UPDATE "public"."target" SET
            "col1" = CASE WHEN "public"."awsdms_changes..."."col1" IS NULL
                     THEN "public"."target"."col1"
                     ELSE CAST("public"."awsdms_changes..."."col1" AS ...) END
        WHERE ...

    Sqlglot doesn't detect implicit table references in UPDATE SET expressions,
    so column-level lineage extraction fails. This function injects an explicit
    FROM clause to make the staging table visible to the parser.
    """
    # Only process UPDATE statements
    if not query.strip().upper().startswith("UPDATE"):
        return query

    # Check if query already has a FROM clause
    if re.search(r"\bFROM\b", query, re.IGNORECASE):
        return query

    # Find DMS staging table references (both quoted and unquoted)
    matches_quoted = _DMS_STAGING_TABLE_PATTERN_QUOTED.findall(query)
    matches_unquoted = _DMS_STAGING_TABLE_PATTERN_UNQUOTED.findall(query)

    # Extract unique staging tables (schema, table pairs)
    staging_tables: set[tuple[str, str]] = set()
    for schema, table in matches_quoted + matches_unquoted:
        if table.lower().startswith("awsdms_"):
            staging_tables.add((schema, table))

    if not staging_tables:
        return query

    # Build FROM clause with all staging tables
    from_parts = [f'"{schema}"."{table}"' for schema, table in staging_tables]
    from_clause = " FROM " + ", ".join(from_parts)

    # Insert FROM clause before WHERE (or at end if no WHERE)
    where_match = re.search(r"\bWHERE\b", query, re.IGNORECASE)
    if where_match:
        insert_pos = where_match.start()
        result = query[:insert_pos] + from_clause + " " + query[insert_pos:]
    else:
        result = query + from_clause

    logger.debug(f"Injected FROM clause for DMS UPDATE: {from_clause}")
    return result
