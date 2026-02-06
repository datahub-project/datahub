import logging
import re
from typing import List, Tuple

logger = logging.getLogger(__name__)

# Regex patterns to fix Sigma Computing malformed SQL.
# Sigma generates SQL with missing spaces between keywords/operators.
# Each tuple is (pattern, replacement).
_SIGMA_SQL_FIX_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    # ::TYPEcast_ -> ::TYPE cast_
    # e.g., "::timestamptzcast_date_to_timestamp" -> "::timestamptz cast_date_to_timestamp"
    (
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
        re.compile(
            r"::(timestamp(?:tz)?|date|time(?:tz)?|int(?:eger)?|bigint|smallint|"
            r"float|real|double|numeric|decimal|varchar|char|text|boolean|bool)"
            r"(is)\b",
            re.IGNORECASE,
        ),
        r"::\1 \2",
    ),
    # casewhen -> case when
    (re.compile(r"\bcasewhen\b", re.IGNORECASE), "case when"),
    # case when<alias>. -> case when <alias>. (e.g., "case whenq11.col" -> "case when q11.col")
    # More specific than generic when<alias>. to ensure case when is handled first
    (re.compile(r"\bcase\s+when([a-z][a-z0-9_]*)\.", re.IGNORECASE), r"case when \1."),
    # case when<identifier><operator> -> case when <identifier> <operator> (no space before operator)
    # e.g., "case whenarr_down>" -> "case when arr_down >"
    (
        re.compile(r"\bcase\s+when([a-z][a-z0-9_]+)(>|<|=|!)", re.IGNORECASE),
        r"case when \1 \2",
    ),
    # case when<identifier> <keyword> -> case when <identifier> <keyword>
    # e.g., "case whenarr_down then" -> "case when arr_down then"
    (
        re.compile(
            r"\bcase\s+when([a-z][a-z0-9_]+)\s+(then|and|or|is\b|in\b|not\b|between\b)",
            re.IGNORECASE,
        ),
        r"case when \1 \2",
    ),
    # distinctcase -> distinct case
    (re.compile(r"\bdistinctcase\b", re.IGNORECASE), "distinct case"),
    # elsenull -> else null
    (re.compile(r"\belsenull\b", re.IGNORECASE), "else null"),
    # SQL keyword + aggregate without space: ormin(, andmax(, ormax(, andmin(, orsum(, andsum(, orcount(, andcount(
    (re.compile(r"\bor(min|max|sum|count|avg)\s*\(", re.IGNORECASE), r"or \1("),
    (re.compile(r"\band(min|max|sum|count|avg)\s*\(", re.IGNORECASE), r"and \1("),
    # notnull -> not null, nulland -> null and, nullor -> null or
    (re.compile(r"\bnotnull\b", re.IGNORECASE), "not null"),
    (re.compile(r"\bnulland\b", re.IGNORECASE), "null and"),
    (re.compile(r"\bnullor\b", re.IGNORECASE), "null or"),
    # nullgroup -> null group (for "is null group by")
    (re.compile(r"\bnullgroup\b", re.IGNORECASE), "null group"),
    # isnull followed by and/or without space: isnulland -> is null and
    (re.compile(r"\bis\s+nulland\b", re.IGNORECASE), "is null and"),
    (re.compile(r"\bis\s+nullor\b", re.IGNORECASE), "is null or"),
    # is nullnull_eq_ -> is null null_eq_ (null followed by identifier starting with null)
    (re.compile(r"\bnull(null_[a-z0-9_]+)", re.IGNORECASE), r"null \1"),
    # endif_ -> end if_ (end followed by identifier starting with if)
    (re.compile(r"\bend(if_[a-z0-9_]+)", re.IGNORECASE), r"end \1"),
    # when<alias>. -> when <alias>. (e.g., "whenq11.col" -> "when q11.col")
    # Exclude common words: whenever, whence
    (re.compile(r"\bwhen(?!ever|ce\b)([a-z][a-z0-9_]*)\.", re.IGNORECASE), r"when \1."),
    # then<alias>. -> then <alias>. (e.g., "thenq11.col" -> "then q11.col")
    # Exclude common words: thence
    (re.compile(r"\bthen(?!ce\b)([a-z][a-z0-9_]*)\.", re.IGNORECASE), r"then \1."),
    # else<alias>. -> else <alias>. (e.g., "elseq11.col" -> "else q11.col")
    # Exclude common words: elsewhere
    (re.compile(r"\belse(?!where\b)([a-z][a-z0-9_]*)\.", re.IGNORECASE), r"else \1."),
    # when<identifier> followed by space/operator -> when <identifier>
    # e.g., "whene_r6m06nuq then" -> "when e_r6m06nuq then"
    # Exclude common words: whenever, whence
    (
        re.compile(
            r"\bwhen(?!ever|ce\b)([a-z][a-z0-9_]+)\s+(then|and|or|>|<|=|!)",
            re.IGNORECASE,
        ),
        r"when \1 \2",
    ),
    # when<identifier><operator> -> when <identifier> <operator> (no space before operator)
    # e.g., "whenarr_down>" -> "when arr_down >"
    # Exclude common words: whenever, whence
    (
        re.compile(r"\bwhen(?!ever|ce\b)([a-z][a-z0-9_]+)(>|<|=|!)", re.IGNORECASE),
        r"when \1 \2",
    ),
    # from<schema>.<table> or from<schema>.t_ (Sigma temp table pattern)
    (re.compile(r"\bfrom([a-z][a-z0-9_]*)\.([a-z])", re.IGNORECASE), r"from \1.\2"),
    # groupby -> group by
    (re.compile(r"\bgroupby\b", re.IGNORECASE), "group by"),
    # orderby -> order by
    (re.compile(r"\borderby\b", re.IGNORECASE), "order by"),
    # or<function>( -> or <function>( for common functions
    (
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
        re.compile(
            r"\band(?!roid|erson|ersen|rew|re\b|y\b|es\b|ora|romeda)([a-z][a-z0-9_]+)\s*(>|<|=|!|is\b|in\b)",
            re.IGNORECASE,
        ),
        r"and \1 \2",
    ),
    # on<short-alias>. -> on <short-alias>. (e.g., "onq3.id" -> "on q3.id")
    # Match aliases like q1, q12, q123 (letter + up to 3 digits) to avoid breaking identifiers like "online_ret"
    (re.compile(r"\bon([a-z]\d{1,3})\.([a-z])", re.IGNORECASE), r"on \1.\2"),
    # Also match short alphanumeric aliases (2-3 chars starting with letter)
    (re.compile(r"\bon([a-z][a-z0-9]{1,2})\.([a-z])", re.IGNORECASE), r"on \1.\2"),
    # or<identifier starting with if_> -> or <identifier>
    # e.g., "orif_542 is null" -> "or if_542 is null"
    (re.compile(r"\bor(if_[a-z0-9_]+)", re.IGNORECASE), r"or \1"),
    # selectstatus -> select status (select followed by status without space)
    (re.compile(r"\bselectstatus\b", re.IGNORECASE), "select status"),
    # select<identifier> followed by comma -> select <identifier>
    # e.g., "selectcol," -> "select col,"
    # Exclude common words: selection, selector, selective, selectivity, selected, selecting
    (
        re.compile(
            r"\bselect(?!ion|or\b|ive|ivity|ed\b|ing\b)([a-z][a-z0-9_]*)\s*,",
            re.IGNORECASE,
        ),
        r"select \1,",
    ),
    # end thencase -> end then case (CASE END followed by THEN CASE without spaces)
    (re.compile(r"\bend\s+thencase\b", re.IGNORECASE), "end then case"),
    # end then<identifier> -> end then <identifier> (e.g., "end thencase when")
    (re.compile(r"\bend\s+then([a-z][a-z0-9_]+)", re.IGNORECASE), r"end then \1"),
    # end else<function>( -> end else <function>( (e.g., "end elsedateadd(")
    (
        re.compile(
            r"\bend\s+else(dateadd|datediff|date_trunc|coalesce|nullif|cast|case)\b",
            re.IGNORECASE,
        ),
        r"end else \1",
    ),
    # CAST(... AS<TYPE> -> CAST(... AS <TYPE> (missing space after AS in CAST)
    # e.g., "CAST(1 ASINT4)" -> "CAST(1 AS INT4)"
    (
        re.compile(
            r"\bAS(INT[248]?|INTEGER|BIGINT|SMALLINT|FLOAT|REAL|DOUBLE|NUMERIC|"
            r"DECIMAL|VARCHAR|CHAR|TEXT|BOOLEAN|BOOL|TIMESTAMP(?:TZ)?|DATE|TIME(?:TZ)?)\b",
            re.IGNORECASE,
        ),
        r"AS \1",
    ),
    # group by<identifier> -> group by <identifier> (missing space after BY)
    # e.g., "group byrep_name" -> "group by rep_name"
    (re.compile(r"\bgroup\s+by([a-z][a-z0-9_]+)", re.IGNORECASE), r"group by \1"),
    # order by<identifier> -> order by <identifier>
    (re.compile(r"\border\s+by([a-z][a-z0-9_]+)", re.IGNORECASE), r"order by \1"),
    # <alias>on <keyword> -> <alias> on <keyword> (alias followed directly by ON)
    # e.g., ") q6on coalesce" -> ") q6 on coalesce"
    (
        re.compile(
            r"\)(\s*)([a-z][a-z0-9_]*)on\s+(coalesce|q\d+\.|[a-z])", re.IGNORECASE
        ),
        r")\1\2 on \3",
    ),
    # UNI on ALL -> UNION ALL (truncated UNION in Redshift query logs)
    # Redshift truncates queries at ~4000 chars, sometimes cutting "UNION" to "UNI"
    (re.compile(r"\bUNI\s+on\s+ALL\b", re.IGNORECASE), "UNION ALL"),
    # on ON -> on (double ON from malformed SQL: ") c on ON con.id")
    (re.compile(r"\bon\s+ON\b", re.IGNORECASE), "on"),
    # <identifier>from " -> <identifier> from " (missing space before FROM after alias)
    # e.g., "cast_181from "public"" -> "cast_181 from "public""
    (re.compile(r"([a-z0-9_])from\s*\"", re.IGNORECASE), r"\1 from \""),
]


def preprocess_query_for_sigma(query: str) -> str:
    """Preprocess query to fix Sigma Computing malformed SQL.

    Sigma generates SQL with missing spaces between keywords, operators,
    and function calls. This causes sqlglot parsing failures.

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
    result = query
    for pattern, replacement in _SIGMA_SQL_FIX_PATTERNS:
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
