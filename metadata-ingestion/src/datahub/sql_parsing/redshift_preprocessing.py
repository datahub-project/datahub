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
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# =============================================================================
# REDSHIFT RTRIM BUG PREPROCESSING
# =============================================================================

# Compound keyword patterns caused by Redshift's internal RTRIM bug.
# When queries are stored in STL_QUERYTEXT (200-char segments) and reconstructed
# for stl_query.querytxt, Redshift applies RTRIM to each segment before
# concatenation, removing meaningful trailing spaces at segment boundaries.
# This affects ANY long query, not just from specific tools like Sigma.

# Static patterns: keyword-keyword compounds (safe, no false positives)
_REDSHIFT_STATIC_PATTERNS: List[Tuple[str, re.Pattern[str], str]] = [
    ("notnull", re.compile(r"\bnotnull\b", re.IGNORECASE), "not null"),
    ("casewhen", re.compile(r"\bcasewhen\b", re.IGNORECASE), "case when"),
    ("nulland", re.compile(r"\bnulland\b", re.IGNORECASE), "null and"),
    ("elsenull", re.compile(r"\belsenull\b", re.IGNORECASE), "else null"),
    ("groupby", re.compile(r"\bgroupby\b", re.IGNORECASE), "group by"),
    ("thencase", re.compile(r"\bthencase\b", re.IGNORECASE), "then case"),
    ("orderby", re.compile(r"\borderby\b", re.IGNORECASE), "order by"),
    # Additional keyword-keyword patterns found in production
    ("distinctcase", re.compile(r"\bdistinctcase\b", re.IGNORECASE), "distinct case"),
    ("nullend", re.compile(r"\bnullend\b", re.IGNORECASE), "null end"),
]

# Dynamic patterns: keyword-identifier/function compounds
# These use regex with capturing groups to preserve the identifier
_WHEN_IDENTIFIER_PATTERN = re.compile(r"\bwhen([a-z])", re.IGNORECASE)
_THEN_IDENTIFIER_PATTERN = re.compile(r"\bthen([a-z])", re.IGNORECASE)
_ELSE_FUNCTION_PATTERN = re.compile(
    r"\belse(dateadd|datediff|date_add)\b", re.IGNORECASE
)

# Words that should NOT be split (false positive prevention)
_WHEN_BLACKLIST = {"whenever", "whence"}
_THEN_BLACKLIST = {"thence", "thenceforth"}


def _apply_dynamic_pattern(
    query: str,
    pattern: re.Pattern[str],
    keyword: str,
    blacklist: Optional[set[str]] = None,
) -> str:
    """Apply a dynamic keyword-identifier pattern with blacklist checking."""
    if blacklist is None:
        blacklist = set()

    def replacer(match: re.Match[str]) -> str:
        full_match = match.group(0)
        # Check if this is a blacklisted word
        # Find the full word starting from match position
        start = match.start()
        end = match.end()
        # Extend to find full word
        while end < len(query) and (query[end].isalnum() or query[end] == "_"):
            end += 1
        full_word = query[start:end].lower()
        if full_word in blacklist:
            return full_match
        # Insert space after keyword
        return f"{keyword} {match.group(1)}"

    return pattern.sub(replacer, query)


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

    Args:
        query: The SQL query to preprocess

    Returns:
        The preprocessed query with fixed spacing
    """
    query_lower = query.lower()

    # Quick check for static patterns
    static_indicators = [ind for ind, _, _ in _REDSHIFT_STATIC_PATTERNS]
    needs_static = any(ind in query_lower for ind in static_indicators)

    # Quick check for dynamic patterns
    needs_dynamic = (
        "when" in query_lower or "then" in query_lower or "else" in query_lower
    )

    if not needs_static and not needs_dynamic:
        return query

    result = query

    # Apply static patterns
    if needs_static:
        for indicator, pattern, replacement in _REDSHIFT_STATIC_PATTERNS:
            if indicator in query_lower:
                result = pattern.sub(replacement, result)

    # Apply dynamic patterns
    if needs_dynamic:
        if "when" in query_lower:
            result = _apply_dynamic_pattern(
                result, _WHEN_IDENTIFIER_PATTERN, "when", _WHEN_BLACKLIST
            )
        if "then" in query_lower:
            result = _apply_dynamic_pattern(
                result, _THEN_IDENTIFIER_PATTERN, "then", _THEN_BLACKLIST
            )
        if "else" in query_lower:
            result = _ELSE_FUNCTION_PATTERN.sub(r"else \1", result)

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
