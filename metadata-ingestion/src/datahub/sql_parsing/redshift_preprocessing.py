"""Redshift SQL preprocessing for AWS DMS malformations.

This module fixes SQL parsing issues caused by:
1. AWS DMS - password redaction merges column names, UPDATE queries lack FROM clause
"""

import logging
import re

logger = logging.getLogger(__name__)

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
