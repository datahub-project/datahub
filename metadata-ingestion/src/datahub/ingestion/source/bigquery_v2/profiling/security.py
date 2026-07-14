# Identifiers (table/column/schema names) cannot be parameterized in BigQuery, so they must
# be validated and backtick-escaped here. Most data values are bound as query parameters,
# but partition values in WHERE-clause filters are interpolated (see FilterBuilder), so
# validate_filter_expression / validate_and_filter_expressions guard that one path.

import logging
from typing import List

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    FILTER_COLUMN_REF_RE,
    FILTER_DANGEROUS_PATTERNS,
    FILTER_OPERATOR_RE,
    PROJECT_ID_RE,
    SQL_ALLOWED_START_PATTERNS,
    SQL_DANGEROUS_PATTERNS,
    TABLE_IDENTIFIER_RE,
    VALID_COLUMN_NAME_PATTERN,
    WHITESPACE_RE,
)

logger = logging.getLogger(__name__)


def _validate_identifier_format(identifier_type: str, clean_identifier: str) -> None:
    if identifier_type == "project":
        if not PROJECT_ID_RE.match(clean_identifier):
            raise ValueError(f"Invalid project ID format: {clean_identifier}")
        if len(clean_identifier) < 6 or len(clean_identifier) > 30:
            raise ValueError(f"Project ID must be 6-30 characters: {clean_identifier}")
        if "--" in clean_identifier:
            raise ValueError(
                f"Project ID cannot contain consecutive hyphens: {clean_identifier}"
            )
    elif identifier_type == "table":
        # BigQuery allows hyphens in table names when backtick-escaped.
        if not TABLE_IDENTIFIER_RE.match(clean_identifier):
            raise ValueError(
                f"Invalid {identifier_type} identifier format: {clean_identifier}"
            )
        if len(clean_identifier) > 1024:
            raise ValueError(
                f"{identifier_type} identifier too long: {len(clean_identifier)} chars"
            )
        if "--" in clean_identifier:
            raise ValueError(
                f"Table identifier cannot contain consecutive hyphens: {clean_identifier}"
            )
    else:
        # Datasets and columns: letters, numbers, underscores only (no hyphens).
        if not VALID_COLUMN_NAME_PATTERN.match(clean_identifier):
            raise ValueError(
                f"Invalid {identifier_type} identifier format: {clean_identifier}"
            )
        if len(clean_identifier) > 1024:
            raise ValueError(
                f"{identifier_type} identifier too long: {len(clean_identifier)} chars"
            )
        if clean_identifier.startswith("__"):
            raise ValueError(
                f"Invalid {identifier_type} identifier cannot start with double underscore: {clean_identifier}"
            )


def validate_bigquery_identifier(
    identifier: str, identifier_type: str = "general"
) -> str:
    if not identifier or not isinstance(identifier, str):
        raise ValueError(
            f"Invalid {identifier_type} identifier: must be non-empty string"
        )

    identifier = identifier.strip()

    if identifier.startswith("INFORMATION_SCHEMA"):
        if identifier == "INFORMATION_SCHEMA" or identifier.startswith(
            "INFORMATION_SCHEMA."
        ):
            return f"`{identifier}`"

    # Injection/escape chars that must never appear in an identifier we backtick ourselves.
    dangerous_patterns = [";", "--", "/*", "*/", '"', "'", "\\", "\n", "\r", "\t", "`"]

    for pattern in dangerous_patterns:
        if pattern in identifier:
            raise ValueError(
                f"Invalid {identifier_type} identifier contains dangerous character '{pattern}': {identifier}"
            )

    clean_identifier = identifier.replace("`", "")

    if any(ord(c) < 32 or ord(c) > 126 for c in clean_identifier):
        raise ValueError(
            f"Invalid {identifier_type} identifier contains non-printable characters: {identifier}"
        )

    _validate_identifier_format(identifier_type, clean_identifier)

    truly_problematic = {
        "__null__",
        "__unpartitioned__",
        "__temp__",
        "null",
        "true",
        "false",
    }

    if clean_identifier.lower() in truly_problematic:
        logger.debug(
            f"Identifier '{clean_identifier}' may cause issues in some BigQuery contexts but is allowed when backticked"
        )

    return f"`{clean_identifier}`"


def build_safe_table_reference(project: str, dataset: str, table: str) -> str:
    if table.startswith("INFORMATION_SCHEMA"):
        safe_project = validate_bigquery_identifier(project, "project")
        safe_dataset = validate_bigquery_identifier(dataset, "dataset")
        return f"{safe_project}.{safe_dataset}.`{table}`"

    safe_project = validate_bigquery_identifier(project, "project")
    safe_dataset = validate_bigquery_identifier(dataset, "dataset")
    safe_table = validate_bigquery_identifier(table, "table")

    return f"{safe_project}.{safe_dataset}.{safe_table}"


def validate_column_name(col_name: str, context: str = "") -> bool:
    if not col_name or not isinstance(col_name, str):
        logger.warning(
            f"Invalid column name{' in ' + context if context else ''}: {col_name}"
        )
        return False

    if not VALID_COLUMN_NAME_PATTERN.match(col_name):
        logger.warning(
            f"Column name fails validation{' in ' + context if context else ''}: {col_name}"
        )
        return False

    return True


def validate_column_names(col_names: List[str], context: str = "") -> List[str]:
    valid_columns = []
    for col in col_names:
        if validate_column_name(col, context):
            valid_columns.append(col)
    return valid_columns


def validate_sql_structure(query: str) -> bool:
    if not query or not isinstance(query, str):
        return False

    normalized_query = WHITESPACE_RE.sub(" ", query.upper().strip())

    for pattern in SQL_DANGEROUS_PATTERNS:
        if pattern.search(normalized_query):
            raise ValueError(f"Query contains dangerous pattern: {pattern.pattern}")

    if not any(p.match(normalized_query) for p in SQL_ALLOWED_START_PATTERNS):
        raise ValueError(f"Query must start with SELECT or WITH: {query[:100]}...")

    return True


def validate_filter_expression(filter_expr: str) -> bool:
    if not filter_expr or not isinstance(filter_expr, str):
        return False

    for pattern in FILTER_DANGEROUS_PATTERNS:
        if pattern.search(filter_expr):
            logger.warning(
                f"Filter contains dangerous pattern {pattern.pattern}: {filter_expr}"
            )
            return False

    if not FILTER_COLUMN_REF_RE.search(filter_expr):
        logger.warning(f"Filter doesn't contain valid column reference: {filter_expr}")
        return False

    if not FILTER_OPERATOR_RE.search(filter_expr):
        logger.warning(f"Filter doesn't contain recognized operators: {filter_expr}")
        return False

    return True


def validate_and_filter_expressions(filters: List[str], context: str = "") -> List[str]:
    validated_filters = []
    for filter_str in filters:
        if validate_filter_expression(filter_str):
            validated_filters.append(filter_str)
        else:
            logger.warning(
                f"Rejecting filter{' in ' + context if context else ''}: {filter_str}"
            )

    if not validated_filters and filters:
        logger.warning(
            f"No valid filters after validation{' in ' + context if context else ''}"
        )

    return validated_filters
