# Identifiers and interpolated partition filters can't be bound as BigQuery query
# parameters, so they are validated and backtick-escaped here instead.

import logging
from typing import List

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    FILTER_COLUMN_REF_RE,
    FILTER_DANGEROUS_PATTERNS,
    FILTER_OPERATOR_RE,
    FLEXIBLE_COLUMN_NAME_PATTERN,
    PROJECT_ID_RE,
    SQL_ALLOWED_START_PATTERNS,
    SQL_DANGEROUS_PATTERNS,
    TABLE_IDENTIFIER_RE,
    VALID_COLUMN_NAME_PATTERN,
    WHITESPACE_RE,
)

logger = logging.getLogger(__name__)


def mask_string_literals(sql: str) -> str:
    """Mask the interior of quoted string literals and comments so the denylist scans see
    SQL structure, not data.

    A partition value (GCS URI, Hive key) interpolated by ``FilterBuilder`` may contain
    ``#``/``--``/``data:`` that are inert inside a literal but injection outside one;
    masking the interior while keeping delimiters gives the scans a quote-aware view.
    Comments and backtick identifiers are consumed here too, so a quote inside them cannot
    open a literal and hide following SQL, and their body stays out of the scans. Backslash
    (``\\'``) and doubled-quote (``''``) escapes are honoured.
    """
    out: List[str] = []
    i = 0
    n = len(sql)
    quote = None  # opening quote char of the string literal we are inside, or None
    while i < n:
        ch = sql[i]
        if quote is None:
            # Line comment (`--`/`#`): keep the delimiter, mask the body to end of line.
            if ch == "#" or (ch == "-" and i + 1 < n and sql[i + 1] == "-"):
                out.append(ch)
                i += 1
                if ch == "-":  # second '-' of the '--' delimiter
                    out.append(sql[i])
                    i += 1
                while i < n and sql[i] != "\n":
                    out.append("x")
                    i += 1
                continue
            # Block comment (`/* ... */`): keep the delimiters, mask the interior.
            if ch == "/" and i + 1 < n and sql[i + 1] == "*":
                out.append(ch)
                out.append(sql[i + 1])
                i += 2
                while i < n and not (sql[i] == "*" and i + 1 < n and sql[i + 1] == "/"):
                    out.append("x")
                    i += 1
                if i + 1 < n:  # emit the closing */
                    out.append(sql[i])
                    out.append(sql[i + 1])
                    i += 2
                continue
            # Backtick identifier: copy verbatim so a quote inside it can't open a literal.
            if ch == "`":
                out.append(ch)
                i += 1
                while i < n and sql[i] != "`":
                    out.append(sql[i])
                    i += 1
                if i < n:
                    out.append(sql[i])
                    i += 1
                continue
            out.append(ch)
            if ch in ("'", '"'):
                quote = ch
            i += 1
            continue
        # Inside a string literal.
        if ch == "\\" and i + 1 < n:
            # Backslash escape: mask both the backslash and the escaped char.
            out.append("x")
            out.append("x")
            i += 2
            continue
        if ch == quote:
            if i + 1 < n and sql[i + 1] == quote:
                # Doubled quote — an escaped quote that stays inside the literal.
                out.append("x")
                out.append("x")
                i += 2
                continue
            # Closing delimiter.
            out.append(ch)
            quote = None
            i += 1
            continue
        # Ordinary literal content.
        out.append("x")
        i += 1
    return "".join(out)


def _validate_identifier_format(identifier_type: str, clean_identifier: str) -> None:
    if identifier_type == "project":
        # fullmatch, not match: `$` under match would accept a trailing newline.
        if not PROJECT_ID_RE.fullmatch(clean_identifier):
            raise ValueError(f"Invalid project ID format: {clean_identifier}")
        if len(clean_identifier) < 6 or len(clean_identifier) > 30:
            raise ValueError(f"Project ID must be 6-30 characters: {clean_identifier}")
        if "--" in clean_identifier:
            raise ValueError(
                f"Project ID cannot contain consecutive hyphens: {clean_identifier}"
            )
    elif identifier_type == "table":
        # Hyphens allowed in backtick-escaped table names; fullmatch to reject a
        # trailing newline.
        if not TABLE_IDENTIFIER_RE.fullmatch(clean_identifier):
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
        if not VALID_COLUMN_NAME_PATTERN.fullmatch(clean_identifier):
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
    upper_identifier = identifier.upper()

    if identifier_type in ("general", "table") and (
        upper_identifier == "INFORMATION_SCHEMA"
        or upper_identifier.startswith("INFORMATION_SCHEMA.")
    ):
        # INFORMATION_SCHEMA views must stay unquoted: backticking the whole dotted name
        # makes BigQuery look for a table literally called "INFORMATION_SCHEMA.VIEW". Only
        # table/general refs reach here; still validate the view suffix to reject injection.
        if upper_identifier != "INFORMATION_SCHEMA":
            view_suffix = identifier[len("INFORMATION_SCHEMA.") :]
            if not VALID_COLUMN_NAME_PATTERN.fullmatch(view_suffix):
                raise ValueError(f"Invalid INFORMATION_SCHEMA view name: {identifier}")
        return identifier

    # Injection/escape chars that must never appear in an identifier we backtick ourselves.
    dangerous_patterns = [";", "--", "/*", "*/", '"', "'", "\\", "\n", "\r", "\t", "`"]

    for pattern in dangerous_patterns:
        if pattern in identifier:
            raise ValueError(
                f"Invalid {identifier_type} identifier contains dangerous character '{pattern}': {identifier}"
            )

    # No backtick strip needed: the loop above already rejects any backtick.
    clean_identifier = identifier

    if any(ord(c) < 32 or ord(c) > 126 for c in clean_identifier):
        raise ValueError(
            f"Invalid {identifier_type} identifier contains non-printable characters: {identifier}"
        )

    _validate_identifier_format(identifier_type, clean_identifier)

    # Reserved literals: allowed once backticked, but logged as they can surprise.
    truly_problematic = {
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
    safe_project = validate_bigquery_identifier(project, "project")
    safe_dataset = validate_bigquery_identifier(dataset, "dataset")

    # Strip + case-fold so ` information_schema.tables ` reaches the unquoted-view path
    # instead of falling through to the table validator and failing on the dot.
    if table.strip().upper().startswith("INFORMATION_SCHEMA"):
        safe_view = validate_bigquery_identifier(table, "general")
        return f"{safe_project}.{safe_dataset}.{safe_view}"

    safe_table = validate_bigquery_identifier(table, "table")
    return f"{safe_project}.{safe_dataset}.{safe_table}"


def validate_column_name(col_name: str, context: str = "") -> bool:
    if not col_name or not isinstance(col_name, str):
        logger.warning(
            f"Invalid column name{' in ' + context if context else ''}: {col_name}"
        )
        return False

    # Flexible pattern: columns are backtick-quoted downstream, so leading-digit /
    # international names are legitimate; fullmatch still rejects a trailing newline.
    if not FLEXIBLE_COLUMN_NAME_PATTERN.fullmatch(col_name):
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

    # Quote-aware view so tokens inside literals aren't misread as injection.
    masked = mask_string_literals(query)
    scan_target = WHITESPACE_RE.sub(" ", masked.upper().strip())

    # A read-only profiling query is one statement; a ';' followed by more SQL (outside a
    # literal, hence on the masked query) is a stacked statement.
    stripped_masked = masked.rstrip().rstrip(";").rstrip()
    if ";" in stripped_masked:
        raise ValueError("Query must be a single statement")

    for pattern in SQL_DANGEROUS_PATTERNS:
        if pattern.search(scan_target):
            raise ValueError(f"Query contains dangerous pattern: {pattern.pattern}")

    if not any(p.match(scan_target) for p in SQL_ALLOWED_START_PATTERNS):
        raise ValueError(f"Query must start with SELECT or WITH: {query[:100]}...")

    return True


def validate_filter_expression(filter_expr: str) -> bool:
    if not filter_expr or not isinstance(filter_expr, str):
        return False

    # Mask literals so a comment/injection token inside a quoted partition value is inert
    # while one outside a literal still trips the guard.
    masked_expr = mask_string_literals(filter_expr)

    # A WHERE-clause predicate never contains ';', so any ';' on the masked filter is
    # outside a literal and stacks a statement (regardless of what follows) — reject it.
    if ";" in masked_expr:
        logger.warning(f"Filter contains statement separator ';': {filter_expr}")
        return False

    for pattern in FILTER_DANGEROUS_PATTERNS:
        if pattern.search(masked_expr):
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
