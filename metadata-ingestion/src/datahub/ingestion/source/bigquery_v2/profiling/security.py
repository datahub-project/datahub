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
    """Blank out the *contents* of quoted string literals so structural denylist scans
    see SQL structure, not data.

    A partition value such as a GCS URI (`gs://b/data:x`), a Hive key, or any STRING
    that legitimately contains ``#``, ``--``, ``data:`` etc. is interpolated inside a
    quoted literal by ``FilterBuilder``. Scanning the raw text would flag those
    characters as injection even though they are inert literal data (false positive).
    Conversely, a token that appears *outside* any literal is real SQL and must still be
    caught. Masking the literal interior — while preserving the delimiters, length, and
    everything outside literals — gives the denylists a quote-aware view that satisfies
    both: injection tokens outside literals survive, data tokens inside literals do not.

    SQL comments are recognised *before* a quote can open a literal and are copied
    through verbatim: a lone quote inside a ``--``/``#`` line comment or a ``/* */`` block
    comment is comment text, not a literal opener, so masking must not let it swallow the
    executable SQL (a ``;`` and a second statement) that follows the comment. Leaving the
    comment body visible also keeps it available to the denylist scans.

    Backtick identifiers are likewise skipped verbatim (they are validated separately by
    ``validate_bigquery_identifier``) so a quote inside a quoted identifier cannot open a
    literal. BigQuery backslash escapes (``\\'``, ``\\\\``) and ANSI doubled-quote (``''``)
    escaping are both honoured so an escaped quote does not prematurely close a literal.
    """
    out: List[str] = []
    i = 0
    n = len(sql)
    quote = None  # opening quote char of the string literal we are inside, or None
    while i < n:
        ch = sql[i]
        if quote is None:
            # Line comment (`--` or `#`): copy to end of line verbatim. A quote here is
            # comment text, so it must not open a literal and hide the SQL after the
            # newline (e.g. `-- it's fine\n; DROP TABLE t`).
            if ch == "#" or (ch == "-" and i + 1 < n and sql[i + 1] == "-"):
                while i < n and sql[i] != "\n":
                    out.append(sql[i])
                    i += 1
                continue
            # Block comment (`/* ... */`): copy verbatim, including a quote inside it.
            if ch == "/" and i + 1 < n and sql[i + 1] == "*":
                out.append(ch)
                out.append(sql[i + 1])
                i += 2
                while i < n and not (sql[i] == "*" and i + 1 < n and sql[i + 1] == "/"):
                    out.append(sql[i])
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
        # fullmatch (not match): a trailing '\n' satisfies the `$` anchor under match, so
        # anchor the whole string even though newlines are already rejected upstream.
        if not PROJECT_ID_RE.fullmatch(clean_identifier):
            raise ValueError(f"Invalid project ID format: {clean_identifier}")
        if len(clean_identifier) < 6 or len(clean_identifier) > 30:
            raise ValueError(f"Project ID must be 6-30 characters: {clean_identifier}")
        if "--" in clean_identifier:
            raise ValueError(
                f"Project ID cannot contain consecutive hyphens: {clean_identifier}"
            )
    elif identifier_type == "table":
        # BigQuery allows hyphens in table names when backtick-escaped. fullmatch so a
        # trailing newline cannot satisfy the `$` anchor.
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
        # INFORMATION_SCHEMA views are referenced by their bare dotted name
        # (project.dataset.INFORMATION_SCHEMA.VIEW). Wrapping "INFORMATION_SCHEMA.VIEW"
        # in a single backtick pair makes BigQuery treat the whole dotted string as one
        # identifier — it then looks for a table literally named "INFORMATION_SCHEMA.VIEW"
        # and the query fails. Validate the view suffix so this branch still rejects
        # injection, then return the reference unquoted. Matched case-insensitively
        # because BigQuery accepts `information_schema`. The fast path is limited to
        # table/general references; a project/dataset must never be INFORMATION_SCHEMA.
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

    # No backtick strip needed: the dangerous-patterns loop above already rejects any
    # identifier containing a backtick.
    clean_identifier = identifier

    if any(ord(c) < 32 or ord(c) > 126 for c in clean_identifier):
        raise ValueError(
            f"Invalid {identifier_type} identifier contains non-printable characters: {identifier}"
        )

    _validate_identifier_format(identifier_type, clean_identifier)

    # Dataset/column identifiers starting with "__" are already rejected in
    # _validate_identifier_format, so only the reserved literals need noting here (and
    # only for the table/general types, where they are allowed once backticked).
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

    # Strip + case-fold the branch test so ` information_schema.tables ` reaches the
    # INFORMATION_SCHEMA path the same way validate_bigquery_identifier does internally,
    # rather than falling through to the table validator and failing on the dot.
    if table.strip().upper().startswith("INFORMATION_SCHEMA"):
        # validate_bigquery_identifier returns the INFORMATION_SCHEMA view as a bare
        # (validated) dotted name, so the canonical `project`.`dataset`.INFORMATION_SCHEMA.VIEW
        # form is produced rather than a single backtick-quoted third component.
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

    # Flexible (not strict) pattern: a column is always backtick-quoted downstream, so a
    # leading digit or an international character is a legitimate BigQuery column name and
    # must not be dropped. fullmatch keeps a trailing newline (and any other injection
    # character) from slipping through.
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

    # Scan a quote-aware view so dangerous tokens inside string literals (partition
    # values, URIs) are not misread as injection, while tokens outside literals — the
    # actual SQL — are still caught.
    masked = mask_string_literals(query)
    scan_target = WHITESPACE_RE.sub(" ", masked.upper().strip())

    # Reject a statement separator that is followed by more SQL: a read-only profiling
    # query is a single statement, so a second statement after ';' is never legitimate.
    # The check runs on the masked query so a ';' inside a literal value is ignored.
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

    # Scan with literal contents masked so a comment/injection token that is really part
    # of a quoted STRING or Hive partition value (e.g. `# ` in a URL, `--` in a path) is
    # not misread as SQL. A token outside any literal — genuine injection — still trips
    # the guard because masking only blanks the literal interior.
    masked_expr = mask_string_literals(filter_expr)
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
