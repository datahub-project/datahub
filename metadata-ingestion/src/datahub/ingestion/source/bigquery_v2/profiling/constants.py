import re

VALID_COLUMN_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Columns are always backtick-quoted, so this is looser than the strict pattern above to
# keep leading-digit / international BigQuery names. `\w` (Unicode) still rejects space,
# hyphen and dot: legal in a quoted name but they can smuggle comment markers past
# FILTER_COLUMN_REF_RE, so such rare names are skipped rather than profiled.
FLEXIBLE_COLUMN_NAME_PATTERN = re.compile(r"\w+", re.UNICODE)

# BigQuery project ID: lowercase letters, numbers, hyphens; 6-30 chars.
PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]*[a-z0-9]$")

# Table name: hyphens allowed (unlike column/dataset identifiers), and a leading
# digit is permitted because BigQuery date-sharded tables (e.g. `20200101`) are
# backtick-quoted digit-leading names.
TABLE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_-]*$")


# Collapses runs of whitespace when normalising a query before injection checks.
WHITESPACE_RE = re.compile(r"\s+")

# DDL, DML, admin, and script-injection patterns that must not appear in profiling queries
SQL_DANGEROUS_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|FUNCTION|PROCEDURE)",
        r"\bDROP\s+(?:TABLE|VIEW|FUNCTION|PROCEDURE|DATABASE|SCHEMA)",
        r"\bALTER\s+(?:TABLE|VIEW|DATABASE|SCHEMA)",
        r"\bTRUNCATE\s+TABLE",
        r"\bINSERT\s+INTO",
        r"\bUPDATE\s+.+\bSET\b",
        r"\bDELETE\s+FROM",
        r"\bMERGE\s+INTO",
        r"\bGRANT\s+",
        r"\bREVOKE\s+",
        r"\bEXEC(?:UTE)?\s+",
        r"\bCALL\s+",
        # EXPORT DATA / LOAD DATA can move or mutate data from an otherwise read-only query.
        r"\bEXPORT\s+DATA\b",
        r"\bLOAD\s+DATA\b",
        r";\s*(?:CREATE|DROP|ALTER|INSERT|UPDATE|DELETE|GRANT|REVOKE|TRUNCATE|MERGE|EXEC(?:UTE)?|CALL|EXPORT|LOAD)",
        # No XSS/URI-scheme markers or comment-body keyword scans: they don't parse as
        # BigQuery SQL and only produced false positives. Stacked statements are caught by
        # the single-statement guard in validate_sql_structure.
    ]
]

# Patterns that a valid profiling query must start with.
SQL_ALLOWED_START_PATTERNS = [
    re.compile(p)
    for p in [
        r"^\s*SELECT\s+",
        r"^\s*WITH\s+",
        r"^\s*\(\s*SELECT\s+",
    ]
]

# Injection patterns that must not appear in WHERE-clause filter expressions
FILTER_DANGEROUS_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        # Any ';' in a filter is a stacked statement (a WHERE predicate never contains one);
        # validate_filter_expression rejects it directly, so no keyword list is needed here.
        r"UNION\s+(?:(?:ALL|DISTINCT)\s+)?(?:\(\s*)*SELECT",
        r"--",
        # '#' / '--' / '/*' are scanned on the literal-masked filter, so a comment marker
        # inside a quoted STRING/Hive value is inert; only one outside a literal is caught.
        r"#",
        r"/\*",
        # xp_cmdshell / sp_executesql omitted: SQL Server builtins that are valid partition
        # values in BigQuery, so matching them would reject legitimate filters.
        r"<script",
        r"javascript:",
        r"eval\s*\(",
    ]
]

# Backtick-quoted column reference in a filter. Must share FLEXIBLE_COLUMN_NAME_PATTERN's
# `\w+` grammar so a column that passed validation isn't rejected here.
FILTER_COLUMN_REF_RE = re.compile(r"`\w+`", re.UNICODE)

# Recognised SQL comparison / membership operators in filter expressions.
FILTER_OPERATOR_RE = re.compile(
    r"(?:=|!=|<>|<|>|<=|>=|BETWEEN\s+|IS\s+(?:NOT\s+)?NULL|LIKE|NOT\s+LIKE|IN\s*\()",
    re.IGNORECASE,
)
