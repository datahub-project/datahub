import re

VALID_COLUMN_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

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
        r"<script[^>]*>",
        r"javascript:",
        r"vbscript:",
        r"data:",
        r"/\*.*(?:union|select|insert|update|delete|drop|create|alter).*\*/",
        r"--.*(?:union|select|insert|update|delete|drop|create|alter)",
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
        r";\s*(?:DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|TRUNCATE|MERGE|GRANT|REVOKE|EXEC(?:UTE)?|CALL|EXPORT|LOAD)\s+",
        r"UNION\s+(?:(?:ALL|DISTINCT)\s+)?SELECT",
        r"--",
        # BigQuery '#' line comment: like '--', a value that closes the literal and
        # appends '# ...' could comment out the rest of the interpolated predicate.
        r"#",
        r"/\*",
        # xp_cmdshell / sp_executesql intentionally omitted: they are SQL Server
        # builtins with no meaning in BigQuery and are valid STRING/Hive partition
        # values, so matching them here would reject legitimate partition filters.
        r"<script",
        r"javascript:",
        r"eval\s*\(",
    ]
]

# A valid backtick-quoted column reference inside a filter expression.
FILTER_COLUMN_REF_RE = re.compile(r"`[a-zA-Z_][a-zA-Z0-9_]*`")

# Recognised SQL comparison / membership operators in filter expressions.
FILTER_OPERATOR_RE = re.compile(
    r"(?:=|!=|<>|<|>|<=|>=|BETWEEN\s+|IS\s+(?:NOT\s+)?NULL|LIKE|NOT\s+LIKE|IN\s*\()",
    re.IGNORECASE,
)
