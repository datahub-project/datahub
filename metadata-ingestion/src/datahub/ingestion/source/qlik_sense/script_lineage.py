import re
from typing import Dict, List, Optional

from pydantic import BaseModel

CONNECT_RE = re.compile(
    r"^(?:(?:LIB|ODBC|OLEDB|CUSTOM)\s+)?CONNECT(?:32|64)?\s+TO\s+"
    r"(?:'(?P<single>[^']*)'|\[(?P<bracket>[^\]]*)\]|\"(?P<double>[^\"]*)\"|(?P<bare>\S+))",
    re.IGNORECASE,
)
DISCONNECT_RE = re.compile(r"^DISCONNECT\b", re.IGNORECASE)
LABEL_RE = re.compile(
    r"^(?:\[(?P<bracket>[^\]]+)\]|\"(?P<double>[^\"]+)\"|(?P<bare>[^\W\d][\w.\-]*))\s*:(?!/)"
)
PREFIX_RE = re.compile(
    r"^(?:(?:NoConcatenate|Concatenate|Join|Left|Right|Inner|Outer|Keep|Mapping|Generic"
    r"|Buffer|Hierarchy|HierarchyBelongsTo|IntervalMatch|CrossTable|Replace|Add|Merge|Only"
    r"|First\s+\d+|Sample\s+[\d.]+)\b\s*(?:\([^)]*\))?\s*)*",
    re.IGNORECASE,
)
SQL_RE = re.compile(
    r"^(?:SQL\s+)?(?P<sql>(?:SELECT|WITH)\b.*)$", re.IGNORECASE | re.DOTALL
)
LOAD_RE = re.compile(r"^LOAD\b", re.IGNORECASE)
LOAD_SOURCE_RE = re.compile(r"\b(?:FROM|RESIDENT|INLINE|AUTOGENERATE)\b", re.IGNORECASE)
QUOTED_RE = re.compile(r"'[^']*'|\"[^\"]*\"|\[[^\]]*\]")
DATABASE_RE = re.compile(
    r"(?:^|[;\s])(?:database|initial catalog|catalog|conncatalog)\s*=\s*[\"'\[]?"
    r"(?P<db>[^;\]\"']+)",
    re.IGNORECASE,
)
QUOTE_PAIRS = {"'": "'", '"': '"', "[": "]"}
STATEMENT_SEPARATOR = ";"
SPACE_SEPARATOR = ":"

# Checked in order, so more specific keywords come first.
PLATFORM_KEYWORDS: Dict[str, str] = {
    "databricks": "databricks",
    "sqlserver": "mssql",
    "sql server": "mssql",
    "mssql": "mssql",
    "sqloledb": "mssql",
    "msoledbsql": "mssql",
    "sqlncli": "mssql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "gbq": "bigquery",
    "redshift": "redshift",
    "postgres": "postgres",
    "oracle": "oracle",
    "mysql": "mysql",
}


class ScriptTable(BaseModel):
    label: str
    connection: Optional[str] = None
    sql: str


class ScriptTableLineage(BaseModel):
    label: str
    identifier: str
    connection: Optional[str] = None
    upstream_urns: List[str] = []
    # Downstream column name to upstream schema field URNs.
    column_upstreams: Dict[str, List[str]] = {}


class DataConnectionInfo(BaseModel):
    name: str
    platform: Optional[str] = None
    database: Optional[str] = None


def _strip_comments(script: str) -> str:
    kept: List[str] = []
    closing: Optional[str] = None
    i = 0
    while i < len(script):
        char = script[i]
        if closing:
            if char == closing:
                closing = None
        elif char in QUOTE_PAIRS:
            closing = QUOTE_PAIRS[char]
        elif script.startswith("/*", i):
            end = script.find("*/", i + 2)
            i = len(script) if end == -1 else end + 2
            continue
        # `//` after a colon is part of a URL or `lib://` path, not a comment.
        elif script.startswith("//", i) and not (
            kept and (kept[-1] == ":" or kept[-1].isalnum() or kept[-1] == "_")
        ):
            end = script.find("\n", i)
            i = len(script) if end == -1 else end
            continue
        kept.append(char)
        i += 1
    return "".join(kept)


def _split_statements(script: str) -> List[str]:
    statements: List[str] = []
    current: List[str] = []
    closing: Optional[str] = None
    for char in script:
        current.append(char)
        if closing:
            if char == closing:
                closing = None
        elif char in QUOTE_PAIRS:
            closing = QUOTE_PAIRS[char]
        elif char == STATEMENT_SEPARATOR:
            statements.append("".join(current[:-1]).strip())
            current = []
    statements.append("".join(current).strip())
    return [statement for statement in statements if statement]


def _first_group(match: "re.Match[str]") -> str:
    return next(value for value in match.groupdict().values() if value is not None)


def parse_load_script(script: str) -> List[ScriptTable]:
    script = _strip_comments(script)
    tables: List[ScriptTable] = []
    connection: Optional[str] = None
    pending_label: Optional[str] = None
    for statement in _split_statements(script):
        connect = CONNECT_RE.match(statement)
        if connect:
            connection = _first_group(connect).strip()
            continue
        if DISCONNECT_RE.match(statement):
            connection = None
            continue

        label: Optional[str] = None
        label_match = LABEL_RE.match(statement)
        if label_match:
            label = _first_group(label_match).strip()
            statement = statement[label_match.end() :].lstrip()
        statement = PREFIX_RE.sub("", statement, count=1)

        sql = SQL_RE.match(statement)
        if sql:
            # Qlik names an unlabelled table after the statement position.
            table_label = label or pending_label or f"table_{len(tables) + 1}"
            tables.append(
                ScriptTable(
                    label=table_label, connection=connection, sql=sql.group("sql")
                )
            )
            pending_label = None
        elif LOAD_RE.match(statement) and not LOAD_SOURCE_RE.search(
            QUOTED_RE.sub("", statement)
        ):
            # A preceding LOAD names the SQL SELECT that follows it.
            pending_label = label
        else:
            pending_label = None
    return tables


def infer_data_connection(name: str, descriptor: str) -> DataConnectionInfo:
    text = f"{name} {descriptor}".lower()
    platform = next(
        (value for keyword, value in PLATFORM_KEYWORDS.items() if keyword in text), None
    )
    database = DATABASE_RE.search(descriptor)
    return DataConnectionInfo(
        name=name,
        platform=platform,
        database=database.group("db").strip() if database else None,
    )


def connection_lookup_names(name: str) -> List[str]:
    # Qlik Cloud scripts reference connections as `<space>:<connection>`.
    short_name = name.rsplit(SPACE_SEPARATOR, 1)[-1]
    return [name] if short_name == name else [name, short_name]
