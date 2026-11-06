import re
from typing import Dict, List, Optional

from pydantic import BaseModel

CONNECT_RE = re.compile(
    r"^(?:(?:LIB|ODBC|OLEDB|CUSTOM)\s+)?CONNECT(?:32|64)?\s+TO\s+"
    r"(?:'(?P<single>[^']*)'|\[(?P<bracket>[^\]]*)\]|\"(?P<double>[^\"]*)\"|(?P<bare>\S+))",
    re.IGNORECASE,
)
DISCONNECT_RE = re.compile(r"^DISCONNECT\b", re.IGNORECASE)
# Control statements end at the line break rather than a semicolon, so they
# lead the statement that follows them.
CONTROL_RE = re.compile(
    r"^(?:(?P<elseif>ELSE\s*IF\b[^\n]*?\bTHEN\b)|(?P<if>IF\b[^\n]*?\bTHEN\b)"
    r"|(?P<else>ELSE\b)|(?P<endif>END\s*IF\b|ENDIF\b)|END\s*SUB\b|END\s*SWITCH\b"
    r"|DEFAULT\b|(?:SUB|FOR|NEXT|DO|LOOP|SWITCH|CASE)\b[^\n]*)\s*",
    re.IGNORECASE,
)
VARIABLE_RE = re.compile(
    r"^(?P<kind>SET|LET)\s+(?P<name>[^\s=]+)\s*=(?P<value>.*)$",
    re.IGNORECASE | re.DOTALL,
)
STRING_LITERAL_RE = re.compile(r"'(?P<single>[^']*)'|\"(?P<double>[^\"]*)\"")
VARIABLE_REFERENCE_RE = re.compile(r"\$\((?P<name>[^()=]+)\)")
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
# A keyword only counts at the start of a word, so "MySQL Server" is not "sql server".
PLATFORM_KEYWORD_RES: Dict[str, "re.Pattern[str]"] = {
    keyword: re.compile(rf"(?<![a-z]){re.escape(keyword)}")
    for keyword in PLATFORM_KEYWORDS
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


class _IfBlock(BaseModel):
    connection_before: Optional[str]
    branch_connections: List[Optional[str]] = []
    has_else: bool = False


class _ConnectionTracker(BaseModel):
    """The connection in effect at each statement. Branches cannot be evaluated,
    so a connection chosen inside an IF only survives it when every branch agrees."""

    current: Optional[str] = None
    blocks: List[_IfBlock] = []

    def strip_control_statements(self, statement: str) -> str:
        while True:
            control = CONTROL_RE.match(statement)
            if not control or not control.end():
                return statement
            if control.group("if"):
                self.blocks.append(_IfBlock(connection_before=self.current))
            elif self.blocks and (control.group("elseif") or control.group("else")):
                block = self.blocks[-1]
                block.branch_connections.append(self.current)
                block.has_else = block.has_else or bool(control.group("else"))
                self.current = block.connection_before
            elif self.blocks and control.group("endif"):
                block = self.blocks.pop()
                outcomes = [*block.branch_connections, self.current]
                if not block.has_else:
                    outcomes.append(block.connection_before)
                self.current = outcomes[0] if len(set(outcomes)) == 1 else None
            statement = statement[control.end() :]


def _record_variable(statement: str, variables: Dict[str, str]) -> bool:
    match = VARIABLE_RE.match(statement)
    if not match:
        return False
    value = match.group("value").strip()
    literal = STRING_LITERAL_RE.fullmatch(value)
    if literal:
        variables[match.group("name")] = _first_group(literal)
    elif match.group("kind").upper() == "SET":
        variables[match.group("name")] = value
    else:
        # LET evaluates an expression, so its value is unknown here.
        variables.pop(match.group("name"), None)
    return True


def _expand_variables(statement: str, variables: Dict[str, str]) -> str:
    return VARIABLE_REFERENCE_RE.sub(
        lambda reference: variables.get(
            reference.group("name").strip(), reference.group(0)
        ),
        statement,
    )


def parse_load_script(script: str) -> List[ScriptTable]:
    script = _strip_comments(script)
    tables: List[ScriptTable] = []
    connection = _ConnectionTracker()
    pending_label: Optional[str] = None
    variables: Dict[str, str] = {}
    for statement in _split_statements(script):
        statement = _expand_variables(
            connection.strip_control_statements(statement), variables
        )
        if not statement:
            continue
        if _record_variable(statement, variables):
            pending_label = None
            continue
        connect = CONNECT_RE.match(statement)
        if connect:
            connection.current = _first_group(connect).strip()
            continue
        if DISCONNECT_RE.match(statement):
            connection.current = None
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
                    label=table_label,
                    connection=connection.current,
                    sql=sql.group("sql"),
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
        (
            value
            for keyword, value in PLATFORM_KEYWORDS.items()
            if PLATFORM_KEYWORD_RES[keyword].search(text)
        ),
        None,
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
