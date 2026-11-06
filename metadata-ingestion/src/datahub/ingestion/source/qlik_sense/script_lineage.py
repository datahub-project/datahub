import re
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

CONNECT_RE = re.compile(
    r"^(?:(?:LIB|ODBC|OLEDB|CUSTOM)\s+)?CONNECT(?:32|64)?\s+TO\s+"
    r"(?:'(?P<single>[^']*)'|\[(?P<bracket>[^\]]*)\]|\"(?P<double>[^\"]*)\"|(?P<bare>\S+))",
    re.IGNORECASE,
)
DISCONNECT_RE = re.compile(r"^DISCONNECT\b", re.IGNORECASE)
# Control statements end at the line break rather than a semicolon, so they
# lead the statement that follows them.
# Quoted text may contain THEN, so only an unquoted THEN ends a condition.
_CONDITION = r"(?:[^\n'\"\[]|'[^'\n]*'|\"[^\"\n]*\"|\[[^\]\n]*\])*?\bTHEN\b"
CONTROL_RE = re.compile(
    rf"^(?:(?P<elseif>ELSE\s*IF\b{_CONDITION})|(?P<if>IF\b{_CONDITION})"
    r"|(?P<else>ELSE\b)|(?P<endif>END\s*IF\b|ENDIF\b)"
    r"|(?P<endswitch>END\s*SWITCH\b)|(?P<default>DEFAULT\b)"
    r"|(?P<switch>SWITCH\b[^\n]*)|(?P<case>CASE\b[^\n]*)"
    r"|END\s*SUB\b|(?:SUB|FOR|NEXT|DO|LOOP)\b[^\n]*)\s*",
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
BACKTICK_QUOTED_PLATFORMS = {"bigquery", "databricks", "hive", "mariadb", "mysql"}
# Qlik's REST, JSON and XML connectors take SQL-like text that names no
# database table, such as SELECT ... FROM JSON (wrap on) "root" WITH CONNECTION (...).
CONNECTOR_SQL_RE = re.compile(
    r"\bFROM\s+(?:JSON|XML|CSV)\b|\bWITH\s+CONNECTION\s*\(", re.IGNORECASE
)
# A bracket after a name or a closing bracket is an index, as in values[0].
BRACKET_IDENTIFIER_RE = re.compile(
    r"'[^']*'|\"[^\"]*\"|`[^`]*`|(?<![\w\])])\[(?P<name>[^\]]*)\]"
)
# A variable the script never set, used as a whole condition such as
# ON a = b $(vWhere) AND c = 1. It cannot name a table, so dropping it only
# loses a filter.
CONDITION_VARIABLE_RE = re.compile(
    r"(?<=\s)\$\([^()]*\)(?=\s+(?:AND|OR)\b)", re.IGNORECASE
)
KEYWORD_BEFORE_CONDITION_RE = re.compile(
    r"(?:\b(?:WHERE|AND|OR)|\()\s*$", re.IGNORECASE
)
LOAD_RE = re.compile(r"^LOAD\b", re.IGNORECASE)
LOAD_SOURCE_RE = re.compile(r"\b(?:FROM|RESIDENT|INLINE|AUTOGENERATE)\b", re.IGNORECASE)
QUOTED_RE = re.compile(r"'[^']*'|\"[^\"]*\"|\[[^\]]*\]")
DATABASE_RE = re.compile(
    r"(?:^|[;\s])(?:database|initial catalog|catalog|conncatalog)\s*=\s*[\"'\[]?"
    r"(?P<db>[^;\]\"']+)",
    re.IGNORECASE,
)
# The values in a set modifier, such as {"Net*"} in {<Region={"Net*"}>}, are
# search strings even when double-quoted.
SET_VALUES_START_RE = re.compile(r"[-+*/]?=\s*\{")
# Single-quoted text is a string literal in a chart expression, not a field.
EXPRESSION_NAME_RE = re.compile(
    r"\[(?P<bracket>[^\]]+)\]|\"(?P<double>[^\"]+)\"|'[^']*'|(?P<bare>[^\W\d][\w.]*)"
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


class _BranchState(BaseModel):
    connection: Optional[str] = None
    variables: Dict[str, str] = {}


class _Block(BaseModel):
    before: _BranchState
    branches: List[_BranchState] = []
    has_default: bool = False
    # Statements between SWITCH and its first CASE run in no branch.
    in_case: bool = False


class _ControlFlowTracker(BaseModel):
    """The connection and variables in effect at each statement. Conditions
    cannot be evaluated, so a value set inside an IF or SWITCH only survives it
    when every branch agrees."""

    state: _BranchState = Field(default_factory=_BranchState)
    blocks: List[_Block] = []

    def strip_control_statements(self, statement: str) -> str:
        while True:
            control = CONTROL_RE.match(statement)
            if not control or not control.end():
                return statement
            if control.group("if") or control.group("switch"):
                self.blocks.append(
                    _Block(
                        before=self.state.model_copy(deep=True),
                        in_case=bool(control.group("if")),
                    )
                )
            elif self.blocks and (
                control.group("elseif")
                or control.group("else")
                or control.group("case")
                or control.group("default")
            ):
                self._next_branch(
                    self.blocks[-1],
                    is_default=bool(control.group("else") or control.group("default")),
                )
            elif self.blocks and (control.group("endif") or control.group("endswitch")):
                self._end_block(self.blocks.pop())
            statement = statement[control.end() :]

    def _next_branch(self, block: _Block, is_default: bool) -> None:
        if block.in_case:
            block.branches.append(self.state)
        block.in_case = True
        block.has_default = block.has_default or is_default
        self.state = block.before.model_copy(deep=True)

    def _end_block(self, block: _Block) -> None:
        outcomes = [*block.branches, self.state] if block.in_case else block.branches
        if not block.has_default:
            outcomes.append(block.before)
        first, *rest = outcomes
        self.state = _BranchState(
            connection=first.connection
            if all(other.connection == first.connection for other in rest)
            else None,
            variables={
                name: value
                for name, value in first.variables.items()
                if all(other.variables.get(name) == value for other in rest)
            },
        )

    def record_variable(self, statement: str) -> bool:
        match = VARIABLE_RE.match(statement)
        if not match:
            return False
        value = match.group("value").strip()
        literal = STRING_LITERAL_RE.fullmatch(value)
        if literal:
            self.state.variables[match.group("name")] = _first_group(literal)
        elif match.group("kind").upper() == "SET":
            self.state.variables[match.group("name")] = value
        else:
            # LET evaluates an expression, so its value is unknown here.
            self.state.variables.pop(match.group("name"), None)
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
    flow = _ControlFlowTracker()
    pending_label: Optional[str] = None
    for statement in _split_statements(script):
        statement = flow.strip_control_statements(statement)
        statement = _expand_variables(statement, flow.state.variables)
        if not statement:
            continue
        if flow.record_variable(statement):
            pending_label = None
            continue
        connect = CONNECT_RE.match(statement)
        if connect:
            flow.state.connection = _first_group(connect).strip()
            continue
        if DISCONNECT_RE.match(statement):
            flow.state.connection = None
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
                    connection=flow.state.connection,
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


def strip_set_values(expression: str) -> str:
    """Drop set modifier values, which can nest braces as in {$(=Only({1} Year))}."""
    kept: List[str] = []
    position = 0
    while match := SET_VALUES_START_RE.search(expression, position):
        kept.append(expression[position : match.start()])
        depth = 1
        position = match.end()
        while position < len(expression) and depth:
            depth += {"{": 1, "}": -1}.get(expression[position], 0)
            position += 1
    kept.append(expression[position:])
    return "".join(kept)


def expression_names(expression: str) -> List[str]:
    """Names an expression may read as fields. Function names and keywords come
    back too, so callers keep only names that are fields of the app."""
    return [
        name
        for match in EXPRESSION_NAME_RE.finditer(strip_set_values(expression))
        if (
            name := match.group("bracket")
            or match.group("double")
            or match.group("bare")
        )
    ]


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


def brackets_to_backticks(sql: str) -> str:
    """ODBC drivers accept [name] identifiers and translate them for engines
    such as Databricks that only quote with backticks."""
    return BRACKET_IDENTIFIER_RE.sub(
        lambda match: (
            f"`{match.group('name')}`"
            if match.group("name") is not None
            else match.group(0)
        ),
        sql,
    )


def drop_condition_variables(sql: str) -> str:
    return CONDITION_VARIABLE_RE.sub(
        lambda match: (
            "1 = 1" if KEYWORD_BEFORE_CONDITION_RE.search(sql, 0, match.start()) else ""
        ),
        sql,
    )
