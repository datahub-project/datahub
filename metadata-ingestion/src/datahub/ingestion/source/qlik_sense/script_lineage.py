import re
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from datahub.metadata.urns import DatasetUrn

CONNECT_RE = re.compile(
    r"^(?:(?:LIB|ODBC|OLEDB|CUSTOM)\s+)?CONNECT(?:32|64)?\s+TO\s+"
    r"(?:'(?P<single>[^']*)'|\[(?P<bracket>[^\]]*)\]|\"(?P<double>[^\"]*)\"|(?P<bare>\S+))",
    re.IGNORECASE,
)
DISCONNECT_RE = re.compile(r"^DISCONNECT\b", re.IGNORECASE)
REM_RE = re.compile(r"REM\b", re.IGNORECASE)
# `--` is a line comment only in SQL that Qlik passes to the database.
SQL_STATEMENT_RE = re.compile(r"\b(?:SQL|SELECT)\b", re.IGNORECASE)
# Control statements end at the line break rather than a semicolon, so they
# lead the statement that follows them.
# Quoted text may contain THEN, so only an unquoted THEN ends a condition.
_CONDITION = r"(?:[^\n'\"\[]|'[^'\n]*'|\"[^\"\n]*\"|\[[^\]\n]*\])*?\bTHEN\b"
CONTROL_RE = re.compile(
    rf"^(?:(?P<elseif>ELSE\s*IF\b{_CONDITION})|(?P<if>IF\b{_CONDITION})"
    r"|(?P<else>ELSE\b)|(?P<endif>END\s*IF\b|ENDIF\b)"
    r"|(?P<endswitch>END\s*SWITCH\b)|(?P<default>DEFAULT\b)"
    r"|(?P<switch>SWITCH\b[^\n]*)|(?P<case>CASE\b[^\n]*)"
    r"|(?P<endsub>END\s*SUB\b)|(?P<sub>SUB\b[^\n]*)|(?P<forloop>FOR\b[^\n]*)"
    r"|(?:NEXT|DO|LOOP)\b[^\n]*)\s*",
    re.IGNORECASE,
)
SUB_NAME_RE = re.compile(
    r"SUB\s+(?P<name>[^\s(]+)\s*(?:\((?P<params>[^)]*)\))?", re.IGNORECASE
)
CALL_RE = re.compile(
    r"^CALL\s+(?P<name>[^\s(]+)\s*(?:\((?P<args>.*)\))?\s*$",
    re.IGNORECASE | re.DOTALL,
)
FOR_VARIABLE_RE = re.compile(r"FOR\s+(?:EACH\s+)?(?P<name>[^\s=]+)", re.IGNORECASE)
# Only an unconditional EXIT SCRIPT; one with WHEN or UNLESS may not run.
EXIT_SCRIPT_RE = re.compile(r"^EXIT\s+SCRIPT\s*$", re.IGNORECASE)
NUMBER_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
# Qlik names an unlabelled table after the table following FROM, cut to 32
# characters.
FROM_TABLE_RE = re.compile(
    r"\bFROM\s+(?P<name>(?:\[[^\]]+\]|\"[^\"]+\"|`[^`]+`|[\w$#@]+)"
    r"(?:\s*\.\s*(?:\[[^\]]+\]|\"[^\"]+\"|`[^`]+`|[\w$#@]+))*)",
    re.IGNORECASE,
)
QLIK_TABLE_NAME_LENGTH = 32
INCLUDE_RE = re.compile(r"\$\(\s*(?:Must_)?Include\s*=[^)]*\)", re.IGNORECASE)
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
# Concatenate and Join add to an existing table, named in parentheses or else
# the table loaded last. Keep (X) is not here: it creates a table of its own.
MERGE_PREFIX_RE = re.compile(
    r"\b(?:Concatenate|Join)\b\s*(?:\(\s*(?P<target>[^)]*?)\s*\))?", re.IGNORECASE
)
SQL_RE = re.compile(
    r"^(?:SQL\s+)?(?P<sql>(?:SELECT|WITH)\b.*)$", re.IGNORECASE | re.DOTALL
)
BACKTICK_QUOTED_PLATFORMS = {"bigquery", "databricks", "hive", "mariadb", "mysql"}
# Qlik's REST, JSON and XML connectors take SQL-like text that names no
# database table, such as SELECT ... FROM JSON (wrap on) "root" WITH CONNECTION (...).
CONNECTOR_SQL_RE = re.compile(
    r"\bFROM\s+(?:JSON|XML|CSV)\s*[(\"]|\bWITH\s+CONNECTION\s*\(", re.IGNORECASE
)
STRING_LITERAL_SQL_RE = re.compile(r"'[^']*'")
# A bracket after a name or a closing bracket is an index, as in values[0].
BRACKET_IDENTIFIER_RE = re.compile(
    r"'[^']*'|\"[^\"]*\"|`[^`]*`|(?<![\w\])])\[(?P<name>[^\]]*)\]"
)
# A variable the script never set, used as a whole condition such as
# ON a = b $(vWhere) AND c = 1. It cannot name a table, so dropping it only
# loses a filter.
CONDITION_VARIABLE_RE = re.compile(r"(?<=[\s(])\$\([^()]*\)(?=[\s);]|$)")
CONDITION_START_RE = re.compile(r"(?:\b(?:WHERE|ON|AND|OR)|\()\s*$", re.IGNORECASE)
OPERAND_END_RE = re.compile(r"[\w'\"`\])]\s*$")
CONDITION_JOIN_RE = re.compile(r"\s+(?:AND|OR)\b", re.IGNORECASE)
LOAD_RE = re.compile(r"^LOAD\b", re.IGNORECASE)
LOAD_SOURCE_RE = re.compile(r"\b(?:FROM|RESIDENT|INLINE|AUTOGENERATE)\b", re.IGNORECASE)
QUOTED_RE = re.compile(r"'[^']*'|\"[^\"]*\"|\[[^\]]*\]")
DATABASE_RE = re.compile(
    r"(?:^|[;\s])(?:database|initial catalog|catalog|conncatalog)\s*=\s*[\"'\[]?"
    r"(?P<db>[^;\]\"']+)",
    re.IGNORECASE,
)
# The values in a set modifier, such as {"Net*"} in {<Region={"Net*"}>}, are
# search strings even when double-quoted. They follow a field name, so the = that
# starts an expression such as ={$<Year={2024}>} Sum(Sales) is not one.
SET_VALUES_START_RE = re.compile(r"(?<=[\w\]\"])\s*[-+*/]?=\s*\{")
# Further element lists combined with the first, as in Region={"A"}+{"B"}.
SET_VALUES_NEXT_RE = re.compile(r"\s*[-+*/]\s*\{")
# Interpretation functions such as Date#(OrderDate) end their name with #.
FUNCTION_CALL_RE = re.compile(r"\s*#?\s*\(")
# Operators and aggregation qualifiers, as in Sum(DISTINCT TOTAL Amount).
EXPRESSION_KEYWORDS = {
    "all",
    "and",
    "bitand",
    "bitnot",
    "bitor",
    "bitxor",
    "distinct",
    "follows",
    "like",
    "nodistinct",
    "not",
    "or",
    "precedes",
    "total",
    "xor",
}
# Aggregation qualifiers, which are field names when nothing follows them, as
# in Sum(Total).
EXPRESSION_QUALIFIERS = {"all", "distinct", "nodistinct", "total"}
QUALIFIER_END_RE = re.compile(r"\s*(?:[),]|$)")
MAPPING_PREFIX_RE = re.compile(r"\bMapping\b", re.IGNORECASE)
# Single-quoted text is a string literal in a chart expression, not a field.
# A quoted field can carry a derived-field suffix, as in [Date].autoCalendar.Year.
EXPRESSION_NAME_RE = re.compile(
    r"\[(?P<bracket>[^\]]+)\](?P<bracket_suffix>(?:\.\w+)*)"
    r"|\"(?P<double>[^\"]+)\"(?P<double_suffix>(?:\.\w+)*)"
    r"|'[^']*'|(?P<bare>[^\W\d][\w.]*)"
)
# A driver or provider names the engine, whatever the rest of the string says.
DRIVER_RE = re.compile(
    r"(?:^|[;\s\[\"'])(?:driver|provider)\s*=\s*(?P<value>\{[^}]*\}|[^;\]\"']*)",
    re.IGNORECASE,
)
# Properties end at a semicolon or, in hand-written strings, a line break.
CONNECTION_PROPERTY_RE = re.compile(
    r"(?P<key>[^;=\n]*)=(?P<value>\s*\{[^}]*\}|\s*\"[^\"]*\"|\s*'[^']*'|[^;\n]*)"
)
# A quoted or braced value can hold its own property list, such as
# Extended Properties="UID=a;PWD=b".
NESTED_VALUE_RE = re.compile(
    r"\{(?P<braced>.*=.*)\}|\"(?P<double>.*=.*)\"|'(?P<single>.*=.*)'", re.DOTALL
)
# User info before a host, with or without a scheme, as in user:secret@host.
USERINFO_RE = re.compile(r"(?:(?<=://)|^(?=\s*[^/@\s;\"']+@))\s*[^/@\s;\"']+@")
# A query string or fragment can carry a token or signature, as in an Azure SAS URL.
QUERY_RE = re.compile(r"[?#][^\s;\"']*")
# Properties that name where the data lives. Any other property may hold a
# secret, such as Auth=Bearer ... or Passphrase=..., so it is not emitted.
PUBLIC_CONNECTION_KEYS = {
    "account",
    "catalog",
    "conncatalog",
    "data source",
    "database",
    "dsn",
    "driver",
    "host",
    "hostname",
    "initial catalog",
    "port",
    "provider",
    "schema",
    "server",
    "warehouse",
}
# Parts a table name needs on platforms whose URNs are database.schema.table
# (schema.table for Oracle, database.table for MySQL and MariaDB). A shorter name means the script left them to a
# connection that did not resolve them, so its URN would never match the table.
QUALIFIED_NAME_PARTS: Dict[str, int] = {
    "bigquery": 3,
    "databricks": 3,
    "mariadb": 2,
    "mssql": 3,
    "mysql": 2,
    "oracle": 2,
    "postgres": 3,
    "redshift": 3,
    "snowflake": 3,
}
QUOTE_PAIRS = {"'": "'", '"': '"', "[": "]"}
STATEMENT_SEPARATOR = ";"
SPACE_SEPARATOR = ":"

# Checked in order, so more specific keywords come first.
PLATFORM_KEYWORDS: Dict[str, str] = {
    "databricks": "databricks",
    "mariadb": "mariadb",
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
    # A mapping table is only used by ApplyMap and dropped from the data model.
    mapping: bool = False


class ScriptTableLineage(BaseModel):
    label: str
    identifier: str
    connection: Optional[str] = None
    upstream_urns: List[str] = []
    # Downstream column name to upstream schema field URNs.
    column_upstreams: Dict[str, List[str]] = {}
    mapping: bool = False


class DataConnectionInfo(BaseModel):
    name: str
    platform: Optional[str] = None
    database: Optional[str] = None


class ResolvedConnection(BaseModel):
    """Where a load script connection's tables live in DataHub."""

    platform: str
    env: str
    platform_instance: Optional[str] = None
    database: Optional[str] = None
    default_schema: Optional[str] = None


def _is_control_only(text: str) -> bool:
    text = text.lstrip()
    while text:
        control = CONTROL_RE.match(text)
        if not control or not control.end():
            return False
        text = text[control.end() :]
    return True


def _strip_comments(script: str, expression: bool = False) -> str:
    """Drops comments outside quotes. Chart expressions have no REM comments,
    and no lib:// paths, so any // starts a comment there."""
    kept: List[str] = []
    closing: Optional[str] = None
    statement_start = True
    # Control statements end at the line break, so a line holding only
    # control statements also starts a statement.
    control_prefix = True
    line_begin = 0
    statement_begin = 0
    i = 0
    while i < len(script):
        char = script[i]
        if closing:
            if char == closing:
                closing = None
        # A REM comment runs to the next semicolon, and an apostrophe in it
        # does not open a quote.
        elif not expression and statement_start and REM_RE.match(script, i):
            end = script.find(STATEMENT_SEPARATOR, i)
            i = len(script) if end == -1 else end + 1
            continue
        elif char in QUOTE_PAIRS:
            closing = QUOTE_PAIRS[char]
        elif script.startswith("/*", i):
            end = script.find("*/", i + 2)
            i = len(script) if end == -1 else end + 2
            # So the text on both sides does not run together.
            kept.append(" ")
            continue
        # `//` after a colon is part of a URL or `lib://` path, not a comment.
        elif (
            script.startswith("//", i)
            and (
                expression
                or not (
                    kept and (kept[-1] == ":" or kept[-1].isalnum() or kept[-1] == "_")
                )
            )
            or not expression
            and script.startswith("--", i)
            and SQL_STATEMENT_RE.search("".join(kept[statement_begin:]))
        ):
            end = script.find("\n", i)
            i = len(script) if end == -1 else end
            continue
        kept.append(char)
        if not closing and char == STATEMENT_SEPARATOR:
            statement_start = True
            control_prefix = True
            line_begin = len(kept)
            statement_begin = len(kept)
        elif not closing and char == "\n":
            if control_prefix and _is_control_only("".join(kept[line_begin:])):
                statement_start = True
            else:
                control_prefix = False
            line_begin = len(kept)
        elif not char.isspace():
            statement_start = False
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

    def apply_control(self, control: "re.Match[str]") -> None:
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
        elif control.group("forloop"):
            # The loop variable takes values that are not known here.
            variable = FOR_VARIABLE_RE.match(control.group("forloop"))
            if variable:
                self.state.variables.pop(variable.group("name"), None)

    def forget_state(self) -> None:
        self.state = _BranchState()

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


def _unquote(name: str) -> str:
    if len(name) > 1 and QUOTE_PAIRS.get(name[0]) == name[-1]:
        return name[1:-1]
    return name


class _Piece(BaseModel):
    """A control statement such as IF ... THEN, or a statement it leads."""

    text: str
    control: bool = False


class _Sub(BaseModel):
    params: List[str] = []
    body: List[_Piece] = []


class _Script(BaseModel):
    pieces: List[_Piece] = []
    # SUBs by lowercased name. They only run when called.
    subs: Dict[str, _Sub] = {}


def _split_top_level(text: str) -> List[str]:
    items: List[str] = []
    depth = 0
    start = 0
    closing: Optional[str] = None
    for index, char in enumerate(text):
        if closing:
            if char == closing:
                closing = None
        elif char in QUOTE_PAIRS:
            closing = QUOTE_PAIRS[char]
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            items.append(text[start:index].strip())
            start = index + 1
    items.append(text[start:].strip())
    return items


def _literal_value(text: str) -> Optional[str]:
    literal = STRING_LITERAL_RE.fullmatch(text)
    if literal:
        return _first_group(literal)
    return text if NUMBER_RE.match(text) else None


def _from_table_name(sql: str) -> Optional[str]:
    match = FROM_TABLE_RE.search(sql)
    # A name cut short by an unexpanded $(variable) is not the table's name.
    if not match or sql.startswith("(", match.end()):
        return None
    parts = re.split(r"\s*\.\s*", match.group("name"))
    name = ".".join(part.strip('[]"`') for part in parts)
    return name[:QLIK_TABLE_NAME_LENGTH]


def _script_pieces(script: str) -> _Script:
    parsed = _Script()
    # Bodies of the SUBs being defined, innermost last.
    defining: List[List[_Piece]] = []
    for statement in _split_statements(_strip_comments(script)):
        pieces: List[_Piece] = []
        while (control := CONTROL_RE.match(statement)) and control.end():
            pieces.append(_Piece(text=control.group(0), control=True))
            statement = statement[control.end() :]
        if statement:
            pieces.append(_Piece(text=statement))
        for piece in pieces:
            control = CONTROL_RE.match(piece.text) if piece.control else None
            if control and control.group("sub"):
                header = SUB_NAME_RE.match(control.group("sub"))
                sub = _Sub(
                    params=[
                        param
                        for param in _split_top_level(header.group("params") or "")
                        if param
                    ]
                    if header
                    else []
                )
                parsed.subs[header.group("name").lower() if header else ""] = sub
                defining.append(sub.body)
            elif control and control.group("endsub") and defining:
                defining.pop()
            else:
                (defining[-1] if defining else parsed.pieces).append(piece)
    return parsed


class _ScriptParser(BaseModel):
    subs: Dict[str, _Sub]
    tables: List[ScriptTable] = []
    flow: _ControlFlowTracker = Field(default_factory=_ControlFlowTracker)
    pending_label: Optional[str] = None
    # A preceding LOAD is waiting for its SELECT. Only the top of a stack of
    # them carries the label.
    pending_preceding: bool = False
    pending_mapping: bool = False
    # A SELECT under a preceding LOAD that adds to an unknown table.
    pending_skip: bool = False
    # The table a Concatenate or Join without a target adds to.
    last_label: Optional[str] = None
    # SUBs being run, so a SUB that calls itself is not replayed forever.
    calling: List[str] = []
    exited: bool = False

    def run(self, pieces: List[_Piece]) -> None:
        for piece in pieces:
            if self.exited:
                return
            if piece.control:
                control = CONTROL_RE.match(piece.text)
                if control:
                    self.flow.apply_control(control)
            else:
                self._statement(piece.text)

    def _clear_pending(self) -> None:
        self.pending_mapping = False
        self.pending_label = None
        self.pending_preceding = False
        self.pending_skip = False

    def _forget(self) -> None:
        self.flow.forget_state()
        self.last_label = None
        self._clear_pending()

    def _call(self, name: str, args: Optional[str]) -> None:
        sub = self.subs.get(name.lower())
        if sub is None or name.lower() in self.calling:
            # The SUB may come from an include, so it could have set anything.
            self._forget()
            return
        # The body runs with the connection and variables in effect here, and
        # its parameters hold the arguments only for the call.
        variables = self.flow.state.variables
        saved = {param: variables.get(param) for param in sub.params}
        values = _split_top_level(args) if args else []
        for index, param in enumerate(sub.params):
            value = _literal_value(values[index]) if index < len(values) else None
            if value is None:
                variables.pop(param, None)
            else:
                variables[param] = value
        self.calling.append(name.lower())
        self.run(sub.body)
        self.calling.pop()
        variables = self.flow.state.variables
        for param, value in saved.items():
            if value is None:
                variables.pop(param, None)
            else:
                variables[param] = value

    def _statement(self, statement: str) -> None:
        statement, includes = INCLUDE_RE.subn("", statement)
        if includes:
            # The included script is not read, so it could have set anything.
            self._forget()
            statement = statement.strip()
        statement = _expand_variables(statement, self.flow.state.variables)
        if not statement:
            return
        if EXIT_SCRIPT_RE.match(statement) and not self.flow.blocks:
            self.exited = True
            return
        if self.flow.record_variable(statement):
            self._clear_pending()
            return
        call = CALL_RE.match(statement)
        if call:
            self._clear_pending()
            self._call(call.group("name"), call.group("args"))
            return
        connect = CONNECT_RE.match(statement)
        if connect:
            self.flow.state.connection = _first_group(connect).strip()
            return
        if DISCONNECT_RE.match(statement):
            self.flow.state.connection = None
            return

        label: Optional[str] = None
        label_match = LABEL_RE.match(statement)
        if label_match:
            label = _first_group(label_match).strip()
            statement = statement[label_match.end() :].lstrip()
        prefix = PREFIX_RE.match(statement)
        merge = MERGE_PREFIX_RE.search(prefix.group(0)) if prefix else None
        mapping = bool(prefix and MAPPING_PREFIX_RE.search(prefix.group(0)))
        if prefix:
            statement = statement[prefix.end() :]
        skip = False
        if merge:
            target = merge.group("target")
            label = _unquote(target) if target else self.last_label
            skip = label is None

        sql = SQL_RE.match(statement)
        if sql:
            if not (skip or self.pending_skip):
                table_label = (
                    label
                    or self.pending_label
                    or _from_table_name(sql.group("sql"))
                    or f"table_{len(self.tables) + 1}"
                )
                self.tables.append(
                    ScriptTable(
                        label=table_label,
                        connection=self.flow.state.connection,
                        sql=sql.group("sql"),
                        mapping=mapping or self.pending_mapping,
                    )
                )
                self.last_label = table_label
            self._clear_pending()
        elif LOAD_RE.match(statement) and not LOAD_SOURCE_RE.search(
            QUOTED_RE.sub("", statement)
        ):
            # A preceding LOAD names the SQL SELECT that follows it.
            if not self.pending_preceding:
                self.pending_label = label
                self.pending_skip = skip
                self.pending_mapping = mapping
            self.pending_preceding = True
        else:
            if LOAD_RE.match(statement):
                self.last_label = label
            self._clear_pending()


def parse_load_script(script: str) -> List[ScriptTable]:
    parsed = _script_pieces(script)
    parser = _ScriptParser(subs=parsed.subs)
    parser.run(parsed.pieces)
    return parser.tables


def _block_end(text: str, position: int, opening: str, closing: str) -> int:
    """Index just past the closing delimiter of a block opened right before
    position, counting nested blocks. Delimiters inside quotes, such as the
    search string in {<Name={"A)"}>}, do not count."""
    depth = 1
    quote_closing: Optional[str] = None
    while position < len(text) and depth:
        char = text[position]
        if quote_closing:
            if char == quote_closing:
                quote_closing = None
        elif char in QUOTE_PAIRS:
            quote_closing = QUOTE_PAIRS[char]
        else:
            depth += {opening: 1, closing: -1}.get(char, 0)
        position += 1
    return position


def strip_set_values(expression: str) -> str:
    """Drop set modifier values, which can nest braces as in {$(=Only({1} Year))}."""
    kept: List[str] = []
    position = 0
    while match := SET_VALUES_START_RE.search(expression, position):
        kept.append(expression[position : match.start()])
        while match:
            position = _block_end(expression, match.end(), "{", "}")
            match = SET_VALUES_NEXT_RE.match(expression, position)
    kept.append(expression[position:])
    return "".join(kept)


def _strip_expansions(expression: str) -> str:
    """Drop $(...) expansions, which can nest parentheses as in $(=Max(Year))."""
    kept: List[str] = []
    position = 0
    while (start := expression.find("$(", position)) != -1:
        kept.append(expression[position:start])
        position = _block_end(expression, start + 2, "(", ")")
        kept.append(" ")
    kept.append(expression[position:])
    return "".join(kept)


def reads_unknown_fields(expression: str) -> bool:
    """Whether a $(variable) expansion outside comments and set modifier values
    may stand for fields the expression reads."""
    return "$(" in strip_set_values(_strip_comments(expression, expression=True))


def expression_names(expression: str) -> List[str]:
    """Names an expression may read as fields. Not every name is a field, so
    callers keep only names that are fields of the app."""
    # The fields behind an expansion are unknown, and its name is not a field.
    expression = _strip_expansions(
        strip_set_values(_strip_comments(expression, expression=True))
    )
    names: List[str] = []
    for match in EXPRESSION_NAME_RE.finditer(expression):
        bare = match.group("bare")
        if bare and (
            (
                bare.lower() in EXPRESSION_KEYWORDS
                and not (
                    bare.lower() in EXPRESSION_QUALIFIERS
                    and QUALIFIER_END_RE.match(expression, match.end())
                )
            )
            # A function call such as Year(OrderDate), even when an app field
            # has the same name.
            or FUNCTION_CALL_RE.match(expression, match.end())
        ):
            continue
        if match.group("bracket"):
            name = match.group("bracket") + match.group("bracket_suffix")
        elif match.group("double"):
            name = match.group("double") + match.group("double_suffix")
        else:
            name = bare
        if name:
            names.append(name)
    return names


def _platform_in(text: str) -> Optional[str]:
    text = text.lower()
    return next(
        (
            value
            for keyword, value in PLATFORM_KEYWORDS.items()
            if PLATFORM_KEYWORD_RES[keyword].search(text)
        ),
        None,
    )


def infer_data_connection(name: str, descriptor: str) -> DataConnectionInfo:
    drivers = [match.group("value") for match in DRIVER_RE.finditer(descriptor)]
    platform = next(
        (
            platform
            for platform in map(_platform_in, [*drivers, descriptor])
            if platform
        ),
        None,
    )
    database = DATABASE_RE.search(descriptor)
    return DataConnectionInfo(
        name=name,
        platform=platform,
        database=database.group("db").strip() if database else None,
    )


def is_inline_connection(connection: str) -> bool:
    # LIB CONNECT names a data connection; ODBC/OLEDB CONNECT can instead
    # carry the whole connection string.
    return "=" in connection


def _is_public_key(key: str) -> bool:
    return key.strip().strip("{}").lower() in PUBLIC_CONNECTION_KEYS


def _redact_value(value: str) -> Optional[str]:
    """The value without secrets, None when nothing public is left."""
    stripped = value.strip()
    nested = NESTED_VALUE_RE.fullmatch(stripped)
    if nested:
        inner = redact_connection(_first_group(nested))
        return f"{stripped[0]}{inner}{stripped[-1]}" if inner else None
    return QUERY_RE.sub("", USERINFO_RE.sub("", value))


def redact_connection(connection: str) -> str:
    """Keeps only the properties of an inline connection string that name
    where the data lives."""
    if not is_inline_connection(connection):
        return connection
    properties: List[str] = []
    for match in CONNECTION_PROPERTY_RE.finditer(connection):
        if not _is_public_key(match.group("key")):
            continue
        value = _redact_value(match.group("value"))
        if value is not None:
            properties.append(f"{match.group('key')}={value}".strip())
    return ";".join(properties)


def has_qualified_name(
    urn: str, platform: str, platform_instance: Optional[str]
) -> bool:
    required = QUALIFIED_NAME_PARTS.get(platform)
    if not required:
        return True
    name = DatasetUrn.from_string(urn).name
    # The SQL parser lowercases the instance in the URN.
    if platform_instance and name.lower().startswith(f"{platform_instance.lower()}."):
        name = name[len(platform_instance) + 1 :]
    return len(name.split(".")) >= required


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


def is_connector_sql(sql: str) -> bool:
    return bool(CONNECTOR_SQL_RE.search(STRING_LITERAL_SQL_RE.sub("''", sql)))


def drop_condition_variables(sql: str) -> str:
    def replace(match: "re.Match[str]") -> str:
        if CONDITION_START_RE.search(sql, 0, match.start()):
            return "1 = 1"
        if OPERAND_END_RE.search(sql, 0, match.start()) and CONDITION_JOIN_RE.match(
            sql, match.end()
        ):
            return ""
        return match.group(0)

    return CONDITION_VARIABLE_RE.sub(replace, sql)
