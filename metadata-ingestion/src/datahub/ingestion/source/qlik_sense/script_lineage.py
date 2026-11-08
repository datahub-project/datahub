import re
from typing import AbstractSet, Dict, List, Optional

from pydantic import BaseModel, Field

from datahub.metadata.urns import DatasetUrn

CONNECT_RE = re.compile(
    r"^(?:(?:LIB|ODBC|OLEDB|CUSTOM)\s+)?CONNECT(?:32|64)?\s+TO\s+"
    r"(?:'(?P<single>[^']*)'|\[(?P<bracket>[^\]]*)\]|\"(?P<double>[^\"]*)\"|(?P<bare>\S+))",
    re.IGNORECASE,
)
DISCONNECT_RE = re.compile(r"^DISCONNECT\b", re.IGNORECASE)
REM_RE = re.compile(r"REM\b", re.IGNORECASE)
# Control statements end at the line break rather than a semicolon, so they
# lead the statement that follows them.
# Quoted text may contain THEN, so only an unquoted THEN ends a condition.
_CONDITION = r"(?:[^\n'\"\[]|'[^'\n]*'|\"[^\"\n]*\"|\[[^\]\n]*\])*?\bTHEN\b"
CONTROL_RE = re.compile(
    rf"^(?:(?P<elseif>ELSE\s*IF\b{_CONDITION})|(?P<if>IF\b{_CONDITION})"
    r"|(?P<else>ELSE\b)|(?P<endif>END\s*IF\b|ENDIF\b)"
    r"|(?P<endswitch>END\s*SWITCH\b)|(?P<default>DEFAULT\b)"
    r"|(?P<switch>SWITCH\b[^\n]*)|(?P<case>CASE\b[^\n]*)"
    r"|(?P<endsub>END\s*SUB\b)|(?P<sub>SUB\b[^\n]*)|(?:FOR|NEXT|DO|LOOP)\b[^\n]*)\s*",
    re.IGNORECASE,
)
SUB_NAME_RE = re.compile(r"SUB\s+(?P<name>[^\s(]+)", re.IGNORECASE)
CALL_RE = re.compile(r"^CALL\s+(?P<name>[^\s(]+)", re.IGNORECASE)
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
LOAD_SOURCE_RE = re.compile(
    r"\b(?:FROM|RESIDENT|INLINE|AUTOGENERATE)\b"
    r"|\bFROM_FIELD\s*\(|\bEXTENSION\s+[\w.]+\s*\(",
    re.IGNORECASE,
)
QUOTED_RE = re.compile(r"'[^']*'|\"[^\"]*\"|\[[^\]]*\]")
LOAD_FIELDS_RE = re.compile(
    r"^LOAD\s+(?:DISTINCT\s+)?(?P<fields>.*)$", re.IGNORECASE | re.DOTALL
)
# Clauses a preceding LOAD may have after its field list.
LOAD_CLAUSE_RE = re.compile(r"\b(?:WHERE|WHILE|GROUP\s+BY|ORDER\s+BY)\b", re.IGNORECASE)
# Qlik key fields are often named with a leading %, such as %OrderKey.
_BARE_FIELD_NAME = r"(?:%|[^\W\d])[\w.%]*"
_FIELD_NAME = (
    rf"(?:\[(?P<bracket>[^\]]+)\]|\"(?P<double>[^\"]+)\"|(?P<bare>{_BARE_FIELD_NAME}))"
)
# Greedy, so the last AS names the field.
LOAD_ALIAS_RE = re.compile(
    rf"^(?P<expr>.+)\s+AS\s+{_FIELD_NAME}$", re.IGNORECASE | re.DOTALL
)
LOAD_FIELD_RE = re.compile(rf"^{_FIELD_NAME}$")
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
EXPRESSION_OPERATORS = {
    "and",
    "bitand",
    "bitnot",
    "bitor",
    "bitxor",
    "follows",
    "like",
    "not",
    "or",
    "precedes",
    "xor",
}
# Aggregation qualifiers only exist in chart expressions, so in a LOAD they
# are column names.
EXPRESSION_KEYWORDS = EXPRESSION_OPERATORS | {"all", "distinct", "nodistinct", "total"}
# A field named by a variable, as in amount AS $(vName), which LOAD_ALIAS_RE
# does not read as a name.
VARIABLE_ALIAS_RE = re.compile(r"\s+AS\s+\$\(", re.IGNORECASE)
# Single-quoted text is a string literal in a chart expression, not a field.
# A quoted field can carry a derived-field suffix, as in [Date].autoCalendar.Year.
EXPRESSION_NAME_RE = re.compile(
    rf"\[(?P<bracket>[^\]]+)\](?P<bracket_suffix>(?:\.\w+)*)"
    rf"|\"(?P<double>[^\"]+)\"(?P<double_suffix>(?:\.\w+)*)"
    rf"|'[^']*'|(?P<bare>{_BARE_FIELD_NAME})"
)
# A driver or provider names the engine, whatever the rest of the string says.
DRIVER_RE = re.compile(
    r"(?:^|[;\s\[\"'])(?:driver|provider)\s*=\s*(?P<value>\{[^}]*\}|[^;\]\"']*)",
    re.IGNORECASE,
)
CONNECTION_PROPERTY_RE = re.compile(
    r"(?P<key>[^;=]*)=(?P<value>\s*\{[^}]*\}|\s*\"[^\"]*\"|\s*'[^']*'|[^;]*)"
)
# A quoted or braced value can hold its own property list, such as
# Extended Properties="UID=a;PWD=b".
NESTED_VALUE_RE = re.compile(
    r"\{(?P<braced>.*=.*)\}|\"(?P<double>.*=.*)\"|'(?P<single>.*=.*)'", re.DOTALL
)
URL_USERINFO_RE = re.compile(r"(?<=://)[^/@\s;\"']+@")
# A query string or fragment can carry a token or signature, as in an Azure SAS URL.
URL_QUERY_RE = re.compile(r"(?<=://)(?P<location>[^?#\s;\"']*)[?#][^\s;\"']*")
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


class LoadField(BaseModel):
    name: str
    # Names the field's expression reads from the statement below it.
    sources: List[str]


class PrecedingLoad(BaseModel):
    # LOAD * passes every field of the statement below through.
    keeps_all: bool = False
    fields: List[LoadField] = []
    # A $(variable) left in the field list may hold any fields.
    unreadable: bool = False


class ScriptTable(BaseModel):
    label: str
    connection: Optional[str] = None
    sql: str
    # The LOAD statements stacked on the SQL, top first.
    preceding_loads: List[PrecedingLoad] = []


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
        elif script.startswith("//", i) and (
            expression
            or not (kept and (kept[-1] == ":" or kept[-1].isalnum() or kept[-1] == "_"))
        ):
            end = script.find("\n", i)
            i = len(script) if end == -1 else end
            continue
        kept.append(char)
        if not closing and char == STATEMENT_SEPARATOR:
            statement_start = True
            control_prefix = True
            line_begin = len(kept)
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


class _Script(BaseModel):
    pieces: List[_Piece] = []
    # SUB bodies by lowercased name. They only run when called.
    subs: Dict[str, List[_Piece]] = {}


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
                name = SUB_NAME_RE.match(control.group("sub"))
                body: List[_Piece] = []
                parsed.subs[name.group("name").lower() if name else ""] = body
                defining.append(body)
            elif control and control.group("endsub") and defining:
                defining.pop()
            else:
                (defining[-1] if defining else parsed.pieces).append(piece)
    return parsed


class _ScriptParser(BaseModel):
    subs: Dict[str, List[_Piece]]
    tables: List[ScriptTable] = []
    flow: _ControlFlowTracker = Field(default_factory=_ControlFlowTracker)
    pending_label: Optional[str] = None
    pending_loads: List[PrecedingLoad] = []
    # A SELECT under a preceding LOAD that adds to an unknown table.
    pending_skip: bool = False
    # The table a Concatenate or Join without a target adds to.
    last_label: Optional[str] = None
    # SUBs being run, so a SUB that calls itself is not replayed forever.
    calling: List[str] = []

    def run(self, pieces: List[_Piece]) -> None:
        for piece in pieces:
            if piece.control:
                control = CONTROL_RE.match(piece.text)
                if control:
                    self.flow.apply_control(control)
            else:
                self._statement(piece.text)

    def _forget(self) -> None:
        self.flow.forget_state()
        self.last_label = None
        self.pending_label = None
        self.pending_loads = []
        self.pending_skip = False

    def _call(self, name: str) -> None:
        body = self.subs.get(name.lower())
        if body is None or name.lower() in self.calling:
            # The SUB may come from an include, so it could have set anything.
            self._forget()
            return
        # The body runs with the connection and variables in effect here.
        self.calling.append(name.lower())
        self.run(body)
        self.calling.pop()

    def _statement(self, statement: str) -> None:
        statement, includes = INCLUDE_RE.subn("", statement)
        if includes:
            # The included script is not read, so it could have set anything.
            self._forget()
            statement = statement.strip()
        statement = _expand_variables(statement, self.flow.state.variables)
        if not statement:
            return
        if self.flow.record_variable(statement):
            self.pending_label = None
            self.pending_loads = []
            return
        call = CALL_RE.match(statement)
        if call:
            self.pending_label = None
            self.pending_loads = []
            self._call(call.group("name"))
            return
        connect = CONNECT_RE.match(statement)
        if connect:
            self.flow.state.connection = _first_group(connect).strip()
            self.pending_label = None
            self.pending_loads = []
            return
        if DISCONNECT_RE.match(statement):
            self.flow.state.connection = None
            self.pending_label = None
            self.pending_loads = []
            return

        label: Optional[str] = None
        label_match = LABEL_RE.match(statement)
        if label_match:
            label = _first_group(label_match).strip()
            statement = statement[label_match.end() :].lstrip()
        prefix = PREFIX_RE.match(statement)
        merge = MERGE_PREFIX_RE.search(prefix.group(0)) if prefix else None
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
                # Qlik names an unlabelled table after the statement position.
                table_label = (
                    label or self.pending_label or f"table_{len(self.tables) + 1}"
                )
                self.tables.append(
                    ScriptTable(
                        label=table_label,
                        connection=self.flow.state.connection,
                        sql=sql.group("sql"),
                        preceding_loads=self.pending_loads,
                    )
                )
                self.last_label = table_label
            self.pending_label = None
            self.pending_loads = []
            self.pending_skip = False
        elif LOAD_RE.match(statement) and not LOAD_SOURCE_RE.search(
            QUOTED_RE.sub("", statement)
        ):
            # A preceding LOAD names and shapes the SQL SELECT that follows it.
            # Only the top one of a stack carries the label.
            if not self.pending_loads:
                self.pending_label = label
                self.pending_skip = skip
            self.pending_loads.append(_parse_load_fields(statement))
        else:
            if LOAD_RE.match(statement):
                self.last_label = label
            self.pending_label = None
            self.pending_loads = []
            self.pending_skip = False


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


def _parse_load_fields(statement: str) -> PrecedingLoad:
    match = LOAD_FIELDS_RE.match(statement)
    fields_text = match.group("fields") if match else ""
    # Mask quoted text so a keyword inside a string does not end the field list.
    masked = QUOTED_RE.sub(lambda quoted: " " * len(quoted.group(0)), fields_text)
    clause = LOAD_CLAUSE_RE.search(masked)
    if clause:
        fields_text = fields_text[: clause.start()]
    load = PrecedingLoad()
    for item in _split_top_level(fields_text):
        if item == "*":
            load.keeps_all = True
            continue
        alias = LOAD_ALIAS_RE.match(item)
        field = None if alias else LOAD_FIELD_RE.match(item)
        named = alias or field
        name = (
            named.group("bracket") or named.group("double") or named.group("bare")
            if named
            else None
        )
        # Only a variable standing for whole fields or naming a field may hold
        # any of the columns below. One inside an expression only changes that
        # field's value.
        if (
            _is_whole_expansion(alias.group("expr") if alias else item)
            or (name is not None and "$(" in name)
            or (name is None and VARIABLE_ALIAS_RE.search(item))
        ):
            load.unreadable = True
            if name is not None and "$(" in name:
                continue
        if alias and name is not None:
            load.fields.append(
                LoadField(
                    name=name,
                    sources=expression_names(
                        alias.group("expr"), keywords=EXPRESSION_OPERATORS
                    ),
                )
            )
        # Qlik names an unaliased expression after its text, which no chart
        # or SQL column would match.
        elif field and name is not None:
            load.fields.append(LoadField(name=name, sources=[name]))
    return load


def _is_whole_expansion(text: str) -> bool:
    """Whether a field list item is only a $(...) expansion, as in LOAD
    $(vFields) or $(=Concat(Field, ',')), whose fields are unknown."""
    text = text.strip()
    return text.startswith("$(") and _block_end(text, 2, "(", ")") == len(text)


def apply_preceding_loads(
    preceding_loads: List[PrecedingLoad],
    column_upstreams: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Rename SQL columns to the fields the LOAD statements above them produce,
    carrying each field's upstream columns."""
    for load in reversed(preceding_loads):
        by_name = {
            name.lower(): upstreams for name, upstreams in column_upstreams.items()
        }
        column_upstreams = {
            # A $(variable) may hold any of the columns below, so they are kept.
            **(column_upstreams if load.keeps_all or load.unreadable else {}),
            **{
                field.name: list(
                    dict.fromkeys(
                        upstream
                        for source in field.sources
                        for upstream in by_name.get(source.lower(), [])
                    )
                )
                for field in load.fields
            },
        }
    return column_upstreams


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


def expression_names(
    expression: str, keywords: AbstractSet[str] = EXPRESSION_KEYWORDS
) -> List[str]:
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
            bare.lower() in keywords
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
    value = URL_USERINFO_RE.sub("", value)
    return URL_QUERY_RE.sub(lambda match: match.group("location"), value)


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
