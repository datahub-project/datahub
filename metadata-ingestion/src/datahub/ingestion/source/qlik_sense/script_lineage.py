import re
from collections import Counter
from typing import AbstractSet, Dict, List, Optional, Set

from pydantic import BaseModel, Field

from datahub.ingestion.source.qlik_sense.file_lineage import FileStorage
from datahub.metadata.urns import DatasetUrn

CONNECT_RE = re.compile(
    r"^(?:(?:LIB|ODBC|OLEDB|CUSTOM)\s+)?CONNECT(?:32|64)?\s+TO\s+"
    r"(?:'(?P<single>[^']*)'|\[(?P<bracket>[^\]]*)\]|\"(?P<double>[^\"]*)\"|(?P<bare>\S+))",
    re.IGNORECASE,
)
DISCONNECT_RE = re.compile(r"^DISCONNECT\b", re.IGNORECASE)
REM_RE = re.compile(r"REM\b", re.IGNORECASE)
# `--` is a line comment only in SQL that Qlik passes to the database.
SQL_STATEMENT_RE = re.compile(r"\b(?:SQL|SELECT|WITH)\b", re.IGNORECASE)
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
    r"|(?P<doloop>DO\b[^\n]*)|(?P<loopend>(?:NEXT|LOOP)\b[^\n]*))\s*",
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
EXPRESSION_NAME_RE = re.compile(r"[^\W\d]\w*")
STRING_LITERAL_RE = re.compile(r"'(?P<single>[^']*)'|\"(?P<double>[^\"]*)\"")
VARIABLE_REFERENCE_RE = re.compile(r"\$\((?P<name>[^()=]+)\)")
LABEL_RE = re.compile(
    r"^(?:\[(?P<bracket>[^\]]+)\]|\"(?P<double>[^\"]+)\"|(?P<bare>[^\W\d][\w.\-]*))\s*:(?!/)"
)
# A prefix's arguments, where a quoted or bracketed name may hold a `)`.
_PREFIX_ARGS = r"\((?:[^()\[\]'\"]|\[[^\]]*\]|'[^']*'|\"[^\"]*\")*\)"
PREFIX_RE = re.compile(
    r"^(?:(?:NoConcatenate|Concatenate|Join|Left|Right|Inner|Outer|Keep|Mapping|Generic"
    r"|Buffer|Hierarchy|HierarchyBelongsTo|IntervalMatch|CrossTable|Replace|Add|Merge|Only"
    rf"|First\s+\d+|Sample\s+[\d.]+)\b\s*(?:{_PREFIX_ARGS})?\s*)*",
    re.IGNORECASE,
)
# Concatenate and Join add to an existing table, named in parentheses or else
# the table loaded last. Keep (X) is not here: it creates a table of its own.
MERGE_PREFIX_RE = re.compile(
    r"\b(?:Concatenate|Join)\b\s*"
    r"(?:\(\s*(?P<target>\[[^\]]*\]|'[^']*'|\"[^\"]*\"|[^)]*?)\s*\))?",
    re.IGNORECASE,
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
_FILE_PATH = r"(?P<path>\[[^\]]*\]|'[^']*'|\"[^\"]*\"|[^\s;(]+)"
_FORMAT_SPEC = r"(?:\s*\((?P<spec>[^)]*)\))?"
# FROM followed by whitespace, so FROM_FIELD is not a file.
FILE_SOURCE_RE = re.compile(rf"\bFROM\s+{_FILE_PATH}{_FORMAT_SPEC}", re.IGNORECASE)
STORE_RE = re.compile(
    rf"^STORE\s+(?P<what>.+?)\s+INTO\s+{_FILE_PATH}{_FORMAT_SPEC}\s*$",
    re.IGNORECASE | re.DOTALL,
)
STORE_FROM_RE = re.compile(r"\bFROM\s+", re.IGNORECASE)
# What may follow a LOAD's file source and its format spec.
LOAD_SOURCE_END_RE = re.compile(
    r"\s*(?:;|$|(?:WHERE|WHILE|GROUP\s+BY|ORDER\s+BY)\b)", re.IGNORECASE
)
# Scripts separate the connection from the path with / or, on Windows, a backslash.
LIB_PATH_RE = re.compile(
    r"^lib://(?P<connection>[^/\\]+)[/\\](?P<path>.+)$", re.IGNORECASE
)
# A file system path, which only legacy mode allows: C:\..., \\server\... or //server/...
ABSOLUTE_PATH_RE = re.compile(r"^(?:[A-Za-z]:[\\/]|\\\\|//)")
QVD_EXTENSION = ".qvd"
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
# Aggregation qualifiers. They are field names when nothing follows them, as
# in Sum(Total).
EXPRESSION_QUALIFIERS = {"all", "distinct", "nodistinct", "total"}
EXPRESSION_KEYWORDS = EXPRESSION_OPERATORS | EXPRESSION_QUALIFIERS
# Only DISTINCT, as in Count(DISTINCT customer_id), is a qualifier in a LOAD;
# the others only exist in chart expressions, so in a LOAD they are names.
LOAD_EXPRESSION_KEYWORDS = EXPRESSION_OPERATORS | {"distinct"}
# A field named by a variable, as in amount AS $(vName), which LOAD_ALIAS_RE
# does not read as a name.
VARIABLE_ALIAS_RE = re.compile(r"\s+AS\s+\$\(", re.IGNORECASE)
QUALIFIER_END_RE = re.compile(r"\s*(?:[),]|$)")
MAPPING_PREFIX_RE = re.compile(r"\bMapping\b", re.IGNORECASE)
# CrossTable and Generic turn columns into attribute and value fields, so the
# field names are not the column names.
RESHAPE_PREFIX_RE = re.compile(r"\b(?:CrossTable|Generic)\b", re.IGNORECASE)
CROSSTABLE_RE = re.compile(r"\bCrossTable\s*\(", re.IGNORECASE)
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
# SQL dialects such as MySQL, Databricks and BigQuery also quote with backticks.
SQL_QUOTE_PAIRS = {**QUOTE_PAIRS, "`": "`"}
SQL_QUOTED_RE = re.compile(r"'[^']*'|\"[^\"]*\"|\[[^\]]*\]|`[^`]*`")
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
    # False for an unaliased expression, which Qlik names after its text. It
    # is not ingested as a field but still takes its place in the field order.
    named: bool = True


class _FieldLineage(BaseModel):
    name: str
    upstreams: List[str]
    # False for an unaliased expression, which a load above can still read
    # by its text but which is not ingested as a field.
    named: bool = True


class CrossTable(BaseModel):
    """CrossTable(attribute, data, qualifiers) keeps the first `qualifiers`
    fields and turns the others into attribute and data fields."""

    attribute: str
    data: str
    qualifiers: int = 1


def _crosstable(prefix: str) -> Optional[CrossTable]:
    match = CROSSTABLE_RE.search(prefix)
    if not match:
        return None
    # A bracketed or quoted field name can hold a `)`, as in [Month)].
    end = _block_end(prefix, match.end(), "(", ")")
    args = [_unquote(arg) for arg in _split_top_level(prefix[match.end() : end - 1])]
    if len(args) == 2:
        return CrossTable(attribute=args[0], data=args[1])
    if len(args) == 3 and args[2].isdigit():
        return CrossTable(attribute=args[0], data=args[1], qualifiers=int(args[2]))
    return None


class PrecedingLoad(BaseModel):
    # LOAD * passes every field of the statement below through.
    keeps_all: bool = False
    fields: List[LoadField] = []
    # A $(variable) left in the field list may hold any fields.
    unreadable: bool = False


class ScriptTable(BaseModel):
    label: str
    # True when the parser made the label up, as the table's name in Qlik
    # could not be read from the statement.
    label_unknown: bool = False
    connection: Optional[str] = None
    sql: str
    # The LOAD statements stacked on the SQL, top first.
    preceding_loads: List[PrecedingLoad] = []
    # A mapping table is only used by ApplyMap and dropped from the data model.
    mapping: bool = False
    # A CrossTable or Generic load, whose fields are not its columns.
    reshaped: bool = False
    # Set when the reshaping is a CrossTable on the statement that makes the
    # table, so its qualifier fields can still be traced.
    crosstable: Optional[CrossTable] = None


class QvdReference(BaseModel):
    """A QVD file a script names: a path in a folder data connection, as in
    `lib://<connection>/<path>`, or, in legacy mode, a file system path."""

    connection: Optional[str] = None
    path: str


class QvdLoad(BaseModel):
    label: str
    file: QvdReference
    # The fields of the topmost LOAD, which names the table.
    fields: PrecedingLoad


class QvdStore(BaseModel):
    label: str
    file: QvdReference
    # The fields of `STORE <fields> FROM <table>`; None stores all the table's.
    fields: Optional[PrecedingLoad] = None


class FileLoad(BaseModel):
    """A file a LOAD reads: its path as the script's own SET and LET
    variables expand it, and as written, for the app's variables from its last
    reload to expand instead."""

    expanded: str
    as_written: str
    # The path uses a variable the script makes unknown, such as a FOR EACH
    # variable over a FileList, so the app's last value names only one file.
    uses_unknown_variable: bool = False


class ParsedScript(BaseModel):
    tables: List[ScriptTable] = []
    qvd_loads: List[QvdLoad] = []
    qvd_stores: List[QvdStore] = []
    # QVD paths that name no single file the connector can resolve, such as a
    # wildcard, a variable it could not expand or a relative path.
    unresolved_qvd_paths: List[str] = []
    # A STORE wrote a QVD whose path can't be resolved, so the QVDs the app
    # writes are not all known.
    has_unresolved_store: bool = False
    # QVDs loaded without a label, which Qlik names itself.
    unlabelled_qvd_loads: List[str] = []
    # Every file a LOAD reads, in any format.
    file_loads: List[FileLoad] = []
    # The script includes other scripts, or calls a SUB it doesn't define,
    # whose loads are not read.
    has_includes: bool = False


class ScriptTableLineage(BaseModel):
    label: str
    identifier: str
    label_unknown: bool = False
    connection: Optional[str] = None
    upstream_urns: List[str] = []
    # Downstream column name to upstream schema field URNs.
    column_upstreams: Dict[str, List[str]] = {}
    mapping: bool = False


class DataConnectionInfo(BaseModel):
    name: str
    platform: Optional[str] = None
    database: Optional[str] = None
    # The folder of a folder data connection, for QVD paths in it.
    folder_path: Optional[str] = None
    # The storage of a Qlik Cloud file data connection, such as Azure Storage.
    file_storage: Optional[FileStorage] = None
    # The platform of its files, known even when its storage is not.
    file_platform: Optional[str] = None


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
            # Quoted text such as 'SELECT' in a LOAD does not make it SQL.
            and SQL_STATEMENT_RE.search(
                QUOTED_RE.sub("", "".join(kept[statement_begin:]))
            )
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
    # Variables the visible script gives values it can't know here, such as a
    # loop variable, so the app's or recipe's single value would be wrong.
    unknown: Set[str] = set()

    def set_variable(self, name: str, value: Optional[str]) -> None:
        self.unknown.discard(name)
        if value is None:
            self.variables.pop(name, None)
        else:
            self.variables[name] = value

    def forget_variable(self, name: str) -> None:
        self.variables.pop(name, None)
        self.unknown.add(name)


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
    # The variables each FOR or DO loop the current statement runs in made
    # unknown, innermost last. After the loop they hold their last value,
    # which is the app's.
    loops: List[Set[str]] = []
    # How often the script assigns each variable, as it runs, and in all. The
    # app keeps a variable's value from its last assignment, which is also
    # its value before the first, as variables persist between reloads.
    assignments: Counter = Field(default_factory=Counter)
    total_assignments: Counter = Field(default_factory=Counter)

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
            self.loops.append(set())
            # The loop variable takes values that are not known here.
            variable = FOR_VARIABLE_RE.match(control.group("forloop"))
            if variable:
                self._forget_variable(variable.group("name"))
        elif control.group("doloop"):
            self.loops.append(set())
        elif control.group("loopend") and self.loops:
            varied = self.loops.pop()
            if self.loops:
                self.loops[-1].update(varied)
            else:
                self.state.unknown.difference_update(
                    name for name in varied if self._assigned_for_the_last_time(name)
                )

    def _assigned_for_the_last_time(self, name: str) -> bool:
        return self.assignments[name] >= self.total_assignments[name]

    def _forget_variable(self, name: str) -> None:
        self.state.forget_variable(name)
        if self.loops:
            self.loops[-1].add(name)

    def forget_state(self) -> None:
        # An included script can't make a loop variable or a disagreeing
        # branch value known again.
        self.state = _BranchState(unknown=self.state.unknown)

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
        variables = {
            name: value
            for name, value in first.variables.items()
            if all(other.variables.get(name) == value for other in rest)
        }
        set_anywhere = {name for outcome in outcomes for name in outcome.variables}
        if self.loops:
            # Like a variable set in the loop, it holds the app's value after.
            self.loops[-1].update(set_anywhere - variables.keys())
        self.state = _BranchState(
            connection=first.connection
            if all(other.connection == first.connection for other in rest)
            else None,
            variables=variables,
            unknown=set().union(*(outcome.unknown for outcome in outcomes))
            | (set_anywhere - variables.keys()),
        )

    def record_variable(self, statement: str, in_sub: bool = False) -> bool:
        match = VARIABLE_RE.match(statement)
        if not match:
            return False
        value = match.group("value").strip()
        literal = STRING_LITERAL_RE.fullmatch(value)
        name = match.group("name")
        is_let = match.group("kind").upper() == "LET"
        self.assignments[name] += 1
        if self._reads_unknown(value, is_let and literal is None):
            # Such as SET vPath = $(vFile) in a loop over files: a copy of an
            # unknown value is unknown too.
            self._forget_variable(name)
        elif literal:
            self.state.set_variable(match.group("name"), _first_group(literal))
        elif match.group("kind").upper() == "SET":
            self.state.set_variable(match.group("name"), value)
        elif (
            self.loops
            or in_sub
            or self.blocks
            or not self._assigned_for_the_last_time(name)
        ):
            # Run more than once, in a branch that may not have run, or set
            # again later, so the value the app's last reload left need not be
            # this one.
            self._forget_variable(name)
        else:
            # LET evaluates an expression the parser can't, so the value the
            # app's last reload left, if any, stands in for it.
            self.state.set_variable(match.group("name"), None)
        return True

    def _reads_unknown(self, value: str, is_expression: bool) -> bool:
        names = {
            reference.group("name").strip()
            for reference in VARIABLE_REFERENCE_RE.finditer(value)
        }
        if is_expression:
            # A LET expression can read a variable by its bare name.
            names.update(EXPRESSION_NAME_RE.findall(STRING_LITERAL_RE.sub("", value)))
        return not names.isdisjoint(self.state.unknown)


# How many levels of variables inside variables a path is expanded through.
_MAX_VARIABLE_DEPTH = 5


def _expand_variables(statement: str, variables: Dict[str, str]) -> str:
    return VARIABLE_REFERENCE_RE.sub(
        lambda reference: variables.get(
            reference.group("name").strip(), reference.group(0)
        ),
        statement,
    )


def unquoted_values(variables: Dict[str, str]) -> Dict[str, str]:
    """A variable set with SET keeps the quotes around its value, which a path
    built from it, such as $(vFolder)orders.qvd, must not hold. A value that
    starts with `=` is an expression Qlik evaluates, so it is left out."""
    unquoted = {}
    for name, value in variables.items():
        value = value.strip()
        if value.startswith("="):
            continue
        if len(value) > 1 and value[0] in ("'", '"') and value[-1] == value[0]:
            value = value[1:-1]
        unquoted[name] = value
    return unquoted


def _expand_nested_variables(text: str, variables: Dict[str, str]) -> str:
    """Expands variables whose values hold other variables, up to a depth."""
    for _ in range(_MAX_VARIABLE_DEPTH):
        expanded = _expand_variables(text, variables)
        if expanded == text:
            break
        text = expanded
    return text


def _load_source(statement: str) -> Optional["re.Match[str]"]:
    """The FROM that names a LOAD's file. FROM is found with quoted text masked,
    so a quoted field or path can't hold it. A field named From, as in
    `LOAD From AS Start ... FROM [lib://...]`, is skipped, as its "path" isn't
    followed by the end of the LOAD or one of its clauses."""
    masked = QUOTED_RE.sub(lambda quoted: " " * len(quoted.group(0)), statement)
    for keyword in STORE_FROM_RE.finditer(masked):
        source = FILE_SOURCE_RE.match(statement, keyword.start())
        if source and LOAD_SOURCE_END_RE.match(masked, source.end()):
            return source
    return None


def _load_path(statement: str) -> Optional[str]:
    source = _load_source(statement)
    return _unquote(source.group("path").strip()).strip() if source else None


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


def _split_top_level(text: str, quote_pairs: Dict[str, str] = QUOTE_PAIRS) -> List[str]:
    items: List[str] = []
    depth = 0
    start = 0
    closing: Optional[str] = None
    for index, char in enumerate(text):
        if closing:
            if char == closing:
                closing = None
        elif char in quote_pairs:
            closing = quote_pairs[char]
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


SELECT_LIST_RE = re.compile(
    r"^\s*SELECT\s+(?:(?:DISTINCT|ALL)\s+)?(?:TOP\s+\(?\d+\)?\s+)?",
    re.IGNORECASE,
)
TOP_LEVEL_FROM_RE = re.compile(r"\bFROM\b", re.IGNORECASE)


def select_list_size(sql: str) -> Optional[int]:
    """How many columns a plain SELECT lists, None if a column is a * or the
    list cannot be found."""
    select = SELECT_LIST_RE.match(sql)
    if not select:
        return None
    masked = SQL_QUOTED_RE.sub(lambda quoted: " " * len(quoted.group(0)), sql)
    end = next(
        (
            match.start()
            for match in TOP_LEVEL_FROM_RE.finditer(masked, select.end())
            if masked.count("(", 0, match.start())
            == masked.count(")", 0, match.start())
        ),
        len(sql),
    )
    items = _split_top_level(sql[select.end() : end], SQL_QUOTE_PAIRS)
    if any(not item or item.endswith("*") for item in items):
        return None
    return len(items)


def _from_table_name(sql: str) -> Optional[str]:
    # FROM in a string literal or a nested query does not name the table.
    masked = STRING_LITERAL_SQL_RE.sub(lambda literal: " " * len(literal.group(0)), sql)
    match = next(
        (
            match
            for match in FROM_TABLE_RE.finditer(masked)
            if masked.count("(", 0, match.start())
            == masked.count(")", 0, match.start())
        ),
        None,
    )
    # A name cut short by an unexpanded $(variable) is not the table's name.
    if not match or masked.startswith("(", match.end()):
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
    # The app's variables from the Engine, for paths the script builds from
    # variables it can't evaluate.
    app_variables: Dict[str, str] = {}
    tables: List[ScriptTable] = []
    qvd_loads: List[QvdLoad] = []
    qvd_stores: List[QvdStore] = []
    unresolved_qvd_paths: List[str] = []
    has_unresolved_store: bool = False
    unlabelled_qvd_loads: List[str] = []
    file_loads: List[FileLoad] = []
    has_includes: bool = False
    # The current statement before the script's variables are expanded.
    raw_statement: str = ""
    flow: _ControlFlowTracker = Field(default_factory=_ControlFlowTracker)
    pending_label: Optional[str] = None
    pending_loads: List[PrecedingLoad] = []
    pending_mapping: bool = False
    pending_reshaped: bool = False
    pending_crosstable: Optional[CrossTable] = None
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
                # A preceding LOAD only stacks on the statement right below it.
                self._clear_pending()
                control = CONTROL_RE.match(piece.text)
                if control:
                    self.flow.apply_control(control)
            else:
                self._statement(piece.text)

    def _clear_pending(self) -> None:
        self.pending_mapping = False
        self.pending_reshaped = False
        self.pending_crosstable = None
        self.pending_label = None
        self.pending_loads = []
        self.pending_skip = False

    def _forget(self) -> None:
        self.flow.forget_state()
        self.last_label = None
        self._clear_pending()

    def _call(self, name: str, args: Optional[str]) -> None:
        sub = self.subs.get(name.lower())
        if sub is None or name.lower() in self.calling:
            # The SUB may come from an include, so it could have set or loaded
            # anything.
            self.has_includes = self.has_includes or sub is None
            self._forget()
            return
        # The body runs with the connection and variables in effect here, and
        # its parameters hold the arguments only for the call.
        state = self.flow.state
        saved = {
            param: (state.variables.get(param), param in state.unknown)
            for param in sub.params
        }
        values = _split_top_level(args) if args else []
        for index, param in enumerate(sub.params):
            value = _literal_value(values[index]) if index < len(values) else None
            if value is None:
                state.forget_variable(param)
            else:
                state.set_variable(param, value)
        self.calling.append(name.lower())
        self.run(sub.body)
        self.calling.pop()
        state = self.flow.state
        for param, (value, unknown) in saved.items():
            if unknown:
                state.forget_variable(param)
            else:
                state.set_variable(param, value)

    def _statement(self, statement: str) -> None:
        statement, includes = INCLUDE_RE.subn("", statement)
        if includes:
            # The included script is not read, so it could have set anything.
            self.has_includes = True
            self._forget()
            statement = statement.strip()
        self.raw_statement = statement
        # Variables the visible script sets take precedence from where it sets
        # them; others come from the app's last reload or the recipe.
        # An app value can hold another variable, such as a table name built
        # from a schema prefix.
        statement = _expand_nested_variables(
            statement, {**self._fallback_variables(), **self.flow.state.variables}
        )
        if not statement:
            return
        if EXIT_SCRIPT_RE.match(statement) and not self.flow.blocks:
            self.exited = True
            return
        if self.flow.record_variable(statement, in_sub=bool(self.calling)):
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
            self._clear_pending()
            return
        if DISCONNECT_RE.match(statement):
            self.flow.state.connection = None
            self._clear_pending()
            return
        store = STORE_RE.match(statement)
        if store:
            self._store(store)
            self._clear_pending()
            return

        label: Optional[str] = None
        label_match = LABEL_RE.match(statement)
        if label_match:
            label = _first_group(label_match).strip()
            statement = statement[label_match.end() :].lstrip()
        prefix = PREFIX_RE.match(statement)
        merge = MERGE_PREFIX_RE.search(prefix.group(0)) if prefix else None
        # Only the prefix keywords, not arguments such as Concatenate ([Mapping]).
        keywords = re.sub(_PREFIX_ARGS, "", prefix.group(0)) if prefix else ""
        mapping = bool(MAPPING_PREFIX_RE.search(keywords))
        reshaped = bool(RESHAPE_PREFIX_RE.search(keywords))
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
                if not self.pending_loads:
                    crosstable = _crosstable(prefix.group(0) if prefix else "")
                else:
                    # A reshaping under the preceding loads hides the fields.
                    crosstable = None if reshaped else self.pending_crosstable
                known_label = (
                    label or self.pending_label or _from_table_name(sql.group("sql"))
                )
                table_label = known_label or f"table_{len(self.tables) + 1}"
                self.tables.append(
                    ScriptTable(
                        label=table_label,
                        label_unknown=known_label is None,
                        connection=self.flow.state.connection,
                        sql=sql.group("sql"),
                        preceding_loads=self.pending_loads,
                        mapping=mapping or self.pending_mapping,
                        reshaped=reshaped or self.pending_reshaped,
                        crosstable=crosstable,
                    )
                )
                self.last_label = table_label
            self._clear_pending()
        elif LOAD_RE.match(statement) and not LOAD_SOURCE_RE.search(
            QUOTED_RE.sub("", statement)
        ):
            # A preceding LOAD names and shapes the SQL SELECT that follows it.
            # Only the top one of a stack carries the label.
            if not self.pending_loads:
                self.pending_label = label
                self.pending_skip = skip
                self.pending_mapping = mapping
                self.pending_crosstable = _crosstable(prefix.group(0) if prefix else "")
            elif reshaped:
                # Reshaped below the top, so the top's fields are not known.
                self.pending_crosstable = None
            self.pending_reshaped = self.pending_reshaped or reshaped
            self.pending_loads.append(_parse_load_fields(statement))
        else:
            if LOAD_RE.match(statement):
                # Every file a LOAD reads is in Qlik's lineage graph, mapping
                # loads included.
                self._record_file_load(statement)
                if not (skip or mapping or self.pending_skip or self.pending_mapping):
                    self._qvd_load(label or self.pending_label, statement)
                # A preceding LOAD's label names the table a later Concatenate
                # or Join without a target adds to.
                self.last_label = label or self.pending_label
            self._clear_pending()

    def _fallback_variables(self) -> Dict[str, str]:
        # Unquoted, as the script's own quoted SET values are, and without
        # expressions (`=...`) Qlik evaluates.
        unknown = self.flow.state.unknown
        return unquoted_values(
            {
                name: value
                for name, value in self.app_variables.items()
                if name not in unknown
            }
        )

    def _expand_app_variables(self, path: str) -> str:
        """A path with the app's variables as its last reload left them, which
        include those set with LET or in an included script that the parser
        can't evaluate. A variable can hold another, such as a folder built
        from a connection prefix."""
        return _expand_nested_variables(path, self._fallback_variables())

    def _qvd_file(
        self, path: str, spec: Optional[str], default_qvd: bool
    ) -> Optional[QvdReference]:
        """The QVD file a LOAD or STORE names, or None when it names another
        format or no single resolvable file."""
        path = _unquote(path.strip()).strip()
        if "$(" in path:
            # Expanded first, as the variable may hold the `.qvd` name.
            path = self._expand_app_variables(path)
        format_name = (spec or "").split(",")[0].strip().lower()
        if format_name:
            is_qvd = format_name == "qvd"
        else:
            # LOAD assumes txt and STORE assumes qvd when the format is omitted.
            is_qvd = default_qvd or path.lower().endswith(QVD_EXTENSION)
        if not is_qvd:
            return None
        if any(
            reference.group("name").strip() in self.flow.state.unknown
            for reference in VARIABLE_REFERENCE_RE.finditer(path)
        ):
            # Such as a connection prefix that varies in a loop: it is not an
            # unset prefix that expands to nothing.
            self.unresolved_qvd_paths.append(path)
            return None
        lib_path = LIB_PATH_RE.match(path)
        if (
            lib_path
            and "$(" not in lib_path.group("path")
            and "*" not in path
            and "?" not in path
        ):
            # A variable left in the connection name is resolved against the
            # folder connections, as one Qlik expands to nothing.
            return QvdReference(
                connection=lib_path.group("connection"),
                path=lib_path.group("path"),
            )
        if "$(" in path or "*" in path or "?" in path:
            self.unresolved_qvd_paths.append(path)
            return None
        if ABSOLUTE_PATH_RE.match(path):
            return QvdReference(path=path)
        # Relative to the Directory statement or the working directory.
        self.unresolved_qvd_paths.append(path)
        return None

    def _record_file_load(self, statement: str) -> None:
        expanded = _load_path(statement)
        if expanded is None:
            return
        as_written = _load_path(self.raw_statement) or expanded
        load = FileLoad(
            expanded=expanded,
            as_written=as_written,
            uses_unknown_variable=any(
                reference.group("name").strip() in self.flow.state.unknown
                for reference in VARIABLE_REFERENCE_RE.finditer(as_written)
            ),
        )
        if load not in self.file_loads:
            self.file_loads.append(load)

    def _qvd_load(self, label: Optional[str], statement: str) -> None:
        source = _load_source(statement)
        if source is None:
            return
        file = self._qvd_file(source.group("path"), source.group("spec"), False)
        # Without a label the table is named by Qlik, which a STORE can't be
        # matched against reliably.
        if file is None:
            return
        if not label:
            self.unlabelled_qvd_loads.append(
                f"lib://{file.connection}/{file.path}" if file.connection else file.path
            )
            return
        # The file's columns go through the statement's own field list and
        # then every preceding load above it.
        fields = _parse_load_fields(statement[: source.start()])
        for load in reversed(self.pending_loads):
            fields = _stack_load(load, fields)
        self.qvd_loads.append(QvdLoad(label=label, file=file, fields=fields))

    def _store(self, store: "re.Match[str]") -> None:
        unresolved = len(self.unresolved_qvd_paths)
        file = self._qvd_file(store.group("path"), store.group("spec"), True)
        if file is None:
            self.has_unresolved_store |= len(self.unresolved_qvd_paths) > unresolved
            return
        what = store.group("what")
        masked = QUOTED_RE.sub(lambda quoted: " " * len(quoted.group(0)), what)
        # The last FROM, as a field list may itself hold one in an expression.
        from_table = next(reversed(list(STORE_FROM_RE.finditer(masked))), None)
        table = what[from_table.end() :] if from_table else what
        fields = (
            _parse_load_fields(f"LOAD {what[: from_table.start()]}")
            if from_table
            else None
        )
        self.qvd_stores.append(
            QvdStore(
                label=_unquote(table.strip()),
                file=file,
                fields=None if fields is not None and fields.keeps_all else fields,
            )
        )


def parse_script(
    script: str, app_variables: Optional[Dict[str, str]] = None
) -> ParsedScript:
    parsed = _script_pieces(script)
    # A first pass counts the assignments the script runs, leaving out
    # comments, SUBs it never calls and anything after EXIT SCRIPT.
    first = _ScriptParser(subs=parsed.subs, app_variables=app_variables or {})
    first.run(parsed.pieces)
    parser = _ScriptParser(subs=parsed.subs, app_variables=app_variables or {})
    parser.flow.total_assignments = first.flow.assignments
    parser.run(parsed.pieces)
    return ParsedScript(
        tables=parser.tables,
        qvd_loads=parser.qvd_loads,
        qvd_stores=parser.qvd_stores,
        unresolved_qvd_paths=parser.unresolved_qvd_paths,
        has_unresolved_store=parser.has_unresolved_store,
        unlabelled_qvd_loads=parser.unlabelled_qvd_loads,
        file_loads=parser.file_loads,
        has_includes=parser.has_includes,
    )


def parse_load_script(script: str) -> List[ScriptTable]:
    return parse_script(script).tables


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


def _stack_load(upper: PrecedingLoad, lower: PrecedingLoad) -> PrecedingLoad:
    """One load equivalent to `upper` reading the fields `lower` produces, so
    each field's sources are names the lower load reads."""
    if upper.unreadable or lower.unreadable:
        return PrecedingLoad(unreadable=True)
    lower_sources = {field.name: field.sources for field in lower.fields}

    def sources(name: str) -> List[str]:
        if name in lower_sources:
            return lower_sources[name]
        # LOAD * passes the names below it through unchanged.
        return [name] if lower.keeps_all else []

    fields = [
        LoadField(
            name=field.name,
            named=field.named,
            sources=list(
                dict.fromkeys(
                    source for name in field.sources for source in sources(name)
                )
            ),
        )
        for field in upper.fields
    ]
    if upper.keeps_all:
        own = {field.name for field in fields}
        fields = [field for field in lower.fields if field.name not in own] + fields
    return PrecedingLoad(keeps_all=upper.keeps_all and lower.keeps_all, fields=fields)


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
                        alias.group("expr"), keywords=LOAD_EXPRESSION_KEYWORDS
                    ),
                )
            )
        # Qlik names an unaliased expression after its text, which no chart
        # or SQL column would match.
        elif field and name is not None:
            load.fields.append(LoadField(name=name, sources=[name]))
        elif not _is_whole_expansion(item):
            load.fields.append(
                LoadField(
                    name=item,
                    sources=expression_names(item, keywords=LOAD_EXPRESSION_KEYWORDS),
                    named=False,
                )
            )
    return load


def _is_whole_expansion(text: str) -> bool:
    """Whether a field list item is only a $(...) expansion, as in LOAD
    $(vFields) or $(=Concat(Field, ',')), whose fields are unknown."""
    text = text.strip()
    if not text.startswith("$("):
        return False
    # Expansions joined together, as in $(vKeys)$(vMeasures), are one item.
    while text.startswith("$("):
        text = text[_block_end(text, 2, "(", ")") :].strip()
    return not text


def apply_preceding_loads(
    preceding_loads: List[PrecedingLoad],
    column_upstreams: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Rename SQL columns to the fields the LOAD statements above them produce,
    carrying each field's upstream columns."""
    return {
        field.name: field.upstreams
        for field in _compose_preceding_loads(preceding_loads, column_upstreams)
        if field.named
    }


def _compose_preceding_loads(
    preceding_loads: List[PrecedingLoad],
    column_upstreams: Dict[str, List[str]],
) -> List[_FieldLineage]:
    fields = [
        _FieldLineage(name=name, upstreams=upstreams)
        for name, upstreams in column_upstreams.items()
    ]
    for load in reversed(preceding_loads):
        fields = _load_field_lineage(load, fields)
    return fields


def _load_field_lineage(
    load: PrecedingLoad, below: List[_FieldLineage]
) -> List[_FieldLineage]:
    """The fields a LOAD produces from the fields below it, in order."""
    # A $(variable) may hold any of the fields below, so they are kept.
    kept = list(below) if load.keeps_all or load.unreadable else []
    upstreams_by_name = {field.name: field.upstreams for field in below}
    return kept + [
        _FieldLineage(
            name=field.name,
            named=field.named,
            upstreams=list(
                dict.fromkeys(
                    upstream
                    for source in field.sources
                    for upstream in _column_upstreams(upstreams_by_name, source)
                )
            ),
        )
        for field in load.fields
    ]


def apply_crosstable(
    crosstable: Optional[CrossTable],
    preceding_loads: List[PrecedingLoad],
    sql_columns: Dict[str, List[str]],
    sql_columns_in_order: bool = True,
) -> Optional[Dict[str, List[str]]]:
    """The fields a CrossTable load produces: its qualifier fields keep their
    upstreams, the attribute field holds column names so it has none, and the
    data field holds the values of the other fields. An unaliased expression
    keeps its place in the field order but is not ingested as a field. None
    when the fields or their order are not known."""
    if crosstable is None:
        return None
    if not preceding_loads:
        # The SELECT's columns are the slots, so their order must be known.
        if not sql_columns_in_order:
            return None
        slots = [
            _FieldLineage(name=name, upstreams=upstreams)
            for name, upstreams in sql_columns.items()
        ]
    else:
        top = preceding_loads[0]
        if top.keeps_all or top.unreadable:
            return None
        slots = _load_field_lineage(
            top, _compose_preceding_loads(preceding_loads[1:], sql_columns)
        )
    if len(slots) <= crosstable.qualifiers:
        return None
    return {
        **{
            slot.name: slot.upstreams
            for slot in slots[: crosstable.qualifiers]
            if slot.named
        },
        crosstable.attribute: [],
        crosstable.data: list(
            dict.fromkeys(
                upstream
                for slot in slots[crosstable.qualifiers :]
                for upstream in slot.upstreams
            )
        ),
    }


def _column_upstreams(columns: Dict[str, List[str]], name: str) -> List[str]:
    """The upstreams of a column named exactly, or else in any case, since the
    SQL parser may change its case. Columns that differ only in case make a
    name in another case ambiguous, so it gets none."""
    if name in columns:
        return columns[name]
    matches = [column for column in columns if column.lower() == name.lower()]
    return columns[matches[0]] if len(matches) == 1 else []


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


_VARIABLE_EXPANSION_RE = re.compile(r"\$\(([^()=$]+)\)")


def expand_variables(expression: str, variables: Dict[str, str]) -> str:
    """Expands $(name) with the variable's definition, as Qlik does when the
    definition is plain text. A definition starting with = expands to its
    calculated value, and an unknown or parameterised variable is left as is."""
    for _ in range(10):  # Bounded, as a variable can expand to itself.

        def definition(match: "re.Match[str]") -> str:
            value = variables.get(match.group(1).strip())
            return (
                match.group(0)
                if value is None or value.lstrip().startswith("=")
                else value
            )

        expanded = _VARIABLE_EXPANSION_RE.sub(definition, expression)
        if expanded == expression:
            break
        expression = expanded
    return expression


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
            (
                bare.lower() in keywords
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


def app_table_identifier(app_id: str, table_name: str) -> str:
    """The dataset name of an app's table, however the table was found: its
    URN must not change between a script, data model and lineage graph read."""
    return f"{app_id}.{table_name}".lower()


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
