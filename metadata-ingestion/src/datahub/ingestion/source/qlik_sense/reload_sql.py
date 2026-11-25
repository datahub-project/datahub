import re
from typing import Dict, List, Set, Tuple

from pydantic import BaseModel, ConfigDict

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.qlik_sense.data_classes import (
    DATABASE_QRI_PREFIX,
    KNOWN_DATA_PLATFORM_MAPPING,
    ReloadStatement,
)
from datahub.ingestion.source.qlik_sense.script_lineage import ScriptTableLineage
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult

# A reload statement that ran SQL: Qlik keeps the `SQL` prefix when the script
# used one, and a bare `SELECT` or `WITH` is SQL too.
RELOAD_SQL_RE = re.compile(
    r"^\s*(?:SQL\b|(?=(?:SELECT|WITH)\b))(?P<sql>.*)$", re.IGNORECASE | re.DOTALL
)


# Other SQL a reload may have run without the prefix, which is never parsed
# for lineage but may read the tables a label names.
RELOAD_UNPARSED_SQL_RE = re.compile(
    r"^\s*(?:EXEC|EXECUTE|INSERT|MERGE|UPDATE|DELETE)\b", re.IGNORECASE
)


# Reload statements whose tables can be load sources of the app.
READ_QUERY_TYPES = {QueryType.SELECT, QueryType.INSERT}


QUOTE_CHARACTERS = '[]"`'


# A file an app's last reload stored or loaded, as Qlik records it, such as
# `STORE - lib://<space>:DataFiles/orders.qvd;` or `{lib://... (txt)};`.
RELOAD_FILE_RE = re.compile(
    r"^\{?(?P<store>STORE - )?(?P<path>lib://[^{}]+?)\}?;?$", re.IGNORECASE
)


class _ReloadFile(BaseModel):
    path: str
    stored: bool


class _ReloadFiles(BaseModel):
    files: List[_ReloadFile] = []
    # Statements that name a `lib://` path in a form RELOAD_FILE_RE doesn't know.
    unrecognised: List[str] = []


def _without_file_format(path: str) -> str:
    """The path without a trailing file format, such as `(qvd)` or
    `(ooxml, filters(Remove(Row, Pos(Top, 1))))`."""
    depth = 0
    for i in range(len(path) - 1, -1, -1) if path.endswith(")") else []:
        depth += {")": 1, "(": -1}.get(path[i], 0)
        if depth == 0:
            return path[:i].rstrip() if path[:i].endswith(" ") else path
    return path


def _reload_files(statements: List[ReloadStatement]) -> _ReloadFiles:
    result = _ReloadFiles()
    for statement in statements:
        discriminator = statement.discriminator.strip()
        match = RELOAD_FILE_RE.match(discriminator)
        if match:
            file = _ReloadFile(
                path=_without_file_format(match.group("path").strip()),
                stored=bool(match.group("store")),
            )
            if file not in result.files:
                result.files.append(file)
        elif "lib://" in discriminator.lower():
            result.unrecognised.append(discriminator)
    return result


def _label_parts(label: str) -> List[str]:
    return [part.strip().strip(QUOTE_CHARACTERS).lower() for part in label.split(".")]


def _names_identifier(sql: str, name: str) -> bool:
    """Whether lowercased SQL names `name` as a whole identifier, so that
    `orders_archive` doesn't count as naming `orders`."""
    return re.search(rf"(?<![\w$#@]){re.escape(name)}(?![\w$#@])", sql) is not None


# Comments, string literals, quoted identifiers, words and single characters.
SQL_TOKEN_RE = re.compile(
    r"/\*[\s\S]*?(?:\*/|\Z)|--[^\n]*"
    r"|'(?:[^']|'')*'?|\[[^\]]*\]?|\"[^\"]*\"?|`[^`]*`?|[\w$#@]+|\S"
)


# Keywords after which SQL names the tables it reads or writes.
TABLE_CLAUSE_KEYWORDS = {"from", "join", "into", "using", "update"}


# SQL Server allows `INSERT t`, `MERGE t` and `DELETE t` without INTO or FROM.
DML_STATEMENT_KEYWORDS = {"insert", "merge", "delete"}


# CROSS APPLY and OUTER APPLY start a table list again.
APPLY_PREFIX_KEYWORDS = {"cross", "outer"}


# Keywords that end such a list of tables.
TABLE_CLAUSE_END_KEYWORDS = {
    "where",
    "group",
    "order",
    "having",
    "union",
    "intersect",
    "except",
    "minus",
    "on",
    "inner",
    "left",
    "right",
    "full",
    "cross",
    "outer",
    "natural",
    "limit",
    "qualify",
    "window",
    "select",
    "values",
    "set",
    "fetch",
    "offset",
    "when",
    "then",
    "pivot",
    "unpivot",
}


# Keywords that end a table name but not the FROM clause it is in.
JOIN_KEYWORDS = {"on", "inner", "left", "right", "full", "cross", "outer", "natural"}


def _table_clause_names(sql: str) -> Set[str]:
    """The lowercased names in the table lists of SQL, after FROM, JOIN and
    INTO, so that a keyword such as ORDER in ORDER BY doesn't count as naming
    a table called `order`. Aliases and the parts of qualified names are
    included, and so are the names in string literals, which may hold SQL
    run dynamically."""
    names: Set[str] = set()
    in_clause = False
    # Still in a FROM clause after a join's ON condition, where a comma
    # starts another table, as in `FROM a JOIN b ON a.id = b.id, orders c`.
    in_from = False
    # Right after FROM, a comma or a dot, a keyword such as ORDER is a name.
    expects_name = False
    previous = ""
    statement_start = True
    stack: List[Tuple[bool, bool]] = []
    for token in SQL_TOKEN_RE.findall(sql):
        if token.startswith(("/*", "--")):
            continue
        is_word = re.match(r"[\w$#@]", token) is not None
        opens_clause = False
        if token.startswith("'"):
            names |= _table_clause_names(token.strip("'").replace("''", "'"))
        elif token == "(":
            stack.append((in_clause, in_from))
            in_clause = in_from = False
        elif token == ")":
            in_clause, in_from = stack.pop() if stack else (False, False)
        elif token == ";":
            in_clause = in_from = False
        elif token == "," and in_from and not in_clause:
            in_clause = opens_clause = True
        elif token[0] in QUOTE_CHARACTERS:
            if in_clause:
                names.add(token.strip(QUOTE_CHARACTERS).strip().lower())
        elif is_word:
            word = token.lower()
            if (statement_start and word in DML_STATEMENT_KEYWORDS) or (
                previous in DML_STATEMENT_KEYWORDS and word in {"into", "from"}
            ):
                in_clause = opens_clause = True
            elif in_clause and expects_name:
                names.add(word)
            elif word in TABLE_CLAUSE_KEYWORDS or (
                word == "apply" and previous in APPLY_PREFIX_KEYWORDS
            ):
                in_clause = opens_clause = True
            elif word in TABLE_CLAUSE_END_KEYWORDS:
                in_clause = False
                in_from = in_from and word in JOIN_KEYWORDS
            elif in_clause:
                names.add(word)
        if opens_clause:
            in_from = True
        expects_name = opens_clause or (in_clause and token in {",", "."})
        previous = token.lower() if is_word else ""
        statement_start = token == ";"
    return names


def _string_literal_table_names(sql: str) -> Set[str]:
    return {
        name
        for token in SQL_TOKEN_RE.findall(sql)
        if token.startswith("'")
        for name in _table_clause_names(token.strip("'").replace("''", "'"))
    }


def _database_qri_platform(source_qri: str) -> str:
    scheme = source_qri[len(DATABASE_QRI_PREFIX) :].partition("://")[0].lower()
    # A scheme that isn't mapped is compared as is, such as `oracle`, so one
    # that names no DataHub platform matches nothing.
    return KNOWN_DATA_PLATFORM_MAPPING.get(scheme, scheme)


def _urn_names_table(urn: str, source_qri: str, label: str) -> bool:
    """Whether a dataset URN is the database table a lineage graph node, such
    as `qri:db:sqlserver://<hash>#<hash>` labelled `dbo.orders`, names. The
    label leaves out the database, and the URN's case may differ from it."""
    dataset = DatasetUrn.from_string(urn)
    platform = _database_qri_platform(source_qri)
    if dataset.platform != builder.make_data_platform_urn(platform):
        return False
    parts = _label_parts(label)
    return dataset.name.lower().split(".")[-len(parts) :] == parts


class _ReloadSql(BaseModel):
    """A SQL statement of an app's last reload, with the tables it reads."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    upstream_urns: List[str]
    result: SqlParsingResult
    # The tables DataHub holds no schema for, when DataHub was asked.
    urns_without_schema: List[str] = []


class _ParsedReloadSql(BaseModel):
    statements: List[_ReloadSql] = []
    # The names in the table lists of statements that may name a label but
    # were not read, for an unresolved connection, a parse error or dynamic
    # SQL, and the names of tables read without their database or schema.
    skipped_table_names: Set[str] = set()
    # A statement kept a variable Qlik did not expand, so it may name any
    # table.
    unexpanded_variable: bool = False


class _UntracedSources(BaseModel):
    """Database sources of a graph table that the load script doesn't trace
    and whose labels may name the same table, such as `dbo.orders` in two
    databases."""

    name: str
    labels: Dict[str, str]
    # How many of them the script traced to a table any of them may name, so
    # without telling which.
    traced_count: int = 0
    # The tables the script linked, which the reload SQL can't link again.
    script_urns: List[str] = []


def _untraced_source_groups(
    labels: Dict[str, str], script_urns: List[str]
) -> List[_UntracedSources]:
    """The database sources, by graph QRI and label, that the script's tables
    don't trace. A script table traces a source only when it is the one source
    of the graph table that could be it."""
    groups: Dict[Tuple[str, str], Dict[str, str]] = {}
    for qri, label in labels.items():
        key = (_database_qri_platform(qri), _label_parts(label)[-1])
        groups.setdefault(key, {})[qri] = label
    untraced: List[_UntracedSources] = []
    for (_, name), group in groups.items():
        sources = _UntracedSources(
            name=name, labels=dict(group), script_urns=script_urns
        )
        for urn in script_urns:
            named = [
                qri for qri, label in group.items() if _urn_names_table(urn, qri, label)
            ]
            if len(named) == 1:
                sources.labels.pop(named[0], None)
            elif named:
                sources.traced_count += 1
        if len(sources.labels) > sources.traced_count:
            untraced.append(sources)
    return untraced


def _merged_lineage(
    script_table: ScriptTableLineage, reload_table: ScriptTableLineage
) -> ScriptTableLineage:
    upstream_urns = list(script_table.upstream_urns)
    upstream_urns.extend(
        urn for urn in reload_table.upstream_urns if urn not in upstream_urns
    )
    column_upstreams = {
        column: list(upstreams)
        for column, upstreams in script_table.column_upstreams.items()
    }
    # The parser may fold the case of the script's columns, so the reload
    # SQL's field joins the script's column of the same name in any case.
    script_columns = {column.lower(): column for column in column_upstreams}
    for column, upstreams in reload_table.column_upstreams.items():
        merged = column_upstreams.setdefault(
            script_columns.get(column.lower(), column), []
        )
        merged.extend(upstream for upstream in upstreams if upstream not in merged)
    return script_table.model_copy(
        update={"upstream_urns": upstream_urns, "column_upstreams": column_upstreams}
    )
