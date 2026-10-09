"""The scope gate for `probe run sql`: one SELECT that reads only catalog relations.

Fail-closed at every step. Refused: an unresolvable dialect or unparseable query;
a write anywhere in the tree; a vendor function; server or session state; a
withheld column name, an alias column list or a NATURAL JOIN; a reference outside
the connector's CatalogScope; and a query that reads no relation at all.

The gate bounds which relations a query may name, and the credential's grants
bound what those relations show. A query that escapes the gate is a security bug.
"""

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.sql_parsing.sqlglot_utils import get_dialect

# The one schema that is catalog metadata by definition, in every dialect that has
# it. Everything beyond this is the connector's to declare.
INFORMATION_SCHEMA = "information_schema"

# The MySQL protocol's information_schema views holding other sessions' SQL
# text (PROCESSLIST.INFO, INNODB_TRX.trx_query), WHERE-clause literals and
# IDENTIFIED BY passwords included. Withheld by default, so a scope admitting
# information_schema whole cannot forget them; where they do not exist,
# withholding them changes nothing.
SESSION_TEXT_RELATIONS: FrozenSet[str] = frozenset({"processlist", "innodb_trx"})


@dataclass(frozen=True)
class CatalogScope:
    """What one dialect considers catalog metadata, declared per connector
    (SQLCommonConfig.probe_catalog_scope, or a provider's catalog_scope).

    Prefer `relations` over `schemas`: a vendor catalog schema is rarely wholly
    metadata (query logs carry WHERE-clause literals), and a whole-schema allow
    with exclusions is a denylist that admits the next such view by default.

    A ceiling, not the bound: the recipe's credential decides what an admitted
    relation reveals. Even `information_schema` can hold other sessions' SQL
    text, so the default withholds SESSION_TEXT_RELATIONS, and a connector whose
    dialect keeps more there adds them to `excluded_relations`.
    """

    # Whole schemas whose every relation is metadata by definition.
    schemas: FrozenSet[str] = field(
        default_factory=lambda: frozenset({INFORMATION_SCHEMA})
    )

    # Individually permitted relations, for a schema that is not wholly safe.
    # "schema.relation", or a bare name where the dialect exposes the relation
    # unqualified (Oracle's dictionary views are public synonyms).
    relations: FrozenSet[str] = field(default_factory=frozenset)

    # Relations refused inside a permitted schema; sound only where the schema
    # is metadata apart from a known few. A scope that sets its own replaces
    # the default, so it keeps SESSION_TEXT_RELATIONS by naming them too.
    excluded_relations: FrozenSet[str] = field(
        default_factory=lambda: SESSION_TEXT_RELATIONS
    )

    # Split an identifier slot holding dots into path parts. Some parsers leave
    # a path's dots inside one slot (`ds.INFORMATION_SCHEMA.TABLES` with the
    # name slot `INFORMATION_SCHEMA.TABLES`), and such a reference matches
    # nothing until it is split. Safe only where an identifier cannot contain a
    # dot: elsewhere a quoted user table named "information_schema.tables"
    # would split into a permitted path. False keeps every slot whole, so such
    # a reference is refused rather than misread.
    split_dotted_identifiers: bool = False

    def permits_path(self, parts: List[str]) -> bool:
        """Whether a reference, given as its dotted path parts, is in scope.

        A `relations` entry matches the reference's suffix, so it pins as much of
        the path as it needs: three parts pin the catalog too, the only way to
        tell a system schema from a user schema of the same name. Bare entries
        are for permits_unqualified only; suffix-matching them would admit a user
        table named like a dictionary view under any schema.
        """
        schema, relation = parts[-2], parts[-1]
        if schema.lower() in {s.lower() for s in self.schemas}:
            return relation.lower() not in {r.lower() for r in self.excluded_relations}
        lowered = [part.lower() for part in parts]
        for entry in self.relations:
            entry_parts = entry.lower().split(".")
            if len(entry_parts) < 2 or len(entry_parts) > len(lowered):
                # Bare, or longer than the reference: cannot be shown to match.
                continue
            if lowered[-len(entry_parts) :] == entry_parts:
                return True
        return False

    def permits(self, schema: str, relation: str) -> bool:
        return self.permits_path([schema, relation])

    def permits_unqualified(self, relation: str) -> bool:
        return relation.lower() in {r.lower() for r in self.relations if "." not in r}

    def describe(self) -> str:
        """What to tell a caller whose reference was refused."""
        parts = [f"schemas {sorted(self.schemas)}"] if self.schemas else []
        if self.relations:
            parts.append(f"{len(self.relations)} individually listed relations")
        return " and ".join(parts) or "nothing"


_DEFAULT_SCOPE = CatalogScope()

# sqlglot leaves vendor functions as exp.Anonymous, and every known way to read
# data without naming a table is one (pg_read_file, dblink, SYSTEM$...,
# EXTERNAL_QUERY). Refusing the class is fail-closed where a denylist of names
# could never be complete. Add a function only once it is shown to return
# catalog metadata and to read no user rows or host files.
_ALLOWED_VENDOR_FUNCTIONS: FrozenSet[str] = frozenset()


# A refusal names the SQL keyword: sqlglot's node name can mislead (FLUSH
# PRIVILEGES parses to an Alias).
_STATEMENT_KEYWORDS: Dict[type, str] = {
    node: keyword
    for node, keyword in (
        (getattr(exp, name, None), keyword)
        for name, keyword in (
            ("Insert", "INSERT"),
            ("Update", "UPDATE"),
            ("Delete", "DELETE"),
            ("Drop", "DROP"),
            ("Create", "CREATE"),
            ("Alter", "ALTER"),
            ("Merge", "MERGE"),
            # `SELECT ... INTO tbl` creates a table (MSSQL, Postgres); sqlglot
            # models it as an arg of the Select, not as a Create or Insert.
            ("Into", "SELECT ... INTO"),
            # `FOR UPDATE` / `FOR SHARE` holds row locks that can block a
            # writer; also an arg of the Select.
            ("Lock", "row-locking SELECT"),
        )
    )
    if node is not None
}


# The same node types for a walk of the whole tree, plus Command: a nested
# statement sqlglot cannot model is as unclearable as a top-level one.
_WRITE_NODES: Tuple[type, ...] = tuple(_STATEMENT_KEYWORDS) + (exp.Command,)


class SqlScopeError(ProbeArgumentError):
    """A query refused as not a read of catalog metadata: the caller's to fix
    (exit 2). The gate bounds what a query may name; the database's grants bound
    what it can see (see probe_interface.md)."""


def check_query_scope(
    sql: str, platform: str, scope: Optional[CatalogScope] = None
) -> None:
    """Raise SqlScopeError unless `sql` is a single SELECT over catalog metadata.

    `scope` is the connector's declared catalog; absent one, `information_schema`
    only. Every doubt is a refusal, never a warning or a guess.
    """
    permitted = scope or _DEFAULT_SCOPE
    dialect = _resolve_dialect(platform)
    _refuse_comments(sql, dialect)
    statement = _parse_single_statement(sql, dialect, platform)

    if isinstance(statement, exp.Command):
        # A statement sqlglot does not model: what it touches cannot be seen.
        raise SqlScopeError(
            f"the probe could not analyze this statement on '{platform}'; "
            f"only SELECT queries over catalog metadata are permitted"
        )
    if not isinstance(statement, exp.Query):
        keyword = _STATEMENT_KEYWORDS.get(type(statement))
        if keyword:
            article = "an" if keyword[0] in "AEIOU" else "a"
            raise SqlScopeError(
                f"only SELECT queries are permitted; this is {article} {keyword} "
                f"statement"
            )
        raise SqlScopeError(
            "only SELECT queries over catalog metadata are permitted; this "
            "statement is not a SELECT"
        )

    # Writes are judged over the whole tree, not the root: a Postgres
    # data-modifying CTE (`WITH x AS (DELETE FROM t RETURNING 1) SELECT * FROM
    # x`) is a Select whose body deletes, and its target resolves to the real
    # table, never the CTE. First, since a write is refused whatever it touches.
    # Annotated: find_all cannot infer the element type from a Tuple[type, ...].
    write: exp.Expr
    for write in statement.find_all(*_WRITE_NODES):
        keyword = _STATEMENT_KEYWORDS.get(type(write))
        # Rewrite guidance names a CTE only when the write is in one.
        in_cte = any(isinstance(ancestor, exp.CTE) for ancestor in _ancestors(write))
        because = (
            " A data-modifying CTE is still a write, however the query reads "
            "at the top level."
            if in_cte
            else " It is refused wherever it appears, including nested inside a SELECT."
        )
        raise SqlScopeError(
            f"only SELECT queries are permitted; this contains "
            f"{'a ' + keyword if keyword else 'a statement'} "
            f"that the probe cannot clear as read-only.{because}"
        )

    # Before the table walk: `SELECT pg_read_file('/etc/passwd')` names no table.
    _check_functions(statement)
    _check_server_state(statement)
    # The first of two layers over redact.WITHHELD_COLUMN_NAMES, deliberately
    # one constant: the gate refuses a query naming a withheld column (the
    # caller chooses output names), mask_identity_columns covers `SELECT *`.
    # Not per connector, so the layers cannot disagree and none has to opt in.
    _check_withheld_columns(statement)

    saw_relation = False
    for table in statement.find_all(exp.Table):
        # `or` would short-circuit and stop checking the rest.
        if _check_table(table, scope=permitted):
            saw_relation = True

    # A query reading no physical relation computes from server state
    # (`SELECT VERSION()`), including builtins sqlglot models as their own
    # nodes. A CTE alias parses as exp.Table but is not a relation.
    if not saw_relation:
        raise SqlScopeError(
            "a probe query must read from a catalog relation (for example a "
            "table in information_schema); this query names none, so it "
            "inspects server state rather than catalog metadata"
        )


def _ancestors(node: exp.Expr) -> List[exp.Expr]:
    """The node's parents, innermost first."""
    chain: List[exp.Expr] = []
    current = node.parent
    while current is not None:
        chain.append(current)
        current = current.parent
    return chain


def _check_withheld_columns(statement: exp.Expr) -> None:
    """Refuse a query that names a column whose values are withheld.

    Masking matches the driver's output names, which the caller chooses
    (`user_name AS u`, `LOWER(user_name)`, `ARRAY_AGG(user_name)`), so the name
    is refused wherever it is written, WHERE included. `SELECT *` names none and
    is left to the masker. Refused outright, since each hides a name from both
    layers: an alias column list (`AS t(a, b)` renames positionally), a NATURAL
    JOIN, and `USING` names (sqlglot keeps them as Identifiers, not Columns).
    """
    # lazy: redact is cheap, but this keeps the import beside its one use
    from datahub.ingestion.agent.redact import WITHHELD_COLUMN_NAMES

    for alias in statement.find_all(exp.TableAlias):
        if alias.args.get("columns"):
            raise SqlScopeError(
                "an alias column list can rename a withheld column out of "
                "sight of both the name check and the output masker, so this "
                "probe does not accept one; alias the table alone, or "
                "`SELECT *` and read the real column names"
            )

    # Refused rather than resolved: the shared columns need schemas the gate
    # does not have.
    for join in statement.find_all(exp.Join):
        if str(join.args.get("method") or "").upper() == "NATURAL":
            raise SqlScopeError(
                "a NATURAL JOIN matches columns implicitly, so it can join on "
                "a withheld column without naming it; say what you are joining "
                "on with ON or USING"
            )

    using_names = [
        identifier
        for join in statement.find_all(exp.Join)
        for identifier in (join.args.get("using") or [])
        if isinstance(identifier, exp.Identifier)
    ]

    for column in list(statement.find_all(exp.Column)) + using_names:
        if column.name.lower() in WITHHELD_COLUMN_NAMES:
            raise SqlScopeError(
                f"'{column.name}' names a person rather than describing shape, "
                f"so this probe does not return it. The relation is readable -- "
                f"select the columns you need, or `SELECT *`, where the value is "
                f"masked on the way out"
            )


def _check_functions(statement: exp.Expr) -> None:
    for func in statement.find_all(exp.Anonymous):
        if func.name.lower() in _ALLOWED_VENDOR_FUNCTIONS:
            continue
        raise SqlScopeError(
            f"'{func.name}' is a vendor-specific function whose output the probe "
            f"cannot verify as catalog metadata; only standard SQL over catalog "
            f"tables is permitted"
        )


# Server and session state sqlglot models as first-class nodes, so neither
# _check_functions nor, beside a real table, the must-read-a-relation rule sees
# them. Clock builtins are absent: they disclose nothing.
_SERVER_STATE_NODES: Dict[type, str] = {
    node: label
    for node, label in (
        (getattr(exp, name, None), label)
        for name, label in (
            ("CurrentUser", "CURRENT_USER"),
            ("SessionUser", "SESSION_USER"),
            ("CurrentVersion", "VERSION()"),
            ("CurrentSchema", "CURRENT_SCHEMA"),
        )
    )
    if node is not None
}


def _check_server_state(statement: exp.Expr) -> None:
    """Refuse server or session state (`@@datadir`, CURRENT_USER, VERSION())
    anywhere, a UNION branch beside a real catalog table included."""
    for param in statement.find_all(exp.SessionParameter):
        raise SqlScopeError(
            f"'@@{param.name}' reads a server/session variable, which is server "
            f"state rather than catalog metadata; only SELECTs over catalog "
            f"tables are permitted"
        )
    for node_type, label in _SERVER_STATE_NODES.items():
        if next(statement.find_all(node_type), None) is None:
            continue
        raise SqlScopeError(
            f"'{label}' reads server or session state rather than catalog "
            f"metadata; only SELECTs over catalog tables are permitted"
        )


def _resolve_dialect(platform: str) -> sqlglot.Dialect:
    try:
        return get_dialect(platform)
    except Exception as exc:
        # Falling back to a default dialect would parse the query against the
        # wrong grammar and clear references it had misread.
        raise SqlScopeError(
            f"cannot resolve a SQL dialect for platform '{platform}', so the "
            f"query cannot be checked"
        ) from exc


def _refuse_comments(sql: str, dialect: sqlglot.Dialect) -> None:
    """Refuse a query holding a comment or an optimizer hint.

    The gate checks the parsed tree, but the provider runs the raw text, and
    the two disagree inside comments: MySQL executes `/*! ... */` and
    `/*+ ... */`, and reads `--1` as `- -1` where sqlglot sees a comment. So
    a `UNION` hidden in one passes the check and then runs. A catalog query
    never needs a comment, so any is refused. Tokenised, not searched for
    `/*`, so the same characters inside a string literal stay allowed.
    """
    try:
        tokens = dialect.tokenize(sql)
    except Exception:
        # _parse_single_statement refuses what cannot be tokenised, by name.
        return
    if any(token.comments or token.token_type == TokenType.HINT for token in tokens):
        raise SqlScopeError(
            "the query holds a comment or an optimizer hint; remove it: some "
            "engines run text inside comments, so a query with one is not "
            "checked"
        )


def _parse_single_statement(
    sql: str, dialect: sqlglot.Dialect, platform: str
) -> exp.Expr:
    try:
        statements: List[exp.Expr] = [
            statement
            for statement in sqlglot.parse(sql, dialect=dialect)
            if statement is not None
        ]
    except Exception as exc:
        raise SqlScopeError(
            f"could not parse the query as {platform} SQL: {exc}"
        ) from exc

    if not statements:
        raise SqlScopeError("no SQL statement found in the query")
    if len(statements) > 1:
        raise SqlScopeError(f"the probe runs a single statement; got {len(statements)}")
    return statements[0]


def _slot_pieces(slot: object, scope: CatalogScope) -> List[str]:
    """The path pieces one identifier slot contributes.

    The scope decides (split_dotted_identifiers), not `Identifier.quoted`: a
    parser may mark a dotted slot quoted when the query did not quote it.
    Unsplit, a quoted dotted name is one unqualified name and is refused.
    """
    if not isinstance(slot, exp.Identifier):
        # No path; _check_table guarantees the name slot is an identifier.
        return []
    text = slot.name
    if not text:
        return []
    if not scope.split_dotted_identifiers:
        return [text]
    return [piece for piece in text.split(".") if piece]


def _visible_cte_names(table: exp.Table) -> Set[str]:
    """CTE names in scope for this table: only those an enclosing WITH declares.

    An inner WITH does not reach an outer FROM, where SQL resolves the name to
    the real table (`SELECT * FROM t WHERE 1 IN (WITH t AS (...) SELECT ...)`).
    """
    names: Set[str] = set()
    node: Optional[exp.Expr] = table.parent
    # Which CTE's body this reference sits in, if any -- set on the way up.
    inside: Optional[exp.Expr] = None
    while node is not None:
        if isinstance(node, exp.CTE):
            inside = node
        # Matched by node type, not by arg key ("with_" on this sqlglot pin),
        # so a rename cannot hide every CTE.
        for value in node.args.values():
            if not isinstance(value, exp.With):
                continue
            ctes = list(value.expressions)
            visible = ctes
            if inside is not None and inside in ctes:
                # A CTE body sees earlier siblings only, and itself only under
                # RECURSIVE: a later sibling's name resolves to a real table.
                cut = ctes.index(inside)
                visible = ctes[: cut + 1] if value.args.get("recursive") else ctes[:cut]
            for cte in visible:
                names.add(cte.alias_or_name.lower())
        node = node.parent
    return names


def _check_table(table: exp.Table, scope: CatalogScope) -> bool:
    """Clear one table reference; True when it is a physical relation (a CTE
    reference parses as exp.Table but is not one)."""
    if not isinstance(table.this, exp.Identifier):
        # A function in FROM position, refused here too so a function sqlglot
        # does model cannot enter through the table walk.
        rendered = getattr(table.this, "name", "") or table.sql()
        raise SqlScopeError(
            f"'{rendered}' is a function in FROM position, not a catalog table"
        )

    # Flattened: dialects disagree about which slot holds what
    # (CatalogScope.split_dotted_identifiers).
    parts = [
        piece
        for slot in (
            table.args.get("catalog"),
            table.args.get("db"),
            table.args.get("this"),
        )
        for piece in _slot_pieces(slot, scope)
    ]

    if len(parts) < 2:
        name = parts[0] if parts else table.name
        # A CTE alias: permitted, but not a relation read.
        if name.lower() in _visible_cte_names(table):
            return False
        # Some dialects expose their catalog unqualified (Oracle's public
        # synonyms such as `dba_tables`).
        if scope.permits_unqualified(name):
            return True
        raise SqlScopeError(
            f"'{name}' is not schema-qualified, so it cannot be shown to be "
            f"catalog metadata; qualify it (e.g. {INFORMATION_SCHEMA}.{name}), or "
            f"use one of the relations this source lists"
        )

    rendered = ".".join(parts)

    if not scope.permits_path(parts):
        raise SqlScopeError(
            f"'{rendered}' is outside the catalog metadata this probe may read; "
            f"this source permits {scope.describe()}"
        )

    return True
