from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

from datahub.sql_parsing.sqlglot_utils import get_dialect

# The one schema that is catalog metadata by definition, in every dialect that has
# it. Everything beyond this is the connector's to declare.
INFORMATION_SCHEMA = "information_schema"


@dataclass(frozen=True)
class CatalogScope:
    """What one dialect considers catalog metadata a probe may read.

    Declared per connector (see SQLCommonConfig.probe_catalog_scope) rather than
    held centrally, because a central table has to know every dialect's catalog
    surface and this one did not: Oracle and Teradata have no `information_schema`
    at all -- their catalogs are `DBA_*`/`ALL_*` and `DBC.*` -- so both advertised
    a `sql` command whose every legitimate query was refused.

    **Prefer `relations` over `schemas`.** A vendor catalog schema is almost never
    wholly metadata, and our own ingestion code is the evidence: it reads
    `system.query_log` on ClickHouse, `DBC.QryLogV` on Teradata and
    `sys.dm_exec_cached_plans` on MSSQL. Those carry the text of user queries --
    WHERE-clause literals included -- so a schema-level allow with a list of
    exclusions is a denylist, and would let the next text-bearing view somebody
    adds through by default. Naming relations keeps the default deny.

    This scope is a ceiling, not the floor. The probe reads with the recipe's
    own database credential, so the credential is the operative bound: it reads
    only what that credential is privileged to read, and a schema admitted whole
    still exposes only the rows the credential may see. `information_schema`
    illustrates both edges -- it also holds operational views like
    `processlist` and `*_privileges`, but MySQL's `processlist` shows other
    sessions' in-flight SQL only to a credential holding the `PROCESS`
    privilege. So give the probe a least-privilege, read-only credential: the
    gate bounds which relations a query may name, and the credential bounds what
    those relations reveal.
    """

    # Whole schemas whose every relation is metadata by definition. In practice
    # this is `information_schema`, and `pg_catalog` where the exclusions below
    # are also declared.
    schemas: FrozenSet[str] = field(
        default_factory=lambda: frozenset({INFORMATION_SCHEMA})
    )

    # Individually permitted relations, for a schema that is not wholly safe.
    # "schema.relation", or a bare name where the dialect exposes the relation
    # unqualified (Oracle's dictionary views are public synonyms).
    relations: FrozenSet[str] = field(default_factory=frozenset)

    # Relations to refuse inside an otherwise-permitted schema. Only sound where
    # the schema really is metadata apart from a known few, which is pg_catalog
    # and its query-text views.
    excluded_relations: FrozenSet[str] = field(default_factory=frozenset)

    def permits_path(self, parts: List[str]) -> bool:
        """Whether a reference, given as its dotted path parts, is in scope.

        A `relations` entry is matched against the *suffix* of the reference, so an
        entry may pin as much of the path as it needs to. Two parts
        ("pg_catalog.svv_table_info") name a schema and relation and accept any
        catalog above them. Three parts ("snowflake.account_usage.tables") pin the
        catalog as well, which is the only way to tell a system schema from a
        user-created one wearing the same name: nothing stops somebody creating a
        database whose schema is called ACCOUNT_USAGE, and matching only the last
        two segments would read their tables as though they were Snowflake's.

        A *bare* entry takes no part in this. Those exist because some dialects
        expose their catalog unqualified -- Oracle's dictionary views are public
        synonyms -- and permits_unqualified is where they are honoured. Suffix
        matching them here would be the same mistake in the other direction:
        "all_tables" would license `hr.all_tables`, so a user table wearing a
        dictionary view's name would read as catalog metadata under any schema.
        """
        schema, relation = parts[-2], parts[-1]
        if schema.lower() in {s.lower() for s in self.schemas}:
            return relation.lower() not in {r.lower() for r in self.excluded_relations}
        lowered = [part.lower() for part in parts]
        for entry in self.relations:
            entry_parts = entry.lower().split(".")
            if len(entry_parts) < 2 or len(entry_parts) > len(lowered):
                # Bare entries are unqualified-only (see above); an entry that
                # pins more of the path than the reference supplies cannot be
                # shown to be that relation.
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

# sqlglot models standard SQL functions as their own node types (count -> exp.Count)
# and leaves anything vendor-specific as exp.Anonymous. That split is doing real
# work for us: every known way to reach data without naming a table is an
# unmodelled function -- pg_read_file, pg_ls_dir, dblink, lo_import, Snowflake's
# SYSTEM$..., BigQuery's EXTERNAL_QUERY. Refusing the whole Anonymous class is
# therefore fail-closed by construction, where a denylist of bad names could
# never be complete.
#
# Extension point, kept empty on purpose: add a vendor function here only after
# confirming it returns catalog metadata and cannot read user rows or host
# files. Metadata that needs such a function today (view DDL, column comments)
# is served by the typed getters in sqlalchemy_probe.py instead.
_ALLOWED_VENDOR_FUNCTIONS: FrozenSet[str] = frozenset()


# A refusal is the caller's only signal for how to rewrite, so it has to be in
# SQL terms. sqlglot's node name usually matches the keyword (Insert -> INSERT),
# but not always: FLUSH PRIVILEGES parses to an Alias, and "got ALIAS" reads like
# a bug in the caller's own query rather than a refused statement type.
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
            # `SELECT ... INTO tbl` creates a table on MSSQL and Postgres,
            # and sqlglot models it as an `into` arg on the Select rather
            # than as Create or Insert -- so the walk below saw no write
            # node at all. It was refused only when the target happened to
            # be unqualified; `SELECT * INTO information_schema.evil FROM
            # information_schema.tables` named a target inside the permitted
            # schema and passed. Caught by accident is not caught.
            ("Into", "SELECT ... INTO"),
            # `FOR UPDATE` / `FOR SHARE`. Not a write, but not a read
            # either: it declares write intent and takes row locks that
            # persist to end of transaction, so a probe could block a
            # production writer on a catalog view. sqlglot models it as a
            # `locks` arg on the Select, so like Into it is invisible to a
            # statement-type check.
            ("Lock", "row-locking SELECT"),
        )
    )
    if node is not None
}


# The same node types as a tuple, for walking the tree rather than reading
# only its root. Command is included: a nested statement sqlglot could not
# model is exactly as unclearable as a top-level one.
_WRITE_NODES: Tuple[type, ...] = tuple(_STATEMENT_KEYWORDS) + (exp.Command,)


class SqlScopeError(ValueError):
    """A query was refused because it is not a read of catalog metadata.

    Deliberately a ValueError: recipe_cli already maps ValueError to the
    user-error exit code, so a refusal reads as "your input was wrong" rather
    than "the source could not be reached".

    This check narrows what a probe query can touch; it is not a security
    boundary. A determined query against a permissive credential is stopped by
    the database's own grants, not here. See probe_interface.md.
    """


def check_query_scope(
    sql: str, platform: str, scope: Optional[CatalogScope] = None
) -> None:
    """Raise SqlScopeError unless `sql` is a single SELECT over catalog metadata.

    `scope` is the connector's declaration of what its dialect's catalog is; with
    none given it defaults to `information_schema` only, which is safe everywhere
    and sufficient for the standard dialects.

    Fail-closed at every step: an unresolvable dialect, an unparseable query, a
    reference the scope does not permit, or an unqualified name the scope does not
    list is a refusal, never a warning and never a guess.
    """
    permitted = scope or _DEFAULT_SCOPE
    dialect = _resolve_dialect(platform)
    statement = _parse_single_statement(sql, dialect, platform)

    if isinstance(statement, exp.Command):
        # sqlglot emits Command for statements it does not model. We cannot see
        # what such a statement touches, so we cannot clear it.
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

    # The check above reads only the ROOT node, and that is not enough: a
    # Postgres data-modifying CTE puts the write *inside* a query.
    #
    #   WITH orders AS (SELECT 1),
    #        x AS (DELETE FROM orders RETURNING 1)
    #   SELECT * FROM x
    #
    # parses to a Select -- an exp.Query -- whose CTE body is an exp.Delete,
    # so it cleared a gate whose entire promise is read-only. Worse, the
    # unqualified DELETE target was then excused by _visible_cte_names as "a
    # CTE alias reads as an unqualified table", which is true of a read
    # reference and false of a write target: Postgres resolves the DELETE to
    # the real table, never to the CTE. Proven against a live Postgres 16 --
    # rows were deleted through `probe run sql`. INSERT, UPDATE and DROP take
    # the same shape.
    #
    # So the statement type is judged over the whole tree. This runs before
    # _check_functions and the table walk because it is the broader refusal:
    # a write is refused whatever it touches, in or out of scope.
    # Annotated because _WRITE_NODES is a Tuple[type, ...], which loses the
    # element type that find_all's overloads infer from literal arguments.
    write: exp.Expr
    for write in statement.find_all(*_WRITE_NODES):
        keyword = _STATEMENT_KEYWORDS.get(type(write))
        # The CTE sentence only when the write is actually in one. It is the
        # caller's rewrite guidance, and a nested `SELECT ... INTO` or a
        # `FOR UPDATE` was being told to look for a CTE it does not have.
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

    # Before walking tables: a projection-only call such as
    # `SELECT pg_read_file('/etc/passwd')` names no table at all, so a
    # table-based check alone never sees it.
    _check_functions(statement)

    # `SELECT @@datadir, @@hostname` reads server state, not a relation: it
    # names no table (so the table walk below is vacuously satisfied) and no
    # Anonymous function (so _check_functions misses it), yet it discloses the
    # data directory, hostname and version. sqlglot models `@@x` as its own
    # node, so refuse the node directly rather than trying to enumerate which
    # variables are sensitive.
    _check_server_state(statement)

    _check_withheld_columns(statement)

    saw_relation = False
    for table in statement.find_all(exp.Table):
        # `or` would short-circuit and stop checking the rest.
        if _check_table(table, scope=permitted, platform=platform):
            saw_relation = True

    # A row-returning query that names no relation is not catalog inspection --
    # it computes a row from server state (`SELECT VERSION()`, `SELECT 1`).
    # The point of the gate is that reads stay within the catalog scope; a
    # query that reads no catalog relation has left that question unanswered
    # rather than answered yes. Requiring one relation closes the whole
    # no-table disclosure class in one place, including built-in functions
    # sqlglot models as first-class nodes (not Anonymous) that _check_functions
    # cannot see. The probe's typed getters cover legitimate no-SQL discovery;
    # the `sql` command a caller reaches for is always catalog-content over a
    # FROM.
    #
    # Physical relations only. A CTE alias parses as exp.Table, so counting
    # every exp.Table let `WITH x AS (SELECT CURRENT_USER) SELECT * FROM x`
    # satisfy this rule while reading nothing -- defeating, through the one
    # node type that is not a relation, the very class this rule closes.
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

    redact.mask_identity_columns blanks these on the way out, and that was
    stated as sufficient -- snowflake_probe said the relation could be
    admitted because "every row leaving sql_result has it replaced with the
    redaction marker". It was not sufficient, because masking matches the
    DRIVER's output column names and the caller chooses those:

        SELECT USER_NAME AS u       FROM ...access_history   -> unmasked
        SELECT LOWER(user_name)     FROM ...access_history   -> unmasked
        SELECT ARRAY_AGG(user_name) FROM ...access_history   -> unmasked,
            and that last one is the account's whole user directory in a
            single row -- the account_usage.users content the exclusion
            list calls personal data.

    So the name is refused where it is written, before any of that. Checked
    over every column reference rather than the projection alone: a
    `WHERE user_name = 'alice'` that returns rows answers the same question
    one bit at a time.

    What this deliberately does NOT refuse is `SELECT *`, which names no
    column -- there the driver's output names are the real ones and the
    masker handles it. The two layers cover what the other cannot, which is
    why they read one shared set.

    That pairing is what an alias column list breaks, so it is refused first:
    `FROM access_history AS t(a, b, c)` renames positionally, which leaves
    `SELECT *` naming no column for this walk AND makes the driver report
    a/b/c, so the masker matches nothing either. Caller-chosen names on both
    layers at once. A column list can only rename, never narrow, so a metadata
    query has no use for one and refusing the form costs nothing.

    `USING` is the other gap, and a parser detail rather than a syntax one:
    sqlglot stores `JOIN ... USING (user_name)` names as bare Identifiers, not
    exp.Column, so the walk below never saw them. Joining on the name and
    returning a count answers "is this person here" one bit at a time -- the
    same question the projection rule refuses.
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


# Server and session state that sqlglot models as FIRST-CLASS nodes, so
# _check_functions -- which only sees exp.Anonymous -- cannot refuse them, and
# the must-read-a-relation rule cannot either once the query also names a real
# table. `@@version` was refused while `VERSION()` was not, which is the same
# fact spelled two ways.
#
# CURRENT_DATE and the other clock builtins are deliberately absent: they
# disclose nothing, and a catalog query may legitimately filter on one.
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
    """Refuse a reference to server or session identity (`@@name`, CURRENT_USER).

    These name neither a relation nor a function, so the table walk and
    _check_functions both pass them through, yet `@@datadir`, `@@hostname` and
    the like disclose server configuration rather than catalog metadata. They
    have no place in a catalog query, so any occurrence is refused -- including
    one smuggled into a UNION branch alongside a real catalog table, which the
    "must name a relation" rule alone would not catch.

    _SERVER_STATE_NODES is here for that last reason. Each discloses identity,
    version or session configuration, and sqlglot models each as its own node
    rather than an Anonymous function, so _check_functions cannot see them
    either. Riding along with a real catalog table is exactly the case the
    relation rule cannot reach, so the nodes themselves are refused.
    """
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


# Dialects whose parser leaves a dot INSIDE one identifier slot, so the slot has
# to be split to recover the path. Measured, not assumed: of the dialects this
# gate serves, only BigQuery does it -- `myds.INFORMATION_SCHEMA.TABLES` parses
# as db='myds', name='INFORMATION_SCHEMA.TABLES'. Everything else fills
# catalog/db/name properly.
#
# This is deliberately an allowlist rather than "split everywhere". Splitting
# every dialect is what turned a BigQuery accommodation into a bypass on all the
# others: a quoted user table named "information_schema.tables" decomposed into
# ["information_schema", "tables"], which every Postgres and MySQL scope
# permits. A dialect absent from here is never split, so a future parser quirk
# fails closed (a refused legitimate query) instead of open.
_DOT_IN_SLOT_DIALECTS = frozenset({"bigquery"})


def _slot_pieces(slot: object, platform: str) -> List[str]:
    """The path pieces one identifier slot contributes.

    The dialect decides, and `Identifier.quoted` deliberately does not get a
    vote: sqlglot reports quoted=True for BigQuery's `myds.INFORMATION_SCHEMA
    .TABLES` even though the SQL carries no quotes, so keying on it blocks the
    split for the one dialect that needs it.

    Splitting BigQuery unconditionally is safe because a BigQuery identifier
    cannot contain a dot, so a dotted name there is always a path and never a
    table's own name. That is a naming rule, not an assumption: BigQuery admits
    only Unicode categories L, M, N, Pc, Pd and Zs in a table name, and `.` is
    Po. (Underscore is Pc, dash is Pd and space is Zs, which is why all three
    ARE legal there and a dot is not.)

    Every other dialect keeps the whole slot, so a quoted user table actually
    named "information_schema.tables" stays one unqualified name and is
    refused, rather than decomposing into a path the scope would permit.
    """
    if not isinstance(slot, exp.Identifier):
        # Not an identifier, so it contributes no path. The name slot is
        # guaranteed to be one by _check_table's own guard.
        return []
    text = slot.name
    if not text:
        return []
    if platform.lower() not in _DOT_IN_SLOT_DIALECTS:
        return [text]
    return [piece for piece in text.split(".") if piece]


def _visible_cte_names(table: exp.Table) -> Set[str]:
    """CTE names in scope for this table: only those declared by an enclosing WITH.

    Collecting them for the whole statement instead let an inner, non-enclosing
    CTE license its name anywhere, including outer scopes where SQL itself
    resolves the name to the real table:

        SELECT * FROM customer_pii
        WHERE 1 IN (WITH customer_pii AS (SELECT 1 AS a) SELECT a FROM customer_pii)

    Postgres does not make an inner WITH visible to an outer FROM, so the outer
    reference is the user's table -- and the gate waved it through. Walking the
    node's own ancestors instead means a name is only excused where the SQL
    engine would actually resolve it to a CTE. A CTE body referring to an
    earlier sibling still works: the enclosing WITH is on its ancestor chain.
    """
    names: Set[str] = set()
    node: Optional[exp.Expr] = table.parent
    # Which CTE's body this reference sits in, if any -- set on the way up.
    inside: Optional[exp.Expr] = None
    while node is not None:
        if isinstance(node, exp.CTE):
            inside = node
        # Scanned by value rather than by key: on this pin the clause sits under
        # "with_", not "with", and reading the wrong key fails silently open --
        # no CTE is ever visible, so every WITH query gets refused. Matching on
        # the node type cannot drift with a rename.
        for value in node.args.values():
            if not isinstance(value, exp.With):
                continue
            ctes = list(value.expressions)
            visible = ctes
            if inside is not None and inside in ctes:
                # A CTE body sees only siblings declared BEFORE it, and itself
                # only when the WITH is RECURSIVE. Admitting all of them let an
                # earlier CTE reference an unqualified user table whose name
                # matches a LATER sibling -- which SQL resolves to the table,
                # not the CTE, so the gate excused a real table read.
                cut = ctes.index(inside)
                visible = ctes[: cut + 1] if value.args.get("recursive") else ctes[:cut]
            for cte in visible:
                names.add(cte.alias_or_name.lower())
        node = node.parent
    return names


def _check_table(table: exp.Table, scope: CatalogScope, platform: str) -> bool:
    """Clear one table reference, and say whether it was a physical relation.

    A CTE reference is not one, and the caller needs to know: a CTE alias
    parses as exp.Table, so counting every exp.Table let a query that reads
    nothing satisfy the "must read a catalog relation" rule.
    """
    if not isinstance(table.this, exp.Identifier):
        # A set-returning function in FROM position. Caught here as well as in
        # _check_functions so that a vendor function sqlglot *does* model still
        # cannot enter through the table walk.
        rendered = getattr(table.this, "name", "") or table.sql()
        raise SqlScopeError(
            f"'{rendered}' is a function in FROM position, not a catalog table"
        )

    # Flattened, because dialects disagree about which slot holds what.
    # BigQuery table names may contain dots, so its dialect parses
    # `mydataset.INFORMATION_SCHEMA.TABLES` as db='mydataset' and
    # name='INFORMATION_SCHEMA.TABLES' -- the schema marker is inside the name.
    # Splitting every slot and reading positionally is the one rule that holds
    # for that as well as Postgres's db.schema.table.
    parts = [
        piece
        for slot in (
            table.args.get("catalog"),
            table.args.get("db"),
            table.args.get("this"),
        )
        for piece in _slot_pieces(slot, platform)
    ]

    if len(parts) < 2:
        name = parts[0] if parts else table.name
        # A CTE alias reads as an unqualified table; refusing it would reject
        # legitimate catalog queries that use WITH. It is not a relation read,
        # though, which is what the False says.
        if name.lower() in _visible_cte_names(table):
            return False
        # Some dialects expose their catalog unqualified: Oracle's dictionary
        # views are public synonyms, so `FROM dba_tables` is the idiomatic read
        # and there is no schema to qualify it with.
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
