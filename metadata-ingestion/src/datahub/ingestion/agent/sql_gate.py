from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Set

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

    def permits(self, schema: str, relation: str) -> bool:
        if schema.lower() in {s.lower() for s in self.schemas}:
            return relation.lower() not in {r.lower() for r in self.excluded_relations}
        return f"{schema}.{relation}".lower() in {r.lower() for r in self.relations}

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
        )
    )
    if node is not None
}


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

    # Before walking tables: a projection-only call such as
    # `SELECT pg_read_file('/etc/passwd')` names no table at all, so a
    # table-based check alone never sees it.
    _check_functions(statement)

    cte_names = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}

    for table in statement.find_all(exp.Table):
        _check_table(table, scope=permitted, cte_names=cte_names)


def _check_functions(statement: exp.Expr) -> None:
    for func in statement.find_all(exp.Anonymous):
        if func.name.lower() in _ALLOWED_VENDOR_FUNCTIONS:
            continue
        raise SqlScopeError(
            f"'{func.name}' is a vendor-specific function whose output the probe "
            f"cannot verify as catalog metadata; only standard SQL over catalog "
            f"tables is permitted"
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


def _check_table(table: exp.Table, scope: CatalogScope, cte_names: Set[str]) -> None:
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
        for part in (table.catalog, table.db, table.name)
        if part
        for piece in part.split(".")
        if piece
    ]

    if len(parts) < 2:
        name = parts[0] if parts else table.name
        # A CTE alias reads as an unqualified table; refusing it would reject
        # legitimate catalog queries that use WITH.
        if name.lower() in cte_names:
            return
        # Some dialects expose their catalog unqualified: Oracle's dictionary
        # views are public synonyms, so `FROM dba_tables` is the idiomatic read
        # and there is no schema to qualify it with.
        if scope.permits_unqualified(name):
            return
        raise SqlScopeError(
            f"'{name}' is not schema-qualified, so it cannot be shown to be "
            f"catalog metadata; qualify it (e.g. {INFORMATION_SCHEMA}.{name}), or "
            f"use one of the relations this source lists"
        )

    relation, schema = parts[-1], parts[-2]
    rendered = ".".join(parts)

    if not scope.permits(schema, relation):
        raise SqlScopeError(
            f"'{rendered}' is outside the catalog metadata this probe may read; "
            f"this source permits {scope.describe()}"
        )
