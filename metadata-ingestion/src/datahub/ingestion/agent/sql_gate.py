from typing import Dict, FrozenSet, List, Set

import sqlglot
from sqlglot import exp

from datahub.sql_parsing.sqlglot_utils import get_dialect, is_dialect_instance

# Schemas whose contents are catalog metadata rather than user data. Everything
# outside these is refused.
INFORMATION_SCHEMA = "information_schema"
PG_CATALOG = "pg_catalog"

_DEFAULT_CATALOG_SCHEMAS: FrozenSet[str] = frozenset({INFORMATION_SCHEMA})
_POSTGRES_CATALOG_SCHEMAS: FrozenSet[str] = frozenset({INFORMATION_SCHEMA, PG_CATALOG})

_POSTGRES_LIKE_DIALECTS = ("postgres", "redshift")

# Relations that live inside a catalog schema but carry the text of user
# queries -- literal values from WHERE clauses, i.e. row data arriving by
# another route. A schema-level allowlist alone would wave these through, which
# is the whole reason this list exists.
_QUERY_TEXT_RELATIONS: FrozenSet[str] = frozenset(
    {"pg_stat_statements", "pg_stat_activity"}
)

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


def check_query_scope(sql: str, platform: str) -> None:
    """Raise SqlScopeError unless `sql` is a single SELECT over catalog metadata.

    Fail-closed at every step: an unresolvable dialect, an unparseable query, an
    unqualified table, or any reference outside the dialect's catalog schemas is
    a refusal, never a warning and never a guess.
    """
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

    allowed = _catalog_schemas(dialect)
    cte_names = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}

    for table in statement.find_all(exp.Table):
        _check_table(table, allowed=allowed, cte_names=cte_names)


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


def _catalog_schemas(dialect: sqlglot.Dialect) -> FrozenSet[str]:
    if is_dialect_instance(dialect, _POSTGRES_LIKE_DIALECTS):
        return _POSTGRES_CATALOG_SCHEMAS
    return _DEFAULT_CATALOG_SCHEMAS


def _check_table(
    table: exp.Table, allowed: FrozenSet[str], cte_names: Set[str]
) -> None:
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
        raise SqlScopeError(
            f"'{name}' is not schema-qualified, so it cannot be shown to be "
            f"catalog metadata; qualify it (e.g. {INFORMATION_SCHEMA}.{name})"
        )

    relation, schema = parts[-1], parts[-2]
    rendered = ".".join(parts)

    if schema.lower() not in allowed:
        raise SqlScopeError(
            f"'{rendered}' is outside the catalog metadata this probe may read; "
            f"permitted schemas: {', '.join(sorted(allowed))}"
        )
    if relation.lower() in _QUERY_TEXT_RELATIONS:
        raise SqlScopeError(
            f"'{rendered}' exposes the text of user queries, which can embed "
            f"row values, so it is excluded even though its schema is catalog"
        )
