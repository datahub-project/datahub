"""SQLAlchemy 2.0 compatibility shim for acryl-pyhive, with re-exports.

acryl-pyhive (pinned to ``acryl-pyhive[hive-pure-sasl]==0.6.18`` in setup.py, the
latest published release) is SQLAlchemy-1.4-era, in two ways:

1. Its ``sqlalchemy_hive`` / ``sqlalchemy_presto`` dialects import names at module load
   time that SQLAlchemy 2.0 moved or removed, so importing them on SA 2.0 raises
   ``ImportError`` / ``AttributeError``. SQLAlchemy loads these modules through their
   entry points, so this also breaks ``create_engine("hive://...")``.
2. Their reflection methods pass raw SQL strings to ``connection.execute()`` (SA 2.0
   requires ``text()``, raising ``ObjectNotExecutableError`` otherwise) and index rows
   by column name (SA 2.0 rows are tuples; name lookup needs ``row._mapping``). The
   same applies to the ``databricks+pyhive`` dialect from databricks-dbapi, which
   subclasses pyhive's ``HiveDialect``.

This module restores the names pyhive needs *before* importing it, re-implements the
affected reflection methods, and re-exports the pyhive symbols the hive/presto sources
use. Anything that may build a pyhive engine (the hive, presto and generic sqlalchemy
sources) imports this module first, so the patches are in place regardless of which
source runs. Behavioural overrides that DataHub layers on top of pyhive (e.g. Hive
``SHOW VIEWS``) stay in the individual sources; everything here exists only for SA 2.0.

Removal: upstream PyHive added SQLAlchemy 2.0 support (dropbox/PyHive#457) and the
project has since moved to Apache Kyuubi. Once DataHub's pinned pyhive fork includes that
support (or the dependency moves to the maintained upstream), delete this module and
import from ``pyhive`` directly again.
"""

import re
import sys
from typing import Any, Dict, List, Optional, Sequence

import sqlalchemy
import sqlalchemy.dialects
from sqlalchemy import exc, text
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Connection, Row, reflection

# `from sqlalchemy import processors`: relocated to sqlalchemy.engine.processors in 2.0.
if not hasattr(sqlalchemy, "processors"):
    from sqlalchemy.engine import processors

    sqlalchemy.processors = processors  # type: ignore[attr-defined]
    sys.modules.setdefault("sqlalchemy.processors", processors)

# `from sqlalchemy.databases import mysql`: the `databases` package was removed in 2.0;
# its dialects now live under sqlalchemy.dialects.
sys.modules.setdefault("sqlalchemy.databases", sqlalchemy.dialects)

# pyhive references the pre-1.0 MySQL type alias `MSTinyInteger` (renamed to `TINYINT`).
if not hasattr(mysql, "MSTinyInteger"):
    mysql.MSTinyInteger = mysql.TINYINT  # type: ignore[attr-defined]

# `hive` is imported only to verify the dependency is installed.
from pyhive import hive, presto  # noqa: E402,F401
from pyhive.sqlalchemy_hive import (  # noqa: E402,F401
    HiveDate,
    HiveDecimal,
    HiveDialect,
    HiveTimestamp,
)
from pyhive.sqlalchemy_presto import PrestoDialect  # noqa: E402
from pyhive.sqlalchemy_sparksql import SparkSqlDialect  # noqa: E402

try:
    from databricks_dbapi.sqlalchemy_dialects.base import (
        DatabricksDialectBase,
    )
except ImportError:
    # databricks-dbapi only ships with the hive extra.
    DatabricksDialectBase = None

# Set on each patched class so re-importing (e.g. importlib.reload) is a no-op.
_PATCHED_SENTINEL = "_datahub_sa2_compat_patched"


def _quote_full_table(
    dialect: Any, table_name: str, schema: Optional[str], separator: str = "."
) -> str:
    full_table = dialect.identifier_preparer.quote_identifier(table_name)
    if schema:
        full_table = (
            dialect.identifier_preparer.quote_identifier(schema)
            + separator
            + full_table
        )
    return full_table


def _show_tables_query(dialect: Any, schema: Optional[str], keyword: str) -> str:
    query = "SHOW TABLES"
    if schema:
        query += f" {keyword} " + dialect.identifier_preparer.quote_identifier(schema)
    return query


# --- HiveDialect -----------------------------------------------------------------


@reflection.cache
def _hive_get_schema_names(self: Any, connection: Connection, **kw: Any) -> List[str]:
    # Equivalent to SHOW DATABASES
    return [row[0] for row in connection.execute(text("SHOW SCHEMAS"))]


@reflection.cache
def _hive_get_table_names(
    self: Any, connection: Connection, schema: Optional[str] = None, **kw: Any
) -> List[str]:
    query = _show_tables_query(self, schema, "IN")
    return [row[0] for row in connection.execute(text(query))]


def _hive_get_table_columns(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str],
    extended: bool = False,
) -> Sequence[Row]:
    full_table = _quote_full_table(self, table_name, schema)
    # TODO using TGetColumnsReq hangs after sending TFetchResultsReq.
    # Using DESCRIBE works but is uglier.
    try:
        formatted = " FORMATTED" if extended else ""
        rows = connection.execute(text(f"DESCRIBE{formatted} {full_table}")).fetchall()
    except exc.OperationalError as e:
        # Does the table exist?
        regex_fmt = r"TExecuteStatementResp.*SemanticException.*Table not found {}"
        regex = regex_fmt.format(re.escape(full_table))
        if re.search(regex, e.args[0]):
            raise exc.NoSuchTableError(full_table) from e
        raise
    # Hive returns a single row for DESCRIBE of a non-existent table.
    regex = r"Table .* does not exist"
    if len(rows) == 1 and re.match(regex, rows[0].col_name):
        raise exc.NoSuchTableError(full_table)
    return rows


def _has_table(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str] = None,
    **kw: Any,
) -> bool:
    # pyhive's has_table takes no **kw, but SA 2.0's Inspector.has_table always passes
    # info_cache, which raises TypeError.
    try:
        self._get_table_columns(connection, table_name, schema)
        return True
    except exc.NoSuchTableError:
        return False


# --- SparkSqlDialect (hive source with `scheme: sparksql`) ------------------------


def _sparksql_get_table_columns(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str],
    extended: bool = False,
) -> Sequence[Row]:
    # Spark Thrift Server is not quoted, unlike Hive.
    full_table = f"{schema}.{table_name}" if schema else table_name
    try:
        # Stop Spark SQL truncating long column types (e.g. structs).
        connection.execute(text("SET spark.sql.debug.maxToStringFields=1000000"))
        formatted = " FORMATTED" if extended else ""
        return connection.execute(text(f"DESCRIBE{formatted} {full_table}")).fetchall()
    except exc.OperationalError as e:
        regex_fmt = (
            r"TExecuteStatementResp.*AnalysisException.*Table or view not found:.*{}"
        )
        hive_regex = (
            r"org.apache.spark.SparkException: Cannot recognize hive type string"
        )
        if re.search(regex_fmt.format(re.escape(table_name)), e.args[0]):
            raise exc.NoSuchTableError(full_table) from e
        if re.search(hive_regex, e.args[0]):
            raise exc.UnreflectableTableError from e
        raise


@reflection.cache
def _sparksql_get_table_names(
    self: Any, connection: Connection, schema: Optional[str] = None, **kw: Any
) -> List[str]:
    query = _show_tables_query(self, schema, "IN")
    # Rows are (database, tableName, isTemporary); temporary views are skipped.
    return [row[1] for row in connection.execute(text(query)) if not row[-1]]


def _sparksql_has_table(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str] = None,
    **kw: Any,
) -> bool:
    try:
        return _has_table(self, connection, table_name, schema)
    except exc.UnreflectableTableError:
        return False


# --- PrestoDialect ---------------------------------------------------------------
# PrestoDialect extends DefaultDialect directly, not HiveDialect, so it needs its own
# patches. presto.py replaces most of its reflection with Trino-based queries; these
# cover what is left, which Table(autoload_with=...) still reaches during profiling
# (get_indexes -> _get_table_columns).


@reflection.cache
def _presto_get_schema_names(self: Any, connection: Connection, **kw: Any) -> List[str]:
    # Positional access drops pyhive's reliance on the "Schema" column label.
    return [row[0] for row in connection.execute(text("SHOW SCHEMAS"))]


@reflection.cache
def _presto_get_table_names(
    self: Any, connection: Connection, schema: Optional[str] = None, **kw: Any
) -> List[str]:
    # For sql_generic's presto:// URIs. The Presto source replaces this with the
    # Trino information_schema query (which excludes views), applied after this.
    query = "SHOW TABLES"
    if schema:
        query += " FROM " + self.identifier_preparer.quote_identifier(schema)
    return [row[0] for row in connection.execute(text(query))]


def _presto_get_table_columns(
    self: Any, connection: Connection, table_name: str, schema: Optional[str]
) -> Sequence[Row]:
    full_table = _quote_full_table(self, table_name, schema)
    try:
        return connection.execute(text(f"SHOW COLUMNS FROM {full_table}")).fetchall()
    except (presto.DatabaseError, exc.DatabaseError) as e:
        # Presto raises its error when the cursor description is fetched; SA 2.0 does
        # that inside execute() and wraps it, so look at the original DB-API error.
        err = e.orig if isinstance(e, exc.DBAPIError) and e.orig is not None else e
        msg = (
            err.args[0].get("message")
            if err.args and isinstance(err.args[0], dict)
            else err.args[0]
            if err.args and isinstance(err.args[0], str)
            else None
        )
        regex = r"Table\ \'.*{}\'\ does\ not\ exist".format(re.escape(table_name))
        if msg and re.search(regex, msg):
            raise exc.NoSuchTableError(table_name) from e
        raise


def _presto_get_indexes(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str] = None,
    **kw: Any,
) -> List[Dict[str, Any]]:
    rows = self._get_table_columns(connection, table_name, schema)
    part_key = "Partition Key"
    col_names = []
    for row in rows:
        mapping = row._mapping
        # Presto puts this information in one of 3 places depending on version:
        # a boolean "Partition Key" column, the "Comment" column, or the "Extra" column.
        is_partition_key = (
            mapping.get(part_key)
            or (mapping.get("Comment") or "").startswith(part_key)
            or "partition key" in (mapping.get("Extra") or "")
        )
        if is_partition_key:
            col_names.append(mapping["Column"])
    if col_names:
        return [{"name": "partition", "column_names": col_names, "unique": False}]
    return []


# --- databricks+pyhive -----------------------------------------------------------


@reflection.cache
def _databricks_get_table_names(
    self: Any, connection: Connection, schema: Optional[str] = None, **kw: Any
) -> List[str]:
    query = _show_tables_query(self, schema, "IN")
    # Databricks returns (database, tableName, isTemporary).
    return [row[1] for row in connection.execute(text(query))]


def _patch(cls: Any, methods: Dict[str, Any]) -> None:
    # Check the class's own __dict__ so a patched parent doesn't mark subclasses done.
    if cls.__dict__.get(_PATCHED_SENTINEL):
        return
    for name, fn in methods.items():
        setattr(cls, name, fn)
    setattr(cls, _PATCHED_SENTINEL, True)


_patch(
    HiveDialect,
    {
        "get_schema_names": _hive_get_schema_names,
        "get_table_names": _hive_get_table_names,
        "_get_table_columns": _hive_get_table_columns,
        "has_table": _has_table,
    },
)
_patch(
    SparkSqlDialect,
    {
        "get_table_names": _sparksql_get_table_names,
        "_get_table_columns": _sparksql_get_table_columns,
        "has_table": _sparksql_has_table,
    },
)
_patch(
    PrestoDialect,
    {
        "get_schema_names": _presto_get_schema_names,
        "get_table_names": _presto_get_table_names,
        "_get_table_columns": _presto_get_table_columns,
        "get_indexes": _presto_get_indexes,
        "has_table": _has_table,
    },
)
if DatabricksDialectBase is not None:
    _patch(DatabricksDialectBase, {"get_table_names": _databricks_get_table_names})
