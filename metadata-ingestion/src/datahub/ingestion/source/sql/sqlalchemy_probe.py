from typing import Dict, List, Optional

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.sql_query import sql_result
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

# SQLAlchemy and sqlglot disagree on a handful of dialect names. An unmapped
# name is passed through so the scope check refuses it rather than guessing a
# grammar (see sql_gate._resolve_dialect).
_SQLALCHEMY_TO_SQLGLOT_DIALECT: Dict[str, str] = {
    "postgresql": "postgres",
    "awsathena": "athena",
    "teradatasql": "teradata",
}


def sqlglot_dialect_for(sqlalchemy_dialect_name: str) -> str:
    return _SQLALCHEMY_TO_SQLGLOT_DIALECT.get(
        sqlalchemy_dialect_name, sqlalchemy_dialect_name
    )


class SqlAlchemyMetadataProbe:
    """Metadata-only probe methods backed by the SQLAlchemy Inspector.

    Every SQLAlchemy-based SQL connector inherits these. No method runs
    user-supplied SQL or reads table rows.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._insp = inspect(engine)

    @classmethod
    def for_config(cls, config: SQLCommonConfig) -> "SqlAlchemyMetadataProbe":
        """Build over an engine of this recipe's own making.

        Engine construction lives here rather than on the config because the
        provider is what needs it, and because `probe_provider_class()` is then
        the config's only statement about which provider it has.
        """
        # lazy: keep sqlalchemy engine construction off the config import path
        from sqlalchemy import create_engine

        from datahub.ingestion.source.sql.sql_probe import engine_options

        return cls(
            create_engine(config.get_sql_alchemy_url(), **engine_options(config))
        )

    def __enter__(self) -> "SqlAlchemyMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._engine.dispose()

    @property
    def sql_dialect(self) -> str:
        return sqlglot_dialect_for(self._engine.dialect.name)

    @probe_method(name="sql", scoped_sql_param="query", row_limit_param="limit")
    def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
        """Run a read-only catalog query. Only a single SELECT over the dialect's
        catalog schemas is permitted -- the framework scope-checks `query` before
        this method is called (see probe_methods._enforce_gates), so a user table,
        a second statement or a vendor function is refused before the engine sees
        it. Returns columns plus positional rows."""
        with self._engine.connect() as conn:
            result = conn.execute(text(query))
            # One row past the limit, so truncation is observed not inferred.
            rows = result.fetchmany(limit + 1)
            return sql_result(list(result.keys()), [list(r) for r in rows], limit)

    @probe_method()
    def foreign_keys(self, schema: str, table: str) -> List[Dict[str, object]]:
        """Foreign-key constraints on a table: each entry lists the local
        constrained columns and the referred schema/table/columns. Use to
        understand cross-table relationships. Metadata only — no row data."""
        return self._insp.get_foreign_keys(table, schema=schema)

    @probe_method(name="view_definition")
    def view_definition(self, schema: str, view: str) -> Optional[str]:
        """The stored CREATE VIEW SQL text for a view (DDL, not query results).
        Returns null if the engine does not expose it."""
        return self._insp.get_view_definition(view, schema=schema)

    @probe_method()
    def primary_key(self, schema: str, table: str) -> Dict[str, object]:
        """The primary-key constraint on a table: the constrained column names
        and the constraint name."""
        return self._insp.get_pk_constraint(table, schema=schema)

    @probe_method()
    def indexes(self, schema: str, table: str) -> List[Dict[str, object]]:
        """Indexes on a table: name, indexed column names, and uniqueness."""
        return self._insp.get_indexes(table, schema=schema)

    @probe_method()
    def columns(self, schema: str, table: str) -> List[Dict[str, object]]:
        """Columns of a table or view: name, data type, nullability, default.
        Structural metadata only — no cell values are read. (schema is the
        container name: the SQL schema, or the database for two-tier sources.)"""
        return [
            {
                "name": c["name"],
                "type": str(c["type"]),
                "nullable": c.get("nullable"),
                "default": str(c["default"]) if c.get("default") is not None else None,
            }
            for c in self._insp.get_columns(table, schema=schema)
        ]

    @probe_method()
    def table_comment(self, schema: str, table: str) -> Dict[str, object]:
        """The table's stored comment/description, if any."""
        return self._insp.get_table_comment(table, schema=schema)
