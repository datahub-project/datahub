from typing import Dict, List, Optional

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.sql_passthrough import CatalogRows, SqlCatalogPassthrough
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig

# SQLAlchemy and sqlglot disagree on a handful of dialect names. An unmapped
# name is passed through so the scope check refuses it rather than guessing a
# grammar (see sql_gate._resolve_dialect).
_SQLALCHEMY_TO_SQLGLOT_DIALECT: Dict[str, str] = {
    "postgresql": "postgres",
    # CockroachDB implements the Postgres wire protocol and dialect, so this names
    # the grammar it actually speaks rather than guessing a near-enough one.
    "cockroachdb": "postgres",
    "awsathena": "athena",
    "teradatasql": "teradata",
}


def sqlglot_dialect_for(sqlalchemy_dialect_name: str) -> str:
    return _SQLALCHEMY_TO_SQLGLOT_DIALECT.get(
        sqlalchemy_dialect_name, sqlalchemy_dialect_name
    )


class SqlAlchemyMetadataProbe(SqlCatalogPassthrough):
    """Metadata-only probe methods backed by the SQLAlchemy Inspector.

    Every SQLAlchemy-based SQL connector inherits these. No method runs
    user-supplied SQL or reads table rows.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._insp = inspect(engine)

    # `containers` returns Schemas on a three-tier source and Databases on a two-tier
    # one, and this class serves both -- so the kind comes from the recipe's config,
    # primed in for_config and read back by run_probe_method.
    kind_overrides: Dict[str, str] = {}

    @classmethod
    def for_config(cls, config: SQLCommonConfig) -> "SqlAlchemyMetadataProbe":
        """Build over an engine of this recipe's own making.

        Engine construction lives here rather than on the config because the
        provider is what needs it, and because `probe_provider_class()` is then
        the config's only statement about which provider it has.
        """
        # lazy: keep sqlalchemy engine construction off the config import path
        from sqlalchemy import create_engine

        from datahub.ingestion.source.sql.sql_probe import (
            effective_budget,
            engine_options,
        )

        # The budget rides on the engine rather than on each statement, because
        # that is the one construction point the whole SQLAlchemy family shares --
        # wiring it per connector would be fifteen chances to forget. It also means
        # the Inspector below inherits it, so the typed listings are bounded too and
        # not just `sql`.
        url = config.get_sql_alchemy_url()
        probe = cls(
            create_engine(url, **engine_options(config, budget=cls.query_budget))
        )
        # Report what this dialect actually enforces, not what the class declared:
        # only some dialects have a knob to apply the timeout through.
        probe.query_budget = effective_budget(url, cls.query_budget)
        # One provider class serves ~15 dialects, so the catalog surface cannot be a
        # class attribute here -- it comes from the connector's own config, which is
        # per dialect.
        probe.catalog_scope = config.probe_catalog_scope()
        probe.kind_overrides = {"containers": str(config.probe_container_kind())}
        return probe

    def __exit__(self, *exc: object) -> None:
        self._engine.dispose()

    @property
    def sql_dialect(self) -> str:
        return sqlglot_dialect_for(self._engine.dialect.name)

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        with self._engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchmany(limit)
            return CatalogRows(
                columns=list(result.keys()), rows=[list(row) for row in rows]
            )

    @probe_method(row_limit_param="limit")
    def containers(self, limit: int = 200) -> List[str]:
        """Every schema this connection can see -- or database, on a two-tier source
        like MySQL; the reported `kind` says which, because the pattern that filters
        them differs. Includes ones the recipe's pattern would exclude, so
        `probe filter` can explain them, and comes from the connector's own Inspector
        rather than a catalog query, so it is the list ingestion itself enumerates."""
        return self._insp.get_schema_names()[:limit]

    @probe_method(
        kind=DatasetSubTypes.TABLE, row_limit_param="limit", parent_params=("schema",)
    )
    def tables(self, schema: str, limit: int = 200) -> List[str]:
        """Tables in one schema, excluding views -- the split ingestion makes when it
        applies table_pattern rather than view_pattern. A catalog query against
        information_schema.tables returns both kinds together, so judging that listing
        as tables gives views a verdict from the wrong pattern.

        The schema travels with the result, so `probe filter` needs no --parent."""
        return self._insp.get_table_names(schema=schema)[:limit]

    @probe_method(
        kind=DatasetSubTypes.VIEW, row_limit_param="limit", parent_params=("schema",)
    )
    def views(self, schema: str, limit: int = 200) -> List[str]:
        """Views in one schema, judged by view_pattern. Separate from `tables` for the
        reason given there."""
        return self._insp.get_view_names(schema=schema)[:limit]

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
