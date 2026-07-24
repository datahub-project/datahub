from typing import Dict, List, Optional

from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from datahub.ingestion.agent.probe_methods import probe_method


class SqlAlchemyMetadataProbe:
    """Metadata-only probe methods backed by the SQLAlchemy Inspector.

    Every SQLAlchemy-based SQL connector inherits these. No method runs
    user-supplied SQL or reads table rows.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._insp = inspect(engine)

    def __enter__(self) -> "SqlAlchemyMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._engine.dispose()

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
                "default": c.get("default"),
            }
            for c in self._insp.get_columns(table, schema=schema)
        ]

    @probe_method()
    def table_comment(self, schema: str, table: str) -> Dict[str, object]:
        """The table's stored comment/description, if any."""
        return self._insp.get_table_comment(table, schema=schema)
