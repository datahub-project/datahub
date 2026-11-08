from typing import Dict, List, NamedTuple, Optional, Tuple, Union

from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import quoted_name

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import echoed
from datahub.ingestion.agent.sql_passthrough import CatalogRows
from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
    JobContainerSubTypes,
)
from datahub.ingestion.source.sql.mssql.query import MSSQLQuery
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource
from datahub.ingestion.source.sql.protocol_probe_settings import probe_url
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_probe import execute_on_cursor
from datahub.ingestion.source.sql.sqlalchemy_probe import (
    SqlAlchemyMetadataProbe,
    build_probe_engine,
)


def _server_spelling(name: str, known: List[str], what: str, hint: str) -> str:
    """The server's spelling of `name`: an exact match, else a unique
    case-insensitive match, else refuse. Only listed names reach SQL or the
    dialect, which reads `a.b` as database.owner."""
    if name in known:
        return name
    folded = name.casefold()
    candidates = [c for c in known if c.casefold() == folded]
    if len(candidates) == 1:
        return candidates[0]
    if candidates:
        raise ProbeArgumentError(
            f"{echoed(name)} matches {what}s "
            f"{', '.join(echoed(c) for c in sorted(candidates))} only by "
            f"case, and this server tells them apart; pass one exactly"
        )
    raise ProbeArgumentError(f"no {what} {echoed(name)} here; {hint}")


class _Located(NamedTuple):
    """Where a command reads: one database's connection, and a schema in it
    as the server spells it -- as a name, and as the argument the dialect
    is handed."""

    inspector: Inspector
    engine: Engine
    database: str
    schema: str
    schema_arg: Union[str, quoted_name]


class SqlServerMetadataProbe(SqlAlchemyMetadataProbe):
    """The shared SQLAlchemy probe, per database, the way SQLServerSource walks.

    A recipe without `database` makes ingestion enumerate every database the
    login can see (get_inspectors) and open each on its own connection, so
    every command here takes `database` and opens the same connection. The
    inherited commands saw only the login's default database -- usually
    `master`, which ingestion never reads.
    """

    def __init__(self, engine: Engine, config: SQLServerConfig) -> None:
        super().__init__(engine)
        self._config = config
        # Per database ingestion opens one connection for, keyed by the
        # server's spelling.
        self._databases: Dict[str, Tuple[Engine, Inspector]] = {}
        self._known_databases: Optional[List[str]] = None
        # From the config the constructor is given, so a provider built
        # without for_config still parses `sql` as T-SQL.
        self.sql_dialect = config.probe_sqlglot_dialect()

    @classmethod
    def for_config(cls, config: SQLCommonConfig) -> "SqlServerMetadataProbe":
        if not isinstance(config, SQLServerConfig):
            raise TypeError(
                f"{cls.__name__} needs a SQLServerConfig, got {type(config).__name__}"
            )
        settings = config.probe_engine_settings(cls.query_budget)
        probe = cls(build_probe_engine(config, probe_url(config), settings), config)
        probe.prime_from_config(config, settings)
        return probe

    def __exit__(self, *exc: object) -> None:
        try:
            for engine, _ in self._databases.values():
                engine.dispose()
        finally:
            self._databases.clear()
            super().__exit__(*exc)

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        with self._engine.connect() as conn:
            return execute_on_cursor(conn.connection.cursor(), query, limit)

    # -- the two server round-trips, separate so tests can stand in for them --

    def _list_databases(self) -> List[str]:
        with self._engine.connect() as conn:
            return self._config.list_databases(conn)

    def _open_database_engine(self, name: str) -> Engine:
        settings = self._config.probe_engine_settings(type(self).query_budget)
        return build_probe_engine(
            self._config, self._config.database_url(name), settings
        )

    # -- resolution --

    def _connection_for(self, database: Optional[str]) -> Tuple[Engine, Inspector, str]:
        """The engine and Inspector ingestion walks `database` with, and the
        name it calls it.

        Refuses rather than guesses: answering from a database ingestion never
        opens is the confidently-wrong result this interface exists to prevent.
        """
        config = self._config
        if config.is_single_database_recipe():
            # The base engine, not inspector.bind, which may be a Connection.
            return self._engine, self._insp, self._check_pin(database)
        if database is None:
            raise ProbeArgumentError(
                "this recipe sets no `database`, so ingestion walks every database "
                "the login can see; pass --database (the `databases` command "
                "lists them)"
            )
        if self._known_databases is None:
            self._known_databases = self._list_databases()
        name = _server_spelling(
            database,
            self._known_databases,
            "database ingestion enumerates",
            "system databases are never read, and `databases` lists the ones that are",
        )
        if name != database:
            self._warn(
                f"{echoed(database)} is spelled {echoed(name)} on the server, "
                f"which is the name ingestion qualifies with; pass {echoed(name)} "
                f"as the --parent to `probe filter`"
            )
        opened = self._databases.get(name)
        if opened is None:
            engine = self._open_database_engine(name)
            try:
                # inspect() connects, so it can fail; the engine is only kept
                # (and later disposed by __exit__) once it has.
                opened = (engine, inspect(engine))
            except Exception:
                engine.dispose()
                raise
            self._databases[name] = opened
        return opened[0], opened[1], name

    def _check_pin(self, database: Optional[str]) -> str:
        config = self._config
        pinned = config.pinned_database_name()
        if database is None or database.casefold() == pinned.casefold():
            return pinned
        source = "sqlalchemy_uri" if config.sqlalchemy_uri else "database"
        if not pinned:
            raise ProbeArgumentError(
                f"this recipe's connection ({source}) names no database, so "
                f"ingestion reads only the login's default one; omit --database"
            )
        raise ProbeArgumentError(
            f"this recipe reads only database {echoed(pinned)} (set by {source}); "
            f"ingestion never opens {echoed(database)}"
        )

    def _schema_arg(self, schema: str) -> Union[str, quoted_name]:
        if not self._config.quote_schemas and "." in schema:
            self._warn(
                f"schema {echoed(schema)} contains a dot and quote_schemas is off, so "
                f"ingestion (and this command) reads it as database.owner; set "
                f"quote_schemas: true to read it as one schema"
            )
        return self._config.schema_argument(schema)

    def _schema(self, schema: str, database: Optional[str]) -> _Located:
        engine, inspector, db_name = self._connection_for(database)
        name = _server_spelling(
            schema,
            list(inspector.get_schema_names()),
            f"schema in database {echoed(db_name)}" if db_name else "schema",
            "`containers` lists them",
        )
        if name != schema:
            # The caller's spelling is what travels in parent_path, and a
            # case-sensitive pattern judges the server's.
            self._warn(
                f"schema {echoed(schema)} is spelled {echoed(name)} on the server, "
                f"which is the name ingestion matches patterns against; pass "
                f"{echoed(name)} as the --parent to `probe filter`"
            )
        return _Located(inspector, engine, db_name, name, self._schema_arg(name))

    def _relation(
        self, schema: str, name: str, database: Optional[str], views_only: bool
    ) -> Tuple[_Located, str]:
        """`_schema`, plus the table or view as the server spells it."""
        located = self._schema(schema, database)
        inspector, schema_arg = located.inspector, located.schema_arg
        known = list(inspector.get_view_names(schema=schema_arg))
        if not views_only:
            known += list(inspector.get_table_names(schema=schema_arg))
        relation = _server_spelling(
            name,
            known,
            f"{'view' if views_only else 'table or view'} in {echoed(located.schema)}",
            f"`{'views' if views_only else 'tables'}` lists them",
        )
        return located, relation

    # -- commands --

    @probe_method(kind=DatasetContainerSubTypes.DATABASE, row_limit_param="limit")
    def databases(self, limit: int = 200) -> List[str]:
        """Databases ingestion would walk. For a recipe without `database` that
        is every database this login can see minus SQL Server's system
        databases -- including ones database_pattern would exclude, so
        `probe filter --kind Database` can explain them. A recipe that sets
        `database` or sqlalchemy_uri reads exactly one, and gets that one back.
        Pass a name to the other commands as --database."""
        if self._config.is_single_database_recipe():
            pinned = self._config.pinned_database_name()
            if not pinned:
                self._warn(
                    "the connection names no database, so ingestion reads the "
                    "login's default one and qualifies nothing with its name"
                )
                return []
            return [pinned]
        return self._list_databases()[:limit]

    @probe_method(row_limit_param="limit", parent_params=("database",))
    def containers(self, database: Optional[str] = None, limit: int = 200) -> List[str]:
        """Schemas in one database, including ones schema_pattern would exclude
        and SQL Server's own (`sys`, `db_owner`, ...), which ingestion does not
        skip either. --database is required unless the recipe pins one."""
        _, inspector, _ = self._connection_for(database)
        return list(inspector.get_schema_names())[:limit]

    @probe_method(
        kind=DatasetSubTypes.TABLE,
        row_limit_param="limit",
        parent_params=("database", "schema"),
    )
    def tables(
        self, schema: str, database: Optional[str] = None, limit: int = 200
    ) -> List[str]:
        """Tables in one schema, excluding views -- the split ingestion makes
        between table_pattern and view_pattern. The database and schema travel
        with the result, so `probe filter` judges `database.schema.table`, the
        identifier ingestion matches. --database is required unless the recipe
        pins one."""
        at = self._schema(schema, database)
        return list(at.inspector.get_table_names(schema=at.schema_arg))[:limit]

    @probe_method(
        kind=DatasetSubTypes.VIEW,
        row_limit_param="limit",
        parent_params=("database", "schema"),
    )
    def views(
        self, schema: str, database: Optional[str] = None, limit: int = 200
    ) -> List[str]:
        """Views in one schema, judged by view_pattern. --database is required
        unless the recipe pins one."""
        at = self._schema(schema, database)
        return list(at.inspector.get_view_names(schema=at.schema_arg))[:limit]

    @probe_method()
    def columns(
        self, schema: str, table: str, database: Optional[str] = None
    ) -> List[Dict[str, object]]:
        """Columns of a table or view: name, data type, nullability, default.
        Structural metadata only -- no cell values are read."""
        at, name = self._relation(schema, table, database, views_only=False)
        return [
            {
                "name": c["name"],
                "type": str(c["type"]),
                "nullable": c.get("nullable"),
                "default": str(c["default"]) if c.get("default") is not None else None,
            }
            for c in at.inspector.get_columns(name, schema=at.schema_arg)
        ]

    @probe_method()
    def foreign_keys(
        self, schema: str, table: str, database: Optional[str] = None
    ) -> List[Dict[str, object]]:
        """Foreign-key constraints on a table: local constrained columns and the
        referred schema/table/columns. Metadata only."""
        at, name = self._relation(schema, table, database, views_only=False)
        fks = at.inspector.get_foreign_keys(name, schema=at.schema_arg)
        return [dict(fk) for fk in fks]

    @probe_method()
    def indexes(
        self, schema: str, table: str, database: Optional[str] = None
    ) -> List[Dict[str, object]]:
        """Indexes on a table: name, indexed column names, and uniqueness."""
        at, name = self._relation(schema, table, database, views_only=False)
        return [dict(ix) for ix in at.inspector.get_indexes(name, schema=at.schema_arg)]

    @probe_method()
    def primary_key(
        self, schema: str, table: str, database: Optional[str] = None
    ) -> Dict[str, object]:
        """The primary-key constraint on a table: column names and constraint name."""
        at, name = self._relation(schema, table, database, views_only=False)
        return dict(at.inspector.get_pk_constraint(name, schema=at.schema_arg))

    @probe_method(name="view_definition")
    def view_definition(
        self, schema: str, view: str, database: Optional[str] = None
    ) -> Optional[str]:
        """The stored CREATE VIEW text for a view (DDL, not query results)."""
        at, name = self._relation(schema, view, database, views_only=True)
        return at.inspector.get_view_definition(name, schema=at.schema_arg)

    @probe_method()
    def table_comment(
        self, schema: str, table: str, database: Optional[str] = None
    ) -> Dict[str, object]:
        """The table's MS_Description extended property -- what ingestion emits
        as its description when include_descriptions is on. SQLAlchemy's mssql
        dialect does not reflect comments, so the generic command could not
        answer here at all."""
        at, name = self._relation(schema, table, database, views_only=False)
        with at.engine.connect() as conn:
            return {
                "text": MSSQLQuery.table_description(conn, schema=at.schema, table=name)
            }

    @probe_method(
        kind=JobContainerSubTypes.STORED_PROCEDURE,
        row_limit_param="limit",
        parent_params=("database", "schema"),
    )
    def procedures(
        self, schema: str, database: Optional[str] = None, limit: int = 200
    ) -> List[str]:
        """Stored procedures in one schema. Ingestion emits each as a DataJob
        and matches procedure_pattern against `database.schema.procedure`.
        Includes ones procedure_pattern would exclude, and is listed whatever
        include_stored_procedures says; `probe filter --kind "Stored
        Procedure"` reports what ingestion keeps. Names only: the procedure
        body is not read. --database is required unless the recipe pins one."""
        at = self._schema(schema, database)
        if not at.database:
            # Ingestion would query `[].[sys].[procedures]` here and fail.
            raise ProbeArgumentError(
                "this recipe's connection names no database, and procedures are "
                "read by database name; name the database in sqlalchemy_uri"
            )
        with at.engine.connect() as conn:
            rows = SQLServerSource._get_stored_procedures(conn, at.database, at.schema)
        return [str(row["name"]) for row in rows][:limit]
