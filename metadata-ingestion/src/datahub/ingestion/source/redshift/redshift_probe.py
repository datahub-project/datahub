import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.sql_passthrough import (
    PROBE_QUERY_LABEL,
    CatalogRows,
    SqlCatalogPassthrough,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.redshift.config import RedshiftConfig

if TYPE_CHECKING:
    from datahub.ingestion.source.redshift.redshift_schema import RedshiftSchema

logger = logging.getLogger(__name__)

# Characters that end or escape a Redshift string literal. A name containing one
# is refused before it can reach RedshiftCommonQuery's f-strings, even when the
# catalog itself returned it.
_LITERAL_BREAKING = ("'", "\\")


@dataclass(frozen=True)
class _Relation:
    schema: str
    name: str
    is_view: bool
    definition: Optional[str]


class RedshiftMetadataProbe(SqlCatalogPassthrough):
    """Probe methods over the connection ingestion itself opens.

    Not the shared SqlAlchemyMetadataProbe, although RedshiftConfig is a
    SQLCommonConfig. RedshiftSource never enumerates through the Inspector: it
    runs RedshiftCommonQuery over a redshift_connector connection, and the two
    disagree on what exists -- materialized and foreign tables, Spectrum tables
    under skip_external_tables, and every object in a datashare-consumer
    database, which pg_class does not hold. sqlalchemy-redshift also forces
    sslmode=verify-full, so a cluster ingestion reaches could refuse the probe,
    and its reflection SQL string-formats the schema name it is given, so a
    caller-supplied schema reached the server unescaped.
    """

    sql_dialect = "redshift"

    def __init__(self, connection: Any, config: RedshiftConfig) -> None:
        self._connection = connection
        self._config = config
        self.warnings: List[str] = []
        self._shared: Optional[bool] = None
        self._all_relations: Optional[List[_Relation]] = None

    @classmethod
    def for_config(cls, config: RedshiftConfig) -> "RedshiftMetadataProbe":
        # lazy: redshift.py pulls in redshift_connector, lineage and sqlglot,
        # none of which `probe methods` or `probe filter` should pay for --
        # both import this module through RedshiftConfig.probe_provider_class
        from datahub.ingestion.source.redshift.redshift import RedshiftSource
        from datahub.ingestion.source.sql.sql_probe import (
            set_redshift_statement_timeout,
        )

        # The label defers to the recipe, as engine_options does for the
        # SQLAlchemy family: a recipe that names its connection has said what
        # it wants it called. Everything else in extra_client_options (IAM,
        # sslmode, cluster_identifier) is passed through untouched, which is
        # the point of going through the ingestion builder. A copy, so the
        # caller's config is not relabelled.
        labelled = config.model_copy(
            update={
                "extra_client_options": {
                    "application_name": PROBE_QUERY_LABEL,
                    **config.extra_client_options,
                }
            }
        )
        connection = RedshiftSource.get_redshift_connection(labelled)
        try:
            timeout = cls.query_budget.timeout_seconds
            if timeout is not None:
                # Fails closed, like the engine listener: a ceiling that
                # silently does not apply is worse than no probe.
                set_redshift_statement_timeout(connection, timeout)
        except Exception:
            connection.close()
            raise
        probe = cls(connection, config)
        # Instance-level, not a class attribute: the scope is declared on
        # RedshiftConfig (test_catalog_scopes reads it there), and declaring it
        # on this class too would make that declaration dead --
        # test_no_config_declares_a_catalog_scope_its_provider_overrides.
        probe.catalog_scope = config.probe_catalog_scope()
        return probe

    def __exit__(self, *exc: object) -> None:
        self._connection.close()

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        cursor = self._connection.cursor()
        try:
            # No args on purpose: redshift_connector rewrites the statement for
            # its paramstyle only when bind values are passed, so a `%` inside
            # a LIKE literal reaches the server as written.
            cursor.execute(query)
            columns = [d[0] for d in (cursor.description or [])]
            return CatalogRows(
                columns=columns, rows=[list(r) for r in cursor.fetchmany(limit)]
            )
        finally:
            cursor.close()

    # The catalog reads below go through RedshiftDataDictionary and
    # RedshiftCommonQuery, the code ingestion runs. RedshiftCommonQuery builds
    # its SQL with f-strings, so the rule throughout is that only the recipe's
    # own `database` and names the catalog itself returned may reach it -- never
    # a string the caller passed. Imported lazily for the reason given in
    # for_config: redshift_schema pulls in redshift_connector and sqlglot.

    def _is_shared_database(self) -> bool:
        # The same test get_workunits_internal makes before choosing which
        # catalog to read. Cached: tables, views and columns each need it.
        from datahub.ingestion.source.redshift.redshift_schema import (
            RedshiftDataDictionary,
        )

        if self._shared is None:
            db = RedshiftDataDictionary.get_database_details(
                self._connection, self._config.database
            )
            self._shared = db is not None and db.is_shared_database()
        return self._shared

    def _schemas(self) -> List["RedshiftSchema"]:
        from datahub.ingestion.source.redshift.redshift_schema import (
            RedshiftDataDictionary,
        )

        # extract_ownership=False whatever the recipe says: the ownership join
        # reads pg_catalog.pg_user, and user names are not schema shape.
        return RedshiftDataDictionary.get_schemas(
            conn=self._connection,
            database=self._config.database,
            extract_ownership=False,
        )

    def _relations(self, schema: str) -> List[_Relation]:
        from datahub.ingestion.source.redshift.query import RedshiftCommonQuery
        from datahub.ingestion.source.redshift.redshift_schema import (
            REDSHIFT_VIEW_TABLE_TYPES,
            RedshiftDataDictionary,
        )

        if self._all_relations is None:
            # list_tables, not get_tables_and_views: the latter first runs
            # enrich_tables (svv_table_info joined to stl_insert), which needs
            # grants a metadata probe should not, and a failure there would
            # sink a listing that needs none of it. The query is
            # database-wide, as ingestion runs it; the schema is matched in
            # Python below, so the caller's string never reaches SQL.
            cursor = RedshiftDataDictionary.get_query_result(
                self._connection,
                RedshiftCommonQuery.list_tables(
                    database=self._config.database,
                    skip_external_tables=self._config.skip_external_tables,
                    is_shared_database=self._is_shared_database(),
                    extract_ownership=False,
                ),
            )
            fields = [d[0] for d in cursor.description]
            at = {
                name: fields.index(name)
                for name in ("schema", "relname", "tabletype", "view_definition")
            }
            self._all_relations = [
                _Relation(
                    schema=row[at["schema"]],
                    name=row[at["relname"]],
                    is_view=row[at["tabletype"]] in REDSHIFT_VIEW_TABLE_TYPES,
                    definition=row[at["view_definition"]],
                )
                for row in cursor.fetchall()
            ]
        found = [r for r in self._all_relations if r.schema == schema]
        if not found:
            # Ingestion reports the same condition ("No tables found in some
            # schemas ... insufficient privileges"); an empty list without it
            # reads as "this schema is empty".
            self.warnings.append(
                f"no tables or views are visible in schema '{schema}': it may "
                f"not exist in database '{self._config.database}', or this user "
                f"may lack privileges on it; `containers` lists the schemas "
                f"this recipe can see"
            )
        return found

    @probe_method(kind=DatasetContainerSubTypes.SCHEMA, row_limit_param="limit")
    def containers(self, limit: int = 200) -> List[str]:
        """Schemas in the recipe's database, as ingestion walks them: local
        schemas from svv_redshift_schemas plus every external (Spectrum)
        schema, which Redshift does not scope to one database. pg_catalog and
        information_schema are left out, as ingestion leaves them out.
        Includes schemas schema_pattern would exclude, so `probe filter --kind
        Schema` can explain them. Metadata only."""
        return [s.name for s in self._schemas()][:limit]

    @probe_method(
        kind=DatasetSubTypes.TABLE, row_limit_param="limit", parent_params=("schema",)
    )
    def tables(self, schema: str, limit: int = 200) -> List[str]:
        """Tables in one schema, as ingestion classifies them: regular, foreign
        and external tables, never views or materialized views. External
        tables are left out when the recipe sets skip_external_tables, because
        ingestion never enumerates them and no pattern decides that. On a
        datashare-consumer database this reads svv_redshift_tables, as
        ingestion does. Includes tables table_pattern would exclude. The schema
        travels with the result, so `probe filter` needs no --parent."""
        return [r.name for r in self._relations(schema) if not r.is_view][:limit]

    @probe_method(
        kind=DatasetSubTypes.VIEW, row_limit_param="limit", parent_params=("schema",)
    )
    def views(self, schema: str, limit: int = 200) -> List[str]:
        """Views and materialized views in one schema, judged by view_pattern
        and then table_pattern, as ingestion judges them. Separate from
        `tables` for the reason given there."""
        return [r.name for r in self._relations(schema) if r.is_view][:limit]

    def _listed_schema(self, schema: str) -> "RedshiftSchema":
        """The catalog's own RedshiftSchema for `schema`, or ValueError.

        list_columns interpolates the schema name into its SQL, so the caller's
        string is never passed on: it is looked up in the catalog listing first,
        and only the name the server returned goes into the query.
        """
        match = next((s for s in self._schemas() if s.name == schema), None)
        if match is None:
            raise ValueError(
                f"no schema named '{schema}' in database "
                f"'{self._config.database}'; run `containers` for the names "
                f"this recipe can see"
            )
        if any(ch in match.name for ch in _LITERAL_BREAKING):
            # Ingestion would send this name as written and fail on it; the
            # probe refuses instead of sending a query whose literal it no
            # longer controls.
            raise ValueError(
                f"schema '{schema}' has a quote or backslash in its name, which "
                f"the Redshift column query cannot take safely; use `sql` "
                f"against svv_redshift_columns instead"
            )
        return match

    @probe_method()
    def columns(self, schema: str, table: str) -> List[Dict[str, object]]:
        """Columns of one table or view as ingestion reads them: name, type,
        nullability, default expression and comment. Covers late-binding views
        and external (Spectrum) tables, and reads SVV_REDSHIFT_COLUMNS on a
        datashare-consumer database. Structural metadata only -- no cell
        values are read. `schema` must be one `containers` lists."""
        from datahub.ingestion.source.redshift.redshift_schema import (
            RedshiftDataDictionary,
        )

        listed = self._listed_schema(schema)
        by_table = RedshiftDataDictionary.get_columns_for_schema(
            conn=self._connection,
            database=self._config.database,
            schema=listed,
            is_shared_database=self._is_shared_database(),
        )
        # Matched in Python: the table name is the caller's and never reaches
        # SQL.
        found = by_table.get(table, [])
        if not found:
            self.warnings.append(
                f"no columns visible for '{schema}.{table}': it may not exist, "
                f"or this user may lack privileges on it; `tables` and `views` "
                f"list what this schema holds"
            )
        return [
            {
                "name": c.name,
                "type": c.data_type,
                "nullable": c.is_nullable,
                "default": c.default,
                "comment": c.comment,
            }
            for c in found
        ]

    @probe_method()
    def view_definition(self, schema: str, view: str) -> Optional[str]:
        """The stored view SQL (DDL, not query results) that ingestion publishes
        as the view's logic. Null for a table, or where the catalog exposes
        none -- as on a datashare-consumer database."""
        return next(
            (
                r.definition
                for r in self._relations(schema)
                if r.name == view and r.is_view
            ),
            None,
        )
