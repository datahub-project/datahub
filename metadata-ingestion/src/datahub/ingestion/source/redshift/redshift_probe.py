import logging
from typing import Dict, Iterable, List, Optional, Tuple

import redshift_connector

from datahub.ingestion.agent.error_policy import sqlstate_code
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import echoed
from datahub.ingestion.agent.sql_gate import INFORMATION_SCHEMA, CatalogScope
from datahub.ingestion.agent.sql_passthrough import (
    PROBE_QUERY_LABEL,
    CatalogRows,
    SqlCatalogPassthrough,
)
from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftDataDictionary,
    RedshiftSchema,
    RedshiftTable,
    RedshiftView,
    is_shared_database,
)
from datahub.ingestion.source.sql.protocol_probe_settings import (
    set_redshift_statement_timeout,
)
from datahub.ingestion.source.sql.sql_probe import execute_on_cursor

logger = logging.getLogger(__name__)

# Characters that end or escape a Redshift string literal. A name containing one
# is refused before it can reach RedshiftCommonQuery's f-strings, even when the
# catalog itself returned it.
_LITERAL_BREAKING = ("'", "\\")


def _case_hint(name: str, candidates: Iterable[str]) -> str:
    """A pointer to the name the caller probably meant, or "".

    Matching is exact, as ingestion's is, so `Public` does not find `public`.
    Saying so is cheaper than leaving the caller to work out why.
    """
    folded = name.lower()
    near = sorted({c for c in candidates if c != name and c.lower() == folded})
    if not near:
        return ""
    return (
        f"; did you mean '{near[0]}'? Names are matched exactly, and Redshift "
        f"folds unquoted identifiers to lower case"
    )


class RedshiftMetadataProbe(SqlCatalogPassthrough):
    """Probe methods over the connection ingestion itself opens.

    Uses ingestion's redshift_connector path; the SQLAlchemy Inspector lists
    different objects (MVs, datashares, Spectrum) and string-formats schema
    names.
    """

    sql_dialect = "redshift"

    # What `sql` may read. pg_catalog is named relation by relation, NOT
    # allowed at schema level, because Redshift keeps executed SQL in that
    # schema: stl_query (querytxt), stl_querytext (text) and svl_statementtext
    # (text) sit right beside the svv_* metadata views. A schema-level allow
    # would also make any relation listed under it dead: permits_path
    # short-circuits on the schema.
    #
    # The list is derived from redshift/query.py: every catalog relation
    # ingestion reads for schema shape belongs here, so the probe can see
    # what the recipe will see (list_databases reads pg_database, say).
    #
    # Deliberately absent, and the reason each is:
    #   stl_query, stl_querytext, svl_statementtext -- executed SQL, which
    #     carries literal values out of users' queries.
    #   pg_user, pg_user_info, svv_user_info, svl_user_info -- user names
    #     rather than schema shape.
    #   stl_insert/delete/scan/load_commits/unload_log,
    #     svl_query_metrics_summary -- operational history feeding lineage
    #     and usage, not shape a probe needs to report.
    catalog_scope = CatalogScope(
        schemas=frozenset({INFORMATION_SCHEMA}),
        relations=frozenset(
            {
                # svv_* metadata views
                "pg_catalog.svv_table_info",
                "pg_catalog.svv_all_schemas",
                "pg_catalog.svv_external_schemas",
                "pg_catalog.svv_external_tables",
                "pg_catalog.svv_external_columns",
                "pg_catalog.svv_redshift_databases",
                "pg_catalog.svv_redshift_schemas",
                "pg_catalog.svv_redshift_tables",
                "pg_catalog.svv_redshift_columns",
                "pg_catalog.svv_datashares",
                "pg_catalog.svv_mv_info",
                "pg_catalog.stv_mv_info",
                # Postgres-inherited catalog: names, columns, comments and
                # dependencies. No statement text in any of these.
                "pg_catalog.pg_database",
                "pg_catalog.pg_class",
                "pg_catalog.pg_class_info",
                "pg_catalog.pg_namespace",
                "pg_catalog.pg_attribute",
                "pg_catalog.pg_attrdef",
                "pg_catalog.pg_depend",
                "pg_catalog.pg_description",
            }
        ),
    )

    def __init__(
        self, connection: redshift_connector.Connection, config: RedshiftConfig
    ) -> None:
        self._connection = connection
        self._config = config
        self._shared: Optional[bool] = None
        self._listing: Optional[
            Tuple[Dict[str, List[RedshiftTable]], Dict[str, List[RedshiftView]]]
        ] = None
        self._schema_listing: Optional[List[RedshiftSchema]] = None

    @classmethod
    def for_config(cls, config: RedshiftConfig) -> "RedshiftMetadataProbe":
        # lazy: redshift.py pulls in lineage, usage and the profiler, which
        # `probe methods` and `probe filter` never need
        from datahub.ingestion.source.redshift.redshift import RedshiftSource

        # A recipe that names its connection keeps its name; everything else
        # in extra_client_options passes through. A copy, so the caller's
        # config is not relabelled.
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
        return cls(connection, config)

    def __exit__(self, *exc: object) -> None:
        try:
            self._connection.close()
        except Exception as e:
            # A broken connection (a statement timeout, a dropped socket) can
            # refuse to close; that must not replace the probe's own error.
            logger.warning("closing the Redshift probe connection failed: %s", e)
        finally:
            super().__exit__(*exc)

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """The SQLSTATE a redshift_connector error carries: it is raised with
        the server's ErrorResponse fields as one dict, the code under "C"."""
        if str(getattr(type(exc), "__module__", "")).split(".")[0] != (
            "redshift_connector"
        ):
            return None
        args = getattr(exc, "args", ())
        fields = args[0] if isinstance(args, tuple) and args else None
        return sqlstate_code(fields.get("C")) if isinstance(fields, dict) else None

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        return execute_on_cursor(self._connection.cursor(), query, limit)

    # Only the recipe's own `database` and names the catalog returned reach
    # RedshiftCommonQuery, which builds its SQL with f-strings; never a string
    # the caller passed.

    def _is_shared_database(self) -> bool:
        # Cached: tables, views and columns each need it.
        if self._shared is None:
            self._shared = is_shared_database(
                RedshiftDataDictionary.get_database_details(
                    self._connection, self._config.database
                )
            )
        return self._shared

    def _schemas(self) -> List[RedshiftSchema]:
        if self._schema_listing is None:
            # extract_ownership=False whatever the recipe says: the ownership
            # join reads pg_catalog.pg_user, and user names are not schema
            # shape.
            self._schema_listing = RedshiftDataDictionary.get_schemas(
                conn=self._connection,
                database=self._config.database,
                extract_ownership=False,
            )
        return self._schema_listing

    def _resolve_schema(self, schema: str) -> RedshiftSchema:
        """The catalog's own RedshiftSchema for `schema`, or a refusal (exit 2),
        so an unlisted schema is a bad argument in every command."""
        listed = self._schemas()
        match = next((s for s in listed if s.name == schema), None)
        if match is None:
            raise ProbeArgumentError(
                f"no schema named {echoed(schema)} in database "
                f"'{self._config.database}'"
                f"{_case_hint(schema, (s.name for s in listed))}; run "
                f"`containers` for the names this recipe can see"
            )
        return match

    def _relations(self, schema: str) -> Tuple[List[RedshiftTable], List[RedshiftView]]:
        """Ingestion's own table and view split for one listed schema."""
        self._resolve_schema(schema)
        if self._listing is None:
            # enrich=False: enrich_tables reads svv_table_info joined to
            # stl_insert, grants a metadata probe should not need. The query
            # is database-wide, as ingestion runs it; the schema is matched in
            # Python, so the caller's string never reaches SQL.
            self._listing = RedshiftDataDictionary(
                is_serverless=self._config.is_serverless
            ).get_tables_and_views(
                conn=self._connection,
                database=self._config.database,
                skip_external_tables=self._config.skip_external_tables,
                is_shared_database=self._is_shared_database(),
                extract_ownership=False,
                enrich=False,
            )
        tables, views = self._listing
        found = (tables.get(schema, []), views.get(schema, []))
        if not found[0] and not found[1]:
            # Ingestion reports the same condition ("No tables found in some
            # schemas ... insufficient privileges"); an empty list without it
            # reads as "this schema is empty".
            self._warn(
                f"no tables or views are visible in schema '{schema}': it may "
                f"be empty, or this user may lack privileges on it"
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
        return [t.name for t in self._relations(schema)[0]][:limit]

    @probe_method(
        kind=DatasetSubTypes.VIEW, row_limit_param="limit", parent_params=("schema",)
    )
    def views(self, schema: str, limit: int = 200) -> List[str]:
        """Views and materialized views in one schema, judged by view_pattern
        and then table_pattern, as ingestion judges them. Separate from
        `tables` for the reason given there."""
        return [v.name for v in self._relations(schema)[1]][:limit]

    def _listed_schema(self, schema: str) -> RedshiftSchema:
        """The catalog's own RedshiftSchema for `schema`, safe to interpolate.

        list_columns interpolates the schema name into its SQL, so the caller's
        string is never passed on: it is looked up in the catalog listing first,
        and only the name the server returned goes into the query.
        """
        match = self._resolve_schema(schema)
        if any(ch in match.name for ch in _LITERAL_BREAKING):
            # Ingestion would send this name as written and fail on it; the
            # probe refuses instead of sending a query whose literal it no
            # longer controls.
            raise ProbeArgumentError(
                f"schema {echoed(schema)} has a quote or backslash in its name, which "
                f"the Redshift column query cannot take safely; use `sql` "
                f"against svv_redshift_columns instead"
            )
        return match

    @probe_method()
    def columns(self, schema: str, table: str) -> List[Dict[str, object]]:
        """Columns of one table or view as ingestion reads them: name, type,
        nullability, default expression and comment. Covers late-binding views
        and external (Spectrum) tables. On a datashare-consumer database it
        reuses ingestion's SVV_REDSHIFT_COLUMNS query, which is untested
        against a live one. Structural metadata only -- no cell values are
        read. `schema` must be one `containers` lists."""
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
            self._warn(
                f"no columns visible for {echoed(f'{schema}.{table}')}: it may not exist, "
                f"or this user may lack privileges on it"
                f"{_case_hint(table, by_table)}; `tables` and `views` list what "
                f"this schema holds"
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
        as the view's logic. Null where the catalog exposes none, as on a
        datashare-consumer database, or (with a warning) for a table."""
        tables, views = self._relations(schema)
        match = next((v for v in views if v.name == view), None)
        if match is not None:
            return match.ddl
        if any(t.name == view for t in tables):
            self._warn(
                f"{echoed(f'{schema}.{view}')} is a table, not a view, so it has "
                f"no view definition"
            )
            return None
        raise ProbeArgumentError(
            f"no view named {echoed(f'{schema}.{view}')}"
            f"{_case_hint(view, (v.name for v in views))}; `views` lists the "
            f"views in this schema"
        )
