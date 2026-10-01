import logging
from typing import Any, List

from datahub.ingestion.agent.sql_passthrough import (
    PROBE_QUERY_LABEL,
    CatalogRows,
    SqlCatalogPassthrough,
)
from datahub.ingestion.source.redshift.config import RedshiftConfig

logger = logging.getLogger(__name__)


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
