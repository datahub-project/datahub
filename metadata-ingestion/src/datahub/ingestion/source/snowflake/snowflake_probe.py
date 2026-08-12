import itertools
from typing import Any

from datahub.ingestion.agent.sql_gate import INFORMATION_SCHEMA, CatalogScope
from datahub.ingestion.agent.sql_passthrough import (
    CatalogRows,
    SqlCatalogPassthrough,
    rows_from_mappings,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)

# ACCOUNT_USAGE views a probe may read, named individually. Drawn from what the
# Snowflake connector itself reads, so a probe can reproduce ingestion -- minus the
# three that carry more than schema shape:
#
#   query_history / access_history  the text of user queries, WHERE-clause literals
#                                   included. This is the hazard the whole rule is
#                                   about, and both are read by usage and lineage.
#   copy_history                    load errors quote the offending row, so a failed
#                                   COPY can surface record data in first_error_message.
#   users                           names and email addresses. Ingestion reads it to
#                                   map ownership; that is personal data, and a probe
#                                   result is read into a model's context.
_ACCOUNT_USAGE_RELATIONS = frozenset(
    f"account_usage.{view}"
    for view in (
        "databases",
        "schemata",
        "tables",
        "views",
        "columns",
        "table_constraints",
        "referential_constraints",
        "object_dependencies",
        "tag_references",
    )
)


class SnowflakeMetadataProbe(SqlCatalogPassthrough):
    """Catalog-query surface for Snowflake.

    Snowflake's own connection is reused rather than a second SQLAlchemy engine,
    so a probe query authenticates and retries exactly as ingestion does.
    """

    sql_dialect = "snowflake"

    # information_schema is safe at schema level here, unlike on BigQuery: Snowflake
    # exposes its query history as INFORMATION_SCHEMA.QUERY_HISTORY(), a table
    # function, and the gate already refuses functions in FROM position. ACCOUNT_USAGE
    # is where the text-bearing views are relations rather than functions, so that
    # schema is admitted by named relation only.
    catalog_scope = CatalogScope(
        schemas=frozenset({INFORMATION_SCHEMA}),
        relations=_ACCOUNT_USAGE_RELATIONS,
    )

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    @classmethod
    def for_config(cls, config: SnowflakeConnectionConfig) -> "SnowflakeMetadataProbe":
        """Reuse the connector's own connection builder, so a probe query
        authenticates and retries exactly as ingestion does."""
        return cls(config.get_connection())

    def __exit__(self, *exc: object) -> None:
        self._connection.close()

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        # Server-side, and set before the query rather than around it: abandoning
        # the cursor client-side would stop us waiting while the warehouse kept
        # running -- and billing -- the statement.
        timeout = self.query_budget.timeout_seconds
        if timeout is not None:
            self._connection.query(
                f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {int(timeout)}"
            )
        # SnowflakeConnection.query uses a DictCursor, so rows arrive as dicts.
        return rows_from_mappings(
            list(itertools.islice(self._connection.query(query), limit))
        )
