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
#   query_history                   QUERY_TEXT is the literal SQL a user ran,
#                                   WHERE-clause values and all, and BIND_VALUES
#                                   holds the parameters. This is the hazard the
#                                   whole rule is about.
#   copy_history                    load errors quote the offending row, so a failed
#                                   COPY can surface record data in first_error_message.
#   users                           names and email addresses. Ingestion reads it to
#                                   map ownership; that is personal data, and a probe
#                                   result is read into a model's context.
#
# access_history IS admitted, and the earlier version of this comment was wrong to
# exclude it alongside query_history "the text of user queries ... included". It
# carries no such column. Its fourteen are QUERY_ID, QUERY_START_TIME, USER_NAME
# and arrays of object and policy *names*; the query text lives in query_history,
# which QUERY_ID references and which stays out.
#
# Admitting it matters because its emptiness is this connector's most common
# silent failure. On Standard edition the view is never populated, so lineage and
# usage return nothing while every privilege is present and test_connection
# reports each capability enabled -- the connector-tests fixture says exactly that
# in its own docstring and calls the edition undeducible. Measured against both
# live fixtures, one `SELECT 1 ... LIMIT 1` here is the only difference between
# them, so this is the read that answers "will lineage actually work".
#
# The trade is USER_NAME, which is identity and so sits against the `users`
# exclusion above. Admitted anyway: an access record that names a user is not a
# directory of them, and the scope model is relation-level, so "count rows but do
# not read USER_NAME" cannot be expressed.
# Catalog-qualified on purpose. ACCOUNT_USAGE is a schema inside the SNOWFLAKE
# database, but nothing stops a user creating their own database with a schema of
# that name -- and a two-part entry would match the last two path segments of
# ATTACKER_DB.ACCOUNT_USAGE.TABLES just as happily as the real system view,
# handing back that user's rows. Pinning the catalog is what distinguishes them.
_ACCOUNT_USAGE_RELATIONS = frozenset(
    f"snowflake.account_usage.{view}"
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
        "access_history",
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
