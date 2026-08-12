import itertools
from typing import Any

from datahub.ingestion.agent.sql_passthrough import (
    CatalogRows,
    SqlCatalogPassthrough,
    rows_from_mappings,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)


class SnowflakeMetadataProbe(SqlCatalogPassthrough):
    """Catalog-query surface for Snowflake.

    Snowflake's own connection is reused rather than a second SQLAlchemy engine,
    so a probe query authenticates and retries exactly as ingestion does.
    """

    sql_dialect = "snowflake"

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
