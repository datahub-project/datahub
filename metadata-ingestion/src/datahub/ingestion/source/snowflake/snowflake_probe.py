import itertools
from typing import Any, Dict, List

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.sql_query import sql_result


class SnowflakeMetadataProbe:
    """Catalog-query surface for Snowflake (see agent.sql_query).

    Snowflake's own connection is reused rather than a second SQLAlchemy engine,
    so a probe query authenticates and retries exactly as ingestion does.
    """

    sql_dialect = "snowflake"

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def __enter__(self) -> "SnowflakeMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._connection.close()

    @probe_method(name="sql", scoped_sql_param="query", row_limit_param="limit")
    def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
        """Run a read-only catalog query. The framework scope-checks `query`
        before calling this, so only a single SELECT over catalog schemas gets
        through. Returns columns plus positional rows."""
        # SnowflakeConnection.query uses a DictCursor, so rows arrive as dicts;
        # the column order comes from the first row rather than being assumed.
        columns: List[str] = []
        rows: List[List[object]] = []
        # One row past the limit, so truncation is observed not inferred.
        for row in itertools.islice(self._connection.query(query), limit + 1):
            if not columns:
                columns = list(row.keys())
            rows.append([row.get(column) for column in columns])
        return sql_result(columns, rows, limit)
