import itertools
from typing import Any, List

from datahub.ingestion.agent.sql_query import SqlRows


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

    def execute_sql(self, query: str, limit: int) -> SqlRows:
        """Run an already-scope-checked query. NOT a @probe_method: annotating
        it would expose raw SQL through `probe run`, skipping the check."""
        # SnowflakeConnection.query uses a DictCursor, so rows arrive as dicts;
        # the column order comes from the first row rather than being assumed.
        columns: List[str] = []
        rows: List[List[object]] = []
        for row in itertools.islice(self._connection.query(query), limit):
            if not columns:
                columns = list(row.keys())
            rows.append([row.get(column) for column in columns])
        return SqlRows(columns=columns, rows=rows)
