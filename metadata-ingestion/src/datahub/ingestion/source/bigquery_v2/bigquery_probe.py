import itertools
from typing import Any, Dict

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.sql_query import sql_result


class BigQueryMetadataProbe:
    """Catalog-query surface for BigQuery (see agent.sql_query).

    BigQuery addresses catalog views as <dataset>.INFORMATION_SCHEMA.<VIEW>, so
    a query must name the dataset; sql_gate understands that shape.
    """

    sql_dialect = "bigquery"

    def __init__(self, client: Any) -> None:
        self._client = client

    def __enter__(self) -> "BigQueryMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._client.close()

    @probe_method(name="sql", scoped_sql_param="query")
    def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
        """Run a read-only catalog query. The framework scope-checks `query`
        before calling this, so only a single SELECT over catalog schemas gets
        through. Returns columns plus positional rows."""
        # max_results caps what BigQuery pages back, so a broad catalog query
        # does not stream an entire result set to be thrown away.
        # One past the limit, so truncation is observed not inferred.
        iterator = self._client.query(query).result(max_results=limit + 1)
        columns = [field.name for field in iterator.schema]
        rows = [
            [row[column] for column in columns]
            for row in itertools.islice(iterator, limit + 1)
        ]
        return sql_result(columns, rows, limit)
